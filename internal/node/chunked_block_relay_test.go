package node

import (
	"bytes"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
)

func TestChunkedBlockStartRejectsUnindexedHeader(t *testing.T) {
	s := regressionService(t)
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	block, _, err := s.buildFundingBlock([][32]byte{nodeSignerPubKey(8)})
	if err != nil {
		t.Fatal(err)
	}
	peer := newPeerConnForTests("127.0.0.1:18444")
	t.Cleanup(peer.close)
	err = s.onBlockStartMessage(peer, p2p.BlockStartMessage{Header: block.Header, TotalSize: uint64(block.EncodedLen())})
	if err == nil {
		t.Fatal("accepted transfer before its header was indexed")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if used := s.blockTransferBudget.Stats().Used; used != 0 || len(entries) != 0 {
		t.Fatalf("rejected transfer used %d spool bytes and created %d files", used, len(entries))
	}
}

func TestChunkedBlockIndexedDescendantRequiresRequestBeforeParentConnects(t *testing.T) {
	s := regressionService(t)
	first, _, err := s.buildFundingBlock([][32]byte{nodeSignerPubKey(8)})
	if err != nil {
		t.Fatal(err)
	}
	branch := detachedChainStateForNodeTest(t, s.chainState.ChainState())
	if _, err := branch.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, branch.UTXOs(), 9, first.Header.Timestamp+600)
	if _, err := s.applyPeerHeaders([]types.BlockHeader{first.Header, second.Header}); err != nil {
		t.Fatal(err)
	}
	hash := consensus.HeaderHash(&second.Header)
	peer := newPeerConnForTests("127.0.0.1:18444")
	t.Cleanup(peer.close)
	raw := second.Encode()
	start := p2p.BlockStartMessage{Header: second.Header, TotalSize: uint64(len(raw)), Checksum: crypto.Sha256d(raw)}
	if err := s.onBlockStartMessage(peer, start); err == nil {
		t.Fatal("accepted an unsolicited descendant outside the active-tip window")
	}
	if used := s.blockTransferBudget.Stats().Used; used != 0 {
		t.Fatalf("rejected descendant retained %d reserved bytes", used)
	}
	if got := s.syncManager().scheduleBlockRequests(peer.addr, [][32]byte{hash}, 1); len(got) != 1 {
		t.Fatalf("could not schedule descendant: %x", got)
	}
	if err := s.onBlockStartMessage(peer, start); err != nil {
		t.Fatalf("requested descendant with known parent header: %v", err)
	}
}

func TestChunkedBlockSpoolBudgetIsSharedAcrossPeers(t *testing.T) {
	s := regressionService(t)
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	block, _, err := s.buildFundingBlock([][32]byte{nodeSignerPubKey(8)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatal(err)
	}
	raw := block.Encode()
	start := p2p.BlockStartMessage{Header: block.Header, TotalSize: uint64(len(raw)), Checksum: crypto.Sha256d(raw)}
	s.blockTransferBudget = p2p.NewPayloadBudget(int64(len(raw)))
	first := newPeerConnForTests("127.0.0.1:18444")
	second := newPeerConnForTests("127.0.0.1:18445")
	t.Cleanup(first.close)
	t.Cleanup(second.close)
	if err := s.onBlockStartMessage(first, start); err != nil {
		t.Fatal(err)
	}
	if err := s.onBlockStartMessage(second, start); !errors.Is(err, p2p.ErrPayloadBudget) {
		t.Fatalf("second peer's spool admission = %v, want payload budget error", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if used := s.blockTransferBudget.Stats().Used; used != int64(len(raw)) || len(entries) != 1 {
		t.Fatalf("spool holds %d bytes and %d files, want %d bytes in one file", used, len(entries), len(raw))
	}
	first.close()
	if err := s.onBlockStartMessage(second, start); err != nil {
		t.Fatalf("closed peer did not free capacity for next transfer: %v", err)
	}
	second.close()
	entries, err = os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if used := s.blockTransferBudget.Stats().Used; used != 0 || len(entries) != 0 {
		t.Fatalf("closed transfers retained %d bytes and %d spool files", used, len(entries))
	}
}

func TestFullBlockServingChunksWhenLengthPrefixExceedsFrameLimit(t *testing.T) {
	genesis := genesisBlock()
	raw := genesis.Encode()
	limit := len(raw) + 3
	s, err := OpenService(ServiceConfig{Profile: types.Regtest, DBPath: t.TempDir(), MaxMessageBytes: limit}, &genesis)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	hash := consensus.HeaderHash(&genesis.Header)
	size, checksum, err := s.chainState.Store().BlockManifest(&hash)
	if err != nil || size != uint64(len(raw)) || checksum != crypto.Sha256d(raw) {
		t.Fatalf("stored block manifest size=%d checksum=%x error=%v", size, checksum, err)
	}

	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	deadline := time.Now().Add(5 * time.Second)
	if err := remote.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	magic := p2p.MagicForProfile(types.Regtest)
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.version.Services |= p2p.ServiceChunkedBlockRelay
	peer.wire = p2p.NewConn(local, magic, limit)
	reader := p2p.NewConn(remote, magic, limit)
	done := make(chan bool, 1)
	go func() {
		done <- s.writePeerEnvelope(peer, outboundMessage{
			blockRef: &outboundBlockReference{
				item:  p2p.InvVector{Type: p2p.InvTypeBlockFull, Hash: hash},
				batch: newBlockServeBatch(int64(limit)),
			},
			enqueuedAt: time.Now(),
		})
	}()

	frame, err := reader.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	start, ok := frame.Message.(p2p.BlockStartMessage)
	frame.Release()
	if !ok || start.Header != genesis.Header || start.TotalSize != size || start.Checksum != checksum {
		t.Fatalf("invalid transfer start: %T %+v", frame.Message, start)
	}
	var received []byte
	for len(received) < len(raw) {
		frame, err := reader.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		chunk, ok := frame.Message.(p2p.BlockChunkMessage)
		if !ok || chunk.BlockHash != hash || chunk.Offset != uint64(len(received)) || len(chunk.Data) == 0 {
			frame.Release()
			t.Fatalf("invalid block chunk: %T at offset %d", frame.Message, chunk.Offset)
		}
		received = append(received, chunk.Data...)
		frame.Release()
	}
	if !bytes.Equal(received, raw) {
		t.Fatal("received block bytes differ from the stored block")
	}
	if !<-done {
		t.Fatal("serving the chunked block closed the peer")
	}
	if used := s.blockServeBudget.Stats().Used; used != 0 {
		t.Fatalf("sent transfer retained %d reserved bytes", used)
	}
}
