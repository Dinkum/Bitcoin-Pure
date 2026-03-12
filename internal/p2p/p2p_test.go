package p2p

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func testCoinbaseTx(height uint64, value uint64) types.Transaction {
	var extraNonce [types.CoinbaseExtraNonceLen]byte
	return types.Transaction{
		Base: types.TxBase{
			Version:            1,
			CoinbaseHeight:     &height,
			CoinbaseExtraNonce: &extraNonce,
			Outputs:            []types.TxOutput{{ValueAtoms: value}},
		},
	}
}

func writeRawMessageForTest(t *testing.T, conn net.Conn, magic uint32, cmd Command, payload []byte, checksum []byte) {
	t.Helper()
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[:4], magic)
	header[4] = byte(cmd)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(payload)))
	copy(header[12:16], checksum)
	if _, err := conn.Write(header); err != nil {
		t.Fatalf("write raw header: %v", err)
	}
	if len(payload) == 0 {
		return
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write raw payload: %v", err)
	}
}

func TestConnMessageRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(HeadersMessage{Headers: []types.BlockHeader{{Version: 1}, {Version: 2}}})
	}()

	msg, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	headers, ok := msg.(HeadersMessage)
	if !ok {
		t.Fatalf("message type = %T, want HeadersMessage", msg)
	}
	if len(headers.Headers) != 2 || headers.Headers[0].Version != 1 || headers.Headers[1].Version != 2 {
		t.Fatalf("unexpected headers payload: %+v", headers.Headers)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestNewConnDefaultPayloadCeilingCoversConsensusFloor(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	conn := NewConn(left, MagicForProfile(types.Regtest), 0)
	if conn.maxPayload < 32_000_000 {
		t.Fatalf("max payload = %d, want at least 32000000", conn.maxPayload)
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 2)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		leftConn := NewConn(conn, MagicForProfile(types.Regtest), 1<<20)
		remote, err := Handshake(leftConn, VersionMessage{Protocol: 1, Height: 5, Nonce: 1, UserAgent: "left"}, time.Second)
		if err != nil {
			errCh <- err
			return
		}
		if remote.UserAgent != "right" {
			errCh <- errors.New("left saw wrong remote user agent")
			return
		}
		errCh <- nil
	}()
	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		rightConn := NewConn(conn, MagicForProfile(types.Regtest), 1<<20)
		remote, err := Handshake(rightConn, VersionMessage{Protocol: 1, Height: 6, Nonce: 2, UserAgent: "right"}, time.Second)
		if err != nil {
			errCh <- err
			return
		}
		if remote.UserAgent != "left" {
			errCh <- errors.New("right saw wrong remote user agent")
			return
		}
		errCh <- nil
	}()
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func TestConnTxBatchRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := TxBatchMessage{
		Txs: []types.Transaction{
			testCoinbaseTx(1, 1),
			testCoinbaseTx(2, 2),
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	batch, ok := received.(TxBatchMessage)
	if !ok {
		t.Fatalf("message type = %T, want TxBatchMessage", received)
	}
	if len(batch.Txs) != 2 || batch.Txs[0].Base.CoinbaseHeight == nil || *batch.Txs[0].Base.CoinbaseHeight != 1 || batch.Txs[1].Base.CoinbaseHeight == nil || *batch.Txs[1].Base.CoinbaseHeight != 2 {
		t.Fatalf("unexpected tx batch payload: %+v", batch.Txs)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestConnTxReconRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := TxReconMessage{TxIDs: [][32]byte{{1}, {2}, {3}}}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	recon, ok := received.(TxReconMessage)
	if !ok {
		t.Fatalf("message type = %T, want TxReconMessage", received)
	}
	if len(recon.TxIDs) != 3 || recon.TxIDs[0] != ([32]byte{1}) || recon.TxIDs[2] != ([32]byte{3}) {
		t.Fatalf("unexpected recon payload: %+v", recon.TxIDs)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestConnAvalanchePollVoteRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	poll := AvaPollMessage{
		PollID: 42,
		Items: []types.OutPoint{
			{TxID: [32]byte{1}, Vout: 3},
			{TxID: [32]byte{2}, Vout: 7},
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(poll)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read poll: %v", err)
	}
	gotPoll, ok := received.(AvaPollMessage)
	if !ok {
		t.Fatalf("message type = %T, want AvaPollMessage", received)
	}
	if gotPoll.PollID != poll.PollID || len(gotPoll.Items) != 2 || gotPoll.Items[1].Vout != 7 {
		t.Fatalf("unexpected poll payload: %+v", gotPoll)
	}
	if err := <-done; err != nil {
		t.Fatalf("write poll: %v", err)
	}

	vote := AvaVoteMessage{
		PollID: 42,
		Votes: []AvaVote{
			{HasOpinion: true, TxID: [32]byte{9}},
			{HasOpinion: false},
		},
	}
	go func() {
		done <- sender.WriteMessage(vote)
	}()

	received, err = receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read vote: %v", err)
	}
	gotVote, ok := received.(AvaVoteMessage)
	if !ok {
		t.Fatalf("message type = %T, want AvaVoteMessage", received)
	}
	if gotVote.PollID != vote.PollID || len(gotVote.Votes) != 2 || !gotVote.Votes[0].HasOpinion || gotVote.Votes[0].TxID != ([32]byte{9}) || gotVote.Votes[1].HasOpinion {
		t.Fatalf("unexpected vote payload: %+v", gotVote)
	}
	if err := <-done; err != nil {
		t.Fatalf("write vote: %v", err)
	}
}

func TestConnXThinRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := XThinBlockMessage{
		Header:   types.BlockHeader{Version: 3, Timestamp: 42},
		Nonce:    99,
		Coinbase: testCoinbaseTx(1, 50),
		ShortIDs: []uint64{11, 22, 33},
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	thin, ok := received.(XThinBlockMessage)
	if !ok {
		t.Fatalf("message type = %T, want XThinBlockMessage", received)
	}
	if thin.Header.Version != 3 || thin.Nonce != 99 || len(thin.ShortIDs) != 3 || thin.ShortIDs[2] != 33 {
		t.Fatalf("unexpected thin block payload: %+v", thin)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestConnXBlockTxRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := XBlockTxMessage{
		BlockHash: [32]byte{9},
		Indexes:   []uint32{1, 3},
		Txs: []types.Transaction{
			testCoinbaseTx(1, 1),
			testCoinbaseTx(2, 2),
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	resp, ok := received.(XBlockTxMessage)
	if !ok {
		t.Fatalf("message type = %T, want XBlockTxMessage", received)
	}
	if len(resp.Indexes) != 2 || resp.Indexes[1] != 3 || len(resp.Txs) != 2 || resp.Txs[0].Base.CoinbaseHeight == nil || *resp.Txs[0].Base.CoinbaseHeight != 1 || resp.Txs[1].Base.CoinbaseHeight == nil || *resp.Txs[1].Base.CoinbaseHeight != 2 {
		t.Fatalf("unexpected xblocktx payload: %+v", resp)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestConnReadMessageRejectsBadChecksum(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	conn := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	errCh := make(chan error, 1)
	go func() {
		payload := []byte{1, 2, 3, 4}
		writeRawMessageForTest(t, right, MagicForProfile(types.Regtest), CmdPing, payload, []byte{0, 0, 0, 0})
		right.Close()
		errCh <- nil
	}()

	_, err := conn.ReadMessage()
	if !errors.Is(err, ErrBadChecksum) {
		t.Fatalf("read message error = %v, want ErrBadChecksum", err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestConnReadMessageRejectsPayloadOverLimit(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	conn := NewConn(left, MagicForProfile(types.Regtest), 3)
	errCh := make(chan error, 1)
	go func() {
		payload := []byte{1, 2, 3, 4}
		header := make([]byte, headerSize)
		binary.LittleEndian.PutUint32(header[:4], MagicForProfile(types.Regtest))
		header[4] = byte(CmdPing)
		binary.LittleEndian.PutUint32(header[8:12], uint32(len(payload)))
		checksum := crypto.Sha256d(payload)
		copy(header[12:16], checksum[:4])
		if _, err := right.Write(header); err != nil {
			t.Fatalf("write raw header: %v", err)
		}
		right.Close()
		errCh <- nil
	}()

	_, err := conn.ReadMessage()
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("read message error = %v, want ErrPayloadTooLarge", err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestDecodeMessageRejectsUnknownCommand(t *testing.T) {
	_, err := decodeMessage(Command(0xff), nil, types.DefaultCodecLimits())
	if !errors.Is(err, ErrBadCommand) {
		t.Fatalf("decodeMessage error = %v, want ErrBadCommand", err)
	}
}

func TestDecodeMessageRejectsOversizedTxReconPayload(t *testing.T) {
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, maxInvPerMessage+1)
	_, err := decodeMessage(CmdTxRecon, payload, types.DefaultCodecLimits())
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("decodeMessage error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestDecodeMessageRejectsTruncatedVersionPayload(t *testing.T) {
	_, err := decodeMessage(CmdVersion, make([]byte, 27), types.DefaultCodecLimits())
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("decodeMessage error = %v, want unexpected EOF", err)
	}
}

func TestMagicForProfileDistinguishesRegtestProfiles(t *testing.T) {
	if got := MagicForProfile(types.RegtestMedium); got == 0 {
		t.Fatal("regtest_medium magic should be non-zero")
	} else if got == MagicForProfile(types.Regtest) {
		t.Fatal("regtest_medium should use distinct network magic")
	}
	if got := MagicForProfile(types.RegtestHard); got == 0 {
		t.Fatal("regtest_hard magic should be non-zero")
	} else if got == MagicForProfile(types.Regtest) || got == MagicForProfile(types.RegtestMedium) {
		t.Fatal("regtest_hard should use distinct network magic")
	}
}
