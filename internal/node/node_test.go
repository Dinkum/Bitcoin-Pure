package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utxochecksum"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"io"
	"math"
	"math/big"
	"net"
	"sort"
	"testing"
	"time"
)

func compactTargetForTest(compact uint32) *big.Int {
	size := byte(compact >> 24)
	mantissa := compact & 0x007fffff
	target := new(big.Int).SetUint64(uint64(mantissa))
	if size <= 3 {
		target.Rsh(target, uint(8*(3-int(size))))
	} else {
		target.Lsh(target, uint(8*(int(size)-3)))
	}
	return target
}

func mineHeaderForNodeTest(header types.BlockHeader) types.BlockHeader {
	target := compactTargetForTest(header.NBits)
	for nonce := uint64(0); ; nonce++ {
		header.Nonce = nonce
		hash := consensus.HeaderHash(&header)
		if new(big.Int).SetBytes(hash[:]).Cmp(target) <= 0 {
			return header
		}
		if nonce == math.MaxUint64 {
			break
		}
	}
	panic("unable to mine header")
}

func nodeSignerPubKey(seed byte) [32]byte {
	return crypto.XOnlyPubKeyFromSecret([32]byte{seed})
}

func merkleLeafForNodeTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x00
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

func merkleNodeForNodeTest(left, right [32]byte) [32]byte {
	var buf [65]byte
	buf[0] = 0x01
	copy(buf[1:33], left[:])
	copy(buf[33:], right[:])
	return crypto.Sha256d(buf[:])
}

func merkleSoloForNodeTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x02
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

func merkleRootForNodeTest(items [][32]byte) [32]byte {
	if len(items) == 0 {
		panic("merkleRootForNodeTest requires at least one item")
	}
	level := make([][32]byte, len(items))
	for i, item := range items {
		level[i] = merkleLeafForNodeTest(item)
	}
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, merkleSoloForNodeTest(level[i]))
				continue
			}
			next = append(next, merkleNodeForNodeTest(level[i], level[i+1]))
		}
		level = next
	}
	return level[0]
}

func spendTxForNodeTest(t *testing.T, spenderSeed byte, prevOut types.OutPoint, value uint64, recipientSeed byte, fee uint64) types.Transaction {
	t.Helper()
	if fee >= value {
		t.Fatalf("fee %d must be less than value %d", fee, value)
	}
	return spendTxForNodeTestToOutputs(t, spenderSeed, prevOut, value, []types.TxOutput{
		types.NewXOnlyOutput(value-fee, nodeSignerPubKey(recipientSeed)),
	})
}

func spendTxForNodeTestToOutputs(t *testing.T, spenderSeed byte, prevOut types.OutPoint, value uint64, outputs []types.TxOutput) types.Transaction {
	t.Helper()
	var outputSum uint64
	for _, output := range outputs {
		outputSum += output.ValueAtoms
	}
	if outputSum > value {
		t.Fatalf("outputs spend %d, want no more than input value %d", outputSum, value)
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: outputs,
		},
	}
	msg, err := consensus.SighashWithParams(&tx, 0, []consensus.UtxoEntry{
		consensus.UtxoEntryFromOutput(types.NewXOnlyOutput(value, nodeSignerPubKey(spenderSeed))),
	}, consensus.RegtestParams())
	if err != nil {
		t.Fatal(err)
	}
	_, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	return tx
}

func spendPQTxForNodeTestToOutputs(t *testing.T, prevOut types.OutPoint, value uint64, verificationKey []byte, privateKey []byte, outputs []types.TxOutput) types.Transaction {
	t.Helper()
	pqLock := consensus.PQLock(verificationKey)
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: outputs,
		},
	}
	msg, err := consensus.SighashWithParams(&tx, 0, []consensus.UtxoEntry{
		consensus.UtxoEntryFromOutput(types.NewPQLockOutput(value, pqLock)),
	}, consensus.RegtestParams())
	if err != nil {
		t.Fatal(err)
	}
	signature, err := crypto.SignMLDSA65(privateKey, msg[:])
	if err != nil {
		t.Fatal(err)
	}
	authPayload := append(append([]byte(nil), verificationKey...), signature...)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{AuthPayload: authPayload}}}
	return tx
}

func genesisBlock() types.Block {
	params := consensus.RegtestParams()
	coinbase := coinbaseTxForHeight(0, []types.TxOutput{{ValueAtoms: 50, PubKey: nodeSignerPubKey(7)}})
	txids := [][32]byte{consensus.TxID(&coinbase)}
	authids := [][32]byte{consensus.AuthID(&coinbase)}
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: txids[0], Vout: 0}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(7)},
	}
	return types.Block{
		Header: types.BlockHeader{
			Version:        1,
			MerkleTxIDRoot: merkleRootForNodeTest(txids),
			MerkleAuthRoot: merkleRootForNodeTest(authids),
			UTXORoot:       consensus.ComputedUTXORoot(utxos),
			Timestamp:      params.GenesisTimestamp,
			NBits:          params.GenesisBits,
		},
		Txs: []types.Transaction{coinbase},
	}
}

func genesisBlockForPubKey(pubKey [32]byte) types.Block {
	params := consensus.RegtestParams()
	coinbase := coinbaseTxForHeight(0, []types.TxOutput{{ValueAtoms: 50, PubKey: pubKey}})
	txids := [][32]byte{consensus.TxID(&coinbase)}
	authids := [][32]byte{consensus.AuthID(&coinbase)}
	utxos := consensus.UtxoSet{
		types.OutPoint{TxID: txids[0], Vout: 0}: {ValueAtoms: 50, PubKey: pubKey},
	}
	return types.Block{
		Header: types.BlockHeader{
			Version:        1,
			MerkleTxIDRoot: merkleRootForNodeTest(txids),
			MerkleAuthRoot: merkleRootForNodeTest(authids),
			UTXORoot:       consensus.ComputedUTXORoot(utxos),
			Timestamp:      params.GenesisTimestamp,
			NBits:          params.GenesisBits,
		},
		Txs: []types.Transaction{coinbase},
	}
}

func nextCoinbaseBlock(prevHeight uint64, prev types.BlockHeader, currentUTXOs consensus.UtxoSet, pubKeySeed byte, timestamp uint64) types.Block {
	params := consensus.RegtestParams()
	pubKey := nodeSignerPubKey(pubKeySeed)
	coinbase := coinbaseTxForHeight(prevHeight+1, []types.TxOutput{{ValueAtoms: 1, PubKey: pubKey}})
	txids := [][32]byte{consensus.TxID(&coinbase)}
	authids := [][32]byte{consensus.AuthID(&coinbase)}
	nextUTXOs := cloneUtxos(currentUTXOs)
	nextUTXOs[types.OutPoint{TxID: txids[0], Vout: 0}] = consensus.UtxoEntry{ValueAtoms: 1, PubKey: pubKey}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: prevHeight, Header: prev}, params)
	if err != nil {
		panic(err)
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  consensus.HeaderHash(&prev),
		MerkleTxIDRoot: merkleRootForNodeTest(txids),
		MerkleAuthRoot: merkleRootForNodeTest(authids),
		UTXORoot:       consensus.ComputedUTXORoot(nextUTXOs),
		Timestamp:      timestamp,
		NBits:          nbits,
	}
	return types.Block{
		Header: mineHeaderForNodeTest(header),
		Txs:    []types.Transaction{coinbase},
	}
}

func blockWithTxsForNodeTest(t *testing.T, prevHeight uint64, prev types.BlockHeader, currentUTXOs consensus.UtxoSet, txs []types.Transaction, timestamp uint64) types.Block {
	t.Helper()
	params := consensus.RegtestParams()
	blockTxs := append([]types.Transaction(nil), txs...)
	if len(blockTxs) > 2 {
		sort.Slice(blockTxs[1:], func(i, j int) bool {
			left := consensus.TxID(&blockTxs[i+1])
			right := consensus.TxID(&blockTxs[j+1])
			return bytes.Compare(left[:], right[:]) < 0
		})
	}

	tempUtxos := cloneUtxos(currentUTXOs)
	claimedInputs := make(map[types.OutPoint]struct{})
	var totalFees uint64
	for i := 1; i < len(blockTxs); i++ {
		tx := &blockTxs[i]
		for _, input := range tx.Base.Inputs {
			if _, ok := claimedInputs[input.PrevOut]; ok {
				t.Fatalf("duplicate claimed input in tx %d: %v", i, input.PrevOut)
			}
			claimedInputs[input.PrevOut] = struct{}{}
		}
		summary, err := consensus.ValidateTxWithParams(tx, currentUTXOs, consensus.RegtestParams(), consensus.DefaultConsensusRules())
		if err != nil {
			t.Fatalf("validate tx %d: %v", i, err)
		}
		totalFees += summary.Fee
	}
	for spent := range claimedInputs {
		delete(tempUtxos, spent)
	}
	for i := 1; i < len(blockTxs); i++ {
		tx := &blockTxs[i]
		txid := consensus.TxID(tx)
		for vout, output := range tx.Base.Outputs {
			tempUtxos[types.OutPoint{TxID: txid, Vout: uint32(vout)}] = consensus.UtxoEntryFromOutput(output)
		}
	}

	coinbase := blockTxs[0]
	if len(coinbase.Base.Outputs) == 0 {
		t.Fatal("coinbase missing outputs")
	}
	coinbase.Base.Outputs[0].ValueAtoms += totalFees
	blockTxs[0] = coinbase
	coinbaseTxID := consensus.TxID(&blockTxs[0])
	for vout, output := range blockTxs[0].Base.Outputs {
		tempUtxos[types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}] = consensus.UtxoEntryFromOutput(output)
	}

	txids := make([][32]byte, 0, len(blockTxs))
	authids := make([][32]byte, 0, len(blockTxs))
	for i := range blockTxs {
		txids = append(txids, consensus.TxID(&blockTxs[i]))
		authids = append(authids, consensus.AuthID(&blockTxs[i]))
	}
	nbits, err := consensus.NextWorkRequired(consensus.PrevBlockContext{Height: prevHeight, Header: prev}, params)
	if err != nil {
		t.Fatalf("next work required: %v", err)
	}
	header := types.BlockHeader{
		Version:        1,
		PrevBlockHash:  consensus.HeaderHash(&prev),
		MerkleTxIDRoot: merkleRootForNodeTest(txids),
		MerkleAuthRoot: merkleRootForNodeTest(authids),
		UTXORoot:       consensus.ComputedUTXORoot(tempUtxos),
		Timestamp:      timestamp,
		NBits:          nbits,
	}
	return types.Block{
		Header: mineHeaderForNodeTest(header),
		Txs:    blockTxs,
	}
}

func encodePackedTransactionsForNodeTest(txs []types.Transaction) string {
	if len(txs) == 0 {
		return ""
	}
	buf := make([]byte, 0)
	for _, tx := range txs {
		encoded := tx.Encode()
		size := make([]byte, 4)
		binary.LittleEndian.PutUint32(size, uint32(len(encoded)))
		buf = append(buf, size...)
		buf = append(buf, encoded...)
	}
	return base64.StdEncoding.EncodeToString(buf)
}

func assertPersistentChecksumMatchesComputed(t *testing.T, persistent *PersistentChainState) {
	t.Helper()

	view, ok := persistent.CommittedView()
	if !ok {
		t.Fatal("missing committed view")
	}
	liveState := persistent.ChainState()
	if liveState == nil {
		t.Fatal("missing live chain state clone")
	}
	want := utxochecksum.Compute(liveState.UTXOs())
	if view.UTXOChecksum != want {
		t.Fatalf("committed view checksum = %x, want %x", view.UTXOChecksum, want)
	}

	if got := liveState.UTXOChecksum(); got != want {
		t.Fatalf("live state checksum = %x, want %x", got, want)
	}

	stored, err := persistent.Store().LoadChainState()
	if err != nil {
		t.Fatalf("LoadChainState: %v", err)
	}
	if stored == nil {
		t.Fatal("missing stored chain state")
	}
	if stored.UTXOChecksum != want {
		t.Fatalf("stored checksum = %x, want %x", stored.UTXOChecksum, want)
	}
}

func withInboundPeerTxRateLimitForTest(t *testing.T, rate float64, burst float64, violations int) {
	t.Helper()
	oldRate := inboundPeerTxRatePerSecond
	oldBurst := inboundPeerTxBurst
	oldViolations := inboundPeerTxViolationLimit
	inboundPeerTxRatePerSecond = rate
	inboundPeerTxBurst = burst
	inboundPeerTxViolationLimit = violations
	t.Cleanup(func() {
		inboundPeerTxRatePerSecond = oldRate
		inboundPeerTxBurst = oldBurst
		inboundPeerTxViolationLimit = oldViolations
	})
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.calls == 2 && l.secondAccept != nil {
		close(l.secondAccept)
		l.secondAccept = nil
	}
	if l.calls > len(l.results) {
		return nil, net.ErrClosed
	}
	result := l.results[l.calls-1]
	return result.conn, result.err
}

func (l *scriptedListener) Close() error { return nil }
func (l *scriptedListener) Addr() net.Addr {
	return deadlineSpyAddr("listener")
}

func (l *scriptedListener) acceptCalls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type temporaryAcceptTestError struct {
	err error
}

func (e temporaryAcceptTestError) Error() string   { return e.err.Error() }
func (e temporaryAcceptTestError) Unwrap() error   { return e.err }
func (e temporaryAcceptTestError) Timeout() bool   { return false }
func (e temporaryAcceptTestError) Temporary() bool { return true }

func (c *deadlineSpyConn) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (c *deadlineSpyConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *deadlineSpyConn) Close() error                { return nil }
func (c *deadlineSpyConn) LocalAddr() net.Addr         { return deadlineSpyAddr("local") }
func (c *deadlineSpyConn) RemoteAddr() net.Addr        { return deadlineSpyAddr("remote") }
func (c *deadlineSpyConn) SetDeadline(time.Time) error { return nil }
func (c *deadlineSpyConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *deadlineSpyConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !deadline.IsZero() {
		c.sawNonZeroWriteTime = true
	}
	return nil
}

func (c *deadlineSpyConn) sawNonZeroWriteDeadline() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sawNonZeroWriteTime
}

type deadlineSpyAddr string

func (a deadlineSpyAddr) Network() string { return "tcp" }
func (a deadlineSpyAddr) String() string  { return string(a) }

func waitForTxReconMessage(t *testing.T, peer *peerConn, timeout time.Duration) p2p.TxReconMessage {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case envelope := <-peer.sendQ:
			recon, ok := envelope.msg.(p2p.TxReconMessage)
			if !ok {
				continue
			}
			return recon
		case <-timer.C:
			t.Fatalf("timed out waiting for TxReconMessage on %s", peer.addr)
		}
	}
}

func waitForTxBatchMessage(t *testing.T, peer *peerConn, timeout time.Duration) p2p.TxBatchMessage {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case envelope := <-peer.sendQ:
			batch, ok := envelope.msg.(p2p.TxBatchMessage)
			if !ok {
				continue
			}
			return batch
		case <-timer.C:
			t.Fatalf("timed out waiting for TxBatchMessage on %s", peer.addr)
		}
	}
}

func waitForTxRequestMessage(t *testing.T, peer *peerConn, timeout time.Duration) p2p.TxRequestMessage {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case envelope := <-peer.sendQ:
			req, ok := envelope.msg.(p2p.TxRequestMessage)
			if !ok {
				continue
			}
			return req
		case <-timer.C:
			t.Fatalf("timed out waiting for TxRequestMessage on %s", peer.addr)
		}
	}
}

func assertNoPeerMessage(t *testing.T, peer *peerConn, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case envelope := <-peer.sendQ:
		t.Fatalf("unexpected peer message on %s: %T", peer.addr, envelope.msg)
	case <-timer.C:
	}
}

func withPendingPeerBlockLimitsForTest(t *testing.T, byteLimit uint64, perPeerLimit int) {
	t.Helper()
	oldByteLimit := maxPendingPeerBlockBytes
	oldPeerLimit := maxPendingPeerBlocksPerPeer
	maxPendingPeerBlockBytes = byteLimit
	maxPendingPeerBlocksPerPeer = perPeerLimit
	t.Cleanup(func() {
		maxPendingPeerBlockBytes = oldByteLimit
		maxPendingPeerBlocksPerPeer = oldPeerLimit
	})
}

func pendingPeerBlockForTest(nonce uint64, outputs int) types.Block {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: types.OutPoint{TxID: [32]byte{byte(nonce)}, Vout: uint32(nonce)}}},
			Outputs: make([]types.TxOutput, outputs),
		},
		Auth: types.TxAuth{Entries: []types.TxAuthEntry{{Signature: [64]byte{byte(nonce)}}}},
	}
	for i := range tx.Base.Outputs {
		tx.Base.Outputs[i] = types.TxOutput{
			ValueAtoms: uint64(i + 1),
			PubKey:     nodeSignerPubKey(byte((int(nonce)+i)%250 + 1)),
		}
	}
	return types.Block{
		Header: types.BlockHeader{
			Version:       1,
			PrevBlockHash: [32]byte{byte(nonce + 1)},
			Timestamp:     nonce + 1,
			Nonce:         nonce,
		},
		Txs: []types.Transaction{tx},
	}
}

func newPeerConnForTests(addr string) *peerConn {
	return &peerConn{
		addr:          addr,
		sendQ:         make(chan outboundMessage, 4),
		closed:        make(chan struct{}),
		queuedInv:     make(map[p2p.InvVector]int),
		queuedTx:      make(map[[32]byte]int),
		knownTx:       make(map[[32]byte]struct{}),
		localRelayTxs: make(map[[32]byte]localRelayFallbackState),
		pendingThin:   make(map[[32]byte]*pendingThinBlock),
		version: p2p.VersionMessage{
			Height:    0,
			Services:  p2p.ServiceNodeNetwork | p2p.ServiceErlayTxRelay | p2p.ServiceCompactBlockRelay | p2p.ServiceGrapheneExtended | p2p.ServiceAvalancheOverlay,
			UserAgent: "bpu/go",
		},
	}
}
