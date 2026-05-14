package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOSReleaseLooksLikeUbuntu(t *testing.T) {
	raw := strings.Join([]string{
		`NAME="Ubuntu"`,
		`ID=ubuntu`,
		`ID_LIKE=debian`,
		"",
	}, "\n")
	if !osReleaseLooksLikeUbuntu(raw) {
		t.Fatal("expected ubuntu os-release to enable dashboard")
	}
	if osReleaseLooksLikeUbuntu("ID=debian\nID_LIKE=debian\n") {
		t.Fatal("expected non-ubuntu os-release to disable dashboard")
	}
}

func TestOpenServiceDefaultsMinerWorkersWhenEnabled(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerEnabled: true,
		MinerPubKey:  nodeSignerPubKey(42),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	wantWorkers := defaultMinerWorkers()
	if svc.cfg.MinerWorkers != wantWorkers {
		t.Fatalf("miner workers = %d, want %d", svc.cfg.MinerWorkers, wantWorkers)
	}
	if svc.cfg.MaxAncestors != 256 {
		t.Fatalf("max ancestors = %d, want 256", svc.cfg.MaxAncestors)
	}
	if svc.cfg.MaxDescendants != 256 {
		t.Fatalf("max descendants = %d, want 256", svc.cfg.MaxDescendants)
	}
	info := svc.Info()
	if !info.MinerEnabled {
		t.Fatal("expected miner to remain enabled")
	}
	if info.MinerWorkers != wantWorkers {
		t.Fatalf("service info miner workers = %d, want %d", info.MinerWorkers, wantWorkers)
	}
}

func TestOpenServiceDefaultsRPCHardening(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		RPCAddr:      "127.0.0.1:18443",
		RPCAuthToken: "test-token",
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	if svc.cfg.RPCIdleTimeout != 30*time.Second {
		t.Fatalf("rpc idle timeout = %s, want 30s", svc.cfg.RPCIdleTimeout)
	}
	if svc.cfg.RPCMaxHeaderBytes != 8<<10 {
		t.Fatalf("rpc max header bytes = %d, want 8192", svc.cfg.RPCMaxHeaderBytes)
	}
	if svc.cfg.MaxMessageBytes < int(consensus.MainnetParams().BlockSizeFloor) {
		t.Fatalf("max message bytes = %d, want at least %d", svc.cfg.MaxMessageBytes, consensus.MainnetParams().BlockSizeFloor)
	}
}

func TestStartCleansUpRPCWhenP2PListenFails(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen occupied addr: %v", err)
	}
	defer occupied.Close()

	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		RPCAddr:      "127.0.0.1:0",
		RPCAuthToken: "test-token",
		P2PAddr:      occupied.Addr().String(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	if err := svc.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail on occupied p2p address")
	}
	rpcAddr := svc.rpcSrv.Addr
	select {
	case <-svc.stopCh:
	default:
		t.Fatal("service stop channel is still open after failed Start")
	}
	rebound, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		t.Fatalf("RPC addr was not cleaned up after failed Start: %v", err)
	}
	rebound.Close()
	if err := svc.Close(); err != nil {
		t.Fatalf("Close after failed Start: %v", err)
	}
}

func TestOpenServiceRejectsWildcardRPCBindWithoutAuth(t *testing.T) {
	genesis := genesisBlock()
	_, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
		RPCAddr: ":18443",
	}, &genesis)
	if err == nil || !strings.Contains(err.Error(), "rpc auth token is required") {
		t.Fatalf("expected wildcard bind auth error, got %v", err)
	}
}

func TestUnauthenticatedLoopbackRPCRequiresJSONContentType(t *testing.T) {
	svc := &Service{logger: slog.Default()}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18443/", strings.NewReader(`{"method":"unknown","params":{}}`))
	req.Header.Set("Content-Type", "text/plain")
	resp := httptest.NewRecorder()

	svc.handleRPC(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusUnauthorized)
	}
}

func TestUnauthenticatedLoopbackRPCRejectsCrossOriginBrowserPost(t *testing.T) {
	svc := &Service{logger: slog.Default()}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18443/", strings.NewReader(`{"method":"unknown","params":{}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://example.invalid")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	resp := httptest.NewRecorder()

	svc.handleRPC(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusUnauthorized)
	}
}

func TestUnauthenticatedLoopbackRPCAllowsNonBrowserJSONClient(t *testing.T) {
	svc := &Service{logger: slog.Default()}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18443/", strings.NewReader(`{"method":"unknown","params":{}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	svc.handleRPC(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	if !strings.Contains(resp.Body.String(), "unknown rpc method") {
		t.Fatalf("response body %q does not show authorized dispatch", resp.Body.String())
	}
}

func TestOpenServiceRejectsMiningWithoutPubKey(t *testing.T) {
	genesis := genesisBlock()
	_, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerEnabled: true,
	}, &genesis)
	if err == nil || !strings.Contains(err.Error(), "miner pubkey is required") {
		t.Fatalf("expected miner pubkey error, got %v", err)
	}
}

func TestApplyPeerHeadersBatchesPersistence(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	state := NewChainState(types.Regtest)
	if _, err := state.InitializeFromGenesisBlock(&genesis); err != nil {
		t.Fatal(err)
	}
	first := nextCoinbaseBlock(0, genesis.Header, state.UTXOs(), 3, genesis.Header.Timestamp+600)
	if _, err := state.ApplyBlock(&first); err != nil {
		t.Fatal(err)
	}
	second := nextCoinbaseBlock(1, first.Header, state.UTXOs(), 4, first.Header.Timestamp+600)

	applied, err := svc.applyPeerHeaders([]types.BlockHeader{first.Header, second.Header})
	if err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	stored, err := svc.chainState.Store().LoadHeaderState()
	if err != nil {
		t.Fatal(err)
	}
	if stored == nil || stored.Height != 2 || stored.TipHeader != second.Header {
		t.Fatal("stored header tip mismatch after batch")
	}
	secondHash := consensus.HeaderHash(&second.Header)
	entry, err := svc.chainState.Store().GetBlockIndex(&secondHash)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || entry.ChainWork == ([32]byte{}) {
		t.Fatal("expected batched header chainwork to persist")
	}
}

func TestSubmitPackedTxBatchRPC(t *testing.T) {
	genesis := genesisBlock()
	genesis.Txs[0].Base.Outputs[0].PubKey = nodeSignerPubKey(7)
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(7)},
	})

	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	params, err := json.Marshal(map[string]string{
		"packed": encodePackedTransactionsForNodeTest([]types.Transaction{tx}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "submitpackedtxbatch",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(rpcSubmitTxBatchResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcSubmitTxBatchResult", result)
	}
	if got := out.Accepted; got != 1 {
		t.Fatalf("accepted = %d, want 1", got)
	}
}

func TestGetUTXOsByPubKeysRPC(t *testing.T) {
	genesis := genesisBlock()
	genesis.Txs[0].Base.Outputs = []types.TxOutput{
		{ValueAtoms: 30, PubKey: nodeSignerPubKey(7)},
		{ValueAtoms: 20, PubKey: nodeSignerPubKey(8)},
	}
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 30, PubKey: nodeSignerPubKey(7)},
		types.OutPoint{TxID: genesisTxID, Vout: 1}: {ValueAtoms: 20, PubKey: nodeSignerPubKey(8)},
	})

	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	pubKey7 := nodeSignerPubKey(7)
	params, err := json.Marshal(map[string][]string{
		"pubkeys": []string{hex.EncodeToString(pubKey7[:])},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getutxosbypubkeys",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(rpcPubKeyUTXOResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcPubKeyUTXOResult", result)
	}
	if len(out.UTXOs) != 1 {
		t.Fatalf("utxo count = %d, want 1", len(out.UTXOs))
	}
	if got := out.UTXOs[0].Value; got != 30 {
		t.Fatalf("value = %v, want 30", got)
	}
}

func TestGetUTXOProofRPC(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	txid := consensus.TxID(&genesis.Txs[0])
	params, err := json.Marshal(map[string]any{
		"txid": hex.EncodeToString(txid[:]),
		"vout": 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getutxoproof",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	raw, ok := result.(RPCAnchoredUTXOProof)
	if !ok {
		t.Fatalf("result type = %T, want RPCAnchoredUTXOProof", result)
	}
	if !raw.Proof.Exists {
		t.Fatal("expected membership proof")
	}
	proof, err := DecodeRPCUTXOProof(raw)
	if err != nil {
		t.Fatalf("DecodeRPCUTXOProof: %v", err)
	}
	if !VerifyAnchoredUTXOProof(proof) {
		t.Fatal("expected proof to verify")
	}
}

func TestVerifyUTXOProofRPCSupportsExclusion(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	proof, err := svc.UTXOProof(types.OutPoint{TxID: [32]byte{9}, Vout: 1})
	if err != nil {
		t.Fatalf("UTXOProof: %v", err)
	}
	raw := EncodeRPCUTXOProof(proof)
	params, err := json.Marshal(map[string]any{
		"proof": raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "verifyutxoproof",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(rpcVerifyUTXOProofResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcVerifyUTXOProofResult", result)
	}
	if !out.Valid {
		t.Fatalf("valid = %v, want true", out.Valid)
	}
	if !out.AnchorMatchesLocal {
		t.Fatalf("anchor_matches_local = %v, want true", out.AnchorMatchesLocal)
	}
}

func TestVerifyUTXOProofRPCRejectsTampering(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	txid := consensus.TxID(&genesis.Txs[0])
	proof, err := svc.UTXOProof(types.OutPoint{TxID: txid, Vout: 0})
	if err != nil {
		t.Fatalf("UTXOProof: %v", err)
	}
	proof.Proof.ValueAtoms++
	params, err := json.Marshal(map[string]any{
		"proof": EncodeRPCUTXOProof(proof),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "verifyutxoproof",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(rpcVerifyUTXOProofResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcVerifyUTXOProofResult", result)
	}
	if out.Valid {
		t.Fatalf("valid = %v, want false", out.Valid)
	}
}

func TestSeedStressLanesRPCAndInfo(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	pubKey := nodeSignerPubKey(7)
	params, err := json.Marshal(map[string]any{
		"pubkeys":               []string{hex.EncodeToString(pubKey[:]), hex.EncodeToString(pubKey[:])},
		"wait_for_confirmation": false,
		"reserve_top_up":        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "seedstresslanes",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC seedstresslanes: %v", err)
	}
	out, ok := result.(rpcSeedStressLanesResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcSeedStressLanesResult", result)
	}
	if got := out.Count; got != 2 {
		t.Fatalf("output count = %d, want 2", got)
	}
	if got := out.Confirmed; got {
		t.Fatal("expected unconfirmed seedstresslanes result")
	}

	infoResult, err := svc.dispatchRPC(rpcRequest{Method: "getstresslaneinfo"})
	if err != nil {
		t.Fatalf("dispatchRPC getstresslaneinfo: %v", err)
	}
	info, ok := infoResult.(StressLaneInfo)
	if !ok {
		t.Fatalf("result type = %T, want StressLaneInfo", infoResult)
	}
	if info.PendingBatches != 1 {
		t.Fatalf("pending batches = %d, want 1", info.PendingBatches)
	}
	if info.ReserveUTXOs == 0 {
		t.Fatal("expected reserve utxo info")
	}
}

func TestGetChainStateRPC(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	result, err := svc.dispatchRPC(rpcRequest{Method: "getchainstate"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(ChainStateInfo)
	if !ok {
		t.Fatalf("result type = %T, want ChainStateInfo", result)
	}
	if out.TipHeight != 0 {
		t.Fatalf("tip height = %d, want 0", out.TipHeight)
	}
	if out.UTXOCount != 1 {
		t.Fatalf("utxo count = %d, want 1", out.UTXOCount)
	}
	if out.UTXOChecksum == "" {
		t.Fatal("expected utxo checksum")
	}
	if out.NextBlockSizeLimit == 0 {
		t.Fatal("expected next block size limit")
	}
}

func TestGetMempoolInfoRPC(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesis.Txs[0].Base.Outputs[0].ValueAtoms = 1_000
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 1_000, PubKey: nodeSignerPubKey(7)},
	})
	svc, err := OpenService(ServiceConfig{
		Profile:            types.Regtest,
		DBPath:             t.TempDir(),
		MinRelayFeePerByte: 1,
		MaxMempoolBytes:    256 << 10,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 1_000, 8, 200)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}
	result, err := svc.dispatchRPC(rpcRequest{Method: "getmempoolinfo"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(MempoolInfo)
	if !ok {
		t.Fatalf("result type = %T, want MempoolInfo", result)
	}
	if out.Count != 1 {
		t.Fatalf("count = %d, want 1", out.Count)
	}
	if out.Bytes <= 0 {
		t.Fatalf("bytes = %d, want > 0", out.Bytes)
	}
	if out.MaxBytes != 256<<10 {
		t.Fatalf("max bytes = %d, want %d", out.MaxBytes, 256<<10)
	}
	if out.CandidateFrontier <= 0 {
		t.Fatalf("candidate frontier = %d, want > 0", out.CandidateFrontier)
	}
}

func TestGetMiningInfoRPC(t *testing.T) {
	genesis := genesisBlock()
	pubKey := nodeSignerPubKey(9)
	svc, err := OpenService(ServiceConfig{
		Profile:      types.Regtest,
		DBPath:       t.TempDir(),
		MinerEnabled: true,
		MinerWorkers: 2,
		MinerPubKey:  pubKey,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	result, err := svc.dispatchRPC(rpcRequest{Method: "getmininginfo"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(MiningInfo)
	if !ok {
		t.Fatalf("result type = %T, want MiningInfo", result)
	}
	if !out.Enabled {
		t.Fatal("expected mining enabled")
	}
	if out.Workers != 2 {
		t.Fatalf("workers = %d, want 2", out.Workers)
	}
	if out.MinerPubKey != hex.EncodeToString(pubKey[:]) {
		t.Fatalf("miner pubkey = %q", out.MinerPubKey)
	}
	if out.CurrentBits == 0 || out.NextBits == 0 {
		t.Fatal("expected current and next bits")
	}
}

func TestGetMetricsRPC(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesis.Txs[0].Base.Outputs[0].ValueAtoms = 1_000
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 1_000, PubKey: nodeSignerPubKey(7)},
	})
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 1_000, 8, 200)
	if _, err := svc.SubmitTx(tx); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}
	svc.noteRelaySent(relayMessageClass{txBatchItems: 4, blockInvItems: 1})
	svc.noteBlockAccepted()
	svc.noteBlockSignatureVerification(consensus.BlockValidationSummary{
		SignatureChecks:        9,
		SignatureBatchFallback: true,
		SignatureVerifyTime:    11 * time.Millisecond,
	})
	svc.noteTemplateRebuild()
	svc.noteTemplateInterruption()
	svc.noteTxReconRetry(2)
	svc.noteDirectFallback(1, 3)
	svc.noteTxRequestsReceived(5)
	svc.noteTxNotFoundSent(2)
	svc.noteTxNotFoundReceived(1)
	svc.noteKnownTxClears(4)
	svc.noteDuplicateSuppression(6)
	svc.noteWriterStarvation(1)
	svc.perf.noteAdmissionDuration(15 * time.Millisecond)
	svc.perf.noteTemplateDuration(25 * time.Millisecond)
	svc.perf.noteTemplateSelectDuration(9 * time.Millisecond)
	svc.perf.noteTemplateAccumulateDuration(4 * time.Millisecond)
	svc.perf.noteTemplateAssembleDuration(12 * time.Millisecond)
	svc.perf.noteBlockApplyDuration(35 * time.Millisecond)
	svc.perf.noteBlockApplyLockWaitDuration(7 * time.Millisecond)
	svc.perf.noteRelayFlushDuration(5 * time.Millisecond)
	svc.perf.noteSyncRequestDuration(8 * time.Millisecond)
	peer := newPeerConnForTests("127.0.0.1:18444")
	peer.svc = svc
	peer.controlQ = make(chan outboundMessage, 4)
	peer.relayPriorityQ = make(chan outboundMessage, 4)
	peer.controlQ <- outboundMessage{}
	peer.relayPriorityQ <- outboundMessage{}
	peer.sendQ <- outboundMessage{}
	peer.localRelayTxs[[32]byte{9}] = localRelayFallbackState{announcedAt: time.Now()}
	svc.peers[peer.addr] = peer

	result, err := svc.dispatchRPC(rpcRequest{Method: "getmetrics"})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(PerformanceMetrics)
	if !ok {
		t.Fatalf("result type = %T, want PerformanceMetrics", result)
	}
	if out.Counters.AdmittedTxs == 0 {
		t.Fatal("expected admitted tx counter")
	}
	if out.Counters.RelayedTxItems != 4 {
		t.Fatalf("relayed tx items = %d, want 4", out.Counters.RelayedTxItems)
	}
	if out.Counters.RelayedBlockItems != 1 {
		t.Fatalf("relayed block items = %d, want 1", out.Counters.RelayedBlockItems)
	}
	if out.Counters.BlocksAccepted != 1 {
		t.Fatalf("blocks accepted = %d, want 1", out.Counters.BlocksAccepted)
	}
	if out.Counters.BlockSigChecks != 9 || out.Counters.BlockSigFallbacks != 1 {
		t.Fatalf("unexpected block signature counters: %+v", out.Counters)
	}
	if out.Counters.TemplateRebuilds != 1 || out.Counters.TemplateInterruptions != 1 {
		t.Fatalf("unexpected template counters: %+v", out.Counters)
	}
	if out.Counters.TxReconRetries != 2 || out.Counters.DirectFallbackBatches != 1 || out.Counters.DirectFallbackTxs != 3 {
		t.Fatalf("unexpected relay counters: %+v", out.Counters)
	}
	if out.Counters.TxRequestsReceived != 5 || out.Counters.TxNotFoundSent != 2 || out.Counters.TxNotFoundReceived != 1 {
		t.Fatalf("unexpected tx request/notfound counters: %+v", out.Counters)
	}
	if out.Counters.KnownTxClears != 4 || out.Counters.DuplicateSuppressions != 6 || out.Counters.WriterStarvation != 1 {
		t.Fatalf("unexpected duplicate/starvation counters: %+v", out.Counters)
	}
	if out.Gauges.MempoolTxs != 1 {
		t.Fatalf("mempool txs = %d, want 1", out.Gauges.MempoolTxs)
	}
	if out.Gauges.CandidateFrontier <= 0 {
		t.Fatalf("candidate frontier = %d, want > 0", out.Gauges.CandidateFrontier)
	}
	if out.Gauges.ControlQueueDepth != 1 || out.Gauges.PriorityQueueDepth != 1 || out.Gauges.SendQueueDepth != 1 || out.Gauges.PendingLocalRelayTxs != 1 {
		t.Fatalf("unexpected relay gauges: %+v", out.Gauges)
	}
	if out.Latency.Admission.Count == 0 || out.Latency.Template.Count == 0 || out.Latency.TemplateSelect.Count == 0 || out.Latency.TemplateAccumulate.Count == 0 || out.Latency.TemplateAssemble.Count == 0 || out.Latency.BlockApply.Count == 0 || out.Latency.BlockSigVerify.Count == 0 || out.Latency.BlockApplyLockWait.Count == 0 || out.Latency.RelayFlush.Count == 0 || out.Latency.SyncReq.Count == 0 {
		t.Fatalf("expected latency samples in all metric groups: %+v", out.Latency)
	}
}

func TestGetBlockHashByHeightRPC(t *testing.T) {
	genesis := genesisBlock()
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	params, err := json.Marshal(map[string]any{"height": uint64(0)})
	if err != nil {
		t.Fatalf("Marshal params: %v", err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getblockhashbyheight",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(rpcGetBlockHashByHeightResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcGetBlockHashByHeightResult", result)
	}
	if got := out.Height; got != 0 {
		t.Fatalf("height = %#v, want 0", out.Height)
	}
	hash := consensus.HeaderHash(&genesis.Header)
	want := hex.EncodeToString(hash[:])
	if got := out.Hash; got != want {
		t.Fatalf("hash = %#v, want %q", out.Hash, want)
	}
}

func TestGetWalletActivityByPubKeysRPC(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	genesisTxID := consensus.TxID(&genesis.Txs[0])
	spend := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	block := blockWithTxsForNodeTest(t, 0, genesis.Header, svc.chainState.ChainState().UTXOs(), []types.Transaction{coinbase, spend}, genesis.Header.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(block); err != nil {
		t.Fatalf("acceptMinedBlock: %v", err)
	}

	pubKey7 := nodeSignerPubKey(7)
	params, err := json.Marshal(map[string]any{
		"pubkeys": []string{hex.EncodeToString(pubKey7[:])},
		"limit":   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "getwalletactivitybypubkeys",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(rpcWalletActivityResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcWalletActivityResult", result)
	}
	if len(out.Activity) != 1 {
		t.Fatalf("activity count = %d, want 1", len(out.Activity))
	}
	if got := out.Activity[0].Sent; got != 50 {
		t.Fatalf("sent = %v, want 50", got)
	}
	if got := out.Activity[0].Fee; got != 1 {
		t.Fatalf("fee = %v, want 1", got)
	}
}

func TestGetWalletActivityRPCRejectsUnboundedLimit(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	svc, err := OpenService(ServiceConfig{
		Profile: types.Regtest,
		DBPath:  t.TempDir(),
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	pubKey7 := nodeSignerPubKey(7)
	params, err := json.Marshal(map[string]any{
		"pubkeys": []string{hex.EncodeToString(pubKey7[:])},
		"limit":   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.dispatchRPC(rpcRequest{Method: "getwalletactivitybypubkeys", Params: params}); err == nil || !strings.Contains(err.Error(), "limit must be positive") {
		t.Fatalf("dispatchRPC err = %v, want positive limit error", err)
	}
}

func TestEstimateFeeRPC(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesis.Txs[0].Base.Outputs[0].ValueAtoms = 5_000
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 5_000, PubKey: nodeSignerPubKey(7)},
	})
	svc, err := OpenService(ServiceConfig{
		Profile:            types.Regtest,
		DBPath:             t.TempDir(),
		MinRelayFeePerByte: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	txA := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 5_000, 8, 2_000)
	if _, err := svc.SubmitTx(txA); err != nil {
		t.Fatalf("SubmitTx: %v", err)
	}
	params, err := json.Marshal(map[string]any{"target_blocks": 1})
	if err != nil {
		t.Fatal(err)
	}
	result, err := svc.dispatchRPC(rpcRequest{
		Method: "estimatefee",
		Params: params,
	})
	if err != nil {
		t.Fatalf("dispatchRPC: %v", err)
	}
	out, ok := result.(rpcEstimateFeeResult)
	if !ok {
		t.Fatalf("result type = %T, want rpcEstimateFeeResult", result)
	}
	if got := out.FeePerByte; got < 2 {
		t.Fatalf("fee_per_byte = %d, want at least 2", got)
	}
}
