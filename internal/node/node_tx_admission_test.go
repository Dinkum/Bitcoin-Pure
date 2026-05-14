package node

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	"errors"
	"testing"
)

func TestSubmitDecodedTxsCachesPermanentRejects(t *testing.T) {
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
	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 1)
	tx.Auth.Entries[0].Signature[0] ^= 0xff

	_, errs, _, _ := svc.submitDecodedTxsFrom([]types.Transaction{tx}, nil)
	if !errors.Is(errs[0], consensus.ErrInvalidSignature) {
		t.Fatalf("first err = %v, want ErrInvalidSignature", errs[0])
	}
	firstStats := svc.rejectCache.snapshot()
	if firstStats.Entries != 1 {
		t.Fatalf("reject cache entries = %d, want 1", firstStats.Entries)
	}
	if firstStats.Hits != 0 {
		t.Fatalf("reject cache hits = %d, want 0", firstStats.Hits)
	}

	_, errs, _, _ = svc.submitDecodedTxsFrom([]types.Transaction{tx}, nil)
	if !errors.Is(errs[0], consensus.ErrInvalidSignature) {
		t.Fatalf("second err = %v, want ErrInvalidSignature", errs[0])
	}
	secondStats := svc.rejectCache.snapshot()
	if secondStats.Entries != 1 {
		t.Fatalf("reject cache entries after replay = %d, want 1", secondStats.Entries)
	}
	if secondStats.Hits != firstStats.Hits+1 {
		t.Fatalf("reject cache hits after replay = %d, want %d", secondStats.Hits, firstStats.Hits+1)
	}
}

func TestSubmitDecodedTxsCachesLowFeeRejects(t *testing.T) {
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
		MinRelayFeePerByte: 2,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	tx := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 1_000, 8, 1)
	_, errs, _, _ := svc.submitDecodedTxsFrom([]types.Transaction{tx}, nil)
	if !errors.Is(errs[0], mempool.ErrRelayFeeTooLow) {
		t.Fatalf("first err = %v, want ErrRelayFeeTooLow", errs[0])
	}
	firstStats := svc.rejectCache.snapshot()
	if firstStats.Entries != 1 {
		t.Fatalf("reject cache entries after low-fee reject = %d, want 1", firstStats.Entries)
	}

	_, errs, _, _ = svc.submitDecodedTxsFrom([]types.Transaction{tx}, nil)
	if !errors.Is(errs[0], mempool.ErrRelayFeeTooLow) {
		t.Fatalf("second err = %v, want ErrRelayFeeTooLow", errs[0])
	}
	secondStats := svc.rejectCache.snapshot()
	if secondStats.Hits != firstStats.Hits+1 {
		t.Fatalf("reject cache hits after low-fee replay = %d, want %d", secondStats.Hits, firstStats.Hits+1)
	}
}

func TestSubmitDecodedTxsTemporaryRejectExpiresAfterStateChange(t *testing.T) {
	genesis := genesisBlockForPubKey(nodeSignerPubKey(7))
	genesis.Txs[0].Base.Outputs = []types.TxOutput{
		{ValueAtoms: 50, PubKey: nodeSignerPubKey(7)},
		{ValueAtoms: 50, PubKey: nodeSignerPubKey(9)},
	}
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(7)},
		types.OutPoint{TxID: genesisTxID, Vout: 1}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(9)},
	})

	filler := spendTxForNodeTest(t, 7, types.OutPoint{TxID: genesisTxID, Vout: 0}, 50, 8, 10)
	candidate := spendTxForNodeTest(t, 9, types.OutPoint{TxID: genesisTxID, Vout: 1}, 50, 10, 1)
	fillerBytes := len(filler.Encode())

	svc, err := OpenService(ServiceConfig{
		Profile:            types.Regtest,
		DBPath:             t.TempDir(),
		MinRelayFeePerByte: 0,
		MaxMempoolBytes:    fillerBytes,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	if _, err := svc.SubmitTx(filler); err != nil {
		t.Fatalf("SubmitTx filler: %v", err)
	}
	_, errs, _, _ := svc.submitDecodedTxsFrom([]types.Transaction{candidate}, nil)
	if !errors.Is(errs[0], mempool.ErrMempoolFull) {
		t.Fatalf("first candidate err = %v, want ErrMempoolFull", errs[0])
	}
	afterFirstReject := svc.rejectCache.snapshot()
	if afterFirstReject.Entries != 1 {
		t.Fatalf("reject cache entries after first reject = %d, want 1", afterFirstReject.Entries)
	}

	_, errs, _, _ = svc.submitDecodedTxsFrom([]types.Transaction{candidate}, nil)
	if !errors.Is(errs[0], mempool.ErrMempoolFull) {
		t.Fatalf("second candidate err = %v, want ErrMempoolFull", errs[0])
	}
	afterCachedReplay := svc.rejectCache.snapshot()
	if afterCachedReplay.Hits != afterFirstReject.Hits+1 {
		t.Fatalf("reject cache hits after replay = %d, want %d", afterCachedReplay.Hits, afterFirstReject.Hits+1)
	}

	block := blockWithTxsForNodeTest(t, 0, genesis.Header, svc.chainState.ChainState().UTXOs(), []types.Transaction{
		coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(11)}}),
		filler,
	}, genesis.Header.Timestamp+600)
	if _, _, err := svc.acceptMinedBlock(block); err != nil {
		t.Fatalf("acceptMinedBlock: %v", err)
	}

	_, errs, _, _ = svc.submitDecodedTxsFrom([]types.Transaction{candidate}, nil)
	if errs[0] != nil {
		t.Fatalf("candidate after tip change err = %v, want nil", errs[0])
	}
	if !svc.pool.Contains(consensus.TxID(&candidate)) {
		t.Fatal("candidate missing from mempool after temporary reject expired")
	}
	afterStateChange := svc.rejectCache.snapshot()
	if afterStateChange.Hits != afterCachedReplay.Hits {
		t.Fatalf("reject cache hits after state change = %d, want %d", afterStateChange.Hits, afterCachedReplay.Hits)
	}
}

func TestApplyPeerBlockCachesOrphanPromotionRejects(t *testing.T) {
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
	chainUTXOs := consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {
			ValueAtoms: 50,
			PubKey:     nodeSignerPubKey(7),
		},
	}
	coinbase := coinbaseTxForHeight(1, []types.TxOutput{{ValueAtoms: 1, PubKey: nodeSignerPubKey(9)}})
	block := blockWithTxsForNodeTest(t, 0, genesis.Header, chainUTXOs, []types.Transaction{coinbase}, genesis.Header.Timestamp+1)
	orphanTx := spendTxForNodeTest(t, 9, types.OutPoint{TxID: consensus.TxID(&coinbase), Vout: 0}, 1, 8, 0)
	orphanTx.Auth.Entries[0].Signature[0] ^= 0xff
	peer := newPeerConnForTests("127.0.0.1:18444")

	if err := svc.onPeerMessage(peer, p2p.TxBatchMessage{Txs: []types.Transaction{orphanTx}}); err != nil {
		t.Fatalf("onPeerMessage orphan batch: %v", err)
	}
	beforePromotion := svc.rejectCache.snapshot()
	if beforePromotion.Entries != 0 {
		t.Fatalf("reject cache entries before parent block = %d, want 0", beforePromotion.Entries)
	}

	if _, err := svc.applyPeerHeaders([]types.BlockHeader{block.Header}); err != nil {
		t.Fatalf("applyPeerHeaders: %v", err)
	}
	if err := svc.acceptPeerBlockMessage(peer, &block); err != nil {
		t.Fatalf("acceptPeerBlockMessage: %v", err)
	}
	afterPromotion := svc.rejectCache.snapshot()
	if afterPromotion.Entries != 1 {
		t.Fatalf("reject cache entries after orphan promotion reject = %d, want 1", afterPromotion.Entries)
	}
	if got := svc.pool.Count(); got != 0 {
		t.Fatalf("mempool count after invalid orphan promotion = %d, want 0", got)
	}
	if got := svc.pool.OrphanCount(); got != 0 {
		t.Fatalf("orphan count after invalid orphan promotion = %d, want 0", got)
	}

	_, errs, _, _ := svc.submitDecodedTxsFrom([]types.Transaction{orphanTx}, peer)
	if !errors.Is(errs[0], consensus.ErrInvalidSignature) {
		t.Fatalf("replayed orphan err = %v, want ErrInvalidSignature", errs[0])
	}
	afterReplay := svc.rejectCache.snapshot()
	if afterReplay.Hits != afterPromotion.Hits+1 {
		t.Fatalf("reject cache hits after orphan replay = %d, want %d", afterReplay.Hits, afterPromotion.Hits+1)
	}
}

func TestSubmitDecodedTxsAcceptsDependentChainBatch(t *testing.T) {
	genesis := genesisBlock()
	genesis.Txs[0].Base.Outputs[0].PubKey = nodeSignerPubKey(7)
	genesisTxID := consensus.TxID(&genesis.Txs[0])
	genesis.Header.MerkleTxIDRoot = merkleRootForNodeTest([][32]byte{genesisTxID})
	genesis.Header.MerkleAuthRoot = merkleRootForNodeTest([][32]byte{consensus.AuthID(&genesis.Txs[0])})
	genesis.Header.UTXORoot = consensus.ComputedUTXORoot(consensus.UtxoSet{
		types.OutPoint{TxID: genesisTxID, Vout: 0}: {ValueAtoms: 50, PubKey: nodeSignerPubKey(7)},
	})

	svc, err := OpenService(ServiceConfig{
		Profile:        types.Regtest,
		DBPath:         t.TempDir(),
		MaxAncestors:   256,
		MaxDescendants: 256,
	}, &genesis)
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	defer svc.Close()

	txs := make([]types.Transaction, 0, 64)
	prevOut := types.OutPoint{TxID: genesisTxID, Vout: 0}
	prevValue := uint64(50)
	currentSeed := byte(7)
	for i := 0; i < 32; i++ {
		nextSeed := currentSeed + 1
		tx := spendTxForNodeTest(t, currentSeed, prevOut, prevValue, nextSeed, 1)
		txs = append(txs, tx)
		prevOut = types.OutPoint{TxID: consensus.TxID(&tx), Vout: 0}
		prevValue = tx.Base.Outputs[0].ValueAtoms
		currentSeed = nextSeed
	}

	admissions, errs := svc.SubmitTxBatch(txs)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("batch err at %d: %v", i, err)
		}
	}
	if got := svc.MempoolCount(); got != len(txs) {
		t.Fatalf("mempool size = %d, want %d", got, len(txs))
	}
	for i, admission := range admissions {
		if admission.Orphaned {
			t.Fatalf("admission %d unexpectedly orphaned", i)
		}
	}
}

func TestAcceptedAdmissionsPopulateValidAuthCache(t *testing.T) {
	pool := mempool.NewWithConfig(mempool.PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	prevOut := types.OutPoint{TxID: [32]byte{1}, Vout: 0}
	utxos := consensus.UtxoSet{
		prevOut: {ValueAtoms: 50, PubKey: nodeSignerPubKey(1)},
	}
	tx := spendTxForNodeTest(t, 1, prevOut, 50, 2, 1)
	admission, err := pool.AcceptTxWithParams(tx, utxos, consensus.RegtestParams(), consensus.DefaultConsensusRules())
	if err != nil {
		t.Fatalf("accept tx: %v", err)
	}
	if got, want := len(admission.Accepted), 1; got != want {
		t.Fatalf("accepted = %d, want %d", got, want)
	}

	svc := &Service{
		cfg:       ServiceConfig{Profile: types.Regtest},
		validAuth: newValidAuthCache(8),
	}
	if svc.validAuth.items != nil || svc.validAuth.order != nil {
		t.Fatal("valid auth cache should allocate backing storage lazily")
	}
	svc.noteAcceptedAdmissions([]mempool.Admission{admission})
	if svc.validAuth.items == nil || svc.validAuth.order == nil {
		t.Fatal("valid auth cache did not allocate on first insert")
	}

	accepted := admission.Accepted[0]
	if !svc.hasValidTxAuth(accepted.TxID, accepted.AuthID, consensus.RegtestParams()) {
		t.Fatal("accepted tx auth was not remembered as valid")
	}
	wrongAuth := accepted.AuthID
	wrongAuth[0] ^= 0xff
	if svc.hasValidTxAuth(accepted.TxID, wrongAuth, consensus.RegtestParams()) {
		t.Fatal("valid auth cache matched a different authid")
	}
}
