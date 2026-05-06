package benchmarks

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	runtimepprof "runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
)

// These benches are the primary optimization loop. They intentionally stay in
// one process, avoid P2P/RPC listeners, and skip report generation so the
// timed section reflects node hot paths instead of harness orchestration.

func BenchmarkTxAdmission(b *testing.B) {
	svc, _, txs, cleanup, err := prepareIndependentSpendFixture(b, max(1, b.N))
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.SubmitTx(txs[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxAdmissionHexDecode(b *testing.B) {
	svc, _, txs, cleanup, err := prepareIndependentSpendFixture(b, max(1, b.N))
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	hexes := make([]string, len(txs))
	for i, tx := range txs {
		hexes[i] = hex.EncodeToString(tx.Encode())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := consensus.DecodeTxHex(hexes[i], types.DefaultCodecLimits())
		if err != nil {
			b.Fatal(err)
		}
		if _, err := svc.SubmitTx(tx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxValidation(b *testing.B) {
	_, utxos, txs, cleanup, err := prepareIndependentSpendFixture(b, max(1, b.N))
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := consensus.ValidateTxWithParams(&txs[i%len(txs)], utxos, consensus.BenchNetParams(), consensus.DefaultConsensusRules()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSignatureVerification(b *testing.B) {
	checkCount := benchmarkSizeFromEnv(b, "BPU_BENCH_SIG_CHECKS", 64)
	checks, cleanup, err := prepareSignatureChecksFixture(b, checkCount)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	b.ReportMetric(float64(checkCount), "sig_checks")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := crypto.VerifySchnorrBatchXOnlyResult(checks)
		if !result.Valid {
			b.Fatal("signature batch unexpectedly failed")
		}
	}
}

func BenchmarkMempoolSelection(b *testing.B) {
	mempoolTxs := benchmarkSizeFromEnv(b, "BPU_BENCH_MEMPOOL_TXS", 2_048)
	_, utxos, txs, cleanup, err := prepareIndependentSpendFixture(b, mempoolTxs)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	pool := mempool.New()
	for _, tx := range txs {
		if _, err := pool.AcceptTxWithParams(tx, utxos, consensus.BenchNetParams(), consensus.DefaultConsensusRules()); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(len(txs)), "mempool_txs")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected, totalFees, overlay := pool.SelectForBlockOverlay(utxos, consensus.DefaultConsensusRules(), 1_000_000)
		if len(selected) == 0 || totalFees == 0 || overlay == nil {
			b.Fatal("mempool selection returned an empty candidate")
		}
	}
}

func BenchmarkBlockBuild(b *testing.B) {
	prefillTxs := benchmarkSizeFromEnv(b, "BPU_BENCH_BLOCK_TXS", 1_024)

	svc, _, txs, cleanup, err := prepareIndependentSpendFixture(b, prefillTxs+max(1, b.N))
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	for _, tx := range txs[:prefillTxs] {
		if _, err := svc.SubmitTx(tx); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(prefillTxs), "seeded_txs")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 {
			b.StopTimer()
			if _, err := svc.SubmitTx(txs[prefillTxs+i-1]); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		block, err := svc.BuildBlockTemplate()
		if err != nil {
			b.Fatal(err)
		}
		if len(block.Txs) <= 1 {
			b.Fatal("block template missing admitted transactions")
		}
	}
}

func BenchmarkSteadyStateBlockCycle(b *testing.B) {
	mempoolTxs := benchmarkSizeFromEnv(b, "BPU_BENCH_MEMPOOL_TXS", 4_096)
	blockTxs := benchmarkSizeFromEnv(b, "BPU_BENCH_BLOCK_TXS", min(1_024, mempoolTxs))
	if blockTxs <= 0 || blockTxs*b.N > mempoolTxs {
		b.Fatalf("need BPU_BENCH_MEMPOOL_TXS >= BPU_BENCH_BLOCK_TXS*b.N, got mempool=%d block=%d n=%d", mempoolTxs, blockTxs, b.N)
	}

	svc, _, txs, cleanup, err := prepareIndependentSpendFixture(b, mempoolTxs)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	for _, tx := range txs {
		if _, err := svc.SubmitTx(tx); err != nil {
			b.Fatal(err)
		}
	}

	stopProfile := startInnerCPUProfile(b)
	b.ReportMetric(float64(mempoolTxs), "seeded_txs")
	b.ReportMetric(float64(blockTxs), "target_block_txs")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block, err := svc.BuildBenchmarkBlockTemplate(blockTxs)
		if err != nil {
			b.Fatal(err)
		}
		if got := len(block.Txs) - 1; got != blockTxs {
			b.Fatalf("block selected %d txs, want %d", got, blockTxs)
		}
		info := svc.ChainStateInfo()
		block.Header.Timestamp = info.TipTimestamp + 1
		if _, _, err := svc.AcceptLocalBenchmarkBlock(block); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	stopProfile()
}

func BenchmarkBlockApply(b *testing.B) {
	blockTxs := benchmarkSizeFromEnv(b, "BPU_BENCH_BLOCK_TXS", 256)
	block, baseState, buildCleanup, err := prepareBenchmarkBlockFixture(b, blockTxs)
	if err != nil {
		b.Fatal(err)
	}
	defer buildCleanup()

	b.ReportMetric(float64(len(block.Txs)-1), "block_txs")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := baseState.Clone()
		if _, err := state.ApplyBlock(&block); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSizeFromEnv(tb testing.TB, key string, fallback int) int {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		tb.Fatalf("%s must be a positive integer, got %q", key, raw)
	}
	return value
}

func startInnerCPUProfile(tb testing.TB) func() {
	tb.Helper()
	path := strings.TrimSpace(os.Getenv("BPU_BENCH_INNER_CPU_PROFILE"))
	if path == "" {
		return func() {}
	}
	file, err := os.Create(filepath.Clean(path))
	if err != nil {
		tb.Fatalf("create inner cpu profile: %v", err)
	}
	if err := runtimepprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		tb.Fatalf("start inner cpu profile: %v", err)
	}
	return func() {
		runtimepprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			tb.Fatalf("close inner cpu profile: %v", err)
		}
	}
}

func openIsolatedBenchmarkService(tb testing.TB) (*node.Service, func(), error) {
	tb.Helper()
	return openBenchmarkServiceAt(tb, filepath.Join(tb.TempDir(), "chain"))
}

func openBenchmarkServiceAt(tb testing.TB, dbPath string) (*node.Service, func(), error) {
	tb.Helper()

	restore := suppressLogs()
	opts := DefaultE2EOptions()
	consensus.SetBenchNetParams(benchNetParamsForRun(opts))

	genesis, err := loadBenchmarkGenesis(types.BenchNet)
	if err != nil {
		restore()
		return nil, nil, err
	}

	svc, err := node.OpenService(node.ServiceConfig{
		Profile:            types.BenchNet,
		DBPath:             dbPath,
		MinRelayFeePerByte: 1,
		MaxTxSize:          1_000_000,
		MaxMempoolBytes:    64 << 20,
		MaxAncestors:       256,
		MaxDescendants:     256,
		MaxOrphans:         128,
		MinerPubKey:        pubKeyForSeed(1),
		GenesisFixture:     benchmarkGenesisLabel(types.BenchNet),
		SyntheticMining:    true,
	}, genesis)
	if err != nil {
		restore()
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Start(ctx)
	}()

	return svc, func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			_ = svc.Close()
			<-errCh
		}
		restore()
	}, nil
}

func prepareIndependentSpendFixture(tb testing.TB, txCount int) (*node.Service, consensus.UtxoSet, []types.Transaction, func(), error) {
	tb.Helper()

	svc, cleanup, err := openIsolatedBenchmarkService(tb)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	outputs, err := seedFundingOutputs(svc, txCount, 2)
	if err != nil {
		cleanup()
		return nil, nil, nil, nil, err
	}

	utxos := make(consensus.UtxoSet, len(outputs))
	childPubKey := pubKeyForSeed(3)
	txs := make([]types.Transaction, 0, len(outputs))
	for _, output := range outputs {
		utxos[output.OutPoint] = consensus.UtxoEntry{
			ValueAtoms: output.Value,
			PubKey:     pubKeyForSeed(2),
		}
		tx, err := buildChildTx(2, output.OutPoint, output.Value, childPubKey, consensus.BenchNetParams())
		if err != nil {
			cleanup()
			return nil, nil, nil, nil, err
		}
		txs = append(txs, tx)
	}

	return svc, utxos, txs, cleanup, nil
}

func prepareSignatureChecksFixture(tb testing.TB, inputCount int) ([]crypto.SchnorrBatchItem, func(), error) {
	tb.Helper()

	svc, cleanup, err := openIsolatedBenchmarkService(tb)
	if err != nil {
		return nil, nil, err
	}

	outputs, err := seedFundingOutputs(svc, inputCount, 2)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	utxos := make(consensus.UtxoSet, len(outputs))
	tx, err := buildAggregateSpendTx(2, outputs, pubKeyForSeed(4))
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	for _, output := range outputs {
		utxos[output.OutPoint] = consensus.UtxoEntry{
			ValueAtoms: output.Value,
			PubKey:     pubKeyForSeed(2),
		}
	}

	prepared, err := consensus.PrepareTxValidationWithLookupAndParams(&tx, consensus.LookupFromSet(utxos), consensus.BenchNetParams(), consensus.DefaultConsensusRules())
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if len(prepared.SignatureChecks) != inputCount {
		cleanup()
		return nil, nil, fmt.Errorf("prepared %d signature checks, want %d", len(prepared.SignatureChecks), inputCount)
	}
	return prepared.SignatureChecks, cleanup, nil
}

func prepareBenchmarkBlockFixture(tb testing.TB, txCount int) (types.Block, *node.ChainState, func(), error) {
	tb.Helper()

	dbPath := filepath.Join(tb.TempDir(), "chain")
	svc, stopService, err := openBenchmarkServiceAt(tb, dbPath)
	if err != nil {
		return types.Block{}, nil, nil, err
	}
	outputs, err := seedFundingOutputs(svc, txCount, 2)
	if err != nil {
		stopService()
		return types.Block{}, nil, nil, err
	}
	txs := make([]types.Transaction, 0, len(outputs))
	for _, output := range outputs {
		tx, err := buildChildTx(2, output.OutPoint, output.Value, pubKeyForSeed(3), consensus.BenchNetParams())
		if err != nil {
			stopService()
			return types.Block{}, nil, nil, err
		}
		txs = append(txs, tx)
	}
	for _, tx := range txs {
		if _, err := svc.SubmitTx(tx); err != nil {
			stopService()
			return types.Block{}, nil, nil, err
		}
	}

	block, err := buildConfirmedBenchmarkBlock(context.Background(), svc, RunOptions{
		Profile:         types.BenchNet,
		SyntheticMining: false,
	})
	if err != nil {
		stopService()
		return types.Block{}, nil, nil, err
	}
	stopService()

	chainState, err := node.OpenPersistentChainStateFromMetaWithRules(dbPath, types.BenchNet, consensus.DefaultConsensusRules())
	if err != nil {
		return types.Block{}, nil, nil, err
	}
	baseState := chainState.ChainState()
	if baseState == nil {
		_ = chainState.Close()
		return types.Block{}, nil, nil, fmt.Errorf("benchmark block fixture missing base chain state")
	}
	return block.block, baseState, func() {
		_ = chainState.Close()
	}, nil
}

func buildAggregateSpendTx(spenderSeed byte, outputs []fundingOutput, outputPubKey [32]byte) (types.Transaction, error) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  make([]types.TxInput, 0, len(outputs)),
			Outputs: []types.TxOutput{{ValueAtoms: 1, PubKey: outputPubKey}},
		},
	}
	spentCoins := make([]consensus.UtxoEntry, 0, len(outputs))
	var inputSum uint64
	for _, output := range outputs {
		tx.Base.Inputs = append(tx.Base.Inputs, types.TxInput{PrevOut: output.OutPoint})
		spentCoins = append(spentCoins, consensus.UtxoEntry{
			ValueAtoms: output.Value,
			PubKey:     output.PubKey,
		})
		inputSum += output.Value
	}

	tx, err := signMultiInputTx(tx, spenderSeed, spentCoins, consensus.BenchNetParams())
	if err != nil {
		return types.Transaction{}, err
	}
	fee := uint64(tx.EncodedLen())
	if inputSum < fee {
		return types.Transaction{}, consensus.ErrInputsLessThanOutputs
	}
	tx.Base.Outputs[0].ValueAtoms = inputSum - fee
	return signMultiInputTx(tx, spenderSeed, spentCoins, consensus.BenchNetParams())
}

func signMultiInputTx(tx types.Transaction, spenderSeed byte, spentCoins []consensus.UtxoEntry, params consensus.ChainParams) (types.Transaction, error) {
	auth := make([]types.TxAuthEntry, len(tx.Base.Inputs))
	for i := range tx.Base.Inputs {
		msg, err := consensus.SighashWithParams(&tx, i, spentCoins, params)
		if err != nil {
			return types.Transaction{}, err
		}
		_, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
		auth[i] = types.TxAuthEntry{Signature: sig}
	}
	tx.Auth = types.TxAuth{Entries: auth}
	return tx, nil
}
