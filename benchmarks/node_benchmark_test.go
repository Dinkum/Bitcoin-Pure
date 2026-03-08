package benchmarks

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
)

func BenchmarkServiceSubmitTx(b *testing.B) {
	svc, txs, cleanup, err := openSingleServiceBenchmark(b, b.N)
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

func BenchmarkServiceSubmitTxHexDecode(b *testing.B) {
	svc, txs, cleanup, err := openSingleServiceBenchmark(b, b.N)
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

func BenchmarkRPCSubmitTxBatch32(b *testing.B) {
	const batchSize = 32
	report, cleanup, err := runBenchmarkForTest(b, RunOptions{
		Scenario:     ScenarioRPCBatch,
		Profile:      types.Regtest,
		NodeCount:    1,
		BatchSize:    batchSize,
		TxCount:      b.N * batchSize,
		Timeout:      30 * time.Second,
		SuppressLogs: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	b.ReportMetric(report.EndToEndTPS, "tx/s")
}

func openSingleServiceBenchmark(tb testing.TB, txCount int) (*node.Service, []types.Transaction, func(), error) {
	tb.Helper()
	restore := suppressLogs()
	ctx := context.Background()
	cluster, cleanup, err := openCluster(ctx, withDefaults(RunOptions{
		Scenario:     ScenarioDirectSubmit,
		Profile:      types.Regtest,
		NodeCount:    1,
		TxCount:      txCount,
		SuppressLogs: true,
	}))
	if err != nil {
		restore()
		return nil, nil, nil, err
	}
	txs, err := seedSpendableFanout(cluster[0].svc, txCount)
	if err != nil {
		cleanup()
		restore()
		return nil, nil, nil, err
	}
	return cluster[0].svc, txs, func() {
		cleanup()
		restore()
	}, nil
}

func runBenchmarkForTest(tb testing.TB, opts RunOptions) (*Report, func(), error) {
	tb.Helper()
	report, err := Run(context.Background(), opts)
	return report, func() {}, err
}
