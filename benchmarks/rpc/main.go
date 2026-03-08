package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"bitcoin-pure/benchmarks"
	"bitcoin-pure/internal/types"
)

func main() {
	opts := benchmarks.DefaultRunOptions()
	opts.Scenario = benchmarks.ScenarioRPCBatch
	opts.Profile = types.Regtest
	opts.NodeCount = 1

	report, err := benchmarks.Run(context.Background(), opts)
	if err != nil {
		panic(err)
	}

	base := filepath.Join("benchmarks", "reports", time.Now().UTC().Format("20060102-150405")+"-rpc-batch")
	if err := benchmarks.WriteReportFiles(report, base+".json", base+".md"); err != nil {
		panic(err)
	}

	fmt.Printf("scenario: %s\n", report.Scenario)
	fmt.Printf("tx_count: %d\n", report.TxCount)
	fmt.Printf("submit_tps: %.2f\n", report.SubmitTPS)
	fmt.Printf("end_to_end_tps: %.2f\n", report.EndToEndTPS)
	fmt.Printf("report_json: %s\n", base+".json")
	fmt.Printf("report_markdown: %s\n", base+".md")
	os.Exit(0)
}
