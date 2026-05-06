package benchmarks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bitcoin-pure/internal/types"
)

const (
	DefaultPerfGateBudgetPath = "benchmarks/perf-gate.json"
	perfGateMicroReportName   = "micro"
	perfGateE2EReportName     = "e2e"
)

type PerfGateBudget struct {
	Micro PerfGateMicroBudget `json:"micro"`
	E2E   PerfGateE2EBudget   `json:"e2e"`
}

type PerfGateMicroBudget struct {
	Package    string              `json:"package,omitempty"`
	Bench      string              `json:"bench"`
	Count      int                 `json:"count,omitempty"`
	Benchtime  string              `json:"benchtime,omitempty"`
	Benchmarks []PerfGateMicroCase `json:"benchmarks"`
}

type PerfGateMicroCase struct {
	Name                        string  `json:"name"`
	MaxNsPerOpRegressionPct     float64 `json:"max_ns_per_op_regression_pct,omitempty"`
	MaxBytesPerOpRegressionPct  float64 `json:"max_bytes_per_op_regression_pct,omitempty"`
	MaxAllocsPerOpRegressionPct float64 `json:"max_allocs_per_op_regression_pct,omitempty"`
}

type PerfGateE2EBudget struct {
	Profile                        string  `json:"profile"`
	Mining                         string  `json:"mining"`
	Nodes                          int     `json:"nodes"`
	Topology                       string  `json:"topology"`
	TxOrigin                       string  `json:"tx_origin"`
	BatchSize                      int     `json:"batch_size,omitempty"`
	TxsPerBlock                    int     `json:"txs_per_block"`
	Blocks                         int     `json:"blocks"`
	BlockInterval                  string  `json:"block_interval"`
	MaxAdmissionTPSDropPct         float64 `json:"max_admission_tps_drop_pct,omitempty"`
	MaxCompletionTPSDropPct        float64 `json:"max_completion_tps_drop_pct,omitempty"`
	MaxConfirmedWallTPSDropPct     float64 `json:"max_confirmed_wall_tps_drop_pct,omitempty"`
	MaxScheduleLagP95MSIncreasePct float64 `json:"max_schedule_lag_p95_ms_increase_pct,omitempty"`
	MaxFinalMempoolTxsIncreasePct  float64 `json:"max_final_mempool_txs_increase_pct,omitempty"`
	MaxMissedIntervalsIncrease     int     `json:"max_missed_intervals_increase,omitempty"`
	MaxFinalBlockLagIncrease       int     `json:"max_final_block_lag_increase,omitempty"`
}

type PerfGateRunReports struct {
	MicroJSON string
	MicroMD   string
	E2EJSON   string
	E2EMD     string
}

type PerfGateComparison struct {
	Failures []string
	Checks   []string
}

func (c PerfGateComparison) Passed() bool {
	return len(c.Failures) == 0
}

func (c PerfGateComparison) Error() error {
	if c.Passed() {
		return nil
	}
	return errors.New(strings.Join(c.Failures, "\n"))
}

func DefaultPerfGateReportPaths(root string) PerfGateRunReports {
	clean := filepath.Clean(root)
	return PerfGateRunReports{
		MicroJSON: filepath.Join(clean, perfGateMicroReportName+".json"),
		MicroMD:   filepath.Join(clean, perfGateMicroReportName+".md"),
		E2EJSON:   filepath.Join(clean, perfGateE2EReportName+".json"),
		E2EMD:     filepath.Join(clean, perfGateE2EReportName+".md"),
	}
}

func LoadPerfGateBudget(path string) (*PerfGateBudget, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var budget PerfGateBudget
	if err := json.Unmarshal(raw, &budget); err != nil {
		return nil, err
	}
	if err := budget.validate(); err != nil {
		return nil, err
	}
	return &budget, nil
}

func (b PerfGateBudget) validate() error {
	if strings.TrimSpace(b.Micro.Bench) == "" {
		return fmt.Errorf("micro bench regex is required")
	}
	if len(b.Micro.Benchmarks) == 0 {
		return fmt.Errorf("at least one micro benchmark budget is required")
	}
	seen := make(map[string]struct{}, len(b.Micro.Benchmarks))
	for _, bench := range b.Micro.Benchmarks {
		name := strings.TrimSpace(bench.Name)
		if name == "" {
			return fmt.Errorf("micro benchmark name is required")
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate micro benchmark budget for %s", name)
		}
		seen[name] = struct{}{}
	}
	if _, err := b.E2E.runOptions(); err != nil {
		return err
	}
	return nil
}

func (b PerfGateBudget) microOptions() MicroBenchOptions {
	return MicroBenchOptions{
		Package:   defaultString(b.Micro.Package, "./benchmarks"),
		Bench:     b.Micro.Bench,
		Count:     defaultInt(b.Micro.Count, 1),
		Benchtime: strings.TrimSpace(b.Micro.Benchtime),
	}
}

func (b PerfGateBudget) Run(ctx context.Context, outDir string) (PerfGateRunReports, error) {
	reportPaths := DefaultPerfGateReportPaths(outDir)
	microReport, err := RunMicroBenchmarks(ctx, b.microOptions())
	if err != nil {
		return PerfGateRunReports{}, err
	}
	if err := WriteMicroReportFiles(microReport, reportPaths.MicroJSON, reportPaths.MicroMD); err != nil {
		return PerfGateRunReports{}, err
	}

	e2eOpts, err := b.E2E.runOptions()
	if err != nil {
		return PerfGateRunReports{}, err
	}
	e2eReport, err := RunE2E(ctx, e2eOpts)
	if err != nil {
		return PerfGateRunReports{}, err
	}
	if err := WriteReportFiles(e2eReport, reportPaths.E2EJSON, reportPaths.E2EMD); err != nil {
		return PerfGateRunReports{}, err
	}
	return reportPaths, nil
}

func (b PerfGateE2EBudget) runOptions() (RunOptions, error) {
	profile, err := types.ParseChainProfile(strings.TrimSpace(b.Profile))
	if err != nil {
		return RunOptions{}, err
	}
	if strings.TrimSpace(b.Mining) == "" {
		return RunOptions{}, fmt.Errorf("e2e mining mode is required")
	}
	if strings.TrimSpace(b.Topology) == "" {
		return RunOptions{}, fmt.Errorf("e2e topology is required")
	}
	if strings.TrimSpace(b.TxOrigin) == "" {
		return RunOptions{}, fmt.Errorf("e2e tx origin is required")
	}
	interval, err := time.ParseDuration(strings.TrimSpace(b.BlockInterval))
	if err != nil {
		return RunOptions{}, fmt.Errorf("parse e2e block interval: %w", err)
	}
	if b.Nodes <= 0 || b.TxsPerBlock <= 0 || b.Blocks <= 0 {
		return RunOptions{}, fmt.Errorf("e2e nodes, txs_per_block, and blocks must all be > 0")
	}
	opts := DefaultE2EOptions()
	opts.Profile = profile
	opts.MiningMode = MiningMode(strings.TrimSpace(b.Mining))
	opts.NodeCount = b.Nodes
	opts.Topology = Topology(strings.TrimSpace(b.Topology))
	opts.TxOriginSpread = TxOriginSpread(strings.TrimSpace(b.TxOrigin))
	opts.BatchSize = defaultInt(b.BatchSize, opts.BatchSize)
	opts.TxsPerBlock = b.TxsPerBlock
	opts.BlockCount = b.Blocks
	opts.BlockInterval = interval
	opts.ProgressWriter = nil
	return opts, nil
}

func ComparePerfGateReports(budget *PerfGateBudget, baselineDir, candidateDir string) (*PerfGateComparison, error) {
	if budget == nil {
		return nil, fmt.Errorf("perf gate budget is required")
	}
	baselinePaths := DefaultPerfGateReportPaths(baselineDir)
	candidatePaths := DefaultPerfGateReportPaths(candidateDir)

	baselineMicro, err := loadMicroReport(baselinePaths.MicroJSON)
	if err != nil {
		return nil, err
	}
	candidateMicro, err := loadMicroReport(candidatePaths.MicroJSON)
	if err != nil {
		return nil, err
	}
	baselineE2E, err := loadE2EReport(baselinePaths.E2EJSON)
	if err != nil {
		return nil, err
	}
	candidateE2E, err := loadE2EReport(candidatePaths.E2EJSON)
	if err != nil {
		return nil, err
	}
	return EvaluatePerfGate(*budget, baselineMicro, candidateMicro, baselineE2E, candidateE2E)
}

func EvaluatePerfGate(budget PerfGateBudget, baselineMicro, candidateMicro *MicroBenchReport, baselineE2E, candidateE2E *Report) (*PerfGateComparison, error) {
	if err := budget.validate(); err != nil {
		return nil, err
	}
	if baselineMicro == nil || candidateMicro == nil || baselineE2E == nil || candidateE2E == nil {
		return nil, fmt.Errorf("both baseline and candidate benchmark reports are required")
	}
	if err := validateMicroReportShape(budget.Micro, baselineMicro, "baseline"); err != nil {
		return nil, err
	}
	if err := validateMicroReportShape(budget.Micro, candidateMicro, "candidate"); err != nil {
		return nil, err
	}
	if err := validateE2EReportShape(budget.E2E, baselineE2E, "baseline"); err != nil {
		return nil, err
	}
	if err := validateE2EReportShape(budget.E2E, candidateE2E, "candidate"); err != nil {
		return nil, err
	}

	comparison := &PerfGateComparison{}
	for _, benchBudget := range budget.Micro.Benchmarks {
		baseBench, ok := findMicroBenchmark(baselineMicro, benchBudget.Name)
		if !ok {
			return nil, fmt.Errorf("baseline micro report missing %s", benchBudget.Name)
		}
		candBench, ok := findMicroBenchmark(candidateMicro, benchBudget.Name)
		if !ok {
			return nil, fmt.Errorf("candidate micro report missing %s", benchBudget.Name)
		}
		checkLowerIsBetter(comparison, benchBudget.Name+" ns/op", baseBench.NsPerOp, candBench.NsPerOp, benchBudget.MaxNsPerOpRegressionPct)
		checkLowerIsBetter(comparison, benchBudget.Name+" B/op", baseBench.BytesPerOp, candBench.BytesPerOp, benchBudget.MaxBytesPerOpRegressionPct)
		checkLowerIsBetter(comparison, benchBudget.Name+" allocs/op", baseBench.AllocsPerOp, candBench.AllocsPerOp, benchBudget.MaxAllocsPerOpRegressionPct)
	}

	checkHigherIsBetter(comparison, "e2e admission_tps", baselineE2E.AdmissionTPS, candidateE2E.AdmissionTPS, budget.E2E.MaxAdmissionTPSDropPct)
	checkHigherIsBetter(comparison, "e2e completion_tps", baselineE2E.CompletionTPS, candidateE2E.CompletionTPS, budget.E2E.MaxCompletionTPSDropPct)
	checkHigherIsBetter(comparison, "e2e confirmed_wall_tps", baselineE2E.ConfirmedWallTPS, candidateE2E.ConfirmedWallTPS, budget.E2E.MaxConfirmedWallTPSDropPct)
	checkLowerIsBetter(comparison, "e2e schedule_lag_p95_ms", baselineE2E.Metrics.ScheduleLagP95MS, candidateE2E.Metrics.ScheduleLagP95MS, budget.E2E.MaxScheduleLagP95MSIncreasePct)
	checkLowerIsBetter(comparison, "e2e final_mempool_txs", float64(baselineE2E.Metrics.FinalMempoolTxs), float64(candidateE2E.Metrics.FinalMempoolTxs), budget.E2E.MaxFinalMempoolTxsIncreasePct)
	checkAbsoluteIncrease(comparison, "e2e missed_intervals", baselineE2E.Metrics.MissedIntervals, candidateE2E.Metrics.MissedIntervals, budget.E2E.MaxMissedIntervalsIncrease)
	checkAbsoluteIncrease(comparison, "e2e final_block_lag", int(baselineE2E.Metrics.FinalBlockLag), int(candidateE2E.Metrics.FinalBlockLag), budget.E2E.MaxFinalBlockLagIncrease)
	return comparison, nil
}

func RenderPerfGateComparison(comparison *PerfGateComparison) string {
	if comparison == nil {
		return ""
	}
	var b strings.Builder
	if comparison.Passed() {
		b.WriteString("performance gate passed\n")
	} else {
		b.WriteString("performance gate failed\n")
	}
	for _, check := range comparison.Checks {
		b.WriteString("- ")
		b.WriteString(check)
		b.WriteByte('\n')
	}
	if len(comparison.Failures) != 0 {
		b.WriteString("\nFailures:\n")
		for _, failure := range comparison.Failures {
			b.WriteString("- ")
			b.WriteString(failure)
			b.WriteByte('\n')
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func loadMicroReport(path string) (*MicroBenchReport, error) {
	var report MicroBenchReport
	if err := loadJSON(path, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func loadE2EReport(path string) (*Report, error) {
	var report Report
	if err := loadJSON(path, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func loadJSON(path string, out any) error {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func validateMicroReportShape(budget PerfGateMicroBudget, report *MicroBenchReport, label string) error {
	if strings.TrimSpace(report.Package) != strings.TrimSpace(defaultString(budget.Package, "./benchmarks")) {
		return fmt.Errorf("%s micro report package = %q, want %q", label, report.Package, defaultString(budget.Package, "./benchmarks"))
	}
	if strings.TrimSpace(report.Bench) != strings.TrimSpace(budget.Bench) {
		return fmt.Errorf("%s micro report bench regex = %q, want %q", label, report.Bench, budget.Bench)
	}
	if want := defaultInt(budget.Count, 1); report.Count != want {
		return fmt.Errorf("%s micro report count = %d, want %d", label, report.Count, want)
	}
	if strings.TrimSpace(report.Benchtime) != strings.TrimSpace(budget.Benchtime) {
		return fmt.Errorf("%s micro report benchtime = %q, want %q", label, report.Benchtime, strings.TrimSpace(budget.Benchtime))
	}
	return nil
}

func validateE2EReportShape(budget PerfGateE2EBudget, report *Report, label string) error {
	interval, err := time.ParseDuration(strings.TrimSpace(budget.BlockInterval))
	if err != nil {
		return err
	}
	if report.Profile != strings.TrimSpace(budget.Profile) {
		return fmt.Errorf("%s e2e profile = %q, want %q", label, report.Profile, budget.Profile)
	}
	if report.Mining != strings.TrimSpace(budget.Mining) {
		return fmt.Errorf("%s e2e mining = %q, want %q", label, report.Mining, budget.Mining)
	}
	if report.NodeCount != budget.Nodes {
		return fmt.Errorf("%s e2e nodes = %d, want %d", label, report.NodeCount, budget.Nodes)
	}
	if report.Topology != strings.TrimSpace(budget.Topology) {
		return fmt.Errorf("%s e2e topology = %q, want %q", label, report.Topology, budget.Topology)
	}
	if report.TxOriginSpread != strings.TrimSpace(budget.TxOrigin) {
		return fmt.Errorf("%s e2e tx_origin = %q, want %q", label, report.TxOriginSpread, budget.TxOrigin)
	}
	if report.BatchSize != defaultInt(budget.BatchSize, DefaultE2EOptions().BatchSize) {
		return fmt.Errorf("%s e2e batch_size = %d, want %d", label, report.BatchSize, defaultInt(budget.BatchSize, DefaultE2EOptions().BatchSize))
	}
	if report.TxsPerBlock != budget.TxsPerBlock {
		return fmt.Errorf("%s e2e txs_per_block = %d, want %d", label, report.TxsPerBlock, budget.TxsPerBlock)
	}
	if report.TargetBlocks != budget.Blocks {
		return fmt.Errorf("%s e2e blocks = %d, want %d", label, report.TargetBlocks, budget.Blocks)
	}
	if math.Abs(report.BlockIntervalMS-float64(interval)/float64(time.Millisecond)) > 0.001 {
		return fmt.Errorf("%s e2e block_interval_ms = %.3f, want %.3f", label, report.BlockIntervalMS, float64(interval)/float64(time.Millisecond))
	}
	return nil
}

func findMicroBenchmark(report *MicroBenchReport, name string) (MicroBenchResult, bool) {
	for _, bench := range report.Benchmarks {
		if bench.Name == name {
			return bench, true
		}
	}
	return MicroBenchResult{}, false
}

func checkHigherIsBetter(comparison *PerfGateComparison, label string, baseline, candidate, maxDropPct float64) {
	if maxDropPct <= 0 || baseline <= 0 {
		return
	}
	minAllowed := baseline * (1 - maxDropPct/100)
	deltaPct := percentDelta(candidate, baseline)
	message := fmt.Sprintf("%s baseline=%.2f candidate=%.2f delta=%+.2f%% min=%.2f", label, baseline, candidate, deltaPct, minAllowed)
	comparison.Checks = append(comparison.Checks, message)
	if candidate+1e-9 < minAllowed {
		comparison.Failures = append(comparison.Failures, message)
	}
}

func checkLowerIsBetter(comparison *PerfGateComparison, label string, baseline, candidate, maxIncreasePct float64) {
	if maxIncreasePct <= 0 || baseline < 0 {
		return
	}
	maxAllowed := baseline * (1 + maxIncreasePct/100)
	if baseline == 0 {
		if candidate == 0 {
			comparison.Checks = append(comparison.Checks, fmt.Sprintf("%s baseline=0.00 candidate=0.00 delta=+0.00%% max=0.00", label))
			return
		}
		message := fmt.Sprintf("%s baseline=0.00 candidate=%.2f delta=+inf max=0.00", label, candidate)
		comparison.Checks = append(comparison.Checks, message)
		comparison.Failures = append(comparison.Failures, message)
		return
	}
	deltaPct := percentDelta(candidate, baseline)
	message := fmt.Sprintf("%s baseline=%.2f candidate=%.2f delta=%+.2f%% max=%.2f", label, baseline, candidate, deltaPct, maxAllowed)
	comparison.Checks = append(comparison.Checks, message)
	if candidate-1e-9 > maxAllowed {
		comparison.Failures = append(comparison.Failures, message)
	}
}

func checkAbsoluteIncrease(comparison *PerfGateComparison, label string, baseline, candidate, maxIncrease int) {
	allowed := baseline + maxIncrease
	message := fmt.Sprintf("%s baseline=%d candidate=%d max=%d", label, baseline, candidate, allowed)
	comparison.Checks = append(comparison.Checks, message)
	if candidate > allowed {
		comparison.Failures = append(comparison.Failures, message)
	}
}

func percentDelta(candidate, baseline float64) float64 {
	if baseline == 0 {
		if candidate == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return ((candidate - baseline) / baseline) * 100
}

func defaultString(raw, fallback string) string {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	return strings.TrimSpace(raw)
}

func defaultInt(raw, fallback int) int {
	if raw == 0 {
		return fallback
	}
	return raw
}
