package benchmarks

import "testing"

func TestEvaluatePerfGatePassesWithinBudget(t *testing.T) {
	budget := PerfGateBudget{
		Micro: PerfGateMicroBudget{
			Bench: "^$",
			Benchmarks: []PerfGateMicroCase{
				{
					Name:                        "BenchmarkTxAdmission",
					MaxNsPerOpRegressionPct:     10,
					MaxBytesPerOpRegressionPct:  20,
					MaxAllocsPerOpRegressionPct: 20,
				},
			},
		},
		E2E: PerfGateE2EBudget{
			Profile:                        "benchnet",
			Mining:                         "synthetic",
			Nodes:                          5,
			Topology:                       "mesh",
			TxOrigin:                       "even",
			BatchSize:                      64,
			TxsPerBlock:                    1024,
			Blocks:                         5,
			BlockInterval:                  "1s",
			MaxAdmissionTPSDropPct:         10,
			MaxCompletionTPSDropPct:        10,
			MaxConfirmedWallTPSDropPct:     10,
			MaxScheduleLagP95MSIncreasePct: 20,
			MaxFinalMempoolTxsIncreasePct:  10,
			MaxMissedIntervalsIncrease:     0,
			MaxFinalBlockLagIncrease:       0,
		},
	}

	baselineMicro := &MicroBenchReport{
		Package: "./benchmarks",
		Bench:   "^$",
		Count:   1,
		Benchmarks: []MicroBenchResult{{
			Name:        "BenchmarkTxAdmission",
			NsPerOp:     100,
			BytesPerOp:  50,
			AllocsPerOp: 10,
		}},
	}
	candidateMicro := &MicroBenchReport{
		Package: "./benchmarks",
		Bench:   "^$",
		Count:   1,
		Benchmarks: []MicroBenchResult{{
			Name:        "BenchmarkTxAdmission",
			NsPerOp:     108,
			BytesPerOp:  55,
			AllocsPerOp: 11,
		}},
	}
	baselineE2E := testPerfGateReport()
	candidateE2E := testPerfGateReport()
	candidateE2E.AdmissionTPS = 970
	candidateE2E.CompletionTPS = 930
	candidateE2E.ConfirmedWallTPS = 930
	candidateE2E.Metrics.ScheduleLagP95MS = 5.8
	candidateE2E.Metrics.FinalMempoolTxs = 1050

	comparison, err := EvaluatePerfGate(budget, baselineMicro, candidateMicro, baselineE2E, candidateE2E)
	if err != nil {
		t.Fatalf("EvaluatePerfGate() error = %v", err)
	}
	if !comparison.Passed() {
		t.Fatalf("comparison failed unexpectedly: %v", comparison.Failures)
	}
}

func TestEvaluatePerfGateFailsOnRegression(t *testing.T) {
	budget := PerfGateBudget{
		Micro: PerfGateMicroBudget{
			Bench: "^$",
			Benchmarks: []PerfGateMicroCase{{
				Name:                    "BenchmarkTxAdmission",
				MaxNsPerOpRegressionPct: 5,
			}},
		},
		E2E: PerfGateE2EBudget{
			Profile:                    "benchnet",
			Mining:                     "synthetic",
			Nodes:                      5,
			Topology:                   "mesh",
			TxOrigin:                   "even",
			BatchSize:                  64,
			TxsPerBlock:                1024,
			Blocks:                     5,
			BlockInterval:              "1s",
			MaxCompletionTPSDropPct:    5,
			MaxMissedIntervalsIncrease: 0,
		},
	}

	baselineMicro := &MicroBenchReport{
		Package: "./benchmarks",
		Bench:   "^$",
		Count:   1,
		Benchmarks: []MicroBenchResult{{
			Name:    "BenchmarkTxAdmission",
			NsPerOp: 100,
		}},
	}
	candidateMicro := &MicroBenchReport{
		Package: "./benchmarks",
		Bench:   "^$",
		Count:   1,
		Benchmarks: []MicroBenchResult{{
			Name:    "BenchmarkTxAdmission",
			NsPerOp: 111,
		}},
	}
	baselineE2E := testPerfGateReport()
	candidateE2E := testPerfGateReport()
	candidateE2E.CompletionTPS = 900
	candidateE2E.Metrics.MissedIntervals = 1

	comparison, err := EvaluatePerfGate(budget, baselineMicro, candidateMicro, baselineE2E, candidateE2E)
	if err != nil {
		t.Fatalf("EvaluatePerfGate() error = %v", err)
	}
	if comparison.Passed() {
		t.Fatal("comparison unexpectedly passed")
	}
	if len(comparison.Failures) != 3 {
		t.Fatalf("failure count = %d, want 3", len(comparison.Failures))
	}
}

func testPerfGateReport() *Report {
	return &Report{
		Mining:           "synthetic",
		Profile:          "benchnet",
		NodeCount:        5,
		Topology:         "mesh",
		TxOriginSpread:   "even",
		BatchSize:        64,
		TxsPerBlock:      1024,
		TargetBlocks:     5,
		BlockIntervalMS:  1000,
		AdmissionTPS:     1000,
		CompletionTPS:    980,
		ConfirmedWallTPS: 980,
		Metrics: ScenarioMetrics{
			ScheduleLagP95MS: 5,
			FinalMempoolTxs:  1024,
			MissedIntervals:  0,
			FinalBlockLag:    1,
		},
	}
}
