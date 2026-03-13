package benchmarks

import (
	"strings"
	"testing"
	"time"
)

func TestParseMicroBenchOutput(t *testing.T) {
	raw := strings.Join([]string{
		"goos: darwin",
		"goarch: arm64",
		"pkg: bitcoin-pure/benchmarks",
		"cpu: Apple M2 Pro",
		"BenchmarkTxAdmission-10            5493            225694 ns/op            7727 B/op              83 allocs/op",
		"BenchmarkBlockBuild-10              10         227259208 ns/op        43418718 B/op          636871 allocs/op",
		"PASS",
		"",
	}, "\n")

	report, err := parseMicroBenchOutput(raw)
	if err != nil {
		t.Fatalf("parseMicroBenchOutput: %v", err)
	}
	if report.GoPackage != "bitcoin-pure/benchmarks" {
		t.Fatalf("package = %q", report.GoPackage)
	}
	if report.CPU != "Apple M2 Pro" {
		t.Fatalf("cpu = %q", report.CPU)
	}
	if len(report.Benchmarks) != 2 {
		t.Fatalf("benchmarks = %d, want 2", len(report.Benchmarks))
	}
	if report.Benchmarks[0].Name != "BenchmarkTxAdmission" {
		t.Fatalf("name = %q", report.Benchmarks[0].Name)
	}
	if report.Benchmarks[0].Procs != 10 {
		t.Fatalf("procs = %d", report.Benchmarks[0].Procs)
	}
	if report.Benchmarks[0].NsPerOp != 225694 {
		t.Fatalf("ns/op = %f", report.Benchmarks[0].NsPerOp)
	}
}

func TestRenderMicroMarkdown(t *testing.T) {
	report := &MicroBenchReport{
		StartedAt:   time.Unix(100, 0).UTC(),
		CompletedAt: time.Unix(101, 0).UTC(),
		DurationMS:  1000,
		Package:     "./benchmarks",
		Bench:       "^BenchmarkTxAdmission$",
		Command:     []string{"go", "test", "./benchmarks", "-run", "^$", "-bench", "^BenchmarkTxAdmission$", "-benchmem"},
		Environment: Environment{GoVersion: "go1.26.0", GoOS: "darwin", GoArch: "arm64", NumCPU: 10},
		GoPackage:   "bitcoin-pure/benchmarks",
		Benchmarks: []MicroBenchResult{{
			Name:        "BenchmarkTxAdmission",
			Procs:       10,
			Iterations:  5493,
			NsPerOp:     225694,
			BytesPerOp:  7727,
			AllocsPerOp: 83,
		}},
	}

	out := RenderMicroMarkdown(report)
	for _, want := range []string{
		"MICRO BENCHMARKS",
		"## Results",
		"`BenchmarkTxAdmission (P=10)`",
		"go test ./benchmarks -run ^$ -bench ^BenchmarkTxAdmission$ -benchmem",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("markdown missing %q", want)
		}
	}
}

func TestDefaultMicroReportPaths(t *testing.T) {
	jsonPath, markdownPath := DefaultMicroReportPaths(time.Date(2026, time.March, 12, 21, 46, 31, 0, time.UTC))
	if want := "benchmarks/reports/micro/20260312/214631/report.json"; jsonPath != want {
		t.Fatalf("json path = %q, want %q", jsonPath, want)
	}
	if want := "benchmarks/reports/micro/20260312/214631/report.md"; markdownPath != want {
		t.Fatalf("markdown path = %q, want %q", markdownPath, want)
	}
}
