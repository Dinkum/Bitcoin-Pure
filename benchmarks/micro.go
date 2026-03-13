package benchmarks

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type MicroBenchOptions struct {
	Package   string
	Bench     string
	Count     int
	Benchtime string
}

type MicroBenchResult struct {
	Name        string             `json:"name"`
	Procs       int                `json:"procs,omitempty"`
	Iterations  int64              `json:"iterations"`
	NsPerOp     float64            `json:"ns_per_op,omitempty"`
	BytesPerOp  float64            `json:"bytes_per_op,omitempty"`
	AllocsPerOp float64            `json:"allocs_per_op,omitempty"`
	Metrics     map[string]float64 `json:"metrics,omitempty"`
}

type MicroBenchReport struct {
	StartedAt   time.Time          `json:"started_at"`
	CompletedAt time.Time          `json:"completed_at"`
	DurationMS  float64            `json:"duration_ms"`
	Package     string             `json:"package"`
	Bench       string             `json:"bench"`
	Count       int                `json:"count,omitempty"`
	Benchtime   string             `json:"benchtime,omitempty"`
	Command     []string           `json:"command"`
	Environment Environment        `json:"environment"`
	GoPackage   string             `json:"go_package,omitempty"`
	CPU         string             `json:"cpu,omitempty"`
	Benchmarks  []MicroBenchResult `json:"benchmarks"`
	RawOutput   string             `json:"raw_output"`
	Notes       []string           `json:"notes,omitempty"`
}

var microBenchLineRE = regexp.MustCompile(`^(Benchmark\S+)\s+(\d+)\s+(.+)$`)

func DefaultMicroBenchOptions() MicroBenchOptions {
	return MicroBenchOptions{
		Package: "./benchmarks",
		Bench:   ".",
		Count:   1,
	}
}

func RunMicroBenchmarks(ctx context.Context, opts MicroBenchOptions) (*MicroBenchReport, error) {
	opts = withMicroDefaults(opts)
	started := time.Now().UTC()
	args := []string{"test", opts.Package, "-run", "^$", "-bench", opts.Bench, "-benchmem"}
	if opts.Count > 0 {
		args = append(args, "-count", strconv.Itoa(opts.Count))
	}
	if strings.TrimSpace(opts.Benchtime) != "" {
		args = append(args, "-benchtime", strings.TrimSpace(opts.Benchtime))
	}

	// Shell through the normal `go test -bench` surface so the archived report
	// always reflects the exact developer command path rather than a reimplemented
	// in-process runner with subtly different semantics.
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = benchmarkRepoRoot()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		out := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
		if out == "" {
			return nil, err
		}
		return nil, fmt.Errorf("%w\n%s", err, out)
	}

	completed := time.Now().UTC()
	report, err := parseMicroBenchOutput(stdout.String())
	if err != nil {
		return nil, err
	}
	report.StartedAt = started
	report.CompletedAt = completed
	report.DurationMS = float64(completed.Sub(started)) / float64(time.Millisecond)
	report.Package = opts.Package
	report.Bench = opts.Bench
	report.Count = opts.Count
	report.Benchtime = strings.TrimSpace(opts.Benchtime)
	report.Command = append([]string{"go"}, args...)
	report.Environment = Environment{
		GoOS:      runtime.GOOS,
		GoArch:    runtime.GOARCH,
		GoVersion: runtime.Version(),
		NumCPU:    runtime.NumCPU(),
	}
	return report, nil
}

func WriteMicroReportFiles(report *MicroBenchReport, jsonPath, markdownPath string) error {
	if err := writeJSON(jsonPath, report); err != nil {
		return err
	}
	return writeText(markdownPath, RenderMicroMarkdown(report))
}

func DefaultMicroReportPaths(now time.Time) (string, string) {
	stamp := now.UTC()
	root := filepath.Join("benchmarks", "reports", "micro", stamp.Format("20060102"), stamp.Format("150405"))
	return filepath.Join(root, "report.json"), filepath.Join(root, "report.md")
}

func RenderMicroMarkdown(report *MicroBenchReport) string {
	var b strings.Builder
	b.WriteString("# Microbenchmark Report\n\n")
	b.WriteString("```text\n")
	b.WriteString("=================\n")
	b.WriteString("MICRO BENCHMARKS\n")
	b.WriteString("=================\n")
	b.WriteString("```\n\n")
	b.WriteString("## Run\n\n")
	b.WriteString(fmt.Sprintf("- Started: `%s`\n", report.StartedAt.Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("- Completed: `%s`\n", report.CompletedAt.Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("- Duration: `%.2f ms`\n", report.DurationMS))
	b.WriteString(fmt.Sprintf("- Package: `%s`\n", report.Package))
	b.WriteString(fmt.Sprintf("- Bench regex: `%s`\n", report.Bench))
	if report.Count > 0 {
		b.WriteString(fmt.Sprintf("- Count: `%d`\n", report.Count))
	}
	if report.Benchtime != "" {
		b.WriteString(fmt.Sprintf("- Benchtime: `%s`\n", report.Benchtime))
	}
	if len(report.Command) != 0 {
		b.WriteString("\nCommand:\n\n```bash\n")
		b.WriteString(strings.Join(report.Command, " "))
		b.WriteString("\n```\n")
	}

	b.WriteString("\n## Environment\n\n")
	b.WriteString(fmt.Sprintf("- Go: `%s`\n", report.Environment.GoVersion))
	b.WriteString(fmt.Sprintf("- OS/Arch: `%s/%s`\n", report.Environment.GoOS, report.Environment.GoArch))
	b.WriteString(fmt.Sprintf("- CPUs: `%d`\n", report.Environment.NumCPU))
	if report.GoPackage != "" {
		b.WriteString(fmt.Sprintf("- Package under test: `%s`\n", report.GoPackage))
	}
	if report.CPU != "" {
		b.WriteString(fmt.Sprintf("- CPU: `%s`\n", report.CPU))
	}

	b.WriteString("\n## Results\n\n")
	b.WriteString("| Benchmark | Iterations | ns/op | B/op | allocs/op |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: |\n")
	for _, bench := range report.Benchmarks {
		name := bench.Name
		if bench.Procs > 0 {
			name = fmt.Sprintf("%s (P=%d)", name, bench.Procs)
		}
		b.WriteString(fmt.Sprintf("| `%s` | %d | %.0f | %.0f | %.0f |\n",
			name,
			bench.Iterations,
			bench.NsPerOp,
			bench.BytesPerOp,
			bench.AllocsPerOp,
		))
	}

	if len(report.Notes) != 0 {
		b.WriteString("\n## Notes\n\n")
		for _, note := range report.Notes {
			b.WriteString("- ")
			b.WriteString(note)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func parseMicroBenchOutput(out string) (*MicroBenchReport, error) {
	report := &MicroBenchReport{RawOutput: out}
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case strings.HasPrefix(line, "pkg: "):
			report.GoPackage = strings.TrimSpace(strings.TrimPrefix(line, "pkg: "))
			continue
		case strings.HasPrefix(line, "cpu: "):
			report.CPU = strings.TrimSpace(strings.TrimPrefix(line, "cpu: "))
			continue
		}
		match := microBenchLineRE.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		bench, err := parseMicroBenchLine(match[1], match[2], match[3])
		if err != nil {
			return nil, fmt.Errorf("parse benchmark line %q: %w", line, err)
		}
		report.Benchmarks = append(report.Benchmarks, bench)
	}
	if len(report.Benchmarks) == 0 {
		return nil, fmt.Errorf("no benchmark rows found in go test output")
	}
	return report, nil
}

func parseMicroBenchLine(nameField, iterField, metricsField string) (MicroBenchResult, error) {
	iterations, err := strconv.ParseInt(iterField, 10, 64)
	if err != nil {
		return MicroBenchResult{}, err
	}
	name, procs := splitBenchProcSuffix(nameField)
	fields := strings.Fields(metricsField)
	if len(fields)%2 != 0 {
		return MicroBenchResult{}, fmt.Errorf("unexpected metric field count")
	}
	result := MicroBenchResult{
		Name:       name,
		Procs:      procs,
		Iterations: iterations,
		Metrics:    make(map[string]float64),
	}
	// Go benchmark rows are emitted as alternating numeric values and units, e.g.
	// `225694 ns/op 7727 B/op 83 allocs/op`, so parsing by pairs keeps the report
	// generic enough to preserve any future extra metrics like MB/s.
	for i := 0; i < len(fields); i += 2 {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return MicroBenchResult{}, err
		}
		unit := fields[i+1]
		result.Metrics[unit] = value
		switch unit {
		case "ns/op":
			result.NsPerOp = value
		case "B/op":
			result.BytesPerOp = value
		case "allocs/op":
			result.AllocsPerOp = value
		}
	}
	return result, nil
}

func splitBenchProcSuffix(raw string) (string, int) {
	idx := strings.LastIndexByte(raw, '-')
	if idx <= 0 || idx == len(raw)-1 {
		return raw, 0
	}
	procs, err := strconv.Atoi(raw[idx+1:])
	if err != nil {
		return raw, 0
	}
	return raw[:idx], procs
}

func withMicroDefaults(opts MicroBenchOptions) MicroBenchOptions {
	defaults := DefaultMicroBenchOptions()
	if strings.TrimSpace(opts.Package) == "" {
		opts.Package = defaults.Package
	}
	if strings.TrimSpace(opts.Bench) == "" {
		opts.Bench = defaults.Bench
	}
	if opts.Count <= 0 {
		opts.Count = defaults.Count
	}
	return opts
}

func benchmarkRepoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}
