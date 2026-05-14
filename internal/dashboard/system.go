package dashboard

import (
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	systemMinimumWindow = 10 * time.Second
	systemRetention     = 12 * time.Minute
)

type SystemStats struct {
	mu      sync.Mutex
	samples []SystemSample
}

type SystemSample struct {
	takenAt       time.Time
	cpuBusyTicks  uint64
	cpuTotalTicks uint64
	rxBytes       uint64
	txBytes       uint64
	memUsedBytes  uint64
	memTotalBytes uint64
	load1         float64
	load5         float64
	load15        float64
	runningProcs  int
	totalProcs    int
	cores         int
}

type SystemSummary struct {
	Window          time.Duration
	CPUPercent      float64
	HasCPU          bool
	RxBytesPerSec   float64
	TxBytesPerSec   float64
	HasNetwork      bool
	AvgMemUsedBytes uint64
	MemTotalBytes   uint64
	HasMemory       bool
	Load1           float64
	Load5           float64
	Load15          float64
	HasLoad         bool
	RunningProcs    int
	TotalProcs      int
	Cores           int
}

func (stats *SystemStats) Record(sample SystemSample) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.samples = append(stats.samples, sample)
	cutoff := sample.takenAt.Add(-systemRetention)
	keep := 0
	for keep < len(stats.samples) && stats.samples[keep].takenAt.Before(cutoff) {
		keep++
	}
	if keep > 0 {
		stats.samples = append([]SystemSample(nil), stats.samples[keep:]...)
	}
}

func (stats *SystemStats) Summary(now time.Time, window time.Duration) SystemSummary {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	if len(stats.samples) == 0 {
		return SystemSummary{}
	}
	start := 0
	cutoff := now.Add(-window)
	for i := range stats.samples {
		if !stats.samples[i].takenAt.Before(cutoff) {
			if i > 0 {
				start = i - 1
			} else {
				start = i
			}
			break
		}
		start = i
	}
	windowSamples := stats.samples[start:]
	if len(windowSamples) == 0 {
		return SystemSummary{}
	}
	first := windowSamples[0]
	last := windowSamples[len(windowSamples)-1]
	summary := SystemSummary{
		Window:        last.takenAt.Sub(first.takenAt),
		MemTotalBytes: last.memTotalBytes,
		RunningProcs:  last.runningProcs,
		TotalProcs:    last.totalProcs,
		Cores:         last.cores,
	}
	var memUsedTotal uint64
	var load1Total float64
	var load5Total float64
	var load15Total float64
	for _, sample := range windowSamples {
		memUsedTotal += sample.memUsedBytes
		load1Total += sample.load1
		load5Total += sample.load5
		load15Total += sample.load15
	}
	summary.AvgMemUsedBytes = memUsedTotal / uint64(len(windowSamples))
	summary.HasMemory = summary.MemTotalBytes > 0
	summary.Load1 = load1Total / float64(len(windowSamples))
	summary.Load5 = load5Total / float64(len(windowSamples))
	summary.Load15 = load15Total / float64(len(windowSamples))
	summary.HasLoad = true
	if len(windowSamples) >= 2 && summary.Window >= systemMinimumWindow {
		totalDelta := last.cpuTotalTicks - first.cpuTotalTicks
		busyDelta := last.cpuBusyTicks - first.cpuBusyTicks
		if totalDelta > 0 {
			summary.CPUPercent = (float64(busyDelta) / float64(totalDelta)) * 100
			summary.HasCPU = true
		}
		elapsed := last.takenAt.Sub(first.takenAt).Seconds()
		if elapsed > 0 {
			summary.RxBytesPerSec = float64(last.rxBytes-first.rxBytes) / elapsed
			summary.TxBytesPerSec = float64(last.txBytes-first.txBytes) / elapsed
			summary.HasNetwork = true
		}
	}
	return summary
}

func ReadSystemSample(now time.Time) (SystemSample, error) {
	if runtime.GOOS != "linux" {
		return SystemSample{}, errors.New("dashboard system stats are only supported on linux")
	}
	busyTicks, totalTicks, err := readProcCPUTicks()
	if err != nil {
		return SystemSample{}, err
	}
	rxBytes, txBytes, err := readProcNetworkBytes()
	if err != nil {
		return SystemSample{}, err
	}
	memUsedBytes, memTotalBytes, err := readProcMemoryUsage()
	if err != nil {
		return SystemSample{}, err
	}
	load1, load5, load15, runningProcs, totalProcs, err := readProcLoadAvg()
	if err != nil {
		return SystemSample{}, err
	}
	return SystemSample{
		takenAt:       now,
		cpuBusyTicks:  busyTicks,
		cpuTotalTicks: totalTicks,
		rxBytes:       rxBytes,
		txBytes:       txBytes,
		memUsedBytes:  memUsedBytes,
		memTotalBytes: memTotalBytes,
		load1:         load1,
		load5:         load5,
		load15:        load15,
		runningProcs:  runningProcs,
		totalProcs:    totalProcs,
		cores:         runtime.NumCPU(),
	}, nil
}

func readProcCPUTicks() (uint64, uint64, error) {
	buf, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(buf), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 8 || fields[0] != "cpu" {
			continue
		}
		values := make([]uint64, 0, len(fields)-1)
		for _, raw := range fields[1:] {
			value, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return 0, 0, err
			}
			values = append(values, value)
		}
		if len(values) < 8 {
			return 0, 0, errors.New("proc stat cpu line missing counters")
		}
		busy := values[0] + values[1] + values[2] + values[5] + values[6] + values[7]
		var total uint64
		for _, value := range values {
			total += value
		}
		return busy, total, nil
	}
	return 0, 0, errors.New("proc stat cpu line not found")
}

func readProcNetworkBytes() (uint64, uint64, error) {
	buf, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0, err
	}
	var rxTotal uint64
	var txTotal uint64
	for _, line := range strings.Split(string(buf), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "" || iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 16 {
			continue
		}
		rx, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		tx, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		rxTotal += rx
		txTotal += tx
	}
	return rxTotal, txTotal, nil
}

func readProcMemoryUsage() (uint64, uint64, error) {
	buf, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	values := make(map[string]uint64)
	for _, line := range strings.Split(string(buf), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		values[key] = value * 1024
	}
	total := values["MemTotal"]
	if total == 0 {
		return 0, 0, errors.New("meminfo missing MemTotal")
	}
	available := values["MemAvailable"]
	if available > total {
		available = total
	}
	return total - available, total, nil
}

func readProcLoadAvg() (float64, float64, float64, int, int, error) {
	buf, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	fields := strings.Fields(string(buf))
	if len(fields) < 4 {
		return 0, 0, 0, 0, 0, errors.New("loadavg missing fields")
	}
	load1, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	load5, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	load15, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	processParts := strings.SplitN(fields[3], "/", 2)
	if len(processParts) != 2 {
		return 0, 0, 0, 0, 0, errors.New("loadavg missing process counts")
	}
	running, err := strconv.Atoi(processParts[0])
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	total, err := strconv.Atoi(processParts[1])
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	return load1, load5, load15, running, total, nil
}
