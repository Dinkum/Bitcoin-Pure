package dashboard

import (
	"testing"
	"time"
)

func TestSystemStatsSummaryComputesWindowRates(t *testing.T) {
	now := time.Unix(1000, 0)
	var stats SystemStats
	stats.Record(SystemSample{
		takenAt:       now.Add(-10 * time.Second),
		cpuBusyTicks:  100,
		cpuTotalTicks: 200,
		rxBytes:       1_000,
		txBytes:       2_000,
		memUsedBytes:  400,
		memTotalBytes: 1_000,
		load1:         1,
		load5:         2,
		load15:        3,
		runningProcs:  1,
		totalProcs:    10,
		cores:         4,
	})
	stats.Record(SystemSample{
		takenAt:       now,
		cpuBusyTicks:  150,
		cpuTotalTicks: 300,
		rxBytes:       1_500,
		txBytes:       3_000,
		memUsedBytes:  600,
		memTotalBytes: 1_000,
		load1:         3,
		load5:         4,
		load15:        5,
		runningProcs:  2,
		totalProcs:    11,
		cores:         4,
	})

	summary := stats.Summary(now, time.Minute)
	if !summary.HasCPU || summary.CPUPercent != 50 {
		t.Fatalf("CPU = %.1f has=%t, want 50 true", summary.CPUPercent, summary.HasCPU)
	}
	if !summary.HasNetwork || summary.RxBytesPerSec != 50 || summary.TxBytesPerSec != 100 {
		t.Fatalf("network = %.1f/%.1f has=%t, want 50/100 true", summary.RxBytesPerSec, summary.TxBytesPerSec, summary.HasNetwork)
	}
	if !summary.HasMemory || summary.AvgMemUsedBytes != 500 || summary.MemTotalBytes != 1_000 {
		t.Fatalf("memory = %d/%d has=%t, want 500/1000 true", summary.AvgMemUsedBytes, summary.MemTotalBytes, summary.HasMemory)
	}
	if !summary.HasLoad || summary.Load1 != 2 || summary.Load5 != 3 || summary.Load15 != 4 {
		t.Fatalf("load = %.1f/%.1f/%.1f has=%t, want 2/3/4 true", summary.Load1, summary.Load5, summary.Load15, summary.HasLoad)
	}
	if summary.RunningProcs != 2 || summary.TotalProcs != 11 || summary.Cores != 4 {
		t.Fatalf("processes/cores = %d/%d/%d, want 2/11/4", summary.RunningProcs, summary.TotalProcs, summary.Cores)
	}
}

func TestSystemStatsSummaryKeepsPreviousSampleForWindowDeltas(t *testing.T) {
	now := time.Unix(1000, 0)
	var stats SystemStats
	stats.Record(SystemSample{takenAt: now.Add(-20 * time.Second), cpuBusyTicks: 0, cpuTotalTicks: 0, rxBytes: 0, txBytes: 0, memTotalBytes: 1, totalProcs: 1, cores: 1})
	stats.Record(SystemSample{takenAt: now.Add(-5 * time.Second), cpuBusyTicks: 50, cpuTotalTicks: 100, rxBytes: 500, txBytes: 500, memTotalBytes: 1, totalProcs: 1, cores: 1})

	summary := stats.Summary(now, 5*time.Second)
	if summary.Window != 15*time.Second {
		t.Fatalf("window = %s, want 15s", summary.Window)
	}
	if !summary.HasCPU || summary.CPUPercent != 50 {
		t.Fatalf("CPU = %.1f has=%t, want 50 true", summary.CPUPercent, summary.HasCPU)
	}
}

func TestSystemStatsSummaryRequiresMinimumWindowForCPUAndNetwork(t *testing.T) {
	now := time.Unix(1000, 0)
	var stats SystemStats
	stats.Record(SystemSample{
		takenAt:       now.Add(-9 * time.Second),
		cpuBusyTicks:  100,
		cpuTotalTicks: 200,
		rxBytes:       1_000,
		txBytes:       2_000,
		memUsedBytes:  2 * 1024 * 1024,
		memTotalBytes: 8 * 1024 * 1024,
		load1:         0.5,
		load5:         0.4,
		load15:        0.3,
		runningProcs:  2,
		totalProcs:    100,
		cores:         2,
	})
	stats.Record(SystemSample{
		takenAt:       now,
		cpuBusyTicks:  150,
		cpuTotalTicks: 260,
		rxBytes:       5_000,
		txBytes:       8_000,
		memUsedBytes:  3 * 1024 * 1024,
		memTotalBytes: 8 * 1024 * 1024,
		load1:         0.6,
		load5:         0.5,
		load15:        0.4,
		runningProcs:  3,
		totalProcs:    110,
		cores:         2,
	})

	summary := stats.Summary(now, time.Minute)
	if summary.HasCPU {
		t.Fatalf("expected cpu summary to stay warming up for windows under %s", systemMinimumWindow)
	}
	if summary.HasNetwork {
		t.Fatalf("expected network summary to stay warming up for windows under %s", systemMinimumWindow)
	}
}
