package benchmarks

import (
	"context"
	"testing"
	"time"
)

func TestRunSimulationDeterministic(t *testing.T) {
	opts := SimulationOptions{
		NodeCount:        24,
		Topology:         SimulationTopologySmallWorld,
		Seed:             7,
		TxCount:          32,
		BlockCount:       8,
		BaseLatency:      5 * time.Millisecond,
		LatencyJitter:    20 * time.Millisecond,
		TxSpacing:        2 * time.Millisecond,
		BlockSpacing:     25 * time.Millisecond,
		ChurnEvents:      4,
		ChurnDuration:    20 * time.Millisecond,
		SmallWorldDegree: 4,
	}
	left, err := RunSimulation(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunSimulation left: %v", err)
	}
	right, err := RunSimulation(context.Background(), opts)
	if err != nil {
		t.Fatalf("RunSimulation right: %v", err)
	}
	if left.Summary.Tx.P95TargetMS != right.Summary.Tx.P95TargetMS {
		t.Fatalf("tx p95 mismatch: left=%.2f right=%.2f", left.Summary.Tx.P95TargetMS, right.Summary.Tx.P95TargetMS)
	}
	if left.Summary.Block.P95TargetMS != right.Summary.Block.P95TargetMS {
		t.Fatalf("block p95 mismatch: left=%.2f right=%.2f", left.Summary.Block.P95TargetMS, right.Summary.Block.P95TargetMS)
	}
	if left.Summary.Messages != right.Summary.Messages {
		t.Fatalf("message stats mismatch: left=%+v right=%+v", left.Summary.Messages, right.Summary.Messages)
	}
}

func TestRunSimulationChurnReducesCoverage(t *testing.T) {
	base := SimulationOptions{
		NodeCount:        20,
		Topology:         SimulationTopologyLine,
		Seed:             9,
		TxCount:          16,
		BlockCount:       4,
		BaseLatency:      3 * time.Millisecond,
		LatencyJitter:    5 * time.Millisecond,
		TxSpacing:        time.Millisecond,
		BlockSpacing:     10 * time.Millisecond,
		SmallWorldDegree: 4,
	}
	noChurn, err := RunSimulation(context.Background(), base)
	if err != nil {
		t.Fatalf("RunSimulation no churn: %v", err)
	}
	withChurn, err := RunSimulation(context.Background(), SimulationOptions{
		NodeCount:        base.NodeCount,
		Topology:         base.Topology,
		Seed:             base.Seed,
		TxCount:          base.TxCount,
		BlockCount:       base.BlockCount,
		BaseLatency:      base.BaseLatency,
		LatencyJitter:    base.LatencyJitter,
		TxSpacing:        base.TxSpacing,
		BlockSpacing:     base.BlockSpacing,
		ChurnEvents:      12,
		ChurnDuration:    50 * time.Millisecond,
		SmallWorldDegree: base.SmallWorldDegree,
	})
	if err != nil {
		t.Fatalf("RunSimulation with churn: %v", err)
	}
	if withChurn.Summary.Tx.AvgCoveragePct >= noChurn.Summary.Tx.AvgCoveragePct {
		t.Fatalf("expected churn to reduce tx coverage: no-churn=%.2f churn=%.2f", noChurn.Summary.Tx.AvgCoveragePct, withChurn.Summary.Tx.AvgCoveragePct)
	}
	if withChurn.Summary.Messages.Dropped == 0 {
		t.Fatal("expected churn simulation to drop some messages")
	}
}
