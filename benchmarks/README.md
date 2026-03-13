# Benchmarks

The benchmark workflow is intentionally split in two:

1. `go test -bench`
   - fast in-process throughput benches for the coding loop
   - no cluster bootstrap
   - no network
   - no mining noise
   - no report plumbing unless you explicitly want archived output
2. `bench micro`
   - thin wrapper around `go test -bench`
   - same in-process hot paths
   - writes timestamped JSON + Markdown snapshots under `benchmarks/reports/micro/`
3. `bench e2e`
   - one thin cluster benchmark
   - fixed defaults
   - fixed report shape
   - answers "does the whole node still hold together?" and "roughly what end-to-end TPS do we get?"

## Daily Loop

Run all core throughput benches:

```bash
go test ./benchmarks -run '^$' -bench . -benchmem
```

Archive the same microbench loop into JSON + Markdown:

```bash
go run ./cmd/bpu-cli bench micro
```

Run one specific hot path:

```bash
go test ./benchmarks -run '^$' -bench '^BenchmarkTxAdmission$' -benchmem
go test ./benchmarks -run '^$' -bench '^BenchmarkTxValidation$' -benchmem
go test ./benchmarks -run '^$' -bench '^BenchmarkSignatureVerification$' -benchmem
go test ./benchmarks -run '^$' -bench '^BenchmarkMempoolSelection$' -benchmem
go test ./benchmarks -run '^$' -bench '^BenchmarkBlockBuild$' -benchmem
go test ./benchmarks -run '^$' -bench '^BenchmarkBlockApply$' -benchmem
```

Archive one specific hot path:

```bash
go run ./cmd/bpu-cli bench micro --bench '^BenchmarkTxAdmission$'
```

Core benches:

- `BenchmarkTxAdmission`
  - `Service.SubmitTx` hot path
- `BenchmarkTxAdmissionHexDecode`
  - decode + admit path for hex transport overhead
- `BenchmarkTxValidation`
  - consensus tx validation against a stable UTXO view
- `BenchmarkSignatureVerification`
  - isolated Schnorr batch verification cost
- `BenchmarkMempoolSelection`
  - mempool package selection for a block candidate
- `BenchmarkBlockBuild`
  - block-template build on a live mempool
- `BenchmarkBlockApply`
  - block acceptance/apply on a fresh follower

All of these benches run on one process with a fresh benchmark-only chain and synthetic mining enabled for setup only. The timed section stays focused on the hot path being measured.

`bench micro` writes reports under `benchmarks/reports/micro/YYYYMMDD/HHMMSS/` by default:

- `report.json`
- `report.md`

It shells out to the same `go test ./benchmarks -run '^$' -bench ... -benchmem` command you would run manually, so it stays a convenience wrapper rather than a second benchmark system.

## End-to-End

Canonical run:

```bash
go run ./cmd/bpu-cli bench e2e
```

Default `bench e2e` shape:

- `--mining synthetic`
- `--nodes 5`
- `--topology mesh`
- `--tx-origin even`
- `--txs-per-block 1024`
- `--block-interval 30s`
- `--blocks 5`

Real-mining variant:

```bash
go run ./cmd/bpu-cli bench e2e --mining real
```

Explicit synthetic variant:

```bash
go run ./cmd/bpu-cli bench e2e \
  --mining synthetic \
  --nodes 5 \
  --topology mesh \
  --tx-origin even \
  --txs-per-block 1024 \
  --block-interval 30s \
  --blocks 5
```

Shared controls:

- `--timeout`
- `--profile-dir`
- `--suppress-logs`
- `--report`
- `--markdown`
- `--db-root`

`bench e2e` controls:

- `--mining synthetic|real`
- `--nodes`
- `--topology line|mesh`
- `--tx-origin one-node|even`
- `--txs-per-block`
- `--block-interval`
- `--blocks`

## Reports

`bench e2e` writes reports under `benchmarks/reports/` by default:

- one JSON report
- one Markdown report
- one ASCII summary on stdout
- live progress heartbeats on stderr during long waits

High-signal report fields:

- `admission_tps`
- `completion_tps`
- `confirmed_processing_tps`
- `confirmed_wall_tps`
- `synthetic_interval_tps`
- `missed_intervals`
- `schedule_lag_avg_ms`
- `schedule_lag_p95_ms`
- `schedule_lag_max_ms`
- `final_mempool_txs`
- `final_block_lag`
- `final_header_lag`

Per-node report sections also surface relay mechanism counters so you can see
whether a run actually used Erlay reconciliation or Graphene-style extended
block relay instead of inferring it from convergence alone:

- `erlay_rounds`
- `erlay_requested_txs`
- `compact_block_plans`
- `graphene_extended_plans`
- `graphene_decode_failures`
- `graphene_extended_recoveries`

## Chain Semantics

Benchmarks run on `benchnet`, a benchmark-only chain profile.

Why:

- every benchmark run starts from genesis anyway
- the harness can safely retune starting difficulty for real-mining runs
- benchmark chain identity stays separate from operator-facing regtest workflows

`regtest` still matters for normal local node work because a persistent regtest datadir lets you keep funded UTXOs and existing chain state between manual runs. That persistence is convenient for interactive debugging, but it is the wrong abstraction for a repeatable benchmark harness.

Synthetic `bench e2e` behavior:

- block cadence is a fixed external clock
- PoW search is skipped
- if the cluster falls behind, transactions accumulate in the mempool
- the benchmark reports missed cadence and lag instead of silently stretching block time

Real `bench e2e` behavior:

- still starts from fresh `benchnet` genesis
- uses the requested `--block-interval` to pick a tuned starting difficulty preset for that run
- actual block timing still varies with nonce-search luck

Treat synthetic mode as the stable regression number. Treat real mining as a realism check, not the primary optimization loop.
