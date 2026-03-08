## Benchmarks

List available benchmark scenarios:

```bash
go run ./cmd/bpu-cli bench list
```

Run a real 5-node relay benchmark and emit JSON plus Markdown reports:

```bash
go run ./cmd/bpu-cli bench run --scenario p2p-relay --nodes 5 --topology mesh --txs 4096 --batch-size 64
```

Run the full benchmark suite with direct submit, RPC batch, chained package, orphan storm, block-template rebuild, 2-node relay, and 5-node relay reports:

```bash
go run ./cmd/bpu-cli bench suite --nodes 5 --txs 4096 --batch-size 64
```

Run direct in-process microbenchmarks:

```bash
go test ./benchmarks -run '^$' -bench . -benchmem
```

Run the standalone loopback RPC batch harness:

```bash
go run ./benchmarks/rpc
```

Current coverage:
- direct `Service.SubmitTx`
- hex decode plus submit
- loopback RPC `submittxbatch`
- chained ancestor-package admission
- orphan pool stress plus promotion
- live block-template rebuild cadence against package-aware selection
- real multi-node P2P relay over actual loopback peer connections
- phase-timed reports for decode, validate/admit, relay fanout, and convergence
- per-peer relay timing plus queue-depth summaries
- native tx-batch relay counters alongside legacy inventory counters
- tx reconciliation/request counters for the Erlay-style relay path
- template rebuild reports now expose incremental frontier behavior rather than full recompute-only timing
- suite summaries with per-case JSON, Markdown, and ASCII output

Reports are written under `benchmarks/reports/` by default. `bench run` writes one JSON and one Markdown file, while `bench suite` writes `suite.json`, `suite.md`, and one JSON/Markdown pair per case.

The current synthetic funding model tops out around the low-4k range of independent 1-in-1-out transactions per seeded run. If you request more than the current funding strategy can support, the CLI now fails early with a clear capacity error.

All benchmark code lives under `benchmarks/`.
