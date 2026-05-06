# Release Notes

## v0.1.18
Major
- Implemented typed transaction outputs and typed UTXO payloads across the node. Transactions, UTXO commitments, sighash construction, storage, wallet code, compact filters, and fixtures now carry explicit output families instead of assuming every coin is an x-only public-key output.
- Added ML-DSA-65 post-quantum authorization support. The node can generate, encode, sign, verify, persist, and relay PQ-lock outputs and PQ authorization payloads using 32-byte lock commitments and explicit verification-key/signature payloads.
- Bound transaction authorization to chain lineage. Consensus sighash domains now include the active chain profile and lineage, preventing signatures from being valid across unrelated BPU networks that share the same transaction shape.
- Added performance gating for CI and release discipline. `bench gate run` and `bench gate compare` now execute the tracked microbenchmarks plus a fixed synthetic multi-node e2e benchmark, with budget enforcement wired through `benchmarks/perf-gate.json` and `scripts/ci-bench-gate.sh`.
- Added steady-state confirmed-block benchmarking for sustained throughput work. The e2e harness can preload a converged backlog, mine fixed-cadence blocks against it, and report confirmed processing TPS, wall TPS, missed intervals, block convergence, and relay mechanism counters.

Minor
- Extended compact filters from x-only pubkey matches to typed watch items, so lite-wallet scans can match both created outputs and spent coins for each supported output family.
- Added watch-item RPC surfaces for wallet and UTXO lookup flows, including typed `getutxosbywatchitems` and `getwalletactivitybywatchitems` queries.
- Added a relay-side transaction reject cache keyed by full transaction identity `(txid, authid)`, reducing repeated validation work for replayed invalid, low-fee, full-mempool, and failed orphan-promotion transactions.
- Added a bounded validated-transaction cache for exact mempool-admitted transactions, allowing block validation to skip already-proven auth and output-payload checks while still enforcing contextual consensus state.
- Improved high-throughput relay behavior with larger benchmark message headroom, requested transaction batches on the relay-priority lane, periodic Avalanche conflict pruning, and richer compact-block recovery counters.
- Reduced allocation pressure in hot paths by adding encoded-length helpers, stack-backed tagged hashing outputs, lower-copy Utreexo hashing, and more direct block/UTXO apply paths.
- Expanded benchmark and runtime telemetry with template-phase timings, relay queue pressure, compact block received/recovered/fallback counters, steady-state backlog reporting, and inner CPU profiling for the block-cycle benchmark.
- Regenerated genesis, bootstrap, and snapshot fixtures for the typed-output and chain-bound authorization model.
- Refreshed public documentation and protocol sub-specs to describe typed outputs, PQ locks, version policy, and typed UTXO commitments.

## v0.1.17
Major
- Added mixed-family transaction support to the published protocol spec. `SPEC.md` now defines typed outputs and typed locking payloads, with an x-only secp256k1 spend family alongside a PQ lock spend family using ML-DSA-65 authorization.
- Split transaction authorization into explicit self-framing auth entries. The spec now defines `Auth` as length-delimited per-input auth payloads, keeps `txid = hash(Base)` and `authid = hash(Auth)`, and preserves the ordered `tx_root` / `auth_root` commitment model under the new spend-family structure.
- Upgraded the canonical transaction and UTXO sub-specs to match the new consensus coin shape. `specs/TX.md` now defines typed outputs plus family-specific auth payload grammars, and `specs/UTXO.md` now commits typed coin payloads in the canonical live-state leaf hash.

Minor
- Updated the main transaction-validity, address, wallet, and compact-filter sections in `SPEC.md` so they describe typed outputs, mixed-family transactions, typed watch items, and PQ lock addresses instead of a Schnorr-only x-only model.
- Clarified the published sighash and state-commitment model so committed outputs and spent coins are encoded as `(type, value_in_atoms, payload32)` across transaction signing and `utxo_root` derivation.
- Refreshed the protocol rationale appendix in `SPEC.md` to describe the current output-locking design: direct x-only public-key outputs for the x-only spend family and 32-byte lock commitments for the PQ spend family.
- Updated the public `README.md` with the current post-quantum direction, external migration references, and the current no-cutover status.

## v0.1.16
Major
- Switched the live node onto a disk-backed UTXO path. Normal startup now opens chainstate from metadata, serves committed UTXO reads directly from Pebble, and no longer depends on preloading the full UTXO map into RAM before the node becomes usable.
- Reworked chainstate and reorg persistence around direct UTXO deltas instead of full-map rewrites. Active-tip extension and reorg handling now keep metadata, block/undo records, `utxo/` keys, locality index updates, and journal rewrites inside the same atomic Pebble commit boundary.
- Hardened transaction signing semantics. Consensus sighash now commits to the full spent coin encoding `(value_in_atoms, pubkey)` instead of only input amounts, and the published `SPEC.md` sighash wording now matches the shipped behavior.
- Landed a broad node hardening pass against runtime failure and peer abuse:
  - inbound peer acceptance now retries transient `Accept()` failures instead of dying permanently
  - long-lived service and peer goroutines now recover panics and log stack traces instead of crashing the process
  - recently rejected invalid blocks are cached and provably invalid block senders are short-term banned
  - pending block buffering is capped by bytes and per-peer limits instead of only block count
  - inbound transaction floods are rate-limited per peer, and repeated bad-signature admissions now escalate to ban/disconnect

Minor
- Added metadata-only storage/open APIs plus direct `GetUTXO`, iterator, and error-aware lookup surfaces so consensus-critical validation can distinguish “missing UTXO” from disk I/O failure cleanly.
- Moved mempool admission, prepared admission, orphan promotion, and block-template selection onto lookup-backed chain snapshots instead of cloned committed UTXO maps.
- Switched snapshot verification and export helpers onto iterator-based UTXO walks, reducing dependence on materialized compatibility maps in node and snapshot flows.
- Added an explicit block timestamp upper bound: headers are now invalid when `timestamp > local_system_time + 7200`, on top of the existing median-time-past floor.
- Improved sighash and validation hot paths with cached tagged-hash prefixes and preallocated shared serialization buffers for prevouts, outputs, amounts/spent-coin context, and related consensus hashing.
- Reduced mining overhead in the hottest loop by comparing PoW targets without per-iteration `big.Int` allocation and by hashing a pre-encoded header buffer with in-place nonce patching.
- Cut repeated small-block consensus overhead with stack-backed `MedianTimePast`, lower parallel Merkle threshold for medium blocks, reused `math/big` constants/scratch values in ASERT and ABLA, and one-pass block encoding reuse during validation.
- Tightened mempool/template performance by replacing floating-point feerate ranking with exact integer cross-products and caching fee histogram summary stats for hot `Stats()` reads.
- Reduced peer-relay overhead by adding a no-allocation fast path for direct-message enqueue and storing pending transaction staging entries by pointer instead of repeatedly copying full transaction structs through maps.
- Capped served block payloads per `getdata` request so a single peer can no longer trigger unbounded block reads and sends from one message.
- Added a store-scan `utxo_checksum` path for snapshot and verification surfaces while keeping block-apply and reorg checksum maintenance incremental.
- Extended regression coverage across consensus, mempool, storage, wallet, snapshot, peer-manager, and node-service flows to lock in the new chainstate, replay-protection, and resilience behavior.

## v0.1.15
Major
- Added full snapshot import/export fast-sync with retained trust anchors and background historical replay. `bpu-cli snapshot export` can now write deterministic tip snapshots, `bpu-cli snapshot import` can seed a fresh node from one of those snapshots, and the node keeps replaying stored history from genesis in the background until the imported chainstate is locally re-verified.
- Added a full lite-client state and proof surface on top of the Utreexo chainstate:
  - batched UTXO proof RPCs for multi-outpoint membership and exclusion checks
  - compact-state packages ordered by the node’s locality index for bridge and lite sync consumers
  - compact filter RPCs (`getblockfilter`, `getfilterheaders`, `getfiltercheckpoint`) for lightweight block scanning
  - an optional MuHash-style `utxo_checksum` exposed through chainstate and snapshot flows for fast cross-checks
- Reworked operator config and deployment around human-edited YAML. The node now prefers `config.yaml`, ships a commented top-level template plus `example.config.yaml`, keeps a JSON sidecar for automation compatibility, and the installer/update scripts derive a default `max_mempool_bytes` from host RAM.
- Added a real mempool memory ceiling with fee-based eviction. Nodes now enforce `max_mempool_bytes`, evict lower-value transactions when full, and expose the configured cap in mempool/chainstate surfaces instead of letting valid relay-fee floods grow memory without bound.
- Simplified and hardened the benchmark story around two real paths:
  - fast in-process `go test -bench` microbenches for tx admission, validation, signature verification, mempool selection, block build, and block apply
  - one thin `bench e2e` cluster benchmark with fixed-cadence synthetic mining, cleaner Markdown/JSON reports, and benchnet fresh-genesis runs tuned for benchmarking instead of long-lived regtest state
- Landed a major measured throughput pass on the hot tx/block path:
  - Erlay reconciliation now coalesces distributed-origin tx announcements more effectively before they fan out
  - block template selection no longer re-verifies signatures that already passed mempool admission, while full block validation still performs authoritative verification before acceptance
  - on the tracked 5-node synthetic `bench e2e` workload (`1024 tx/block`, `1s` cadence, `20` blocks, `mesh`), sustained even-origin throughput improved from `908.36 tx/s` to `998.35 tx/s`, a `9.91%` increase
  - on that same workload, block build time dropped from about `216.30 ms` to `34.61 ms`, an `84.00%` reduction

Minor
- Added a benchnet benchmark chain that always starts from fresh genesis, tunes real-mining difficulty from the requested benchmark cadence, and keeps benchmark runs isolated from persistent manual regtest history.
- Added archived microbenchmark reports under `benchmarks/reports/micro/YYYYMMDD/HHMMSS/` with matching Markdown and JSON outputs for saved before/after performance comparisons.
- Refined node logging around state instead of chatter. Operators now get stable `node=` tagging, periodic node-health snapshots, explicit sync/mempool state transitions, and far less noisy peer/known-peer/shutdown logging at `INFO` and `WARN`.
- Added snapshot-protected reorg guards for imported fast-sync state so the node refuses deep pre-import disconnects until background historical verification has backfilled and checked that history locally.
- Added a non-consensus locality-preserving UTXO index so proof serving and future snapshot packing can use recently-touched / locality-ordered coins without changing the canonical outpoint-keyed commitment.
- Upgraded relay diagnostics and benchmark observability with template-phase timing, relay batching ratios, Erlay/Graphene counters, node-runtime status snapshots, and clearer progress output during long synthetic benchmark runs.
- Raised the per-peer exact `knownTx` window and tightened relay duplicate suppression so high-TPS nodes churn less relay state under heavy mempool traffic.
- Added commented YAML config rendering and example files while keeping legacy JSON load support, so existing automation can keep working during the cutover.

## v0.1.14
Major
- Added deterministic UTXO snapshot verification tooling. The repo now ships fixed-height snapshot fixtures plus `bpu-cli snapshot root` and `bpu-cli snapshot verify` so canonical snapshot state can be reconstructed and checked against the committed header `utxo_root`.
- Added optional `utxo_root`-anchored UTXO proof APIs. The node now exposes single-outpoint membership and exclusion proofs with anchored verification over the active chain, and the accumulator proof format is documented for external consumers.
- Added an explicit Dandelion++ transaction-relay mode with a real operator flag. The tx relay path can now stem first-seen transactions to one outbound peer before fluffing them onto the normal relay plane, while remaining disabled by default for conservative rollout.
- Added a dedicated confirmed-block throughput benchmark and cleaned up benchmark terminology. Reports now distinguish `admission_tps`, `completion_tps`, and `confirmed_tps`, support configurable `txs_per_block` and `tx-origin` spread, and emit compact Markdown plus ASCII summaries with attached profile artifacts.
- Landed the biggest confirmed-throughput optimization pass so far on the steady-state block path:
  - active-tip block extensions now validate against an immutable snapshot outside the exclusive chainstate lock, then commit only if the tip is unchanged
  - large block Schnorr signature sets now batch-verify across worker chunks in parallel while preserving exact validity on fallback
  - on the tracked 3-node `regtest` confirmed-block benchmark (`2048 tx`, `512 tx/block`, `batch-size 64`), confirmed throughput improved from `781.28 tx/s` to `996.59 tx/s`, a `27.6%` increase
  - on that same workload, average block convergence dropped from `338.11 ms` to `192.58 ms`, a `43.0%` improvement
  - average block signature verify time dropped from `127.37 ms` to `45.76 ms`, a `64.1%` reduction

Minor
- Locked in the deterministic median-time-past header rule in consensus and the main spec: block timestamps must be strictly greater than the median of the previous 11 timestamps on the same branch.
- Added a standalone stealth-address design stub under `specs/` that maps cleanly onto the existing keyhash and x-only pubkey model without introducing consensus changes.
- Added standalone relay specs for Dandelion++ and compact-block fallback behavior, plus supporting relay/spec notes under `specs/`.
- Added a non-consensus `SPEC.md` note recommending batch Schnorr verification with exact per-signature fallback before final rejection.
- Added a first-class compact-block relay capability as the conservative compatibility fallback block-propagation mode, with matching telemetry and tests.
- Hardened missing-block catch-up so canonical active-header ancestry drives block requests even when the rebuildable active height index is sparse or lagging, removing a repair-only detour from the steady-state sync path.
- Expanded node and benchmark observability with block-signature counters, batch-fallback counters, signature-verify timing, richer timeout state dumps, and cleaner high-signal benchmark Markdown sections.
- Fixed benchmark accounting so submit/admission timing no longer includes warmup and seeding work, making reported throughput match the actual workload window.

## v0.1.13
Major
- Fixed full-block validation so later non-coinbase transactions can spend outputs created earlier in the same block, matching the spec's atomic block-apply rule.
- Switched transaction and authorization Merkle commitments to the tagged leaf / node / solo construction defined in the published block spec, and regenerated the shipped genesis and bootstrap fixtures under those corrected roots.
- Expanded mining search space with a 64-bit header nonce plus a fixed-width coinbase extra nonce, so miners can keep rolling search state without discarding a useful template after the old nonce window is exhausted.
- Hardened active-chain repair and persistence around competing branches: consensus-critical Pebble writes now sync before success is reported, and missing-block recovery now follows the canonical active header chain even when rebuildable height indexes lag behind.

Minor
- `getheaders`, locator matching, and missing-block request generation now distinguish canonical active-header state from the asynchronously rebuilt height indexes, which removes a fork-point blind spot after higher-work header promotion.
- Chainstate read APIs and committed views now return defensive snapshots instead of exposing live mutable tip metadata, UTXO maps, or accumulator handles to callers.
- Consensus and node coverage now anchor Merkle expectations to independent spec-side helpers and assert same-block spend acceptance directly, so a green test run is a stronger signal of actual spec conformance.
- Published the checked-in root protocol spec and `specs/` reference documents with the repo, and refreshed fixture/benchmark tooling for the widened mining nonce model.

## v0.1.9
Major
- Expanded the built-in wallet surface in `bpu-cli`. Wallets can now report confirmed and available balances, show recent on-chain history, and query fee estimates in addition to the earlier create/list/receive/send flow. The node RPC surface was extended so wallet tooling can scan confirmed chain activity and estimate spend rates directly from the running node.
- Hardened the live node around peer sync and large-network behavior. The service now tracks stalled download peers, rotates preferred sync peers when headers, blocks, or transaction fetches stop making progress, and keeps richer peer lifecycle state through the dedicated peer, sync, and relay manager split. This materially improves long-running sync resilience and makes `getpeerinfo` much more useful for diagnosing network health.
- Added a deterministic network-scale simulation harness under `bpu-cli bench sim`. It can model seeded topologies, asymmetric link latency, bounded link backlogs, and scheduled churn so relay and sync behavior can be exercised cheaply without spinning up a large live cluster.
- Reworked hot-path UTXO handling around overlay / copy-on-write views instead of repeated whole-map cloning. Block validation, mempool admission, template assembly, and reorg evaluation now validate against shared immutable chain views and only materialize fresh UTXO maps when the next committed state is finalized, which removes one of the biggest remaining single-node efficiency cliffs.

Minor
- Added structured operator RPCs `getchainstate`, `getmempoolinfo`, and `getmininginfo` so external tooling can poll committed chainstate, mempool summary, and miner/template telemetry directly.
- Finished the incremental canonical `utxo_root` pipeline with committed-view reuse and parallelized bulk commitment construction, so UTXO commitment maintenance now scales better across cores and avoids rebuilding state ad hoc in reporting and template-adjacent paths.
- Improved live sync robustness around competing headers, duplicate relay, out-of-order blocks, sparse height-index repair, in-flight request reassignment, and watchdog-driven catch-up.
- Refined the public text-mode node explorer with cleaner block flow rendering, link styling, mempool/system layout, and clearer timing labels for observed block gaps versus target spacing.
- Added a dedicated `regtest_hard` profile for slower live multi-node VPS testing and updated fixtures, installer wiring, and tests accordingly.

## v0.1.8
Major
- Added a dedicated `regtest_hard` network profile for slower, more realistic multi-node testing on small VPS hardware. The harder profile has its own chain parameters, network magic, genesis fixture, installer support, and regtest-style funding/stress RPC compatibility so live mining, relay, and sync tests no longer collapse into instant empty-block races.
- Fixed the most important live peer/sync instability paths. Duplicate transaction relay and duplicate block delivery are now non-fatal, out-of-order block delivery triggers catch-up instead of disconnecting the peer, and header persistence no longer lets inactive side branches overwrite the active height map used by locator-based sync.
- Tightened the public text-mode node explorer into a cleaner live monitor. The block flow renderer now uses single connectors with stable box borders, dashboard links render as plain underlined text, PoW timing labels distinguish target spacing from observed recent block gaps, and the page warms host metrics after a real sampling window instead of showing misleading placeholder timing.

Minor
- Extended node and storage test coverage for competing-header persistence, active-chain locator matching, duplicate relay handling, out-of-order block catch-up, and the new `regtest_hard` profile wiring.
- Updated the Ubuntu install/bootstrap profile handling to accept the harder test profile cleanly and refreshed the local architecture notes to reflect the shipped sync and dashboard behavior.
- The repository now carries the checked-in benchmark harness and the regenerated `regtest_hard` genesis fixture needed by the public build and live VPS testing flow.

## v0.1.7
Major
- Added a first-class wallet surface to `bpu-cli`. The node now ships `wallet create`, `wallet list`, `wallet receive`, and `wallet send`, backed by a local file-based wallet store that can generate receive addresses, track pending spends, build and sign transactions, and broadcast them over the authenticated RPC interface.
- Rebuilt the public node monitor into a richer text-only explorer served directly from the node. `GET /` now presents a larger ASCII layout with recent block flow, fee and PoW/DAA context, a last-hour TPS chart, mining and peer health sections, and cached drilldown pages for recent blocks and transactions at `/block/<hash>` and `/tx/<txid>`.
- Upgraded the high-throughput path across validation, relay, and block assembly. The node now maintains the canonical `utxo_root` through an incremental Merklix-style accumulator, persists normal best-tip growth with block-local chainstate deltas, batches header persistence, uses compact blocks as the default optimized block relay path, and spreads dense transaction reconciliation across a bounded fanout set instead of pushing every batch down the same peers.

Minor
- Hardened long-running node behavior with outbound peer reconnect/backoff, learned peer retention from `addr`, bounded control-vs-relay peer queues, and better cleanup of buffered peer relay state during disconnects and shutdown.
- Improved transaction ingress and package-heavy traffic handling. Regtest stress traffic now has a packed batch submission RPC, batch admission retries a drifted suffix from a fresh snapshot, same-batch parent/child chains advance against an evolving admission snapshot, and the default mempool package policy is raised to `256 / 256`.
- Tightened block-template performance by reusing mutable selection UTXO views, incremental candidate frontiers, in-place package selection, and parallel transaction/auth root construction for larger blocks.
- Refined the Ubuntu deployment path so `./install` and `./install --update` are more idempotent, stage releases more cleanly, and finish with a clearer ASCII summary of service state, paths, endpoints, and next-step commands.
- Refreshed the public repository surface with a cleaner protocol-first `README.md`, updated fixture vectors for the shipped consensus/runtime behavior, and new wallet, node, storage, relay, and UTXO-commitment test coverage.

## v0.1.5
Major
- Rewrote the public `README.md` around the Bitcoin Pure protocol itself instead of a generic node feature dump. The top-level page now leads with protocol identity, calls out the core consensus differentiators, and presents the project as the reference implementation in a cleaner, more public-facing format.
- Restructured installation and operator guidance for faster scanning. The Ubuntu-only install path, important `./install` flags, staged `--update` behavior, operator surface, and top-level project structure are now presented with higher signal and much less internal noise.

Minor
- Tightened the protocol highlight section around the strongest public-facing claims already reflected in the code and spec surface, including the scriptless spend model, split transaction/auth commitments, live UTXO state commitments, ASERT, and adaptive blocksize.
- Reworded the reference implementation section so it describes the runtime in plain English instead of protocol-family jargon, covering sync, reconciliation, relay, RPC, and the public ASCII node monitor.

## v0.1.4
Major
- Hardened the live node based on real two-VPS testing: the chainstate undo path now handles valid same-block dependency chains correctly, peer height reporting reflects the highest observed sync progress instead of stale handshake values, and the public monitor now includes smoothed host stats for CPU, network traffic, RAM usage, and server load.
- Reworked Ubuntu deployment around a staged installer pipeline. `./install` is now a thin entrypoint, `scripts/bootstrap.sh` performs locked atomic cutovers with health checks and rollback, and `scripts/update.sh` prepares staged releases so `./install --update` can safely fetch from Git and upgrade a running node.
- Upgraded node operations for long-running hosts: mining defaults are wired through worker counts rather than interval ticks, the public dashboard now exposes more useful live status, and the shipped installer can preserve config while replacing the release underneath a systemd-managed service.

Minor
- Added dashboard tests for the new human-readable host stats section and peer-height reporting, plus a persistent-chain regression test that exercises a block with an intra-block spend and verifies disconnect/undo behavior.
- Refined `.gitignore` for local scratch paths and operational artifacts so working copies stay cleaner during deployment and benchmarking.
- Updated the public README install instructions to cover the new `./install` flow and the staged `./install --update` path.

## v0.1.3
Major
- Expanded the node into a much more complete live runtime: binary P2P now has transaction reconciliation, coalesced tx batch relay, Xthinner-style preferred block relay with graceful full-block fallback, and Ubuntu deployments now expose a public ASCII dashboard on `GET /` so you can watch tip, peers, mempool, and relay health directly from the node IP.
- Shipped a serious performance and benchmarking upgrade across the stack, including phase-timed benchmark reports, chained-package/orphan-storm/template-rebuild scenarios, per-peer queue and latency telemetry, incremental block-template reuse, split prepare/commit mempool admission, and a relay pipeline built around bounded per-peer queues instead of synchronous fanout.
- Upgraded the mempool and mining path for higher throughput under load: admission now supports parallel prevalidation against stable snapshots, package-aware template selection is maintained incrementally, and template rebuilds reuse prior selection state instead of redoing full-pool work on every rebuild.

Minor
- Added richer benchmark outputs under `benchmarks/` with JSON, Markdown, and ASCII summaries that surface decode, validate/admit, relay fanout, and convergence timing as well as peer-level relay pressure.
- Added new relay protocol coverage and node tests for tx reconciliation, Xthin block reconstruction, missing-tx recovery, and full-block fallback behavior.
- Updated the public README and internal architecture/roadmap docs to reflect the current node capabilities, shipped relay modes, and future consensus/runtime priorities, while `.gitignore` now treats root-level scratch markdown as local-only except for `README.md`.

## v0.1.2
Major
- Added block-tree-aware persistent chainstate with best-chain selection, branch evaluation, undo records, and restart-safe reorg handling so the node no longer assumes a single straight active chain.
- Upgraded persisted block metadata to track parent links, cumulative chainwork, validation state, and per-block block-size state, laying the core storage needed for a real public full node.

Minor
- Added explicit reorg coverage that builds competing branches, confirms higher-work takeover, and verifies the winning chain survives reopen without chainstate drift.
- Expanded storage roundtrip tests to cover validated block metadata and undo persistence.
- Updated the public README with Ubuntu install commands and refreshed the roadmap and architecture docs to reflect the new reorg-capable chain core.

## v0.1.1
Major
- Added a long-running `bpu-cli serve` node runtime with operator config, HTTP JSON-RPC, live peer sync, mempool-backed block assembly, and on-demand or interval mining on regtest-style deployments.
- Split header validation from full block replay and added a headers-first sync path, giving the node a real persisted header chain, indexed block gating, and fixture-driven IBD flows that survive restarts.
- Replaced the temporary bootstrap difficulty path with integer-only genesis-anchored ASERT while keeping the legacy Bitcoin retarget helper in code for reference and future compatibility work.

Minor
- Added `internal/config`, `internal/mempool`, `internal/node/service.go`, `internal/node/headers.go`, and `internal/node/ibd.go` to round out node orchestration, peer messaging, and transaction admission.
- Expanded storage and chainstate handling with persisted header metadata, height indexes, block lookups by height, and richer reopen behavior on Pebble-backed databases.
- Added rotating `node.log` runtime logging with source-tagged service, storage, chain, header, RPC, peer, mempool, and mining events, capped at 20 MB with backup rollover.
- Updated `README.md`, `ARCHITECTURE.md`, and `ROADMAP.md` to reflect the current Go node layout and shipped operator/runtime surfaces.
- Added `secaudit.md` and `perfaudit.md` with P0/P1 security and performance review findings for the current Go node.

## v0.1.0
Major
- Hard-cut the node and tooling from the Rust workspace to a Go module with strict consensus codec, hashing, validation, and CLI flows.
- Added Pebble-backed persisted chainstate and block index replay with fixture-backed genesis bootstrap and strict `utxo_root` enforcement.

Minor
- Preserved `bpu-cli` tx, block, chain init, and fixture replay commands across in-memory and persisted paths.
- Added Go test coverage for codec roundtrips, Schnorr validation, block application, storage reopen checks, and deterministic fixture replay.
