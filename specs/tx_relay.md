# Erlay-Style Transaction Relay (Recommended)
============================================

Status
------
This document specifies recommended non-consensus transaction-relay behavior.
It does not affect transaction or block validity.

Overview
--------
BPU nodes use an Erlay-style relay protocol as the sole steady-state mempool
dissemination mechanism.

Design goals:
- reduce transaction-gossip bandwidth,
- preserve low end-to-end transaction propagation latency,
- avoid duplicate transaction storms,
- keep the steady-state tx control plane simple and singular.

Ownership
---------
- Erlay-style relay is the only intended steady-state mempool dissemination
  mechanism.
- Legacy transaction relay messages remain compatibility fallback only.
- Graphene-based mempool synchronization is not part of normal tx convergence.
- A peer MUST NOT use Graphene mempool repair as part of its normal tx
  convergence schedule while Erlay-style relay is active for that peer.

Capability Negotiation
----------------------
Peers SHOULD explicitly negotiate support for:
- Erlay-style tx reconciliation,
- legacy tx relay fallback.

If both peers support Erlay-style reconciliation, they SHOULD use it as the
normal tx relay mode.
If either peer does not, they SHOULD fall back to legacy relay behavior for
that peer connection only.

Identifiers
-----------
The main spec defines relay-safe transaction identification behavior.

For tx relay and reconciliation:
- peers SHOULD identify full signed transactions by `relayid = hash(txid || authid)`,
  or an equivalent identifier that binds to both `txid` and `authid`,
- peers MUST NOT assume `txid` alone uniquely identifies the full signed payload.

Per-Peer State
--------------
Each peer connection SHOULD maintain reconciliation state including:
- whether Erlay-style reconciliation is active,
- reconciliation window / epoch state,
- recently announced or learned transaction identifiers,
- outstanding transaction requests,
- recent reconciliation successes and failures,
- fallback count and last-fallback reason,
- rolling estimates of peer usefulness and synchronization quality.

Steady-State Relay Model
------------------------

1. New Transaction Admission
----------------------------
When a node accepts a new transaction into its mempool:
- it validates the transaction against local policy,
- it derives the relay-safe transaction identifier,
- it announces the transaction to only a small bounded subset of
  reconciliation-capable peers,
- it uses legacy transaction relay only for peers that do not support
  reconciliation.

The announcement fanout is implementation-defined, but SHOULD remain small and
bounded.

2. Periodic Reconciliation Sessions
-----------------------------------
Peers SHOULD run pairwise reconciliation sessions periodically.

Each session SHOULD:
- summarize a recent eligible transaction set or window,
- identify which transaction identifiers are missing on each side,
- request and transmit only the missing transactions needed to converge.

The exact sketch or summary encoding is implementation-defined.
The protocol objective is to exchange compact set summaries first and full
transactions only when needed.

3. Transaction Requests
-----------------------
After reconciliation identifies missing transactions:
- the receiver SHOULD request only the missing full transactions,
- the sender SHOULD transmit only those transactions,
- duplicate re-announcement of already-known transactions SHOULD be avoided.

4. Session Failure and Recovery
-------------------------------
If a reconciliation session cannot complete successfully:
- peers MAY retry once with adjusted limits,
- peers MAY request direct identifiers or direct transactions,
- peers SHOULD fall back to legacy transaction relay for that peer if repeated
  reconciliation failure persists.

Repeated failure on one peer connection MUST NOT disable Erlay-style relay
globally.

5. Disconnect / Reconnect
-------------------------
On disconnect:
- per-peer transient reconciliation state SHOULD be dropped.

On reconnect:
- peers SHOULD begin with fresh reconciliation state,
- stale outstanding transaction requests MUST NOT be reused blindly.

Scheduling
----------
Implementations MAY choose:
- reconciliation cadence,
- announcement fanout,
- session window sizes,
- retry limits,
- fallback thresholds.

Recommended objective:
- minimize expected transaction-relay bytes while keeping transaction
  propagation latency small relative to the target block interval.

Safety and Simplicity Rules
---------------------------
- Erlay-style relay is the only intended steady-state tx control plane.
- Legacy tx relay is compatibility behavior, not a co-equal long-term design.
- Graphene-based mempool repair MUST remain explicitly outside the normal tx
  scheduler.
- Nodes SHOULD avoid duplicate announcements across multiple relay paths for
  the same peer.
- Mixed-capability networks SHOULD degrade cleanly without causing tx storms.

Telemetry
---------
Nodes SHOULD expose operator-visible metrics including:
- reconciliation sessions started,
- reconciliation sessions completed,
- reconciliation failures,
- missing transactions requested,
- missing transactions served,
- legacy fallback activations,
- duplicate-announcement suppression counts,
- peer-level reconciliation success rate.

Testing Guidance
----------------
Implementations SHOULD test at least:
- two Erlay-capable peers converging by reconciliation while transferring only
  missing transactions,
- mixed-capability peers falling back cleanly,
- disconnect / reconnect state reset,
- duplicate transaction suppression under overlapping announcements,
- invariant enforcement that Graphene mempool repair never activates during
  normal Erlay operation.

Consensus Boundary
------------------
Nothing in this document changes:
- transaction validity,
- block validity,
- canonical ordering,
- txid / authid commitment rules,
- utxo_root,
- mining or wallet consensus behavior.

This document only describes recommended non-consensus tx relay behavior.