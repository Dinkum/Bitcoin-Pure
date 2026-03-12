# Graphene-Family Block Relay (Recommended)
===========================================

Status
------
This document specifies recommended non-consensus block-relay behavior.
It does not affect block validity.

Terminology
-----------
For clarity in this document:
- "Graphene default block relay" means the normal Graphene compact block path
  with applicable v2 improvements.
- "Graphene Extended" means the separate poor-overlap / repair path (Protocol 2).
- "Graphene-based mempool synchronization" means the optional intermittent
  mempool synchronization feature and is not part of normal block relay.
- "Graphene-family relay" refers collectively to Graphene default block relay
  and Graphene Extended.

Overview
--------
BPU uses Graphene-family compact relay as the preferred high-efficiency block
propagation mode between capable peers.

The relay architecture is:
- Graphene block relay with applicable Phase 1 (v2) improvements for the normal
  good-overlap path,
- Graphene Extended for the poor-overlap or decode-failure path,
- Compact-Block-style relay as the universal compatibility fallback,
- full-block transfer as the last-resort fallback.

Graphene-based mempool synchronization remains optional repair-only behavior and
is not part of normal block relay.

Capability Negotiation
----------------------
Peers SHOULD explicitly negotiate support for:
- Graphene default block relay,
- Graphene Extended,
- Compact-Block-style fallback,
- optional Graphene-based mempool repair.

If both peers support Graphene default block relay and Graphene Extended, the
sender SHOULD use the block relay planner defined below.
If Graphene-family relay is not mutually supported, peers SHOULD use
Compact-Block-style relay or direct full-block transfer.

Relay Identifier Binding
------------------------
The main spec requires block-reconstruction protocols to bind to the full signed
transaction payload.

Accordingly:
- short identifiers or equivalent compact transaction references used for
  Graphene-family relay SHOULD be derived from `relayid`, or otherwise bind to
  both `txid` and `authid`,
- implementations MUST NOT assume `txid` alone uniquely identifies the full
  transaction payload for block reconstruction.

Planner
-------
A sender-side planner SHOULD choose the initial block relay path.

Planner objective:
- minimize expected bytes plus expected extra round trips,
- preserve fast reliable propagation under realistic mempool divergence.

Planner inputs MAY include:
- peer capability negotiation result,
- recent success/failure rate for default Graphene block relay with that peer,
- estimated peer mempool overlap,
- recent missing-transaction patterns,
- block transaction count and block size,
- local heuristics derived from prior relay outcomes.

Planner result types:
- `GrapheneDefault`
- `GrapheneExtended`
- `CompactBlockFallback`
- `FullBlockFallback`

Planner Rules
-------------
- If predicted mempool overlap is sufficient, the planner SHOULD choose
  `GrapheneDefault`.
- If predicted mempool overlap is poor, the planner SHOULD choose
  `GrapheneExtended` directly.
- If Graphene-family support is unavailable or unsuitable, the planner SHOULD
  choose `CompactBlockFallback`.
- The planner MAY choose `FullBlockFallback` directly for pathological or
  repeated-failure cases.

Default Graphene Block Relay
----------------------------
Default Graphene block relay is the normal compact block path for good-overlap
peers.

Recommended behavior:
- sender constructs a Graphene compact block encoding using applicable Phase 1
  (v2) improvements,
- receiver reconstructs the candidate block using its mempool plus the received
  compact data,
- if reconstruction succeeds, the receiver validates the block normally,
- if reconstruction fails, the receiver SHOULD escalate to Graphene Extended.

Graphene Extended
-----------------
Graphene Extended is the poor-overlap and recovery path.

Recommended behavior:
- sender MAY choose Graphene Extended directly when poor overlap is predicted,
- receiver SHOULD request or enter Graphene Extended when default Graphene block
  decode fails,
- sender and receiver exchange the additional repair data required for the
  receiver to reconstruct the block,
- if Extended reconstruction succeeds, the receiver validates and relays the
  block normally.

Graphene Extended is therefore both:
- a preemptive poor-overlap path, and
- a post-failure repair path.

Fallback Behavior
-----------------
If Graphene-family relay is unavailable, unsupported, or unsuccessful:
- peers SHOULD fall back to Compact-Block-style relay, or
- direct recovery of missing transactions, or
- full-block transfer.

All full nodes SHOULD support Compact-Block-style relay as the universal
compatibility fallback.

Phase 1 (v2) Improvements
-------------------------
Implementations MAY apply Graphene Phase 1 (v2) improvements on the default
Graphene block path.

Examples include:
- improved cheap-hash seeding or salting,
- randomized reconciliation seeds,
- overlap-estimation support,
- improved parameter selection,
- optional expedited Graphene-style sender heuristics.

These improvements apply to the default Graphene block path only.

Optional Graphene Mempool Synchronization
-----------------------------------------
Graphene-based mempool synchronization MAY be implemented only as an optional
recovery feature.

Default behavior:
- disabled by default.

Allowed uses:
- peer catch-up,
- cold-start recovery,
- severe desynchronization recovery.

Hard invariant:
- It MUST NOT become a second normal owner of steady-state mempool convergence.
- It MUST NOT be invoked by the normal tx scheduler while Erlay-style tx relay
  is active for that peer.

Telemetry
---------
Nodes SHOULD expose operator-visible metrics including:
- planner selection counts by result type,
- default Graphene block relay success rate,
- preemptive Graphene Extended rate,
- default Graphene decode-failure rate,
- Graphene Extended recovery success rate,
- Compact-Block fallback rate,
- full-block fallback rate,
- peer-level relay success statistics.

Testing Guidance
----------------
Implementations SHOULD test at least:
- good-overlap peers planned to default Graphene block relay,
- poor-overlap peers planned directly to Graphene Extended,
- default Graphene decode failure escalating to Graphene Extended,
- non-supporting peers falling back cleanly,
- repeated-failure peers degrading to fallback paths,
- invariant enforcement that Graphene mempool repair never becomes part of
  normal steady-state tx convergence.

Consensus Boundary
------------------
Nothing in this document changes:
- block validity,
- transaction validity,
- LTOR,
- tx_root,
- auth_root,
- utxo_root,
- proof-of-work,
- difficulty adjustment,
- mining or wallet consensus behavior.

Optional Alternate Relay Modes
------------------------------
Nodes MAY implement additional non-consensus block relay modes, such as
Xthinner-style relay, as optional alternate behavior.

Such modes:
- are not part of the primary recommended relay architecture,
- MUST NOT replace Compact-Block-style relay as the universal compatibility
  fallback,
- SHOULD be negotiated explicitly between capable peers.

This document only describes recommended non-consensus block relay behavior.