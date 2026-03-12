# Dandelion++ Transaction Relay

Status: non-consensus relay policy.

This document defines the initial BPU Dandelion++ relay cut. It is a
privacy-oriented first-hop policy layered on top of the existing tx relay
transport. It does not change transaction validity, block validity, or wire
message formats.

This relay mode is optional and disabled by default in the current node.

## Scope

- Applies to newly accepted mempool transactions first seen from:
  - local submission paths, or
  - a peer relay message.
- Does not apply to periodic local rebroadcast retries after the first relay.

## Stem Phase

- A node computes the current eligible stem set as the connected outbound peers.
- If the eligible stem set is empty, the node skips stem relay and immediately
  uses normal diffusion.
- Otherwise, the node selects exactly one stem peer:
  - sort the eligible stem peers by peer address in ascending lexical order,
  - choose the first peer in that order.
- During stem relay, the node announces the transaction only to that one stem
  peer using the normal transaction announcement path:
  - `txrecon` for Erlay-capable peers,
  - legacy `inv` for non-Erlay peers.

## Fluff Phase

- After a fixed 2 second embargo, the node rechecks whether the transaction is
  still present in the mempool.
- If the transaction is no longer in the mempool, no fluff relay occurs.
- If the transaction is still present, the node relays it over the normal tx
  diffusion path to all connected peers except the immediate source peer, if
  any.

## Notes

- This is intentionally a minimal first cut:
  - no new P2P message types,
  - no epoch graph construction,
  - no per-transaction randomness in stem-peer selection.
- The steady-state network diffusion plane remains the existing Erlay/legacy
  relay implementation; Dandelion++ only shapes the first-hop exposure.
