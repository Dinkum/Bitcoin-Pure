# Bitcoin Pure (BPU) — Protocol + Node Behavior Spec
======================================================

Note - Additional, more technical specs can be found in /specs/:
- ASERT.md

## Overview
-----------
Bitcoin Pure is a minimalist, payments-only proof-of-work chain. The goal is to make a lean, cash focused version of Bitcoin that cleans up problamatic parts of the original protocol. We aim to better manifest the spirit of Bitcion, not change it. Bitcoin can scale to cheap, fast, secure, scarce, online cash for the entire world if we let it.

- One spend rule, one signature scheme.
- No scripts, no smart contracts, no timelocks, no OP_RETURN.
- 10-minute blocks, SHA256d PoW, ASERT difficulty with a 1-day half-life.
- Dynamic block size cap with a 32 MB floor, growing slowly with real usage.
- Long-tail halving schedule: ~1 BPU/block around year ~500, subsidy → 0 around ~2235. Permanent 1-atom tail emission continues forever thereafter.
- Canonical transaction ordering (LTOR) in consensus.
- Canonical Merkle transaction commitments in block headers.
- Header-committed Merklix-style canonical UTXO state root for snapshots, fast sync, and proof anchoring.
- Modern non-consensus stack: compact filters, optional Utreexo-like compact-state proofs, snapshot fast-sync, Avalanche-style fast pre-consensus.


========================
## PART A — CONSENSUS LAYER
========================

1. Units and Time
-----------------
- Base unit (consensus): atom (smallest indivisible unit).
- Display unit: BPU.
- Fixed relation: 1 BPU = 1,000,000,000 atoms.

Consensus amount type:
- All on-chain amounts are serialized and validated as unsigned 64-bit integers in atoms.
- Any transaction or block causing amount overflow is invalid.

- Target block interval: 600 seconds (10 minutes).
- Approx blocks per year: ~52,560.

All on-chain amounts are integers in atoms. BPU is UI only.


2. Transaction Model (Scriptless UTXO)
--------------------------------------
Outputs:
- Each output:
  - value: uint64 integer number of atoms,
  - pubkey: 32-byte x-only secp256k1 public key.
- Design rationale: see Appendix A.1.

Inputs / spends:
- Each input references a previous output by (txid, output_index).
- To spend an output, the transaction carries per input:
  - a 64-byte Schnorr signature.

Validation for an input:
- Verify the Schnorr signature against the referenced UTXO pubkey and the sighash message defined in §3.

There is exactly one output type and one spend rule.
No script language, no multisig, no timelocks, no hashlocks, no OP_RETURN in consensus.


3. Signatures and Sighash (What Is Signed)
------------------------------------------
Cryptography:
- Curve: secp256k1.
- Public keys: 32-byte x-only (Taproot/BIP340-style).
- Signatures: 64-byte Schnorr.
- Hash: SHA-256, with tagged hashing for domain separation.

Conceptual sighash:
For each input i, the signature signs a hash of:

- Transaction version.
- Index i of the input being signed.
- The list of all input prevouts:
  - For each input: (prev_txid, prev_output_index).
- The list of all outputs:
  - For each output: (value_in_atoms, pubkey).
- The list of all spent coins:
  - For each input: the canonical encoding of the referenced UTXO as (value_in_atoms, pubkey).

The spent-coin encoding is identical to the canonical output encoding used by consensus for a transaction output.

In words:
“Input i authorizes this exact set of inputs and outputs while committing to the exact coins being spent.”

Properties:
- Exactly one sighash mode; no SIGHASH flags.
- All inputs share the same global context, differing only by the index i.
- Implementations may pre-hash shared components for efficiency; in particular, the shared prevout list, output list, and spent-coin list may be hashed once per transaction and reused across all inputs.


### txid vs Signatures (Malleability & Commitment)
-------------------------------------------------
Internal transaction structure:

- Base:
  - version,
  - list of inputs (prevouts: txid + index),
  - list of outputs (value + pubkey).
- Auth:
  - for each non-coinbase input: (signature).

Identifiers:
- txid   = hash(Base) only (signatures excluded).
- authid = hash(Auth).

Blocks commit to both:
- tx_root: canonical Merkle root over txids in block order.
- auth_root: canonical Merkle root over authids in the same order.

Merkle rules:
- The Merkle construction for tx_root and auth_root is consensus-defined.
- Leaves, internal-node hashing, odd-leaf handling, single-leaf handling, and root hashing are all consensus-defined.
- Because non-coinbase transactions are canonically ordered by LTOR, tx_root and auth_root commit to ordered sequences, not keyed maps.

Effects:
- txid is stable under any signature changes → no txid malleability.
- Signatures remain fully committed via auth_root.
- For a given ordered transaction list, all fully-valid nodes must derive the same tx_root and auth_root.




### Block Structure and Proof-of-Work
------------------------------------
Each block:

Header:
- version,
- prev_block_hash,
- tx_root,
- auth_root,
- utxo_root (canonical UTXO state commitment; see below),
- timestamp (UNIX time),
- nBits (compact difficulty target),
- nonce (uint64).

Header timestamp validity:
- Let `MTP(prev)` be the median of the timestamps of the previous 11 headers on the same branch.
  - If fewer than 11 previous headers exist, use all available previous headers back to genesis.
- A block is valid only if `header.timestamp > MTP(prev)`.
- A block is valid only if `header.timestamp <= local_system_time + 7200`.
  - `local_system_time` is the validating node's current Unix time from its local system clock, not peer-adjusted or median network time.

Body:
- Ordered list of transactions.
- tx[0] must be the coinbase.
- All non-coinbase transactions must follow LTOR as defined below.

Transaction commitments:
- tx_root:
  - canonical Merkle root over txids in block order.
- auth_root:
  - canonical Merkle root over authids in the same order.

Merkle rules:
- Exact leaf encoding, internal-node encoding, odd-leaf handling, single-leaf handling, and root hashing are consensus-defined.
- Node implementations may construct these commitments however they want internally, but the consensus roots must be exactly tx_root and auth_root as specified.

Proof-of-Work:
- PoW hash = SHA256(SHA256(serialized header)).
- PoW hash must be ≤ target derived from nBits.
- Miners vary `header.nonce` first.
- After exhausting the `header.nonce` space for a candidate block, miners MAY vary the coinbase transaction's fixed-width `coinbase_extra_nonce`, recompute the dependent `tx_root`, and continue hashing.
- No other miner-controlled arbitrary coinbase payload exists in consensus encoding.


6. Difficulty Adjustment — ASERT (1-Day Half-Life)
--------------------------------------------------
Goal:
- Maintain ~600s average block interval.
- React quickly and smoothly to hash-rate shifts.

Mechanism:
- Use ASERT difficulty adjustment as defined normatively in `specs/ASERT.md`.
  - This section is informative only.
- Parameters:
  - Target block time: 600 seconds.
  - Half-life: 86400 seconds (~144 blocks, ~1 day).
    - If blocks are consistently too fast/slow, difficulty error is cut in half about every 144 blocks.


Properties:
- Smooth, per-block adjustments (no 2016-block epochs).
- Fast recovery from hash-rate shocks (days, not weeks).
- Implemented with integer fixed-point math.
- Consensus: each block’s `nBits` must equal the value required by `specs/ASERT.md`; blocks with incorrect difficulty are invalid.


7. Block Size Rule — Adaptive Block Size Limit (ABLA)
-----------------------------------------------------
BPU uses a consensus adaptive block size limit algorithm defined normatively in `specs/ABLA.md`.

Consensus rule:
- For each block height `h`, consensus derives a maximum serialized block size limit `L(h)` in bytes.
- A block at height `h` is valid only if `block_size(h) ≤ L(h)`.

Initialization:
- ABLA is active from genesis.
- Initial ABLA state:
  - `epsilon(0) = 16,000,000`
  - `beta(0) = 16,000,000`
  - `L(0) = epsilon(0) + beta(0) = 32,000,000`

State:
- ABLA maintains two consensus-derived state variables at each height:
  - `epsilon(h)` = control size
  - `beta(h)` = elastic buffer
- The block size limit is:
  - `L(h) = epsilon(h) + beta(h)`

Derivation:
- For `h > 0`, the values `epsilon(h)`, `beta(h)`, and `L(h)` are derived from:
  - `epsilon(h-1)`,
  - `beta(h-1)`,
  - `L(h-1)`,
  - and `block_size(h-1)`,
  exactly as specified in `specs/ABLA.md`.

Normative requirements:
- `specs/ABLA.md` defines all constants, comparisons, arithmetic, division semantics, clipping, and reorg behavior.
- All fully-valid nodes must derive identical `epsilon(h)`, `beta(h)`, and `L(h)` values at every height.
- Any block whose serialized size exceeds `L(h)` is invalid.

### Monetary Policy — Long Tail Emission
---------------------------------------
Units:
- Consensus amounts in atoms.
- Display unit: 1 BPU = 1,000,000,000 atoms.

Parameters:
- Halving interval: 2,500,000 blocks (~47.6 years).
- Initial block subsidy: 1,000 BPU per block = 1,000,000,000,000 atoms.
- Permanent subsidy floor: 1 atom per block.

Rule:
- Let k be the number of full 2,500,000-block intervals that have passed.
- Define the main schedule:
  - main_subsidy_atoms(h) = floor(1,000,000,000,000 atoms / 2^k).
- Block subsidy at height h:
  - subsidy_atoms(h) = max(1, main_subsidy_atoms(h)).
- Therefore, once the main schedule falls below 1 atom, the subsidy remains fixed at 1 atom per block forever.

Approximate behavior:
- Year 0: 1,000 BPU/block.
- ~48 years: 500 BPU/block.
- ~96 years: 250 BPU/block.
- …
- ~10 halvings (~480 years): ≈ 0.9765625 BPU/block (~1 BPU/block).
- ~20 halvings (~950 years): ≈ 0.00095367431 BPU/block.
- Subsidy reaches 0 atoms after 47 halvings (~2235 years), then fee-only forever.
- Subsidy never reaches 0; after the main schedule falls below 1 atom, issuance continues at a permanent floor of 1 atom per block forever.
- At 1 atom per block, annual tail issuance is ~52,560 atoms/year = 0.00005256 BPU/year.

Coinbase constraint:
- For block at height h:
  - sum(coinbase outputs in atoms) ≤ subsidy_atoms(h) + sum(all fees in that block in atoms).


### Coinbase Maturity

Rule:
- Outputs created by the coinbase transaction at height H MUST NOT be spent
  by any transaction included in a block with height < H + 100.
- A transaction that attempts to spend a coinbase output with fewer than
  100 confirmations is invalid.

Definition:
- A coinbase transaction is the first transaction in a block (tx[0]) and has
  no real inputs; its inputs are defined by consensus as a special coinbase
  input.
- Its `base` commits to both `coinbase_height` and a fixed-width
  `coinbase_extra_nonce`, which is miner-controlled and part of `txid`.
- Coinbase outputs are outputs of coinbase transactions.

Validation:
- When validating a non-coinbase transaction input that references a coinbase
  output created at height H:
  - the spending transaction is only valid in blocks with height ≥ H + 100.
- Nodes MAY accept such transactions into their mempool once the coinbase
  has 99 confirmations, but MUST NOT include them in a block before height
  H + 100.

### Canonical Transaction Ordering (LTOR)
-----------------------------------------

Rule:
- In every block:
  - tx[0] must be the coinbase transaction.
  - All non-coinbase transactions must appear in ascending order by txid.

Ordering:
- txid ordering is lexical ascending order of the 32-byte txid.
- A block that violates this ordering is invalid.

Validation semantics:
- Serialization order is LTOR.
- A block is validated as an atomic patch to the pre-block UTXO set.
- Every non-coinbase input in the block must reference a UTXO that is either:
  - present in the pre-block UTXO set, or
  - created by some transaction in the same block.
- No UTXO (whether pre-block or created in this block) may be spent more than once in the block.
- Implementations may process transactions in any internal order as long as:
  - the resulting post-block UTXO set is exactly the pre-block UTXO set
    plus all creations minus all spends, and
  - all consensus rules, including tx_root, auth_root, and utxo_root, are satisfied.


### UTXO State Commitment (Canonical State Root)
------------------------------------------------
Goal:
- Commit to the current live UTXO state in each block header so that:
  - snapshots can be anchored to consensus,
  - nodes can fast-sync from a known state root,
  - lite and bridge-node protocols can anchor proofs to chain consensus,
  - identical live UTXO states always produce the same root.

Header field:
- utxo_root: 32-byte hash in each header.

Meaning:
- utxo_root is a commitment to the entire set of currently unspent outputs after applying all transactions in this block.
- utxo_root is a commitment to the current live UTXO map, not to an ordered transaction sequence.
- Representation: canonical authenticated radix tree / Merklix-style tree:
  - Each live UTXO is keyed by outpoint:
    - outpoint = (txid, vout).
  - Each live UTXO leaf commits to:
    - value_in_atoms as uint64,
    - pubkey,
    - the canonical committed coin payload for BPU v1:
      - value_in_atoms as uint64,
      - pubkey.
  - Internal nodes commit to child hashes using fixed tagged hashing.
  - Empty branches, path compression rules, key encoding, leaf encoding,
    branch encoding, and root hashing are all consensus-defined.

Rationale:
- A Merklix-style authenticated radix tree is used because the live UTXO set is a keyed map by outpoint, not an ordered list.
- This structure gives a canonical state commitment suitable for deterministic snapshots, proof anchoring, and future incremental or locality-aware proof-serving implementations.

Properties:
- utxo_root is a pure function of the current live UTXO state.
- If two fully-valid nodes have the same live UTXO set, they must derive the same utxo_root.
- Node implementations may store the UTXO set however they want internally,
  but the consensus root must be computed exactly as specified.

Block processing:
- A full node maintains:
  - the current UTXO set,
  - the canonical state tree or an equivalent structure able to derive the same utxo_root.
- To validate block h:
  - Start from the previous live UTXO state.
  - For each non-coinbase input in the block:
    - the referenced UTXO must exist in the union of:
      - the previous live UTXO state, and
      - outputs created by transactions in this block,
    - the referenced UTXO must not be claimed by any other input in the block.
  - After all input references are validated, apply the block state transition:
    - remove every spent UTXO referenced by the block,
    - insert every new output created by the block under its outpoint.
  - Compute utxo_root_candidate from the resulting canonical live state.
  - Block is valid only if utxo_root_candidate == utxo_root in the header.

Security:
- Forging membership or exclusion proofs against utxo_root requires breaking
  the security of the underlying hash construction.
- No trusted setup; hash-based only.


### Transaction and Block Validity (Summary)
-------------------------------------------

Transaction is valid if:
- Structure well-formed.
- All referenced inputs exist in the chain UTXO state and are unspent:
  - for consensus block validation, each input must correspond to a UTXO that is
    unspent in the union of:
      - the pre-block UTXO set, and
      - outputs created by other transactions in the same block,
    and no UTXO is referenced more than once in the block.
- No duplicate input references within the tx.
- Sum(inputs) ≥ sum(outputs); difference is fee.
- For each non-coinbase input:
  - Schnorr signature verifies under the referenced UTXO pubkey and the sighash as defined in §3.

Block is valid if:
- Header has correct PoW (hash ≤ target).
- nBits matches ASERT difficulty rule.
- First transaction is a valid coinbase for height h.
- All non-coinbase transactions follow LTOR.
- All other transactions valid as above.
- No UTXO is spent more than once in the block.
- Total coinbase value obeys subsidy+fee rule.
- Block size ≤ L(h), where L(h) is the consensus block-size limit derived by `specs/ABLA.md`.
- tx_root and auth_root match the tx list.
- utxo_root matches the canonical live UTXO state after applying the block (see §10).






===============================
## PART B — NON-CONSENSUS BEHAVIOR
===============================

P2P, Relay, and Mempool (Recommended)
-----------------------------------------
Addresses:
- CashAddr-style addresses (e.g. `bpu:...`) encoding:
  - address type + payload-size version bits,
  - 32-byte x-only pubkey payload,
  - CashAddr checksum committed to the human-readable prefix.

P2P / relay architecture:
- Transport encryption is not required by spec; nodes MAY use plaintext transport by default.
- Transactions:
  - Nodes SHOULD use Erlay-style transaction relay as the sole steady-state mempool dissemination mechanism.
  - Detailed recommended behavior is defined in `specs/tx_relay.md`.
- Blocks:
  - Nodes SHOULD use Graphene block relay as the preferred high-efficiency block propagation mode when both peers support it.
  - A sender-side planner SHOULD choose between the default Graphene block path and Graphene Extended (phase 2) based on predicted mempool overlap.
  - If default Graphene block decode fails, the receiver SHOULD escalate to Graphene Extended automatically.
  - Detailed recommended behavior is defined in `specs/block_relay.md`.
- Compatibility fallback:
  - Compact-block relay is the universal compatibility fallback when Graphene-family relay is unavailable, unsupported, or fails.
- Graphene-based mempool synchronization:
  - Optional, disabled by default, and reserved for peer catch-up, cold-start recovery, or severe desynchronization.
  - It MUST NOT become a second normal owner of mempool convergence beside Erlay.

Mempool:
- Default policy:
  - No replace-by-fee: once a tx is accepted, higher-fee double spends are not used to evict it by default.
  - Child-pays-for-parent (CPFP) encouraged for fee bumping.
- Mempool limits:
  - Nodes enforce size/cpu limits; evict lowest-fee-rate txs when full.


### Transaction Identification for Relay and Reconstruction
----------------------------------------------------------
Because txid = hash(Base), txid identifies the base transaction only, not the full signed transaction.

For relay, mempool synchronization, and block reconstruction:
- Protocols that need to identify the full transaction SHOULD use:
  - relayid = hash(txid || authid),
  - or equivalently the ordered pair (txid, authid).
- Short-ID-based reconstruction schemes (e.g. Compact-Block-style, Xthinner-style, Graphene-style) SHOULD derive short IDs from relayid, or otherwise bind to both txid and authid, not txid alone.
- Protocols MUST NOT assume txid alone uniquely identifies the full transaction payload.

Mempool policy:
- Nodes MAY treat transactions with the same txid but different authid as alternate signed variants of the same base transaction.
- By default, nodes SHOULD keep and relay only the first-seen variant for a given txid, unless local policy explicitly supports storing alternates.

Consensus boundary:
- This section is non-consensus only.
- Consensus transaction ordering, outpoint references, and dependencies continue to use txid.
- The full signed contents included in a block are committed by auth_root.


### Block Relay Modes (Recommended)
------------------------------------
Goals:
- Minimize block-propagation bandwidth,
- preserve fast and reliable relay under real-world mempool divergence,
- exploit canonical transaction ordering (LTOR),
- always provide a robust fallback path.

Architecture:
- Nodes SHOULD support Graphene-family compact block relay.
- Preferred high-efficiency block relay is the default Graphene block path with applicable Phase 1 (v2) improvements.
- A sender-side planner SHOULD choose:
  - the default Graphene block path when predicted mempool overlap is sufficient,
  - Graphene Extended when predicted mempool overlap is predicted to be poor.
- If the default Graphene block path is attempted and decode fails, the receiver SHOULD escalate to Graphene Extended automatically.
- Graphene-based mempool synchronization is not part of normal block relay operation and remains optional repair-only behavior.

Baseline interoperability:
- All full nodes SHOULD support Compact-Block-style relay.
- Compact-block relay is the universal fallback mechanism when Graphene-family relay is unavailable, unsupported, or fails.

Transaction relay:
- Nodes SHOULD support Erlay-style transaction relay / set reconciliation
  as the sole steady-state mempool dissemination mechanism.
- Detailed recommended tx relay behavior is defined in `specs/tx_relay.md`.
- Transaction relay mode is non-consensus.

Batch Schnorr verification:
- Nodes SHOULD use BIP340-style batch Schnorr verification as a local validation accelerator when many signatures are checked together.
- Batch verification is non-consensus only and MUST NOT change transaction or block validity.
- If a probabilistic batch verifier is used, implementations SHOULD fall back to exact per-signature verification before treating a batch failure as final.

Optional repair features:
- Nodes MAY implement Graphene-based mempool synchronization as an optional repair primitive.
- Graphene-based mempool synchronization SHOULD be disabled by default.
- If enabled, it SHOULD be negotiated explicitly between capable peers.
- It MAY be used only for peer catch-up, cold-start recovery, or severe desynchronization.
- It MUST NOT become a second normal owner of mempool convergence beside Erlay.

Failure handling:
- Any advanced relay mode MUST degrade gracefully.
- If Graphene-family block reconstruction fails, nodes SHOULD fall back to:
  - Compact-block relay, or
  - direct recovery of missing transactions, or
  - full-block transfer.

Status:
- All relay modes in this section are non-consensus only.
- They do not affect block validity.
- They only affect how valid blocks are encoded and reconstructed between peers.


### Utreexo-Like Compact State and Proofs 
--------------------------------------------------------
Goal:
- Reduce storage requirements for validating nodes,
- support proof-carrying spends and bridge-node architectures,
- accelerate sync and proof serving,
- all without affecting consensus.

Model:
- Nodes MAY maintain an additional Utreexo-style dynamic accumulator over the
  current live UTXO set as a local optimization.
- The Utreexo representation is a forest of perfect binary Merkle trees.
- Each active UTXO is represented as a leaf in the accumulator.
- Nodes MAY exchange and serve:
  - membership proofs,
  - proof-update data,
  - compact state packages,
  - optional proof material attached to transactions or blocks.

Consensus boundary:
- Utreexo proofs, forest layout, and accumulator state are **not consensus**.
- No block header commits to a Utreexo root.
- Failure to construct, import, or verify Utreexo-related data does not make an
  otherwise valid block invalid.
- The consensus source of truth remains:
  - block validity rules,
  - the canonical live UTXO state,
  - utxo_root in the header.

Recommended uses:
- Bridge nodes that serve proofs to low-storage validators.
- Optional proof-assisted sync modes.
- Optional compact snapshot packages that include auxiliary proof material.

Trust and validation:
- Utreexo data is an optimization layer only.
- Any imported proof or compact-state package must ultimately agree with the
  node’s validated chain and consensus utxo_root.



### Optional Locality-Preserving UTXO Index
-------------------------------------------
Goal:
- Reduce average proof size for proof-serving nodes,
- improve proof-serving locality for recently created and recently spent UTXOs,
- improve compact snapshot packing,
- accelerate bridge-node and lite-client proof delivery,
- without changing consensus.

Model:
- Nodes MAY maintain an additional non-consensus locality-preserving index over the live UTXO set.
- In this index, UTXOs MAY be arranged by creation order, append order, birth height, or another locality-preserving scheme that tends to co-locate recently created coins.
- This index exists only as an implementation and proof-serving optimization.
- It does not replace the canonical consensus UTXO state keyed by outpoint.

Recommended uses:
- Serving smaller batched membership proofs.
- Improving proof locality for recently created / recently spent outputs.
- Packing snapshots in a locality-friendly layout.
- Accelerating optional Utreexo-style or bridge-node proof services.
- Reducing proof bandwidth for lite-client-facing infrastructure.

Consensus boundary:
- The locality-preserving index is not consensus.
- No block header commits to it.
- Disagreement on its layout, numbering, balancing, serialization, or proof format does not make any block invalid.
- The consensus source of truth remains:
  - block validity rules,
  - the canonical live UTXO state keyed by outpoint,
  - utxo_root in the block header.

Validation requirement:
- Any proof, compact-state package, or snapshot format derived from a locality-preserving index MUST ultimately reconstruct or agree with the validated live UTXO state and the consensus utxo_root.

### Node Policy / Mempool / Mining Behavior
- first-seen mempool acceptance
- conflicting unconfirmed spends are rejected from mempool
- no transaction replacement policy (RBF)
- CPFP is permitted
- miners may consider ancestor-descendant fee relationships for package selection
- package relay and package mining are policy, not consensus

12. Wallet Behavior 
---------------------------------
- Generate a fresh address (key) per receive; use HD derivation for UX.
- Display balances in BPU with decimals; store and transmit values as atoms.
- Fee estimation:
  - Use recent block fullness and mempool pressure.
- Use CPFP for fee-bumping stuck txs.
- Multi-party custody MAY be implemented off-chain via key/signature aggregation, but consensus remains unchanged: each input presents exactly one Schnorr signature and spends exactly one x-only pubkey output.
- Please find a non consensus spec for wallet multisig in specs/musig_wallet.md

### Block Filters for Lite Clients (BIP157/158-Style)
-----------------------------------------------------
Goal:
- Let lite clients discover relevant transactions without revealing addresses
  or downloading full blocks.

Mechanism:
- For each block, full nodes SHOULD serve a compact filter (BPU-Filter)
  similar to BIP158.
- The filter is built over:
  - all output pubkeys created in the block,
  - all spent-prevout pubkeys consumed by inputs in the block.
- Implemented as a Golomb-coded or similar probabilistic set.

Meaning:
- Output pubkey entries let a lite client detect newly received funds.
- Spent-prevout pubkey entries let a lite client detect spends of coins
  previously controlled by that pubkey.
- This is the scriptless-UTXO analogue of script-based compact block filters.

Lite clients:
- Sync headers (including utxo_root and PoW).
- Download filters for each block.
- Only fetch full blocks or relevant txs when filters indicate probable matches.
- Full nodes and bridge nodes MAY use non-consensus locality-preserving UTXO indexes to make proof serving smaller and faster.
- Combine:
  - transaction-inclusion proofs against tx_root,
  - optional proofs against utxo_root or optional non-consensus Utreexo-style proof services,
  to verify payments with strong guarantees.

Status:
- Filters are not consensus; invalid filters do not invalidate blocks.
- Standard thin-client discovery path for BPU.


### Fast Sync via UTXO Snapshots
--------------------------------
Goal:
- Let new nodes become usable quickly by jumping into a recent UTXO state
  anchored by utxo_root, while still converging to full historical validation.

Snapshot:
- A UTXO snapshot at height H consists of:
  - serialized live UTXO state for height H,
  - metadata including expected utxo_root(H),
  - optionally, auxiliary non-consensus proof material
    (e.g. Utreexo-style compact-state data).
  - snapshots MAY additionally use a locality-preserving serialization or auxiliary proof index for compactness and proof-serving efficiency, but such layout choices are non-consensus.

Fast-sync procedure:
1) Node syncs headers and verifies PoW up to at least height H.
2) Node obtains a snapshot claimed to match utxo_root(H).
3) Node verifies:
   - the header at height H has utxo_root = R,
   - the snapshot deterministically reconstructs utxo_root = R.
4) If match, node adopts snapshot as its live UTXO state at H.
5) Node downloads and fully validates blocks from H+1 to tip normally.

Background full verification:
- A node MAY enter an immediately-usable state after successful snapshot import
  and validation from H+1 to tip.
- A node SHOULD then continue verifying historical blocks in the background
  from genesis to H.
- Once historical verification completes successfully, the node no longer
  depends on external snapshot trust beyond PoW and consensus validation.
- If background verification finds a mismatch, the node MUST treat the imported
  snapshot state as invalid and discard it.

Trust:
- Fast-sync trusts PoW + consensus utxo_root for chain anchoring.
- Before background historical verification completes, the node additionally
  relies on the imported snapshot having faithfully represented the historical
  live state at height H.
- Any snapshot whose reconstructed root disagrees with utxo_root(H) is rejected.

Status:
- Snapshot format is policy, not consensus.
- Auxiliary proof material is optional and non-consensus.

### Optional UTXO Set Checksum (MuHash-Style)
---------------------------------------------
Goal:
- Provide a cheap non-consensus checksum for cross-checking live UTXO sets,
- improve snapshot verification workflows,
- support operational debugging and node-health checks,
- without replacing the consensus utxo_root.

Model:
- Nodes MAY maintain an additional MuHash-style checksum over the current live
  UTXO set as a local optimization.
- The checksum is computed over canonical per-UTXO encodings derived from the
  live UTXO map.
- Insertions and deletions update the checksum incrementally as the live UTXO
  set changes.

Consensus boundary:
- The MuHash-style checksum is not consensus.
- No block header commits to it.
- Disagreement on a MuHash-style checksum does not by itself make a block or
  chain invalid.
- The consensus source of truth remains:
  - block validity rules,
  - the canonical live UTXO state,
  - utxo_root in the header.

Recommended uses:
- Cross-checking independently built snapshots.
- Fast equality checks between nodes that claim to have the same live UTXO set.
- Operational debugging and corruption detection.
- Sanity checks during import/export of UTXO-state packages.

Relationship to utxo_root:
- utxo_root is the consensus commitment and proof anchor.
- A MuHash-style checksum is only an auxiliary checksum.
- utxo_root supports canonical state commitment and proof anchoring;
  MuHash-style checksums support cheap equality checking.


### Avalanche Pre-Consensus Overlay (Fast Finality)
---------------------------------------------------
Goal:
- Give users fast, probabilistic finality (seconds) without altering base PoW.

Model:
- Optional Avalanche/Snowball-style voting overlay on the mempool:
  - Nodes query random peers about preferences between conflicting txs.
  - Local preferences update based on repeated random sampling.
  - Each tx accumulates a “confidence” score.

Client UX:
- Pending tx with low confidence: “seen/unconfirmed”.
- Tx with high Avalanche confidence:
  - Treated as practically final for day-to-day payments (seconds-level).
- Large or critical transfers:
  - Still require N PoW confirmations on-chain.

Consensus:
- Avalanche state/messages are non-consensus.
- Ignoring Avalanche does not change block validity.
- Recommended overlay; PoW longest-chain remains the source of truth.
- Avalanche may bias mempool, relay, mining, and wallet policy, but a
  consensus-valid higher-work PoW chain always overrides local Avalanche state.


====================================
END — Bitcoin Pure (Consensus + UX)
====================================

Appendix A — Design Rationale (Non-Normative)
=============================================

A.1 Direct Public-Key Outputs vs Hashed Public-Key Outputs
----------------------------------------------------------

BPU v1 commits outputs directly to 32-byte x-only public keys rather than to hashed public keys.

Reasoning:
- This removes the need to carry a 32-byte public key again in every spend.
- The result is smaller inputs and better cash efficiency.
- Hashed public-key outputs offer only a partial hedge against long-exposure quantum risk.
- A serious post-quantum transition would require a future consensus decision regardless.
- That future hard fork may introduce new output forms, spend typing, alternate signature schemes, and/or block-height-based migration rules.
- BPU v1 therefore does not impose a permanent transaction-size penalty on every spend for a partial defense that would still require a later migration strategy.
