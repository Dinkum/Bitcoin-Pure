# Bitcoin Pure — Avalanche Pre-Consensus Overlay (Stake-Weighted)

Status: Non-consensus, optional overlay.  
Scope: Mempool, relay, block-template selection, and wallet policy only.  
Consensus: Unchanged. SHA256d PoW longest valid chain is always the source of truth and overrides local Avalanche state.

Avalanche is a probabilistic, stake-weighted voting overlay that gives fast
finality (seconds) for everyday payments and strong protection against
double-spends, without modifying BPU consensus rules.

Nodes MAY run without Avalanche and remain fully consensus-compatible.

---

## 1. Design Goals

- Fast, practical finality for normal payments (seconds, not minutes).
- Strong double-spend resistance before a tx hits a block.
- No changes to:
  - block format,
  - utxo_root,
  - consensus rules.
- Sybil resistance via stake proofs (UTXO-based, non-locking).
- Avalanche-guided mempool, relay, mining, and wallet policy.
- PoW override: a consensus-valid higher-work chain always overrides local
  Avalanche state.

---

## 2. Terminology

- **Transaction (tx)**: fully valid BPU transaction.
- **Outpoint**: `(prev_txid, prev_vout)` pair.
- **Conflict set**: all transactions that spend the same outpoint.
- **Color**: a particular transaction inside a conflict set.
- **Preference**: node’s currently preferred color for a conflict set.
- **Finalized tx**: tx whose conflict set has converged via Avalanche.
- **Sample**: set of peers polled in one Avalanche round.
- **Vote**: a peer’s reported preferred color in a conflict set.
- **Weight**: stake-based voting power of a peer.
- **Proof master**: public key authorized by stake owners to control an
  Avalanche Proof.
- **Delegated voting key**: hot key authorized by the proof master for live
  network participation.
- **Logical Avalanche peer**: one accepted proof identity. Multiple TCP
  connections bound to the same proof count once for sampling and weight.

---

## 3. Stake Proofs and Voting Weight

Avalanche uses stake to weight votes and resist Sybil attacks. Stake is
represented by **Avalanche Proofs**.

### 3.1 Avalanche Proof

An Avalanche Proof is an off-chain signed structure that authorizes a
`proof_master_pubkey` to use stake UTXOs for Avalanche voting. Proofs are not
consensus objects and do not lock coins.

- Fields:
  - `version` (uint32)
  - `sequence` (uint64, monotonic replacement counter)
  - `expiration_mtp` (UNIX timestamp, uint64, compared to active-chain MTP)
  - `proof_master_pubkey` (32-byte x-only Schnorr pubkey)
  - `stakes[]`:
    - list of entries:
      - `txid` (32 bytes)
      - `vout` (uint32)
      - `amount_atoms` (uint64)
      - `height` (uint32)
      - `is_coinbase` (bool)
      - `stake_pubkey` (32-byte x-only Schnorr pubkey)
      - `stake_sig` (64-byte Schnorr signature)
  - `proof_sig` (64-byte Schnorr signature)

Canonical proof requirements:

- `stakes[]` MUST be non-empty.
- `stakes[]` MUST be unique by `(txid, vout)`.
- `stakes[]` MUST be sorted lexicographically by `(txid, vout)` so proof IDs
  and encodings are stable across implementations.

Derived values:

- `stake_commitment = TaggedHash("BPU_AVAX_STAKE_COMMIT", version || sequence || expiration_mtp || proof_master_pubkey)`
- `stake_hash = TaggedHash("BPU_AVAX_STAKE", stake_commitment || txid || vout || amount_atoms || height || is_coinbase || stake_pubkey)`
- `limited_proof_id = TaggedHash("BPU_AVAX_PROOF_BODY", version || sequence || expiration_mtp || stakes_without_signatures)`
- `proof_id = TaggedHash("BPU_AVAX_PROOF_ID", limited_proof_id || proof_master_pubkey)`

Where `stakes_without_signatures` means the `stakes[]` entries without
`stake_sig`.

### 3.2 Proof Validity

A node considers an Avalanche Proof valid if:

- `proof_sig` verifies under `proof_master_pubkey` over `limited_proof_id`.
- `expiration_mtp` is in the future relative to the active chain tip MTP.
- For each stake entry:
  - The outpoint `(txid, vout)` exists in the current UTXO set.
  - The UTXO amount equals `amount_atoms`.
  - The UTXO height equals `height`.
  - The UTXO coinbase flag equals `is_coinbase`.
  - The UTXO is of a supported single-key spend type.
  - `stake_pubkey` hashes to the keyhash committed in the UTXO under the
    normal spend rule.
  - `stake_sig` verifies under `stake_pubkey` over `stake_hash`.
  - The UTXO has at least `STAKE_CONFIRMATIONS` confirmations
    (RECOMMENDED default: 100).
- The sum of `amount_atoms` across all stake entries is ≥ `MIN_STAKE_ATOMS`.

Recommended policy defaults:

- `MIN_STAKE_BPU = 10_000 BPU`
- `MIN_STAKE_ATOMS = 10_000 * 1_000_000_000 = 10_000_000_000_000 atoms`
- `STAKE_CONFIRMATIONS = 100`

Nodes MAY use higher `MIN_STAKE_ATOMS` or confirmation counts as a local
policy. These are not consensus parameters.

### 3.3 Delegation and Peer Binding

The proof itself authorizes `proof_master_pubkey`, but the live network peer
does not need to use that key directly.

- `proof_master_pubkey` MAY delegate to a hot `delegated_voting_key`.
- Delegation MAY have multiple levels, but MUST be bounded by
  `MAX_DELEGATION_LEVELS`.
- Each delegation level signs the next delegated pubkey and the current
  delegation identifier.
- During connection setup, the final delegated key MUST sign a challenge
  derived from both peers' nonces.
- A node assigns non-zero Avalanche weight to a peer only after:
  - the proof is valid,
  - the delegation chain is valid, and
  - the peer demonstrates live possession of the final delegated key.

Multiple transport connections bound to the same `proof_id` constitute one
logical Avalanche peer and MUST NOT multiply voting weight.

### 3.4 Weight Function

For each logical Avalanche peer `p`, the node computes:

- `weight(p) = sum(amount_atoms for all stakes in p’s accepted Avalanche Proof)`

If no valid proof is known for `p`, then:

- `weight(p) = 0`
- Node MAY ignore such peers for Avalanche sampling.

Proof selection and conflicts:

- For a given `proof_master_pubkey`, only the live proof with the highest
  `sequence` is eligible for weight.
- If two live proofs for the same `proof_master_pubkey` have the same
  `sequence`, the proof with the lexicographically lower `proof_id` wins.
- If any stake outpoint appears in more than one otherwise-valid live proof
  with different `proof_id`s, those proofs are conflicting.
- Conflicting proofs MUST contribute zero weight until the conflict disappears
  by proof expiry, stake spend, or explicit operator removal of one proof.

---

## 4. Conflict Sets

Avalanche operates over per-outpoint conflict sets.

For each outpoint `o = (prev_txid, prev_vout)`:

- Conflict set `C(o)`:
  - `C.members`: set of txids spending `o` (seen by this node).
  - `C.pref`: current preferred txid (or `NULL`).
  - `C.counter[txid]`: non-negative integer, Snowball confidence counter.
  - `C.final[txid]`: boolean, Avalanche final flag.
  - `C.last_success`: boolean flag for last voting round.

Multiple inputs:

- A transaction that spends several outpoints belongs to multiple conflict
  sets. Avalanche treats each input’s outpoint separately. A transaction is
  considered globally final at this node when it is final in **all** its
  input conflict sets.

---

## 5. Parameters

Recommended defaults:

- `K_SAMPLE = 16`  
  Number of peers sampled per poll.

- `ALPHA_NUMERATOR = 3`  
  Success threshold numerator.

- `ALPHA_DENOMINATOR = 4`  
  Success threshold denominator (`3 / 4 = 75%`).

- `BETA_TX_FINAL = 15`  
  Required number of consecutive successful rounds for a given preference
  to mark it final.

- `POLL_INTERVAL_MS = 200`  
  Target interval between Avalanche rounds.

- `MAX_DELEGATION_LEVELS = 20`
  Maximum verified delegation depth.

Nodes MAY expose these as configuration flags, but SHOULD use these defaults
on mainnet unless a future spec updates them.

---

## 6. Voting Algorithm (Snowball)

Each Avalanche-enabled node runs a periodic loop (every `POLL_INTERVAL_MS`):

### 6.1 Sampling

1. Select active conflict sets:
   - Recently updated,
   - Not yet fully finalized,
   - Associated with txs in mempool or recent blocks.

2. Select a peer sample `S` of size up to `K_SAMPLE`:
   - Only logical Avalanche peers with `weight(p) > 0`.
   - Sampling MUST be stake-weighted without replacement over distinct logical
     Avalanche peers.

3. For each conflict set `C` in the active set, send an Avalanche poll to
   peers in `S`.

### 6.2 Poll Messages

Define a generic poll message `AVA_POLL`:

- Fields:
  - `poll_id` (random identifier)
  - `items[]`: list of `conflict_id`s

For BPU-AVAX v1:

- `conflict_id` is exactly the outpoint `(prev_txid, prev_vout)`.

Define a response message `AVA_VOTE`:

- Fields:
  - `poll_id`
  - `votes[]`: one per `items[]` entry
    - each `vote` is either:
      - a `txid` in the conflict set (preferred color), or
      - `NULL` meaning “no opinion / no known valid tx”.

Nodes SHOULD compactly encode `txid`s (e.g. short IDs), but that is not
consensus-meaningful.

### 6.3 Tallying

For each conflict set `C` in the poll:

1. For the current sample `S`:

   - Total sampled weight:
     - `W_total = sum(weight(p) for p in S)`
   - For each color `t` (txid) that appears in votes:
     - `W_t = sum(weight(p) for peers p that voted for color t)`

2. Determine winning color:

   - `t_win = argmax_t W_t`
   - A successful round occurs if:
     - `t_win` is not `NULL`, and
     - `W_total > 0`, and
     - `W_t_win * ALPHA_DENOMINATOR >= ALPHA_NUMERATOR * W_total`
   - If multiple colors tie for maximal `W_t`:
     - If `C.pref` is one of the tied colors, set `t_win = C.pref`.
     - Otherwise the round is unsuccessful.

3. Update Snowball state:

   - If successful:
     - If `C.pref == t_win`:
       - If `C.last_success == true`:
         - `C.counter[t_win] += 1`
       - Else:
         - `C.counter[t_win] = 1`
     - Else (preference change):
       - `C.pref = t_win`
       - `C.counter[t_win] = 1`
     - Set `C.last_success = true`.
   - If not successful:
     - Do not change `C.pref`.
     - Set `C.last_success = false`.

### 6.4 Finalization

For a conflict set `C`:

- If `C.pref = t` and `C.counter[t] >= BETA_TX_FINAL`:
  - Set `C.final[t] = true` (Avalanche-final for that outpoint).

For a transaction `tx`:

- Let `I(tx)` be its set of input outpoints.
- tx is **Avalanche-final** at this node if:
  - For every `o ∈ I(tx)` there exists `C(o)` such that:
    - `C(o).final[txid] == true`.

Nodes MAY maintain and expose a single scalar “Avalanche confidence” per tx
for UX:

- `confidence_percent(tx) = min(100, floor(min_over_o(C(o).counter[txid]) * 100 / BETA_TX_FINAL))`

---

## 7. Mempool Policy

When a node receives a new transaction `tx_new`:

1. Standard validation:
   - If tx_new fails normal consensus validation, reject.

2. For each input outpoint `o` of tx_new:
   - Let `C(o)` be its conflict set, if any.
   - If `C(o).final[t_final] == true` for some `t_final != tx_new.txid`:
     - The new tx conflicts with an Avalanche-finalized tx at this node.
     - Node MUST:
       - Reject `tx_new` from the mempool.
       - SHOULD NOT relay `tx_new`.

3. If no Avalanche-final conflict exists:
   - Apply normal mempool policy (first-seen, no-RBF, CPFP allowed).
   - Node MAY store multiple conflicting txs internally for analysis, but
     SHOULD only relay the current preference per conflict set.

---

## 8. Mining Policy

When constructing a block template:

- Miners SHOULD NOT include any transaction that conflicts with an
  Avalanche-finalized transaction at their node.
- For each conflict set:
  - If there is a finalized tx `t_final`, only include `t_final` and exclude
    all other members of that conflict set.

This mining policy is strongly recommended but not consensus-mandated.

---

## 9. Block Reception and PoW Override

When an Avalanche-enabled node receives a new block `B` that passes all
consensus validation:

1. Node MUST process, store, and attach `B` according to normal consensus
   validation and PoW chain-selection rules.

2. If `B` does not become part of the selected best chain:

   - Avalanche state is unchanged except for ordinary mempool and recent-block
     bookkeeping.

3. If `B` becomes part of the selected best chain and conflicts with local
   Avalanche-finalized state:

   - The selected PoW chain wins.
   - Node MUST accept `B` and the selected chain as usual.
   - Node MUST remove from the mempool any tx that is no longer valid against
     the selected chain.
   - For each affected conflict set:
     - Clear `C.final[*]`.
     - Reset Snowball counters for colors invalidated by the selected chain.
     - Update local preference to reflect the spend confirmed in the selected
       chain, if any.
   - Node SHOULD emit an operator-visible diagnostic indicating that local
     Avalanche state was overridden by PoW.

Note:

- Avalanche MUST NOT prevent acceptance, attachment, storage, or relay of a
  consensus-valid block under normal PoW chain-selection rules.
- Nodes without Avalanche ignore this logic and follow pure PoW.

If a supermajority of stake-weighted Avalanche nodes follow this spec,
conflicting transactions are less likely to be relayed or mined before PoW
confirms one side, which materially raises the cost of practical
double-spend attempts.

---

## 10. Wallet and UX

Wallets connected to an Avalanche-enabled node SHOULD expose per-transaction
status at least as:

- `unconfirmed`:
  - In mempool, confidence_percent < 100.
- `avalanche_final`:
  - In mempool or in a block, and Avalanche-final at this node.
- `confirmed[N]`:
  - In the best chain with N PoW confirmations.

Recommended UX:

- Everyday payments:
  - Treat `avalanche_final` as practically final.
- Large / critical transfers:
  - Require `avalanche_final` + some PoW confirmations (e.g. 1–6).

Nodes without Avalanche simply never report `avalanche_final`.

---

## 11. Configuration

Recommended node flags:

- `-avalanche=0|1`
  - Enable or disable Avalanche overlay (default: 1 on mainnet, 0 on testnets
    until proven).
- `-avalancheminstakeatoms=<integer>`
  - Override `MIN_STAKE_ATOMS`.
- `-avalanchek=<integer>`
  - Override `K_SAMPLE`.
- `-avalanchealphanumerator=<integer>`
  - Override `ALPHA_NUMERATOR`.
- `-avalanchealphadenominator=<integer>`
  - Override `ALPHA_DENOMINATOR`.
- `-avalanchebeta=<integer>`
  - Override `BETA_TX_FINAL`.

Changing these parameters does not affect consensus validity, but
misconfiguration can weaken Avalanche’s practical security.

---

## 12. Security Notes

- Avalanche is probabilistic. With the recommended parameters and honest
  supermajority of stake, the chance of “finality flip” is negligible for
  practical purposes.
- Stake-weighting means economic power and Avalanche influence are aligned.
  To attack Avalanche, an adversary must acquire significant BPU stake.
- PoW consensus remains the only source of formal block validity.
  Avalanche can bias mempool, relay, mining, and wallet policy in practice,
  but it cannot override the selected valid higher-work chain.
- Avalanche finality is local and advisory. A later valid higher-work chain may
  overturn prior Avalanche-finalized state, and nodes MUST converge to the PoW
  chain when that occurs.

---

END — BPU Avalanche Pre-Consensus Overlay (Stake-Weighted)
