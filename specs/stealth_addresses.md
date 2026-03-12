# Stealth Addresses

Status: non-consensus wallet specification stub.

This document sketches a stealth-address scheme for Bitcoin Pure that fits the
existing transaction model with no consensus changes.

It is intentionally narrow:

- no new output type,
- no new script system,
- no marker outputs,
- no `OP_RETURN`,
- no P2P message changes,
- no block, mempool, or consensus rule changes.

The goal is simple: let a sender derive a one-time destination pubkey from a
recipient's long-lived stealth address so the on-chain output looks like an
ordinary BPU output but is unlinkable to the recipient's published address.

## 1. Why This Fits BPU

BPU outputs already commit to:

- `value_atoms`
- `pubkey`

That means BPU already has the core shape needed for a stealth-address protocol:

- the sender can derive a one-time pubkey,
- the sender places that one-time pubkey directly into the output,
- the recipient later spends with the matching one-time private key using the
  normal Schnorr auth path.

No consensus-visible output redesign is required.

## 2. Stealth Address Data Model

A stealth address consists of two long-lived x-only secp256k1 public keys:

- `scan_pubkey`
- `spend_pubkey`

The corresponding private keys are:

- `scan_seckey`
- `spend_seckey`

Intuition:

- `scan_pubkey` lets the recipient detect payments meant for them.
- `spend_pubkey` is the base spend key from which one-time destination keys are
  derived.

Final user-facing string encoding is out of scope for this stub. A later wallet
spec may define a CashAddr-like or dedicated BPU stealth string format.

## 3. Sender Construction

To create a stealth payment output, the sender uses the private keys of the
transaction inputs they control together with the recipient's stealth address.

### 3.1 Eligible Inputs

The sender defines the eligible input set as the transaction inputs for which it
controls the signing private keys.

For the first BPU stealth cut:

- the eligible input set SHOULD be all non-coinbase inputs in the transaction,
- the sender MUST know the private key for every eligible input,
- the sender MUST use the same ordered input set when deriving the stealth
  tweak and when constructing the final transaction.

### 3.2 Shared Sender Secret

Let the sender's eligible input private keys be:

- `a_0, a_1, ..., a_(n-1)`

Let the corresponding x-only public keys be:

- `A_0, A_1, ..., A_(n-1)`

Define:

- `a_sum = (a_0 + a_1 + ... + a_(n-1)) mod n`
- `A_sum = A_0 + A_1 + ... + A_(n-1)` on secp256k1

where `n` is the secp256k1 group order.

The sender computes the shared point:

- `S = a_sum * scan_pubkey`

### 3.3 Stealth Tweak

For output index `i`, define:

- `t_i = H_tag("bpu-stealth-v1", xonly(S) || xonly(A_sum) || xonly(scan_pubkey) || xonly(spend_pubkey) || LE32(i)) mod n`

where:

- `xonly(P)` is the 32-byte x-only encoding of point `P`,
- `LE32(i)` is the output index encoded as little-endian 32-bit,
- `H_tag` is a tagged SHA-256 hash.

### 3.4 One-Time Destination Key

The sender derives:

- `P_i = spend_pubkey + t_i * G`

and then places this ordinary BPU output on chain:

- `pubkey = xonly(P_i)`

The output itself is otherwise a normal BPU output.

## 4. Receiver Scanning

The recipient scans transactions using their `scan_seckey`.

For each candidate transaction:

1. Read the transaction inputs.
2. Resolve the referenced prevouts and recover the ordered input pubkeys from those prevout outputs.
3. Compute `A_sum` from the same ordered input pubkeys used by the sender.
4. Compute the shared point:
   - `S' = scan_seckey * A_sum`
5. For each output index `i`, compute:
   - `t_i = H_tag("bpu-stealth-v1", xonly(S') || xonly(A_sum) || xonly(scan_pubkey) || xonly(spend_pubkey) || LE32(i)) mod n`
   - `P_i = spend_pubkey + t_i * G`
   - `candidate_pubkey = xonly(P_i)`
6. If `candidate_pubkey` equals the on-chain output pubkey, the output belongs
   to the recipient.

Because:

- `S' = scan_seckey * A_sum = a_sum * scan_pubkey = S`

the sender and receiver derive the same one-time destination key.

## 5. Receiver Spend Key

Once a recipient identifies a matching output, they derive the one-time spend
private key:

- `p_i = (spend_seckey + t_i) mod n`

The output is then spent exactly like any other BPU output:

- produce the normal Schnorr signature with `p_i`.

Consensus sees only a normal signature under the referenced output pubkey.

## 6. Important Properties

- No consensus changes.
- No address reuse on chain if wallets derive a fresh one-time output key for
  each payment.
- No explicit notification output is required.
- The receiver can detect payments from transaction data plus referenced prevouts already visible in BPU:
  - input pubkeys are recoverable from the referenced prevout outputs,
  - outputs already commit directly to one-time pubkeys.

## 7. Limitations of This Stub

This is a first-cut design note, not a production wallet standard yet.

Open items for a later revision:

- exact x-only point-lifting and parity rules,
- precise tagged-hash naming and encoding test vectors,
- multi-recipient same-transaction conventions,
- change-output handling guidance,
- wallet string encoding,
- light-client scan support,
- coin-selection guidance to avoid cross-input ownership leakage,
- interaction with future MuSig or aggregated-auth designs.

## 8. Non-Goals

This document does not define:

- consensus validity rules,
- a new transaction format,
- a new output format,
- encrypted memo payloads,
- reusable payment codes beyond the `scan_pubkey + spend_pubkey` pair,
- network-layer anonymity.

It is purely an application-layer address and scanning convention built on the
existing BPU transaction model.
