# MuSig2 Wallet Behavior for Bitcoin Pure (BPU)

Status: Draft  
Version: 1  
Layer: Wallet / Application  
Depends on: BPU consensus, BIP340, BIP327, BIP328  
Inspired by: BIP174, BIP370, BIP373

## 1. Purpose

This specification standardizes cooperative n-of-n custody for Bitcoin Pure without changing consensus.

BPU consensus permits exactly one public key and one Schnorr signature per input. Cooperative custody therefore lives entirely in wallet behavior. This document defines a common way for wallets, coordinators, hardware signers, exchanges, and watch-only systems to:

- create aggregate MuSig2 keys,
- derive fresh receive and change addresses,
- export watch-only wallet state,
- coordinate nonce exchange and partial signing,
- detect and verify change outputs,
- produce a final on-chain BPU input that is indistinguishable from a normal single-signer input.

This specification is consensus-neutral.

## 2. Scope

This document defines:

- n-of-n MuSig2 wallet setup,
- aggregate key derivation and address derivation,
- wallet descriptor/export conventions,
- a logical PSBT-like transaction coordination model for BPU,
- signer and coordinator behavior,
- basic hardware-wallet and anti-exfil guidance.

This document does not define:

- general k-of-n threshold signing,
- script paths,
- timelocks,
- recovery policies beyond n-of-n,
- network relay behavior,
- consensus changes.

A wallet conforming to this specification is always n-of-n.

## 3. Normative Language

The key words MUST, MUST NOT, REQUIRED, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in RFC 2119.

## 4. Terms

- participant key: a signer’s individual 33-byte compressed secp256k1 public key used as input to BIP327 KeyAgg.
- aggregate plain pubkey: the 33-byte compressed aggregate public key returned from BIP327 key aggregation context.
- aggregate x-only pubkey: the 32-byte BIP340-compatible aggregate public key.
- wallet id: a stable identifier for a MuSig2 wallet record.
- synthetic xpub: the BIP328 extended public key built from the aggregate plain pubkey.
- derivation path: an unhardened path below the synthetic xpub root.
- coordinator: software that assembles unsigned transactions, collects public nonces and partial signatures, and finalizes the transaction.
- signer: software or hardware that controls one participant secret key and produces MuSig2 partial signatures.
- PBST: a logical “Partially Signed BPU Transaction” object used in this document. Its exact wire encoding is not fixed here; the required fields and behavior are.

## 5. Relationship to Consensus

For any BPU output controlled by a MuSig2 wallet:

- the on-chain output keyhash is `SHA256(aggregate_xonly_pubkey_child)`,
- the spend reveals exactly one 32-byte x-only aggregate pubkey,
- the spend includes exactly one 64-byte Schnorr signature,
- validators verify it exactly as any other BPU spend.

Consensus remains unaware of the underlying signer set.

## 6. Wallet Setup

### 6.1 Inputs

A MuSig2 wallet setup starts with `n >= 2` participant public keys:

- `pk_1 ... pk_n`
- each `pk_i` is a 33-byte compressed secp256k1 public key.

### 6.2 Canonical participant ordering

Wallets MUST use canonical lexicographic ordering over the 33-byte compressed participant public keys.

This ordering is the participant order for:

- KeyAgg,
- wallet exports,
- wallet id derivation,
- public nonce lists,
- partial signature attribution,
- change metadata,
- all inter-wallet interoperability.

This specification calls the resulting list `participants_sorted`.

### 6.3 Duplicate keys

Wallets SHOULD reject duplicate participant public keys during setup by default.

A wallet MAY allow duplicates only via explicit operator override.

Rationale: BIP327 permits duplicates, but operational wallet setup is more likely to encounter accidental duplicate seeds than a legitimate need for duplicate keys.

### 6.4 Aggregate key creation

Using `participants_sorted`, wallets compute:

- `keyagg_ctx = KeyAgg(participants_sorted)`
- `aggpk_plain = GetPlainPubkey(keyagg_ctx)`
- `aggpk_xonly = GetXonlyPubkey(keyagg_ctx)`

`aggpk_plain` MUST be stored in the wallet record.

`aggpk_xonly` MUST be stored in the wallet record.

### 6.5 Wallet identifier

Wallets MUST compute:

`wallet_id = tagged_hash("BPU/MuSig2/wallet-id", aggpk_plain || participants_sorted_concat)`

Where:

- `participants_sorted_concat = pk_1 || pk_2 || ... || pk_n` in canonical order.
- `tagged_hash(tag, msg)` follows the BIP340 tagged-hash pattern.

`wallet_id` is 32 bytes.

Wallets SHOULD display at least the first 8 bytes of `wallet_id` during setup and spending approval.

### 6.6 Wallet setup record

All participants MUST persist the same setup record.

Minimum record:

```yaml
wallet:
  type: bpu-musig2
  version: 1
  policy: n-of-n
  wallet_id: <32-byte hex>
  participants_sorted:
    - <33-byte compressed pubkey hex>
    - <33-byte compressed pubkey hex>
  aggpk_plain: <33-byte compressed pubkey hex>
  aggpk_xonly: <32-byte x-only pubkey hex>
  synthetic_xpub: <base58 xpub>
  path_scheme: /<account>/<chain>/<index>
```

Implementations MAY include fingerprints, labels, local key origins, and device names as auxiliary metadata.

## 7. Synthetic XPUB and HD Derivation

### 7.1 Synthetic xpub

Wallets MUST derive the watch-only root using BIP328.

The synthetic xpub is constructed from `aggpk_plain` with:

- depth = `0`
- child number = `0`
- chaincode = `868087ca02a6f974c4598924c36b57762d32cb45717167e300622c7167e38965`

This chaincode is fixed.

### 7.2 Derivation model

All derivation in this specification is unhardened.

Wallets MUST NOT use hardened derivation anywhere below the synthetic xpub.

Default derivation path format:

`/<account>/<chain>/<index>`

All components are unsigned 32-bit unhardened child indices.

Reserved chains:

- `chain = 0`: external receive addresses
- `chain = 1`: internal change addresses
- `chain >= 2`: reserved for future application-level conventions

Default account:

- `account = 0`

### 7.3 Child aggregate pubkey derivation

Given a derivation path `/<account>/<chain>/<index>`:

1. Derive `child_plain` using normal unhardened BIP32 CKDpub from the synthetic xpub.
2. Convert `child_plain` to `child_xonly` using BIP340 x-only conversion.
3. Compute `keyhash = SHA256(child_xonly)`.
4. Encode the BPU address from the standard BPU address version plus `keyhash`.

The derived `child_xonly` is the pubkey that MUST appear on-chain when spending that output.

### 7.4 Signing for child keys

When signing for a derived child pubkey, signers MUST compute the sequence of BIP32 tweaks used by CKDpub at each derivation step.

For each path step:

- the tweak is `I_L` from CKDpub,
- the tweak mode is plain,
- the tweaks are applied in path order inside the MuSig2 signing session.

Signers MUST NOT sign for a derived child unless the locally computed derived `child_xonly` hashes to the referenced UTXO keyhash.

### 7.5 Discovery

Watch-only and full wallets SHOULD support address discovery with a gap limit of at least 20 on external chains.

Change chains MAY be discovered with the same gap limit.

## 8. Descriptor / Export Conventions

A portable MuSig2 wallet export SHOULD include at minimum:

```yaml
descriptor:
  type: bpu-musig2
  version: 1
  wallet_id: <32-byte hex>
  order: lexicographic-compressed-pubkey
  aggpk_plain: <33-byte hex>
  aggpk_xonly: <32-byte hex>
  synthetic_xpub: <base58 xpub>
  participants_sorted:
    - pubkey: <33-byte hex>
    - pubkey: <33-byte hex>
  default_paths:
    external: /0/0/*
    change: /0/1/*
```

Wallets MAY include:

- local BIP32 origin data for each participant,
- labels,
- per-device fingerprints,
- account discovery state,
- next unused index metadata.

A watch-only wallet requires only the descriptor.

A spending wallet requires the descriptor plus one valid secret key for each participant.

## 9. PBST: Logical Partially Signed BPU Transaction Model

### 9.1 Overview

A PBST is a coordination object for unsigned BPU transactions and MuSig2 signing state.

This specification does not mandate a binary encoding.

Any PBST encoding MUST preserve the fields and semantics defined here.

Implementations SHOULD use a PSBT-like key-value map if they want a portable binary form.

### 9.2 Required global fields

```yaml
pbst:
  version: 1
  unsigned_tx_base: <serialized BPU Base transaction>
  inputs:
    - ...
  outputs:
    - ...
```

`unsigned_tx_base` MUST contain the BPU transaction base serialization only:

- version,
- prevouts,
- outputs.

It MUST NOT contain signatures.

### 9.3 Required input fields

Each input entry MUST contain:

```yaml
input:
  prevout:
    txid: <32-byte hex>
    vout: <uint32>
  amount: <uint64 atoms>
  utxo_keyhash: <32-byte hex>
  wallet_id: <32-byte hex>
  participants_sorted:
    - <33-byte compressed pubkey hex>
  aggpk_plain: <33-byte hex>
  aggpk_xonly: <32-byte hex>
  derivation_path:
    account: <uint32>
    chain: <uint32>
    index: <uint32>
  pubnonces:
    <participant_pubkey_hex>: <66-byte hex>
  partial_sigs:
    <participant_pubkey_hex>: <32-byte hex>
  final_pubkey: <32-byte x-only hex>
  final_sig: <64-byte hex>
```

Required behavior:

- `amount` MUST match the referenced UTXO amount.
- `utxo_keyhash` MUST match the referenced UTXO keyhash.
- `wallet_id`, `participants_sorted`, `aggpk_plain`, `aggpk_xonly`, and `derivation_path` MUST all agree.
- `final_pubkey` MUST equal the locally derived `child_xonly` for the input path.
- `SHA256(final_pubkey)` MUST equal `utxo_keyhash`.

`pubnonces`, `partial_sigs`, `final_pubkey`, and `final_sig` are added progressively during signing.

### 9.4 Output metadata for change detection

Outputs that belong to the same MuSig2 wallet SHOULD include change metadata.

Recommended fields:

```yaml
output:
  value: <uint64 atoms>
  keyhash: <32-byte hex>
  is_change: <bool>
  wallet_id: <32-byte hex optional>
  aggpk_plain: <33-byte hex optional>
  derivation_path:
    account: <uint32>
    chain: <uint32>
    index: <uint32>
```

If `is_change = true`, then:

- `wallet_id` MUST be present,
- `aggpk_plain` SHOULD be present,
- `derivation_path` MUST be present,
- signers MUST recompute the child key and verify that `SHA256(child_xonly) == keyhash`.

Absent valid change metadata, signers SHOULD treat the output as external unless local policy says otherwise.

## 10. Signing Protocol

### 10.1 Per-input sighash

For each input `i`, signers MUST compute the BPU consensus sighash exactly as required by BPU consensus.

This specification refers to that 32-byte message as `msg_i`.

### 10.2 Session identifier

For each input, wallets MUST compute:

`session_id_i = tagged_hash("BPU/MuSig2/session", wallet_id || unsigned_tx_base || ser32(i) || aggpk_plain || ser_path(account,chain,index))`

This value is used only as wallet-level session binding.

### 10.3 Nonce generation

For each input and each participating signer, the signer MUST generate a fresh nonce pair using BIP327 `NonceGen` or a safe BIP327-approved variant.

The standard input tuple is:

- `sk = participant secret key`
- `pk = participant compressed pubkey`
- `aggpk = final_pubkey` as x-only 32-byte key
- `m = msg_i`
- `extra_in = session_id_i`

This specification REQUIRES `pk`, `aggpk`, `m`, and `extra_in` to be supplied.

Implementations SHOULD also supply `sk` to get the hardened auxiliary mixing behavior described by BIP327.

A signer MUST NEVER reuse a `secnonce`.

After `Sign` consumes `secnonce`, the signer MUST securely erase it.

### 10.4 First round

For each input:

1. Each signer computes `(secnonce, pubnonce)`.
2. Each signer sends `pubnonce` with signer attribution.
3. The coordinator stores `pubnonce` under that input and participant pubkey.

The coordinator SHOULD distribute the full ordered pubnonce set to all signers, not just the aggregate nonce.

### 10.5 Aggregate nonce

For each input, signers or the coordinator compute:

`aggnonce = NonceAgg(pubnonce_1 ... pubnonce_n)`

The pubnonce list MUST be ordered exactly as `participants_sorted`.

### 10.6 Signer validation before partial signing

Before producing a partial signature for an input, the signer MUST verify all of the following:

- the referenced prevout and amount are correct,
- the locally derived `child_xonly` matches `final_pubkey`,
- `SHA256(child_xonly) == utxo_keyhash`,
- the derivation path belongs to this wallet,
- the participant set and aggregate key match the local wallet record,
- the full unsigned transaction base is acceptable,
- all outputs are acceptable,
- all change outputs marked as internal are valid derived change outputs,
- the fee is acceptable,
- the input index being signed is the intended input.

If any check fails, the signer MUST abort.

### 10.7 Partial signature generation

For each input, the signer constructs the MuSig2 session context using:

- `participants_sorted`,
- `aggnonce`,
- `msg_i`,
- the ordered plain tweaks derived from the BIP328 path,
- the final aggregate key implied by those tweaks.

The signer then runs `Sign` and stores the resulting partial signature in:

`partial_sigs[participant_pubkey]`

### 10.8 Partial signature verification

Before final aggregation, the coordinator SHOULD verify every received partial signature using BIP327 `PartialSigVerify`.

A signer MAY also verify all peer partial signatures.

If verification fails, the session MUST abort.

### 10.9 Identifiable aborts

To preserve identifiable abort behavior:

- participant messages SHOULD be transported over authenticated channels,
- coordinators SHOULD retain signer attribution for every pubnonce and partial signature,
- coordinators SHOULD forward the full ordered contribution sets,
- signers SHOULD prefer independently recomputing `aggnonce` from the full pubnonce set.

### 10.10 Final signature

When all partial signatures are present and valid, the coordinator or final signer computes:

`final_sig = PartialSigAgg(partial_sig_1 ... partial_sig_n)`

For each BPU input, the final on-chain auth material is:

- `pubkey = final_pubkey`
- `signature = final_sig`

`final_sig` MUST verify as a BIP340 Schnorr signature under `final_pubkey` and `msg_i`.

## 11. Coordinator Requirements

A coordinator conforming to this specification MUST:

- preserve exact unsigned transaction data,
- preserve exact per-input amount data,
- preserve participant ordering,
- preserve derivation metadata,
- keep pubnonce and partial signature attribution,
- never mutate any field after a signer has approved it,
- never aggregate across mismatched wallet ids,
- never reuse old nonces or partial signatures in a new session,
- finalize only after all required partial signatures verify.

A coordinator SHOULD:

- verify every partial signature,
- distribute the full ordered nonce and partial-signature sets,
- support offline / file-based round trips,
- support multi-input transactions where different inputs may belong to different MuSig2 wallets.

## 12. Signer and Hardware Wallet Requirements

A signer conforming to this specification MUST:

- store the local wallet record,
- display the wallet fingerprint or wallet id prefix during setup,
- display outputs and fee before approval,
- verify all change outputs independently,
- use fresh nonces for every signing session,
- erase consumed secnonces,
- refuse to sign if wallet metadata and input metadata disagree.

A hardware signer SHOULD also:

- display the participant count,
- display a short fingerprint for the aggregate key,
- display the derivation path of every internal output,
- require explicit confirmation if change metadata is missing,
- support air-gapped PBST exchange.

If a signer implements CounterNonceGen or another approved BIP327 nonce variant, it MUST satisfy the persistence and uniqueness requirements of that variant.

## 13. Recovery, Watch-Only, and Failure Semantics

### 13.1 Watch-only

A watch-only wallet needs only:

- wallet descriptor/export,
- synthetic xpub,
- current discovery state.

This is sufficient to derive receive addresses, detect change, and monitor balance.

### 13.2 Spending recovery

There is no aggregate private key.

Spending requires all participant secret keys or an out-of-scope recovery system.

Loss of any required participant secret key makes funds unspendable unless a separate, non-standard recovery policy exists.

### 13.3 Coordinator failure

Coordinator loss does not destroy funds if all signers still possess:

- the wallet record,
- their own secret keys,
- a compatible coordinator implementation.

### 13.4 Nonce/session failure

If a signing session aborts after any pubnonce has been revealed but before final signature aggregation, implementations SHOULD discard the entire session and restart with fresh nonces.

## 14. Interoperability Defaults

Conforming wallets SHOULD implement the following defaults:

- participant order: lexicographic compressed pubkey order,
- duplicate keys: reject by default,
- path scheme: `/<account>/<chain>/<index>` unhardened,
- default receive path: `/0/0/*`,
- default change path: `/0/1/*`,
- external gap limit: 20,
- change gap limit: 20,
- watch-only export: descriptor + synthetic xpub + wallet id,
- signing container: PBST-compatible logical fields.

## 15. Security Notes

- MuSig2 is n-of-n, not threshold.
- Aggregate keys look like ordinary BIP340 keys; this is good for privacy.
- Synthetic xpubs inherit normal xpub privacy issues.
- Nonce reuse can expose secret keys.
- A malicious coordinator can cause aborts or confusion if signers do not verify transaction details and contribution sets.
- Duplicate participant keys may be valid mathematically but are dangerous operationally.
- Because BPU outputs commit only to `SHA256(pubkey)`, the signer must verify that the derived aggregate child pubkey hashes to the referenced UTXO keyhash before signing.

## 16. Suggested Integration Into BPU Main Spec

This file is intended to be referenced from Part B of the BPU spec.

Recommended addition under Wallet Behavior:

> Wallets SHOULD support MuSig2 cooperative custody as specified in `musig_wallet.md`. This standard is non-consensus and defines n-of-n aggregate-key wallets, synthetic-xpub-based address derivation, PSBT-like coordination, nonce exchange, partial signing, and hardware-wallet safety requirements.

## 17. References

- BPU consensus spec
- BIP340 Schnorr Signatures for secp256k1
- BIP327 MuSig2 for BIP340-compatible Multi-Signatures
- BIP328 Derivation Scheme for MuSig2 Aggregate Keys
- BIP174 PSBT
- BIP370 PSBTv2
- BIP373 MuSig2 PSBT Fields
