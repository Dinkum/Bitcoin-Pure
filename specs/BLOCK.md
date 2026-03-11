# BLOCK.md

Status: normative consensus specification for Bitcoin Pure block structure and block encoding.

If an implementation conflicts with this document on a block-structure matter, this document wins until replaced by a newer normative block specification.

## 1. Scope

This document defines only:

- the logical structure of a block,
- the exact serialized structure of a block,
- the exact serialized structure of a block header,
- the ordering of transactions inside a block,
- the block-header commitment fields,
- the bytes hashed for proof of work,
- the measurement of serialized block size.

This document does **not** define:

- transaction structure or transaction validation,
- the UTXO state transition,
- block acceptance rules beyond structural requirements,
- timestamp acceptance rules,
- difficulty adjustment,
- adaptive block-size evolution,
- reorg behavior.

Those subjects are defined in other specifications.

## 2. Normative Language

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** in this document are normative requirement words.

## 3. External Definitions

The following items are defined outside this document:

- `Transaction`, transaction serialization, `txid(tx)`, `authid(tx)`, and coinbase transaction structure: `specs/TX.md`
- canonical varint encoding, integer encoding, compact-target decoding, and shared serialization rules: `specs/SERIALIZATION.md`
- canonical live-UTXO commitment and `utxo_root` derivation: `specs/UTXO.md`
- required difficulty field derivation: `specs/ASERT.md`
- adaptive block-size limit derivation: `specs/ABLA.md`
- network-specific genesis block and chain parameters: `specs/PARAMS.md`

## 4. Informative Structure View

This section is informative only and does not define consensus encoding. Consensus encoding is defined only in the serialization sections below.

```text
Block
├─ Header
│  ├─ version : uint32
│  ├─ prev_block_hash : bytes32
│  ├─ tx_root : bytes32
│  ├─ auth_root : bytes32
│  ├─ utxo_root : bytes32
│  ├─ timestamp : uint64
│  ├─ nBits : uint32
│  └─ nonce : uint64
└─ transactions[] : Transaction
   ├─ tx[0]     = coinbase
   └─ tx[1..n]  = non-coinbase, ascending by txid
```

## 5. Block Data Model

A block consists of exactly two top-level parts:

1. `header`
2. `transactions[]`

The header commits to:

- the parent block,
- the ordered transaction identity list,
- the ordered auth identity list,
- the post-block UTXO state commitment,
- the claimed proof-of-work target and nonce.

The body contains an ordered transaction list:

- `transactions[0]` MUST be the coinbase transaction,
- `transactions[1..n-1]`, if any, MUST be non-coinbase transactions,
- non-coinbase transactions MUST appear in strict ascending lexical `txid` order.

## 6. Header Fields

The header contains exactly the following fields in exactly this order:

1. `version`
2. `prev_block_hash`
3. `tx_root`
4. `auth_root`
5. `utxo_root`
6. `timestamp`
7. `nBits`
8. `nonce`

### 6.1 `version`

- Type: `uint32`
- Encoding: little-endian

### 6.2 `prev_block_hash`

- Type: `bytes32`
- Encoding: raw 32 bytes
- Meaning: parent block hash

### 6.3 `tx_root`

- Type: `bytes32`
- Encoding: raw 32 bytes
- Meaning: canonical root over `txid`s in block order

### 6.4 `auth_root`

- Type: `bytes32`
- Encoding: raw 32 bytes
- Meaning: canonical root over `authid`s in the same block order

### 6.5 `utxo_root`

- Type: `bytes32`
- Encoding: raw 32 bytes
- Meaning: canonical commitment to the live UTXO state after this block

### 6.6 `timestamp`

- Type: `uint64`
- Encoding: little-endian
- Meaning: Unix time in seconds

### 6.7 `nBits`

- Type: `uint32`
- Encoding: little-endian
- Meaning: compact proof-of-work target representation

### 6.8 `nonce`

- Type: `uint64`
- Encoding: little-endian
- Meaning: primary fixed-width header search-space field for proof-of-work

## 7. Header Serialization

The serialized header is the exact concatenation of the header fields in the order listed in §6, with no omitted fields and no length prefixes:

1. `version` as 4 little-endian bytes
2. `prev_block_hash` as 32 raw bytes
3. `tx_root` as 32 raw bytes
4. `auth_root` as 32 raw bytes
5. `utxo_root` as 32 raw bytes
6. `timestamp` as 8 little-endian bytes
7. `nBits` as 4 little-endian bytes
8. `nonce` as 8 little-endian bytes

The serialized header length is therefore exactly:

- `4 + 32 + 32 + 32 + 32 + 8 + 4 + 8 = 152` bytes

No alternate header encoding is valid.

## 8. Block Serialization

A serialized block is exactly:

1. `serialized_header`
2. `tx_count` as canonical varint
3. `transactions[0]`
4. `transactions[1]`
5. ...
6. `transactions[tx_count - 1]`

There is no separate witness section, no auxpow section, no extension-block section, and no trailing consensus extension area.

### 8.1 `tx_count`

- `tx_count` MUST be encoded as a canonical varint.
- `tx_count` MUST be at least `1`.
- `tx_count` counts all transactions in the block, including the coinbase.

### 8.2 Transaction Bytes

Each transaction is serialized exactly as defined in `specs/TX.md`.

## 9. Transaction Ordering

Consensus ordering inside a block is:

1. `transactions[0]` MUST be the coinbase transaction
2. all remaining transactions MUST be non-coinbase transactions
3. all remaining transactions MUST be sorted by strict ascending lexical `txid`

Formally, for all `i` such that `1 <= i < tx_count - 1`:

- `txid(transactions[i]) < txid(transactions[i + 1])`

Comparison is performed on the raw 32-byte `txid` values.

## 10. Header Commitments

### 10.1 `tx_root`

`tx_root` commits to the ordered list:

- `txid(transactions[0])`
- `txid(transactions[1])`
- ...
- `txid(transactions[tx_count - 1])`

### 10.2 `auth_root`

`auth_root` commits to the ordered list:

- `authid(transactions[0])`
- `authid(transactions[1])`
- ...
- `authid(transactions[tx_count - 1])`

The ordering used for `auth_root` is identical to the ordering used for `tx_root`.

### 10.3 Canonical Root Construction

For both `tx_root` and `auth_root`, the canonical root construction is:

- defined over a non-empty ordered list of 32-byte leaf values,
- identical for both roots,
- defined normatively as follows.

Let:

- `Leaf(x)  = SHA256d(0x00 || x)`
- `Node(l,r) = SHA256d(0x01 || l || r)`
- `Solo(x)  = SHA256d(0x02 || x)`

Where:

- `x`, `l`, and `r` are each exactly 32 bytes,
- `||` is byte concatenation,
- `SHA256d(m)` means `SHA256(SHA256(m))`.

Root algorithm:

1. initialize `level[i] = Leaf(values[i])`
2. while `len(level) > 1`:
   - scan left to right
   - if two adjacent entries remain, combine them as `Node(left, right)`
   - if one final unpaired entry remains, combine it as `Solo(entry)`
3. the final remaining entry is the root

Because every valid block contains at least one transaction, the input list is never empty.

## 11. Proof-of-Work Header Hash

The block header hash used for proof of work is:

- `SHA256d(serialized_header)`

where `serialized_header` is exactly the 152-byte header defined in §7.

This document defines only the bytes hashed.

Interpretation of `nBits` and any required relationship between `nBits` and block height are defined elsewhere.

## 12. Serialized Block Size

`block_size` is the exact number of bytes in the canonical serialized block defined in §8.

It includes:

- the full serialized header,
- the encoded `tx_count`,
- every byte of every serialized transaction.

It excludes:

- transport framing,
- p2p message headers,
- compression wrappers,
- storage-layer metadata,
- undo data,
- indexes,
- caches.

## 13. Consensus Non-Features of the Block Format

The BPU block format defined here does **not** include:

- segregated witness sections
- extension blocks
- auxpow payloads
- embedded merkle branches in the block body
- witness commitments
- script commitments
- consensus trailing metadata areas
- uncle or ommer lists

## 14. Cross-References

This specification is intended to be read together with:

- `SPEC.md`
- `specs/TX.md`
- `specs/SERIALIZATION.md`
- `specs/UTXO.md`
- `specs/ASERT.md`
- `specs/ABLA.md`
- `specs/PARAMS.md`
