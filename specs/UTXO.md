# UTXO.md

Status: normative consensus specification for the canonical live-UTXO
commitment and `utxo_root` derivation used in Bitcoin Pure block headers.

## 1. Scope

This document defines:

- the canonical per-UTXO leaf encoding,
- the exact keyed-tree commitment structure,
- the exact derivation of `utxo_root` from a live UTXO set,
- the empty-set root.

This document does **not** define:

- transaction validity,
- block validity beyond `utxo_root` matching,
- coin selection,
- storage layout,
- optional proof-serving indexes,
- non-consensus accumulator services.

Those subjects are defined elsewhere.

## 2. Normative Language

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** in this document
are normative requirement words.

## 3. External Definitions

The following items are defined outside this document:

- outpoints and transaction identifiers: [TX.md](/Users/blake/Desktop/Bitcoin-Pure/specs/TX.md)
- block headers and `utxo_root` field placement: [BLOCK.md](/Users/blake/Desktop/Bitcoin-Pure/specs/BLOCK.md)
- shared integer and byte encoding rules: [SERIALIZATION.md](/Users/blake/Desktop/Bitcoin-Pure/specs/SERIALIZATION.md)

## 4. Canonical Live Coin Record

The live UTXO set is a keyed map from outpoint to coin record.

For Bitcoin Pure v1, the canonical coin record committed by `utxo_root`
contains exactly:

1. `value_atoms` as `uint64`
2. `pubkey` as `bytes32`

Coinbase maturity and other chain semantics may depend on data maintained by
the validating node, but they are not part of the committed `utxo_root` leaf
payload in v1.

## 5. Key Encoding

Each UTXO is keyed by outpoint:

- `outpoint = (txid, vout)`

The canonical key encoding is exactly 36 bytes:

1. `txid` as 32 raw bytes
2. `vout` as 4 little-endian bytes

Call this 36-byte value `key(outpoint)`.

Ordering rule:

- keys are compared lexicographically over the full 36-byte encoding

## 6. Leaf and Branch Hashing

Tagged-hash domain separators:

- `UTXOLeafTag   = "BPU/UtxoLeafV1"`
- `UTXOBranchTag = "BPU/UtxoBranchV1"`
- `UTXORootTag   = "BPU/UtxoRootV1"`

For one live UTXO leaf:

- `LeafHash(utxo) = TaggedHash(UTXOLeafTag, key(outpoint) || value_atoms || pubkey)`

where:

- `value_atoms` is the canonical 8-byte little-endian `uint64`
- `pubkey` is the canonical 32-byte raw x-only public key

For a binary branch:

- `BranchHash(left, right) = TaggedHash(UTXOBranchTag, left || right)`

where `left` and `right` are each 32-byte child hashes.

## 7. Tree Shape

The canonical structure is a keyed binary radix tree over the 288-bit outpoint
key space.

Bit-walk rules:

- the tree examines key bits from most significant to least significant
- bit index `0` is the top bit of byte `0`
- bit index `287` is the low bit of byte `35`

Leaf placement:

- a leaf occupies the unique path determined by its full 288-bit key

Unary-node rule:

- if a node has only one child, the node hash is exactly that child hash
- there is no extra unary-node hashing step

Binary-node rule:

- if a node has both left and right children, the node hash is
  `BranchHash(left.hash, right.hash)`

Duplicate-key rule:

- duplicate outpoints are invalid when deriving the canonical root

## 8. Root Construction

Input:

- the current live UTXO set as a set of `(outpoint, value_atoms, pubkey)`
  tuples

Algorithm:

1. if the live UTXO set is empty:
   - `utxo_root = TaggedHash(UTXORootTag, "")`
2. else:
   - encode every live UTXO as a keyed leaf
   - sort the leaves by `key(outpoint)` ascending
   - reject the set if any two adjacent sorted leaves have identical keys
   - build the keyed binary radix tree over the full 288-bit keys
   - let `inner_root` be the hash of the resulting tree root node
   - `utxo_root = TaggedHash(UTXORootTag, inner_root)`

All fully-valid nodes with the same live UTXO set MUST derive the same
`utxo_root`.

## 9. Relationship to Block Validation

The `utxo_root` committed in a block header is the root of the live UTXO set
after applying that block’s accepted state transition.

The exact block transition rules are defined elsewhere, but once the resulting
live UTXO set is determined, `utxo_root` MUST be computed exactly by this
document.

## 10. Implementation Freedom

Nodes MAY store UTXOs in any internal database or cache layout.

Nodes MAY maintain additional non-consensus structures, including:

- compact-state accumulators,
- locality-preserving indexes,
- snapshot-specific layouts,
- auxiliary checksums.

No such internal structure changes the consensus meaning of `utxo_root`.

## 11. Cross-References

This specification is intended to be read together with:

- [SPEC.md](/Users/blake/Desktop/Bitcoin-Pure/SPEC.md)
- [TX.md](/Users/blake/Desktop/Bitcoin-Pure/specs/TX.md)
- [BLOCK.md](/Users/blake/Desktop/Bitcoin-Pure/specs/BLOCK.md)
- [SERIALIZATION.md](/Users/blake/Desktop/Bitcoin-Pure/specs/SERIALIZATION.md)
