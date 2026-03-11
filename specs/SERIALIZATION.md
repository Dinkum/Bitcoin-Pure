# SERIALIZATION.md

Status: normative consensus specification for shared binary encoding rules used
by Bitcoin Pure transaction, block, and target serialization.

## 1. Scope

This document defines:

- fixed-width integer byte order,
- raw fixed-width byte-string encoding,
- canonical varint encoding,
- compact target decoding and encoding,
- shared concatenation rules referenced by other consensus documents.

This document does **not** define:

- transaction structure,
- block structure,
- Merkle construction,
- sighash semantics,
- proof-of-work validity,
- UTXO state transitions.

Those subjects are defined elsewhere.

## 2. Normative Language

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** in this document
are normative requirement words.

## 3. Fixed-Width Integers

Unless a field definition explicitly says otherwise:

- `uint32` encodes as 4 little-endian bytes.
- `uint64` encodes as 8 little-endian bytes.

No alternate byte order is valid.

## 4. Fixed-Width Byte Strings

Unless a field definition explicitly says otherwise:

- `bytes32` encodes as exactly 32 raw bytes.
- `bytes64` encodes as exactly 64 raw bytes.

These byte strings are serialized exactly as stored. There is no byte reversal,
hex normalization, or textual framing in consensus encoding.

## 5. Canonical Varint

Varints use the shortest possible encoding:

- values `0x00` through `0xfc`: one byte
- `0xfd` followed by `uint16`: values `0x00fd` through `0xffff`
- `0xfe` followed by `uint32`: values `0x0001_0000` through `0xffff_ffff`
- `0xff` followed by `uint64`: values `0x1_0000_0000` and above

Canonicality rules:

- a longer-than-necessary varint encoding is invalid
- a decoder MUST reject non-canonical varints
- counts and heights that reference varint in other consensus documents use
  this exact encoding

## 6. Compact Target Encoding

Bitcoin Pure uses the Bitcoin-style 32-bit compact target representation for
`nBits`.

Let `bits` be a 32-bit unsigned integer:

- `size = bits >> 24`
- `mantissa = bits & 0x007fffff`
- `negative = (bits & 0x00800000) != 0`

### 6.1 `bits_to_target(bits)`

`bits_to_target(bits)` is valid only if:

- `mantissa != 0`
- `negative == false`

Then:

1. initialize `target = mantissa`
2. if `size <= 3`, shift `target` right by `8 * (3 - size)` bits
3. else shift `target` left by `8 * (size - 3)` bits
4. if the resulting `target == 0`, the encoding is invalid

Any encoding that violates these rules is an invalid compact target encoding.

### 6.2 `target_to_bits(target)`

`target_to_bits(target)` is valid only if `target > 0`.

To encode:

1. let `bytes` be the minimal big-endian byte representation of `target`
2. let `size = len(bytes)`
3. if `bytes[0] >= 0x80`:
   - prefix one zero byte to `bytes`
   - increment `size` by 1
4. if `size <= 3`:
   - left-pad the mantissa within 3 bytes
5. else:
   - take the first 3 bytes as the mantissa
6. return `(size << 24) | mantissa`

No negative compact target encoding is valid in Bitcoin Pure consensus.

## 7. Concatenation Rule

Whenever another consensus document defines a byte string as:

- `a || b || c`

it means exact byte concatenation of the canonical encodings of `a`, `b`, and
`c`, in that order, with:

- no separators,
- no length prefixes unless explicitly specified,
- no textual encoding,
- no omitted zero bytes.

## 8. Cross-References

This specification is intended to be read together with:

- [SPEC.md](/Users/blake/Desktop/Bitcoin-Pure/SPEC.md)
- [TX.md](/Users/blake/Desktop/Bitcoin-Pure/specs/TX.md)
- [BLOCK.md](/Users/blake/Desktop/Bitcoin-Pure/specs/BLOCK.md)
- [ASERT.md](/Users/blake/Desktop/Bitcoin-Pure/specs/ASERT.md)
