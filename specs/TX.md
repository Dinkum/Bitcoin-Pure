# TX.md

Status: normative consensus specification for Bitcoin Pure transaction structure and transaction encoding.

If an implementation conflicts with this document on a transaction-structure matter, this document wins until replaced by a newer normative transaction specification.

## 1. Scope

This document defines only:

- the logical structure of a transaction,
- the exact serialized structure of a transaction,
- the exact serialized structure of transaction inputs, outputs, and auth entries,
- the exact serialized structure of supported family-specific auth payload layouts,
- the structural distinction between coinbase and non-coinbase transactions,
- the base/auth split,
- the derivation of `txid` and `authid`.

This document does **not** define:

- spend validity,
- signature verification,
- sighash semantics,
- fee rules,
- amount-balance rules,
- duplicate-input rules,
- UTXO lookup rules,
- block-level ordering rules,
- block acceptance rules.

Those subjects are defined in other specifications.

## 2. Normative Language

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** in this document are normative requirement words.

## 3. External Definitions

The following items are defined outside this document:

- shared integer encoding, canonical varint encoding, and common serialization rules: `specs/SERIALIZATION.md`
- block-level placement, transaction ordering, and block commitments: `specs/BLOCK.md`
- sighash construction, spend verification, and transaction semantic validity: `SPEC.md` or successor consensus documents
- network-specific constants and version policy: `specs/PARAMS.md`

## 4. Informative Structure View

This section is informative only and does not define consensus encoding. Consensus encoding is defined only in the serialization sections below.

### 4.1 Non-Coinbase Transaction

```text
Transaction
├─ Base
│  ├─ version : uint32
│  ├─ input_count : varint
│  ├─ inputs[] : TxInput
│  │  ├─ prev_txid : bytes32
│  │  └─ prev_output_index : uint32
│  ├─ output_count : varint
│  └─ outputs[] : TxOutput
│     ├─ type : varint
│     ├─ value_atoms : uint64
│     └─ payload32 : bytes32
└─ Auth
   ├─ auth_count : varint
   └─ entries[] : TxAuthEntry
      ├─ auth_len : varint
      └─ auth_payload : bytes[auth_len]
```

### 4.2 Coinbase Transaction

```text
Transaction
├─ Base
│  ├─ version : uint32
│  ├─ input_count : varint = 0
│  ├─ coinbase_height : varint
│  ├─ coinbase_extra_nonce : bytes16
│  ├─ output_count : varint
│  └─ outputs[] : TxOutput
│     ├─ type : varint
│     ├─ value_atoms : uint64
│     └─ payload32 : bytes32
└─ Auth
   └─ auth_count : varint = 0
```

## 5. Transaction Data Model

A transaction consists of exactly two top-level parts:

1. `base`
2. `auth`

The split is consensus-visible and intentional:

- `txid` commits only to `base`
- `authid` commits only to `auth`

A serialized transaction is exactly:

- `base_encoding || auth_encoding`

with no additional wrapper or outer framing.

## 6. Common Field Types

### 6.1 `version`

- Type: `uint32`
- Encoding: little-endian

### 6.2 `prev_txid`

- Type: `bytes32`
- Encoding: raw 32 bytes

### 6.3 `prev_output_index`

- Type: `uint32`
- Encoding: little-endian

### 6.4 `value_atoms`

- Type: `uint64`
- Encoding: little-endian

### 6.5 `type`

- Type: canonical varint
- Meaning:
  - `OUTPUT_XONLY_P2PK = 0x00`
  - `OUTPUT_PQ_LOCK32  = 0x01`

### 6.6 `payload32`

- Type: `bytes32`
- Encoding: raw 32 bytes
- Meaning depends on `type`:
  - if `type == OUTPUT_XONLY_P2PK`
    - `payload32` is a 32-byte x-only secp256k1 public key
  - if `type == OUTPUT_PQ_LOCK32`
    - `payload32` is a 32-byte PQ lock commitment

### 6.7 `auth_len`

- Type: canonical varint
- Meaning: byte length of `auth_payload`

### 6.8 `auth_payload`

- Type: `bytes[auth_len]`
- Encoding: exactly `auth_len` raw bytes
- Raw transaction decoding is length-delimited only; family-specific interpretation is defined by the referenced output type and the supported auth payload layouts in §12.3.

### 6.9 `coinbase_height`

- Type: canonical varint
- Meaning: block height committed by a coinbase transaction

`coinbase_height` is part of `base` and therefore part of `txid`.

### 6.10 `coinbase_extra_nonce`

- Type: `bytes16`
- Encoding: raw 16 bytes
- Meaning: fixed-width miner-controlled coinbase search-space field

`coinbase_extra_nonce` is part of `base` and therefore part of `txid`.

## 7. Structural Components

### 7.1 `TxInput`

A transaction input contains exactly:

1. `prev_txid`
2. `prev_output_index`

Serialized size of one input:

- `32 + 4 = 36` bytes

There is no `scriptSig`, no `sequence`, no annex, and no witness structure.

### 7.2 `TxOutput`

A transaction output contains exactly:

1. `type`
2. `value_atoms`
3. `payload32`

Serialized size of one output:

- `varint(type) + 8 + 32` bytes

There is exactly one consensus output envelope.
Family semantics are selected by `type`.

### 7.3 `TxAuthEntry`

A transaction auth entry contains exactly:

1. `auth_len`
2. `auth_payload`

Serialized size of one auth entry:

- `varint(auth_len) + auth_len` bytes

Auth entries are positional:

- auth entry `i` corresponds to non-coinbase input `i`
- raw transaction decoding of auth entries is determined by `auth_len` only

## 8. Coinbase vs Non-Coinbase Shape

Transaction kind is determined structurally.

### 8.1 Non-Coinbase Transaction Shape

A non-coinbase transaction has:

- `input_count >= 1`
- no `coinbase_height` field
- no `coinbase_extra_nonce` field
- `auth_count == input_count`
- `output_count >= 1`

### 8.2 Coinbase Transaction Shape

A coinbase transaction has:

- `input_count == 0`
- a present `coinbase_height` field
- a present `coinbase_extra_nonce` field
- `auth_count == 0`
- `output_count >= 1`

There is no arbitrary coinbase script field in the transaction structure defined here.
The only miner-controlled coinbase payload field is the fixed-width `coinbase_extra_nonce`.

## 9. Primitive Encoding Rules

- `uint32` and `uint64` encode little-endian
- counts encode as canonical varints
- `type` encodes as canonical varint
- `auth_len` encodes as canonical varint
- `alg_id` encodes as canonical varint
- `vk_len` encodes as canonical varint
- `sig_len` encodes as canonical varint
- `coinbase_height` encodes as canonical varint
- `coinbase_extra_nonce` encodes as exactly 16 raw bytes
- byte arrays encode as raw bytes with no prefix unless explicitly stated
- non-canonical varints are invalid encodings

### 9.1 Canonical Varint

Varint uses the shortest possible encoding:

- `0x00` to `0xfc`: one byte
- `0xfd` + `uint16`: values `0x00fd` through `0xffff`
- `0xfe` + `uint32`: values `0x0001_0000` through `0xffff_ffff`
- `0xff` + `uint64`: values `0x1_0000_0000` and above

A longer-than-necessary varint encoding is invalid.

## 10. Base Encoding

The transaction `base` region encodes in one of exactly two forms.

### 10.1 Non-Coinbase Base Encoding

A non-coinbase `base` is exactly:

1. `version`
2. `input_count` as canonical varint
3. each input in order
4. `output_count` as canonical varint
5. each output in order

Where:

- `input_count >= 1`
- `output_count >= 1`

### 10.2 Coinbase Base Encoding

A coinbase `base` is exactly:

1. `version`
2. `input_count` as canonical varint, where `input_count == 0`
3. `coinbase_height` as canonical varint
4. `coinbase_extra_nonce` as 16 raw bytes
5. `output_count` as canonical varint
6. each output in order

Where:

- `output_count >= 1`

## 11. Input and Output Encoding

### 11.1 Input Encoding

Each input encodes exactly as:

1. `prev_txid` as 32 raw bytes
2. `prev_output_index` as 4 little-endian bytes

### 11.2 Output Encoding

Each output encodes exactly as:

1. `type` as canonical varint
2. `value_atoms` as 8 little-endian bytes
3. `payload32` as 32 raw bytes

## 12. Auth Encoding

The transaction `auth` region is exactly:

1. `auth_count` as canonical varint
2. each auth entry in input order

Each auth entry encodes exactly as:

1. `auth_len` as canonical varint
2. `auth_payload` as exactly `auth_len` raw bytes

Additional auth bytes are not permitted.

Transactions MUST remain decodable without referenced-UTXO lookup; therefore auth entries are framed only by `auth_len`, and referenced output type does not affect raw transaction decoding.

### 12.1 Non-Coinbase Auth Shape

For a non-coinbase transaction:

- `auth_count == input_count`

### 12.2 Coinbase Auth Shape

For a coinbase transaction:

- `auth_count == 0`
- no auth entries follow

### 12.3 Supported Family Auth Payload Layouts (Structure Only)

This subsection defines supported `auth_payload` byte layouts only.
Whether a given `auth_payload` is valid for a referenced output type is defined in `SPEC.md`.

If the referenced output type is `OUTPUT_XONLY_P2PK`, the interpreted `auth_payload` layout is exactly:

1. `signature` as 64 raw bytes

If the referenced output type is `OUTPUT_PQ_LOCK32`, the interpreted `auth_payload` layout is exactly:

1. `alg_id` as canonical varint
2. `vk_len` as canonical varint
3. `vk` as exactly `vk_len` raw bytes
4. `sig_len` as canonical varint
5. `sig` as exactly `sig_len` raw bytes

For a supported family layout, the interpreted layout MUST consume the entire `auth_payload`; trailing bytes are invalid.

## 13. Full Transaction Encoding

A serialized transaction is exactly:

1. `base_encoding`
2. immediately followed by `auth_encoding`

There is no witness section, no extension area, no annex area, and no trailing consensus metadata.

A transaction decoder MUST consume the entire serialized transaction. Trailing bytes after `auth_encoding` are not part of the transaction.

## 14. Derived Identifiers

### 14.1 `txid`

`txid` is defined as:

- `txid = SHA256d(base_encoding)`

where `base_encoding` is exactly the byte sequence defined in §10.

### 14.2 `authid`

`authid` is defined as:

- `authid = SHA256d(auth_encoding)`

where `auth_encoding` is exactly the byte sequence defined in §12.

For a coinbase transaction, `authid` is the hash of the canonical zero-entry auth encoding:

- `auth_count = 0` encoded as canonical varint
- followed by no auth entries

No alternate identifier construction is valid.

## 15. Structural Non-Features of the Transaction Format

The BPU transaction format defined here does **not** include:

- script
- `scriptSig`
- `scriptPubKey`
- opcodes
- witness stack
- annex data
- locktime
- sequence
- timelocks
- hashlocks
- multisig fields in consensus
- `OP_RETURN`
- sighash flags
- a script interpreter
- a top-level auth-kind discriminator

## 16. Cross-References

This specification is intended to be read together with:

- `SPEC.md`
- `specs/BLOCK.md`
- `specs/SERIALIZATION.md`
- `specs/PARAMS.md`
