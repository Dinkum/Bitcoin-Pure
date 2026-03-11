# PARAMS.md

Status: normative chain-profile and network-parameter specification for
Bitcoin Pure.

## 1. Scope

This document defines:

- named chain profiles,
- profile-specific genesis and proof-of-work parameters,
- profile-specific network magic values,
- current version policy defaults.

This document does **not** define:

- transaction structure,
- block structure,
- difficulty adjustment mathematics,
- adaptive block-size mathematics,
- wallet policy.

Those subjects are defined elsewhere.

## 2. Normative Language

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** in this document
are normative requirement words.

## 3. Shared Mainline Constants

Unless a profile section explicitly overrides a field, Bitcoin Pure profiles
use:

- `target_spacing_secs = 600`
- `asert_half_life_secs = 86400`
- `halving_interval = 2_500_000`
- `initial_subsidy_atoms = 1_000_000_000_000`
- `block_size_floor = 32_000_000`
- `coinbase_maturity = 100`
- `address_prefix = "bpu"`
- `recommended_tx_version = 1`
- `recommended_block_version = 1`

## 4. Version Policy

Transaction and block `version` fields are serialized `uint32` values.

Current Bitcoin Pure consensus does not define any version-gated block or
transaction behavior. Therefore:

- no chain profile currently rejects a transaction solely because its
  `version != 1`
- no chain profile currently rejects a block solely because its
  `version != 1`

However:

- the canonical shipped genesis fixtures use version `1`
- miners, wallets, and test fixtures SHOULD emit version `1` unless a future
  consensus upgrade defines another value explicitly

## 5. Chain Profiles

### 5.1 `mainnet`

- `profile_id = "mainnet"`
- `pow_limit_bits = 0x1d00ffff`
- `genesis_bits = 0x1d00ffff`
- `genesis_timestamp = 1700000000`
- `p2p_magic = 0x4250554d`
- `genesis_header_hash = 8bc7ff3501b964dec7833036705a34540569e10951270930d8406fe75f655277`
- `genesis_txid = ac8e8c460defd637c2049fe58f946978e6958600033cab8fb733607777d7a104`
- `genesis_authid = 1406e05881e299367766d313e26c05564ec91bf721d31726bd6e46e60689539a`
- `utxo_root_after_genesis = b0c9fc2b98acde1ff8a76f094c256ae725d679e80e50241e538102727e9b8134`

### 5.2 `regtest`

- `profile_id = "regtest"`
- `pow_limit_bits = 0x207fffff`
- `genesis_bits = 0x207fffff`
- `genesis_timestamp = 1700000600`
- `p2p_magic = 0x42505552`
- `genesis_header_hash = 2cf876d357f510bf0aa1a9c07937b21b5aa26204ccbd627e1e4e90662851be6c`
- `genesis_txid = 731dff1ef935b0a85488f896c065f059b5335da23eda1aaadd188729a16d9b5a`
- `genesis_authid = 1406e05881e299367766d313e26c05564ec91bf721d31726bd6e46e60689539a`
- `utxo_root_after_genesis = cb53dac1dae7f767b24f19c26b45464b928a70e57ee33666dbc660c7a1ccb31e`

### 5.3 `regtest_medium`

- `profile_id = "regtest_medium"`
- `pow_limit_bits = 0x1e026ef9`
- `genesis_bits = 0x1e026ef9`
- `genesis_timestamp = 1700000600`
- `p2p_magic = 0x42505553`
- `genesis_header_hash = 0000006992a1d3477f4b3f37d15c866df7b634cd1e854319f5e9b1bba5e902a8`
- `genesis_txid = 731dff1ef935b0a85488f896c065f059b5335da23eda1aaadd188729a16d9b5a`
- `genesis_authid = 1406e05881e299367766d313e26c05564ec91bf721d31726bd6e46e60689539a`
- `utxo_root_after_genesis = cb53dac1dae7f767b24f19c26b45464b928a70e57ee33666dbc660c7a1ccb31e`

### 5.4 `regtest_hard`

- `profile_id = "regtest_hard"`
- `pow_limit_bits = 0x1d4ddf3d`
- `genesis_bits = 0x1d4ddf3d`
- `genesis_timestamp = 1700000600`
- `p2p_magic = 0x42505548`
- `genesis_header_hash = 00000034a4039d7fafe27aa632eb603a14247e4aee13acd9882525500b455705`
- `genesis_txid = 731dff1ef935b0a85488f896c065f059b5335da23eda1aaadd188729a16d9b5a`
- `genesis_authid = 1406e05881e299367766d313e26c05564ec91bf721d31726bd6e46e60689539a`
- `utxo_root_after_genesis = cb53dac1dae7f767b24f19c26b45464b928a70e57ee33666dbc660c7a1ccb31e`

## 6. Genesis Fixture Boundary

The profile parameters above and the canonical shipped genesis fixtures MUST
agree.

At minimum, a valid shipped genesis fixture for a profile MUST match:

- `profile_id`
- `genesis_timestamp`
- `genesis_bits`
- `genesis_header_hash`
- `genesis_txid`
- `genesis_authid`
- `utxo_root_after_genesis`

## 7. Cross-References

This specification is intended to be read together with:

- [SPEC.md](/Users/blake/Desktop/Bitcoin-Pure/SPEC.md)
- [TX.md](/Users/blake/Desktop/Bitcoin-Pure/specs/TX.md)
- [BLOCK.md](/Users/blake/Desktop/Bitcoin-Pure/specs/BLOCK.md)
- [ASERT.md](/Users/blake/Desktop/Bitcoin-Pure/specs/ASERT.md)
- [ABLA.md](/Users/blake/Desktop/Bitcoin-Pure/specs/ABLA.md)
