# Consensus Bootstrap Vectors

This folder contains deterministic vectors used by the bootstrap implementation.

- `consensus_bootstrap_vectors.json`
  - Fixed hashes for genesis block material in `fixtures/genesis`.
  - Used to verify deterministic txid/authid/header-hash and UTXO-root behavior.

When consensus encoding or hashing changes, these vectors must be regenerated in the same commit.

Additional replay fixtures live in `fixtures/chains/`:

- `regtest_bootstrap.json`
  - Deterministic post-genesis regtest chain with matching `utxo_root` commitments.
  - Used to replay fixture blocks through the node bootstrap path with strict root enforcement enabled.
