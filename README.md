# Bitcoin Pure (BPU)

Bitcoin Pure is a minimal, payments-only proof-of-work chain implementation.

## Ubuntu install
```bash
git clone https://github.com/Dinkum/Bitcoin-Pure.git
cd Bitcoin-Pure
sudo ./install

# Optional:
sudo ./install --mining off
sudo ./install --peer 203.0.113.10:18444
sudo ./install --update
sudo ./install --update --repo-url git@github.com:Dinkum/Bitcoin-Pure.git --ref main
```

## Scope (current)
- Canonical consensus types + strict encoding
- Scriptless transaction validation
- Block validation core (PoW target, subsidy/fees, merkle roots)
- Utreexo-style deterministic `utxo_root` computation with strict header enforcement
- Fixture-driven genesis bootstrap, header-chain replay, headers-first sync, and full-block replay for node and CLI
- Pebble-backed persisted chainstate with block index, undo data, best-chain selection, and reorg support
- Long-running node service with authenticated HTTP JSON-RPC, bounded binary P2P transport, peer sync, and rotating `node.log` runtime logging
- Ubuntu `./install` builds and enables a systemd-managed node with a public cached ASCII-only status page on `GET /` alongside authenticated JSON-RPC on `POST /`
- `./install --update` stages a fresh release from Git, verifies it, swaps the live node atomically, and rolls back automatically if the updated service fails health checks
- Dependency-aware in-memory mempool with orphan handling, tx relay policy, package-aware block assembly, and continuous mining
- Checked-in benchmark suite covering direct submit, RPC batch decode, chained packages, orphan storms, block-template rebuilds, and multi-node relay timing
- Deterministic vectors and tests
