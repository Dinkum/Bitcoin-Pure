# Bitcoin Pure (BPU)

Bitcoin Pure is a minimal, payments-only proof-of-work chain implementation.

## Ubuntu install
```bash
sudo apt-get update
sudo apt-get install -y git build-essential curl ca-certificates
curl -LO https://go.dev/dl/go1.26.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.26.0.linux-amd64.tar.gz
export PATH=/usr/local/go/bin:$PATH
git clone https://github.com/Dinkum/Bitcoin-Pure.git
cd Bitcoin-Pure
go test ./...
go run ./cmd/bpu-cli chain init --profile regtest
```

## Scope (current)
- Canonical consensus types + strict encoding
- Scriptless transaction validation
- Block validation core (PoW target, subsidy/fees, merkle roots)
- Utreexo-style deterministic `utxo_root` computation with strict header enforcement
- Fixture-driven genesis bootstrap, header-chain replay, headers-first sync, and full-block replay for node and CLI
- Pebble-backed persisted chainstate with block index, undo data, best-chain selection, and reorg support
- Long-running node service with authenticated HTTP JSON-RPC, bounded binary P2P transport, peer sync, and rotating `node.log` runtime logging
- Ubuntu server installs expose a cached ASCII-only node status page on `GET /` alongside authenticated JSON-RPC on `POST /`
- Dependency-aware in-memory mempool with orphan handling, tx relay policy, package-aware block assembly, and mining
- Checked-in benchmark suite covering direct submit, RPC batch decode, chained packages, orphan storms, block-template rebuilds, and multi-node relay timing
- Deterministic vectors and tests
