# Bitcoin Pure

Bitcoin Pure is a lean, payments-only proof-of-work protocol. The goal is to preserve the spirit of Bitcoin while making the system simpler, more scalable, and more focused on cash use.

## Protocol Highlights

- One output type, one spend rule, no scripting
- x-only Schnorr signatures for transactions
- Block headers commit not just to transactions, but to the live UTXO state itself
- Transactions separate base contents from authorization data via `txid` / `authid` and `tx_root` / `auth_root`
- Canonical transaction and block encoding
- SHA256d proof of work with per-block ASERT difficulty adjustment
- Adaptive blocksize with a 32 MB floor

## Node Reference Implementation Highlights

- Full node runtime with persisted chainstate, restart-safe replay, and best-chain tracking.
- Integrated miner and package-aware mempool management with orphan handling.
- Binary peer-to-peer transport with header-first sync, transaction reconciliation, batched relay, and short-ID block propagation with full-block fallback.
- Authenticated HTTP JSON-RPC for node control and automation.
- Public ASCII status page served directly from the node so you can watch tip, peers, mempool, mining, and host health from the server IP.

## Install

Ubuntu is the supported install target.

```bash
git clone https://github.com/Dinkum/Bitcoin-Pure.git
cd Bitcoin-Pure
sudo ./install
```

Useful flags:

- `--mining off` disables mining during install.
- `--peer <host:port>` seeds the node with a peer.
- `--update` pulls a fresh checkout from Git and deploys it atomically.

Examples:

```bash
sudo ./install --mining off
sudo ./install --peer 203.0.113.10:18444
sudo ./install --update --repo-url git@github.com:Dinkum/Bitcoin-Pure.git --ref main
```

What `./install` does:

- works with no required flags and sensible defaults
- is safe to rerun; existing config is preserved where possible
- verifies the host is Ubuntu
- installs system dependencies
- builds `bpu-cli`
- writes node config
- installs and enables a `systemd` service
- brings up the node with the public monitor page and RPC surface
- uses staged deploys and rollback checks for `--update`

## Structure

```text
Bitcoin-Pure/
├── install            # Ubuntu installer / updater entrypoint
├── scripts/           # staged deploy, bootstrap, and update helpers
├── cmd/bpu-cli/       # CLI entrypoint
├── internal/
│   ├── consensus/     # validation, PoW, subsidy, roots
│   ├── mempool/       # tx admission, packages, mining candidates
│   ├── node/          # service runtime, sync, RPC, mining, dashboard
│   ├── p2p/           # wire protocol, relay, peer transport
│   ├── storage/       # persisted chainstate and block metadata
│   └── types/         # canonical node data structures
├── fixtures/          # deterministic test and replay fixtures
└── version.json       # release version source of truth
```

## Operator Surface

- `GET /` serves the cached public ASCII node monitor.
- `POST /` serves authenticated JSON-RPC.
- `bpu-cli` exposes local chain, tx, block, and node commands.
