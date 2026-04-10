# Bitcoin Pure

Bitcoin Pure is a lean, payments-only proof-of-work protocol. The goal is to preserve the spirit of Bitcoin while making the system simpler, more scalable, and more focused on cash use.

** DISCLAIMER: This project is in beta and very much a proof of concept **

## Protocol Highlights

- One output type, one spend rule, no scripting
- x-only Schnorr signatures for transactions
- Block headers commit not just to transactions, but to the live UTXO state itself
- Transactions separate base contents from authorization data via `txid` / `authid` and `tx_root` / `auth_root`
- Canonical transaction and block encoding
- SHA256d proof of work with per-block ASERT difficulty adjustment
- Adaptive blocksize with a 32 MB floor

## Post-Quantum Direction

Bitcoin Pure no longer treats post-quantum safety as a distant problem. Recent public moves by Google and Cloudflare, including 2029 migration targets and an explicit focus on authentication and signatures, reinforce that long-lived signature systems need a transition plan now, not later.

See:
- [Google: Quantum frontiers may be closer than they appear](https://blog.google/innovation-and-ai/technology/safety-security/cryptography-migration-timeline/)
- [Cloudflare: Post-quantum roadmap](https://blog.cloudflare.com/post-quantum-roadmap/)

BPU’s direction is to move from a pubkey-native coin model toward a typed lock model. The goal is to preserve the current x-only secp256k1 lane for now, add an optional compact post-quantum lane, and make any later hard cutover a clean simplification rather than a second architectural redesign.

No cutoff heights, cutoff dates, or forced migration rules are implemented today. Any future hard cutover would be introduced explicitly and separately.

## Node Reference Implementation Highlights

- Full node runtime with persisted chainstate, restart-safe replay, and best-chain tracking.
- Integrated miner and package-aware mempool management with orphan handling.
- Local wallet management for receive addresses, balance/history, fee estimation, and signed transaction send flow.
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

Fresh installs keep mining off until `miner_pubkey_hex` is configured, so a new node does not mine to an unknown destination by default.

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
- keeps mining disabled until a destination key hash is configured
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
│   ├── types/         # canonical node data structures
│   └── wallet/        # local wallet storage, receive addresses, signing
├── fixtures/          # deterministic test and replay fixtures
└── version.json       # release version source of truth
```

## Operator Surface

- `GET /` serves the cached public ASCII node monitor.
- `POST /` serves authenticated JSON-RPC.
- `bpu-cli` exposes local chain, wallet, tx, block, and node commands.
