package node

import (
	"log/slog"
	"slices"
	"strings"

	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
)

// relayScheduler owns peer fanout and batching policy for block/tx relay.
type relayScheduler struct {
	svc *Service
}

func (r *relayScheduler) RelayPeerStats() []PeerRelayStats {
	r.svc.peerMu.RLock()
	defer r.svc.peerMu.RUnlock()
	out := make([]PeerRelayStats, 0, len(r.svc.peers))
	for _, peer := range r.svc.peers {
		out = append(out, peer.telemetry.snapshot(peer.addr, peer.outbound, peer.queueDepths(), peer.pendingLocalRelayCount()))
	}
	slices.SortFunc(out, func(a, b PeerRelayStats) int {
		return strings.Compare(a.Addr, b.Addr)
	})
	return out
}

func (r *relayScheduler) broadcastInv(items []p2p.InvVector) {
	r.broadcastInvToPeers(r.svc.peerSnapshot(), items)
}

func (r *relayScheduler) broadcastInvToPeers(peers []*peerConn, items []p2p.InvVector) {
	if len(items) == 0 || len(peers) == 0 {
		return
	}
	for _, peer := range peers {
		filtered := peer.filterQueuedInv(items)
		if len(filtered) == 0 {
			continue
		}
		if err := peer.send(p2p.InvMessage{Items: filtered}); err != nil {
			r.svc.logger.Debug("relay inv enqueue failed",
				slog.String("addr", peer.addr),
				slog.Any("error", err),
			)
		}
	}
}

func (r *relayScheduler) broadcastAcceptedTxsToPeers(peers []*peerConn, accepted []mempool.AcceptedTx) {
	if len(peers) == 0 || len(accepted) == 0 {
		return
	}
	txids := make([][32]byte, 0, len(accepted))
	for _, item := range accepted {
		txids = append(txids, item.TxID)
	}
	for _, batch := range planTxRelayRecon(peers, txids) {
		if err := batch.peer.enqueueTxRecon(p2p.TxReconMessage{TxIDs: batch.txids}); err != nil {
			r.svc.logger.Debug("relay txrecon enqueue failed",
				slog.String("addr", batch.peer.addr),
				slog.Int("count", len(batch.txids)),
				slog.Any("error", err),
			)
		}
	}
}
