package mempool

import (
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"testing"
)

func signerPubKey(seed byte) [32]byte {
	return crypto.XOnlyPubKeyFromSecret([32]byte{seed})
}

func testCoinbase(height uint64, outputs []types.TxOutput) types.Transaction {
	var extraNonce [types.CoinbaseExtraNonceLen]byte
	return types.Transaction{
		Base: types.TxBase{
			Version:            1,
			CoinbaseHeight:     &height,
			CoinbaseExtraNonce: &extraNonce,
			Outputs:            outputs,
		},
	}
}

func spendTx(t *testing.T, spenderSeed byte, prevOut types.OutPoint, value uint64, recipientSeed byte, fee uint64) types.Transaction {
	t.Helper()
	return signedSpendTx(t, spenderSeed, prevOut, value, recipientSeed, fee)
}

func signedSpendTx(tb testing.TB, spenderSeed byte, prevOut types.OutPoint, value uint64, recipientSeed byte, fee uint64) types.Transaction {
	tb.Helper()
	if fee >= value {
		tb.Fatalf("fee %d must be less than value %d", fee, value)
	}
	recipientPubKey := signerPubKey(recipientSeed)
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: value - fee, PubKey: recipientPubKey}},
		},
	}
	msg, err := consensus.Sighash(&tx, 0, []consensus.UtxoEntry{{
		ValueAtoms: value,
		PubKey:     signerPubKey(spenderSeed),
	}})
	if err != nil {
		tb.Fatal(err)
	}
	_, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	return tx
}

func signedXOnlySpendToOutputs(tb testing.TB, spenderSeed byte, prevOut types.OutPoint, value uint64, outputs []types.TxOutput) types.Transaction {
	tb.Helper()
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: outputs,
		},
	}
	msg, err := consensus.Sighash(&tx, 0, []consensus.UtxoEntry{
		consensus.UtxoEntryFromOutput(types.NewXOnlyOutput(value, signerPubKey(spenderSeed))),
	})
	if err != nil {
		tb.Fatal(err)
	}
	_, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{types.NewXOnlyAuthEntry(sig)}}
	return tx
}

func signedPQSpendToOutputs(tb testing.TB, prevOut types.OutPoint, value uint64, verificationKey []byte, privateKey []byte, outputs []types.TxOutput) types.Transaction {
	tb.Helper()
	pqLock := consensus.PQLock(verificationKey)
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: outputs,
		},
	}
	msg, err := consensus.Sighash(&tx, 0, []consensus.UtxoEntry{
		consensus.UtxoEntryFromOutput(types.NewPQLockOutput(value, pqLock)),
	})
	if err != nil {
		tb.Fatal(err)
	}
	signature, err := crypto.SignMLDSA65(privateKey, msg[:])
	if err != nil {
		tb.Fatal(err)
	}
	authPayload := append(append([]byte(nil), verificationKey...), signature...)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{AuthPayload: authPayload}}}
	return tx
}

func orphanTx(t *testing.T, prevOut types.OutPoint, recipientSeed byte) types.Transaction {
	t.Helper()
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: 49, PubKey: signerPubKey(recipientSeed)}},
		},
		Auth: types.TxAuth{Entries: []types.TxAuthEntry{{}}},
	}
	return tx
}

func findSnapshot(t *testing.T, entries []SnapshotEntry, txid [32]byte) SnapshotEntry {
	t.Helper()
	for _, entry := range entries {
		if entry.TxID == txid {
			return entry
		}
	}
	t.Fatalf("missing txid %x", txid)
	return SnapshotEntry{}
}

func containsSnapshotTxID(entries []SnapshotEntry, txid [32]byte) bool {
	for _, entry := range entries {
		if entry.TxID == txid {
			return true
		}
	}
	return false
}

func commitTipHash(seed byte) [32]byte {
	return [32]byte{seed}
}

func makeThreeStepChain(t *testing.T, prevOut types.OutPoint) (types.Transaction, types.Transaction, [32]byte, [32]byte) {
	t.Helper()
	parent := spendTx(t, 1, prevOut, 50, 2, 1)
	parentTxID := consensus.TxID(&parent)
	child := spendTx(t, 2, types.OutPoint{TxID: parentTxID, Vout: 0}, 49, 3, 1)
	childTxID := consensus.TxID(&child)
	return parent, child, parentTxID, childTxID
}

func benchmarkSnapshotPool(tb testing.TB, txCount int) *Pool {
	tb.Helper()
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := make(consensus.UtxoSet, txCount)
	for i := 0; i < txCount; i++ {
		prevOut := types.OutPoint{TxID: [32]byte{byte(i + 1), byte((i + 1) >> 8)}, Vout: 0}
		utxos[prevOut] = consensus.UtxoEntry{ValueAtoms: 50, PubKey: signerPubKey(byte((i % 200) + 1))}
		tx := signedSpendTx(tb, byte((i%200)+1), prevOut, 50, byte((i%200)+2), 1)
		if _, err := pool.AcceptTx(tx, utxos, consensus.DefaultConsensusRules()); err != nil {
			tb.Fatalf("accept tx %d: %v", i, err)
		}
	}
	return pool
}

func benchmarkSelectionPool(tb testing.TB, txCount int) (*Pool, consensus.UtxoSet) {
	tb.Helper()
	pool := NewWithConfig(PoolConfig{
		MinRelayFeePerByte: 0,
		MaxTxSize:          1_000_000,
		MaxAncestors:       25,
		MaxDescendants:     25,
		MaxOrphans:         8,
	})
	utxos := make(consensus.UtxoSet, txCount)
	for i := 0; i < txCount; i++ {
		prevOut := types.OutPoint{TxID: [32]byte{byte(i + 1), byte((i + 1) >> 8)}, Vout: 0}
		utxos[prevOut] = consensus.UtxoEntry{ValueAtoms: 50, PubKey: signerPubKey(byte((i % 200) + 1))}
		tx := signedSpendTx(tb, byte((i%200)+1), prevOut, 50, byte((i%200)+2), 1)
		if _, err := pool.AcceptTx(tx, utxos, consensus.DefaultConsensusRules()); err != nil {
			tb.Fatalf("accept tx %d: %v", i, err)
		}
	}
	return pool, utxos
}
