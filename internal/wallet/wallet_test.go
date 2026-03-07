package wallet

import (
	"errors"
	"path/filepath"
	"testing"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
)

func TestCreateWalletAndReceiveAddress(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	entry, first, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	if entry.Name != "alice" {
		t.Fatalf("wallet name = %q, want alice", entry.Name)
	}
	if first.Index != 0 || first.Change {
		t.Fatalf("first address = %+v, want index 0 external", first)
	}
	second, err := store.NewReceiveAddress("alice")
	if err != nil {
		t.Fatalf("NewReceiveAddress: %v", err)
	}
	if second.Index != 1 || second.Change {
		t.Fatalf("second address = %+v, want index 1 external", second)
	}
	reopened, err := Open(filepath.Join(filepath.Dir(store.path), StoreFileName))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	loaded, err := reopened.Wallet("alice")
	if err != nil {
		t.Fatalf("Wallet: %v", err)
	}
	if len(loaded.Addresses) != 2 {
		t.Fatalf("address count = %d, want 2", len(loaded.Addresses))
	}
	if latest := loaded.LatestReceiveAddress(); latest == nil || latest.Index != 1 {
		t.Fatalf("latest receive address = %+v, want index 1", latest)
	}
}

func TestBuildSendCreatesSignedTransactionAndChange(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, first, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	keyHash, err := ParseAddress(first.Address)
	if err != nil {
		t.Fatalf("ParseAddress: %v", err)
	}
	dest, err := store.NewReceiveAddress("alice")
	if err != nil {
		t.Fatalf("NewReceiveAddress: %v", err)
	}
	plan, err := store.BuildSend("alice", dest.Address, 70, 2, []SpendableUTXO{
		{
			OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
			Value:    50,
			KeyHash:  keyHash,
		},
		{
			OutPoint: types.OutPoint{TxID: [32]byte{2}, Vout: 1},
			Value:    30,
			KeyHash:  keyHash,
		},
	})
	if err != nil {
		t.Fatalf("BuildSend: %v", err)
	}
	if len(plan.Inputs) != 2 {
		t.Fatalf("inputs = %d, want 2", len(plan.Inputs))
	}
	if plan.Change != 8 {
		t.Fatalf("change = %d, want 8", plan.Change)
	}
	if len(plan.Transaction.Base.Outputs) != 2 {
		t.Fatalf("outputs = %d, want 2", len(plan.Transaction.Base.Outputs))
	}
	view := consensus.UtxoSet{
		plan.Inputs[0].OutPoint: {ValueAtoms: plan.Inputs[0].Value, KeyHash: mustParseKeyHash(plan.Inputs[0].Address.KeyHashHex)},
		plan.Inputs[1].OutPoint: {ValueAtoms: plan.Inputs[1].Value, KeyHash: mustParseKeyHash(plan.Inputs[1].Address.KeyHashHex)},
	}
	if _, err := consensus.ValidateTx(&plan.Transaction, view, consensus.DefaultConsensusRules()); err != nil {
		t.Fatalf("ValidateTx: %v", err)
	}
	loaded, err := store.Wallet("alice")
	if err != nil {
		t.Fatalf("Wallet: %v", err)
	}
	if len(loaded.Addresses) != 3 {
		t.Fatalf("wallet addresses = %d, want 3 including change", len(loaded.Addresses))
	}
	if !loaded.Addresses[2].Change {
		t.Fatal("expected generated change address to be marked as change")
	}
}

func TestReconcilePendingDropsMissingMempoolTransactions(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, first, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	keyHash, err := ParseAddress(first.Address)
	if err != nil {
		t.Fatalf("ParseAddress: %v", err)
	}
	plan, err := store.BuildSend("alice", first.Address, 10, 1, []SpendableUTXO{{
		OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
		Value:    50,
		KeyHash:  keyHash,
	}})
	if err != nil {
		t.Fatalf("BuildSend: %v", err)
	}
	spent := make([]types.OutPoint, 0, len(plan.Inputs))
	for _, input := range plan.Inputs {
		spent = append(spent, input.OutPoint)
	}
	if err := store.MarkSubmitted("alice", plan.TransactionID, spent); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	removed, err := store.ReconcilePending("alice", map[[32]byte]struct{}{})
	if err != nil {
		t.Fatalf("ReconcilePending: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	loaded, err := store.Wallet("alice")
	if err != nil {
		t.Fatalf("Wallet: %v", err)
	}
	if len(loaded.Pending) != 0 {
		t.Fatalf("pending len = %d, want 0", len(loaded.Pending))
	}
}

func TestBuildSendRejectsInsufficientFunds(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, first, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	keyHash, err := ParseAddress(first.Address)
	if err != nil {
		t.Fatalf("ParseAddress: %v", err)
	}
	_, err = store.BuildSend("alice", first.Address, 100, 1, []SpendableUTXO{{
		OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
		Value:    50,
		KeyHash:  keyHash,
	}})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("BuildSend err = %v, want ErrInsufficientFunds", err)
	}
}
