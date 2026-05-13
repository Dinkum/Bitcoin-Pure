package wallet

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

func TestCashAddrReferenceExample(t *testing.T) {
	payload, err := hex.DecodeString("211b74ca4686f81efda5641767fc84ef16dafe0b")
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	addr, err := encodeCashAddress("bitcoincash", cashAddrTypeP2PK, payload)
	if err != nil {
		t.Fatalf("encodeCashAddress: %v", err)
	}
	if want := "bitcoincash:qqs3kax2g6r0s8ha54jpwelusnh3dkh7pvu23rzrru"; addr != want {
		t.Fatalf("cashaddr = %q, want %q", addr, want)
	}
}

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
	if !strings.HasPrefix(first.Address, "bpu:") {
		t.Fatalf("first address = %q, want cashaddr prefix", first.Address)
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

func TestCreateAndImportWalletReturnClonedWallets(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	created, _, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	created.Addresses[0].Address = "mutated"
	loaded, err := store.Wallet("alice")
	if err != nil {
		t.Fatalf("Wallet: %v", err)
	}
	if loaded.Addresses[0].Address == "mutated" {
		t.Fatal("CreateWallet returned mutable store internals")
	}

	export := ExportFile{Wallet: loaded}
	imported, err := store.ImportWallet(export, "bob")
	if err != nil {
		t.Fatalf("ImportWallet: %v", err)
	}
	imported.Addresses[0].Address = "also-mutated"
	export.Wallet.Addresses[0].Address = "export-mutated"
	export.Wallet.Addresses[0].PayloadHex = "export-payload-mutated"
	reloaded, err := store.Wallet("bob")
	if err != nil {
		t.Fatalf("Wallet bob: %v", err)
	}
	if reloaded.Addresses[0].Address == "also-mutated" || reloaded.Addresses[0].Address == "export-mutated" || reloaded.Addresses[0].PayloadHex == "export-payload-mutated" {
		t.Fatal("ImportWallet shared wallet slice backing arrays")
	}
}

func TestBuildPQAuthPayloadUsesSpecFixedLayout(t *testing.T) {
	verificationKey := make([]byte, bpcrypto.MLDSA65VerificationKeySize)
	signature := make([]byte, bpcrypto.MLDSA65SignatureSize)
	for i := range verificationKey {
		verificationKey[i] = byte(i)
	}
	for i := range signature {
		signature[i] = byte(255 - i)
	}

	payload := buildPQAuthPayload(verificationKey, signature)
	if len(payload) != bpcrypto.MLDSA65VerificationKeySize+bpcrypto.MLDSA65SignatureSize {
		t.Fatalf("payload len = %d, want %d", len(payload), bpcrypto.MLDSA65VerificationKeySize+bpcrypto.MLDSA65SignatureSize)
	}
	if got := hex.EncodeToString(payload[:len(verificationKey)]); got != hex.EncodeToString(verificationKey) {
		t.Fatal("payload does not begin with the raw verification key")
	}
	if got := hex.EncodeToString(payload[len(verificationKey):]); got != hex.EncodeToString(signature) {
		t.Fatal("payload does not end with the raw signature")
	}
}

func TestParseAddressRejectsMixedCase(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, first, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	mixed := strings.ToUpper(first.Address[:4]) + first.Address[4:]
	if _, err := ParseAddress(mixed); !errors.Is(err, ErrInvalidAddress) {
		t.Fatalf("ParseAddress mixed case err = %v, want ErrInvalidAddress", err)
	}
}

func TestOpenNormalizesLegacyStoredAddressStrings(t *testing.T) {
	path := filepath.Join(t.TempDir(), StoreFileName)
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	created, _, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	legacy := created
	legacy.Addresses[0].Address = "bpu1" + legacy.Addresses[0].PubKeyHex
	buf, err := json.Marshal([]Wallet{legacy})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(path, append(buf, '\n'), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	loaded, err := reopened.Wallet("alice")
	if err != nil {
		t.Fatalf("Wallet: %v", err)
	}
	if got := loaded.Addresses[0].Address; !strings.HasPrefix(got, "bpu:") {
		t.Fatalf("normalized address = %q, want cashaddr", got)
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
			PubKey:   keyHash,
		},
		{
			OutPoint: types.OutPoint{TxID: [32]byte{2}, Vout: 1},
			Value:    30,
			PubKey:   keyHash,
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
		plan.Inputs[0].OutPoint: {ValueAtoms: plan.Inputs[0].Value, PubKey: mustParsePubKey(plan.Inputs[0].Address.PubKeyHex)},
		plan.Inputs[1].OutPoint: {ValueAtoms: plan.Inputs[1].Value, PubKey: mustParsePubKey(plan.Inputs[1].Address.PubKeyHex)},
	}
	if _, err := consensus.ValidateTxWithParams(&plan.Transaction, view, consensus.MainnetParams(), consensus.DefaultConsensusRules()); err != nil {
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

func TestBuildSendRejectsAmountFeeOverflow(t *testing.T) {
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
	_, err = store.BuildSend("alice", first.Address, ^uint64(0), 1, []SpendableUTXO{{
		OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
		Value:    ^uint64(0),
		PubKey:   keyHash,
	}})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("BuildSend err = %v, want ErrInsufficientFunds", err)
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
		PubKey:   keyHash,
	}})
	if err != nil {
		t.Fatalf("BuildSend: %v", err)
	}
	if err := store.MarkSubmitted("alice", plan.TransactionID, plan.Transaction, plan.Inputs); err != nil {
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
		PubKey:   keyHash,
	}})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("BuildSend err = %v, want ErrInsufficientFunds", err)
	}
}

func TestBuildSendSkipsImmatureCoinbase(t *testing.T) {
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
	_, err = store.BuildSend("alice", first.Address, 10, 1, []SpendableUTXO{{
		OutPoint:      types.OutPoint{TxID: [32]byte{1}, Vout: 0},
		Value:         50,
		PubKey:        keyHash,
		Coinbase:      true,
		Confirmations: 12,
		Mature:        false,
	}})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("BuildSend err = %v, want ErrInsufficientFunds", err)
	}
}

func TestBalanceTracksConfirmedAvailableAndReserved(t *testing.T) {
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
	plan, err := store.BuildSend("alice", first.Address, 10, 1, []SpendableUTXO{
		{OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0}, Value: 50, PubKey: keyHash},
		{OutPoint: types.OutPoint{TxID: [32]byte{2}, Vout: 1}, Value: 25, PubKey: keyHash},
	})
	if err != nil {
		t.Fatalf("BuildSend: %v", err)
	}
	if err := store.MarkSubmitted("alice", plan.TransactionID, plan.Transaction, plan.Inputs); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	summary, err := store.Balance("alice", []SpendableUTXO{
		{OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0}, Value: 50, PubKey: keyHash},
		{OutPoint: types.OutPoint{TxID: [32]byte{2}, Vout: 1}, Value: 25, PubKey: keyHash},
	})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if summary.Confirmed != 75 {
		t.Fatalf("confirmed = %d, want 75", summary.Confirmed)
	}
	if summary.Reserved == 0 {
		t.Fatal("expected reserved balance from pending spend")
	}
	if summary.Available >= summary.Confirmed {
		t.Fatalf("available = %d, want less than confirmed %d", summary.Available, summary.Confirmed)
	}
	if summary.PendingCount != 1 {
		t.Fatalf("pending count = %d, want 1", summary.PendingCount)
	}
}

func TestBalanceSeparatesImmatureCoinbase(t *testing.T) {
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
	summary, err := store.Balance("alice", []SpendableUTXO{
		{OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0}, Value: 50, PubKey: keyHash},
		{OutPoint: types.OutPoint{TxID: [32]byte{2}, Vout: 0}, Value: 25, PubKey: keyHash, Coinbase: true, Confirmations: 12, Mature: false},
	})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if summary.Confirmed != 75 || summary.Mature != 50 || summary.Immature != 25 || summary.Available != 50 {
		t.Fatalf("summary = %+v, want confirmed=75 mature=50 immature=25 available=50", summary)
	}
}

func TestBalanceRejectsOverflowingTotals(t *testing.T) {
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
	_, err = store.Balance("alice", []SpendableUTXO{
		{OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0}, Value: ^uint64(0), PubKey: keyHash},
		{OutPoint: types.OutPoint{TxID: [32]byte{2}, Vout: 0}, Value: 1, PubKey: keyHash},
	})
	if err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("Balance err = %v, want overflow", err)
	}
}

func TestParseAndFormatBPUAmounts(t *testing.T) {
	tests := []struct {
		raw  string
		want uint64
	}{
		{raw: "1", want: AtomsPerBPU},
		{raw: "1.25", want: 1_250_000_000},
		{raw: "0.000000001 BPU", want: 1},
		{raw: "42 atoms", want: 42},
	}
	for _, test := range tests {
		got, err := ParseAmount(test.raw)
		if err != nil {
			t.Fatalf("ParseAmount(%q): %v", test.raw, err)
		}
		if got != test.want {
			t.Fatalf("ParseAmount(%q) = %d, want %d", test.raw, got, test.want)
		}
	}
	if got := FormatAmount(1_250_000_000); got != "1.25 BPU" {
		t.Fatalf("FormatAmount = %q, want 1.25 BPU", got)
	}
	if _, err := ParseAmount("0.0000000001"); err == nil {
		t.Fatal("expected too many BPU decimals to fail")
	}
	if _, err := ParseAmount("18446744073.709551616"); err == nil {
		t.Fatal("expected overflowing BPU decimal to fail")
	}
}

func TestWalletReadAPIsReturnDeepCopies(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, first, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	store.wallets[0].Pending = append(store.wallets[0].Pending, PendingTx{
		TxID:  strings.Repeat("01", 32),
		Spent: []PendingOutPoint{{Vout: 1}},
		Outputs: []PendingOutput{{
			Vout:      0,
			Value:     1,
			Address:   first.Address,
			PubKeyHex: first.PubKeyHex,
		}},
	})

	listed := store.List()
	listed[0].Addresses[0].Address = "mutated"
	listed[0].Pending[0].Spent[0].Vout = 99
	listed[0].Pending[0].Outputs[0].Value = 99
	loaded, err := store.Wallet("alice")
	if err != nil {
		t.Fatalf("Wallet: %v", err)
	}
	if loaded.Addresses[0].Address == "mutated" || loaded.Pending[0].Spent[0].Vout == 99 || loaded.Pending[0].Outputs[0].Value == 99 {
		t.Fatalf("List returned mutable store internals: %+v", loaded)
	}

	loaded.Addresses[0].Address = "also-mutated"
	reloaded, err := store.Wallet("alice")
	if err != nil {
		t.Fatalf("Wallet reload: %v", err)
	}
	if reloaded.Addresses[0].Address == "also-mutated" {
		t.Fatal("Wallet returned mutable store internals")
	}
}

func TestBackupRestoreExportImportWallet(t *testing.T) {
	root := t.TempDir()
	store, err := Open(filepath.Join(root, StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, _, err := store.CreateWallet("alice"); err != nil {
		t.Fatalf("CreateWallet alice: %v", err)
	}
	if _, _, err := store.CreateWallet("bob"); err != nil {
		t.Fatalf("CreateWallet bob: %v", err)
	}

	backupPath := filepath.Join(root, "backup.json")
	if err := store.Backup(backupPath); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	mode, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("Stat backup: %v", err)
	}
	if mode.Mode().Perm() != 0o600 {
		t.Fatalf("backup mode = %o, want 600", mode.Mode().Perm())
	}

	restored, err := Open(filepath.Join(root, "restored.json"))
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	if err := restored.RestoreBackup(backupPath); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if wallets := restored.List(); len(wallets) != 2 {
		t.Fatalf("restored wallets = %d, want 2", len(wallets))
	}

	export, err := store.ExportWallet("alice")
	if err != nil {
		t.Fatalf("ExportWallet: %v", err)
	}
	exportPath := filepath.Join(root, "alice-export.json")
	if err := SaveExportFile(exportPath, export); err != nil {
		t.Fatalf("SaveExportFile: %v", err)
	}
	loadedExport, err := LoadExportFile(exportPath)
	if err != nil {
		t.Fatalf("LoadExportFile: %v", err)
	}
	importedStore, err := Open(filepath.Join(root, "imported.json"))
	if err != nil {
		t.Fatalf("Open imported: %v", err)
	}
	imported, err := importedStore.ImportWallet(loadedExport, "carol")
	if err != nil {
		t.Fatalf("ImportWallet: %v", err)
	}
	if imported.Name != "carol" || len(imported.Addresses) == 0 {
		t.Fatalf("imported wallet = %+v", imported)
	}
}

func TestBuildSendAutoChoosesFeeFromEstimatedSize(t *testing.T) {
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
	plan, err := store.BuildSendAuto("alice", dest.Address, 60, 2, []SpendableUTXO{
		{OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0}, Value: 2_000, PubKey: keyHash},
	})
	if err != nil {
		t.Fatalf("BuildSendAuto: %v", err)
	}
	if plan.FeeRate != 2 {
		t.Fatalf("fee rate = %d, want 2", plan.FeeRate)
	}
	if plan.EstimatedBytes != EstimateSignedTxBytes(1, 2) {
		t.Fatalf("estimated bytes = %d, want %d", plan.EstimatedBytes, EstimateSignedTxBytes(1, 2))
	}
	if plan.Fee != uint64(plan.EstimatedBytes)*plan.FeeRate {
		t.Fatalf("fee = %d, want %d", plan.Fee, uint64(plan.EstimatedBytes)*plan.FeeRate)
	}
	if plan.Change == 0 || plan.ChangeAddress == nil {
		t.Fatal("expected auto-fee send to keep change")
	}
}

func TestBuildSendAutoRejectsFeeOverflow(t *testing.T) {
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
	_, err = store.BuildSendAuto("alice", first.Address, 1, ^uint64(0), []SpendableUTXO{{
		OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
		Value:    ^uint64(0),
		PubKey:   keyHash,
	}})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("BuildSendAuto err = %v, want ErrInsufficientFunds", err)
	}
}

func TestMarkSubmittedTracksWalletOwnedOutputsForCPFP(t *testing.T) {
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
	plan, err := store.BuildSend("alice", dest.Address, 10, 1, []SpendableUTXO{{
		OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
		Value:    50,
		PubKey:   keyHash,
	}})
	if err != nil {
		t.Fatalf("BuildSend: %v", err)
	}
	if err := store.MarkSubmitted("alice", plan.TransactionID, plan.Transaction, plan.Inputs); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	pendingUTXOs, err := store.PendingSpendableUTXOs("alice", &plan.TransactionID)
	if err != nil {
		t.Fatalf("PendingSpendableUTXOs: %v", err)
	}
	if len(pendingUTXOs) == 0 {
		t.Fatal("expected wallet-owned pending outputs")
	}
}

func TestBuildCPFPUsesPendingWalletOutput(t *testing.T) {
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
	parent, err := store.BuildSend("alice", dest.Address, 10, 1, []SpendableUTXO{{
		OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
		Value:    1_000,
		PubKey:   keyHash,
	}})
	if err != nil {
		t.Fatalf("BuildSend: %v", err)
	}
	if err := store.MarkSubmitted("alice", parent.TransactionID, parent.Transaction, parent.Inputs); err != nil {
		t.Fatalf("MarkSubmitted: %v", err)
	}
	child, err := store.BuildCPFP("alice", parent.TransactionID, 2)
	if err != nil {
		t.Fatalf("BuildCPFP: %v", err)
	}
	if child.ParentTxID != parent.TransactionID {
		t.Fatalf("parent txid = %x, want %x", child.ParentTxID, parent.TransactionID)
	}
	if child.Input.OutPoint.TxID != parent.TransactionID {
		t.Fatalf("child input txid = %x, want parent %x", child.Input.OutPoint.TxID, parent.TransactionID)
	}
	if child.Fee == 0 || child.Amount == 0 {
		t.Fatalf("child fee/amount = %d/%d, want positive values", child.Fee, child.Amount)
	}
}

func TestBuildCPFPRejectsFeeOverflow(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), StoreFileName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, first, err := store.CreateWallet("alice")
	if err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	parentTxID := [32]byte{9}
	store.wallets[0].Pending = append(store.wallets[0].Pending, PendingTx{
		TxID: hex.EncodeToString(parentTxID[:]),
		Outputs: []PendingOutput{{
			Vout:       0,
			Value:      ^uint64(0),
			Address:    first.Address,
			Type:       first.OutputType(),
			PayloadHex: first.PayloadHex,
			PubKeyHex:  first.PubKeyHex,
			Change:     true,
		}},
	})
	_, err = store.BuildCPFP("alice", parentTxID, ^uint64(0))
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("BuildCPFP err = %v, want ErrInsufficientFunds", err)
	}
}
