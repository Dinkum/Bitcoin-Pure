package wallet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bitcoin-pure/internal/types"
)

func TestWalletMutationsReloadStaleHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), StoreFileName)
	left, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := left.CreateWallet("main"); err != nil {
		t.Fatal(err)
	}
	right, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := left.NewReceiveAddress("main")
	if err != nil {
		t.Fatal(err)
	}
	second, err := right.NewReceiveAddress("main")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := left.CreateWallet("second"); err != nil {
		t.Fatal(err)
	}
	latest, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := latest.Wallet("main")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Addresses) != 3 || !walletHasAddress(w, first.Address) || !walletHasAddress(w, second.Address) || second.Index != first.Index+1 {
		t.Fatal("a successful mutation lost an address or reused an index")
	}
	for _, addr := range w.Addresses {
		if err := validateWalletAddress(addr); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWalletConcurrentProcessWriter(t *testing.T) {
	path := os.Getenv("BPU_TEST_WALLET_PATH")
	if path == "" {
		return
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("BPU_TEST_WALLET_READY"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(path + ".start"); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < 4; i++ {
		if _, err := store.NewReceiveAddress("main"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWalletConcurrentProcessesPreserveKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), StoreFileName)
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateWallet("main"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var children []*exec.Cmd
	for i := 0; i < 4; i++ {
		ready := fmt.Sprintf("%s.ready-%d", path, i)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWalletConcurrentProcessWriter$")
		cmd.Env = append(os.Environ(), "BPU_TEST_WALLET_PATH="+path, "BPU_TEST_WALLET_READY="+ready)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		children = append(children, cmd)
		for {
			if _, err := os.Stat(ready); err == nil {
				break
			}
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := os.WriteFile(path+".start", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range children {
		if err := cmd.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	latest, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := latest.Wallet("main")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Addresses) != 17 {
		t.Fatalf("stored %d addresses, want all 17", len(w.Addresses))
	}
	seen := make(map[string]bool)
	for i, addr := range w.Addresses {
		if addr.Index != i || seen[addr.Address] {
			t.Fatal("duplicate address/index")
		}
		seen[addr.Address] = true
		if err := validateWalletAddress(addr); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWalletLockRejectsSymlink(t *testing.T) {
	path := filepath.Join(t.TempDir(), StoreFileName)
	target := path + ".target"
	if err := os.WriteFile(target, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path+".lock"); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateWallet("main"); err == nil {
		t.Fatal("accepted symlink lock")
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "unchanged" {
		t.Fatal("changed lock target")
	}
}

func TestWalletStaleRestoreCannotDiscardNewKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), StoreFileName)
	left, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := left.CreateWallet("main"); err != nil {
		t.Fatal(err)
	}
	backup := path + ".backup"
	if err := left.Backup(backup); err != nil {
		t.Fatal(err)
	}
	right, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := right.NewReceiveAddress("main")
	if err != nil {
		t.Fatal(err)
	}
	if err := left.RestoreBackup(backup); err == nil || !strings.Contains(err.Error(), "wallet changed") {
		t.Fatalf("stale restore result: %v", err)
	}
	latest, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := latest.Wallet("main")
	if err != nil {
		t.Fatal(err)
	}
	if !walletHasAddress(w, created.Address) {
		t.Fatal("restore lost concurrent key")
	}
}

func TestWalletPendingReservationChecksReloadedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), StoreFileName)
	left, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := left.CreateWallet("main"); err != nil {
		t.Fatal(err)
	}
	right, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	inputs := []SelectedInput{{OutPoint: types.OutPoint{TxID: [32]byte{1}, Vout: 1}}}
	if err := left.MarkSubmitted("main", [32]byte{2}, types.Transaction{}, inputs, nil); err != nil {
		t.Fatal(err)
	}
	if err := right.MarkSubmitted("main", [32]byte{3}, types.Transaction{}, inputs, nil); err == nil {
		t.Fatal("stale sender reserved the same input")
	}
	if err := right.MarkSubmitted("main", [32]byte{2}, types.Transaction{}, inputs, nil); err != nil {
		t.Fatalf("idempotent reservation failed: %v", err)
	}
}
