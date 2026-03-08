package main

import (
	"path/filepath"
	"testing"

	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/wallet"
)

func TestEnsureMiningWalletProvisionedCreatesWalletAndPersistsKeyhash(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	cfg.MinerEnabled = true
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	addr, walletPath, err := ensureMiningWalletProvisioned(configPath, &cfg)
	if err != nil {
		t.Fatalf("ensureMiningWalletProvisioned: %v", err)
	}
	if addr.KeyHashHex == "" {
		t.Fatal("expected generated miner keyhash")
	}
	if cfg.MinerKeyHashHex != addr.KeyHashHex {
		t.Fatalf("config miner keyhash = %q, want %q", cfg.MinerKeyHashHex, addr.KeyHashHex)
	}

	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.MinerKeyHashHex != addr.KeyHashHex {
		t.Fatalf("persisted miner keyhash = %q, want %q", loaded.MinerKeyHashHex, addr.KeyHashHex)
	}

	store, err := wallet.Open(walletPath)
	if err != nil {
		t.Fatalf("Open wallet store: %v", err)
	}
	minerWallet, err := store.Wallet("miner")
	if err != nil {
		t.Fatalf("Wallet(miner): %v", err)
	}
	latest := minerWallet.LatestReceiveAddress()
	if latest == nil || latest.KeyHashHex != addr.KeyHashHex {
		t.Fatalf("latest receive address = %+v, want keyhash %q", latest, addr.KeyHashHex)
	}
}

func TestEnsureMiningWalletProvisionedRequiresConfigPathForPersistence(t *testing.T) {
	cfg := config.Default()
	cfg.MinerEnabled = true
	_, _, err := ensureMiningWalletProvisioned("", &cfg)
	if err == nil {
		t.Fatal("expected error without config path")
	}
}

func TestEnsureMiningWalletProvisionedReusesExistingMinerWallet(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	cfg.MinerEnabled = true
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	first, _, err := ensureMiningWalletProvisioned(configPath, &cfg)
	if err != nil {
		t.Fatalf("first ensureMiningWalletProvisioned: %v", err)
	}
	cfg.MinerKeyHashHex = ""
	second, _, err := ensureMiningWalletProvisioned(configPath, &cfg)
	if err != nil {
		t.Fatalf("second ensureMiningWalletProvisioned: %v", err)
	}
	if second.KeyHashHex != first.KeyHashHex {
		t.Fatalf("reused keyhash = %q, want %q", second.KeyHashHex, first.KeyHashHex)
	}

	store, _, err := openWalletStore("", cfg)
	if err != nil {
		t.Fatalf("openWalletStore: %v", err)
	}
	minerWallet, err := store.Wallet("miner")
	if err != nil {
		t.Fatalf("Wallet(miner): %v", err)
	}
	if len(minerWallet.Addresses) != 1 {
		t.Fatalf("miner wallet address count = %d, want 1", len(minerWallet.Addresses))
	}
}

func TestEnsureMiningWalletProvisionedNoopsWhenKeyhashConfigured(t *testing.T) {
	cfg := config.Default()
	cfg.MinerEnabled = true
	cfg.MinerKeyHashHex = "abcd"
	addr, walletPath, err := ensureMiningWalletProvisioned("", &cfg)
	if err != nil {
		t.Fatalf("ensureMiningWalletProvisioned: %v", err)
	}
	if addr != (wallet.Address{}) || walletPath != "" {
		t.Fatalf("unexpected provisioning result: addr=%+v walletPath=%q", addr, walletPath)
	}
}

func TestEnsureMiningWalletProvisionedNoopsWhenMiningDisabled(t *testing.T) {
	cfg := config.Default()
	addr, walletPath, err := ensureMiningWalletProvisioned("", &cfg)
	if err != nil {
		t.Fatalf("ensureMiningWalletProvisioned: %v", err)
	}
	if addr != (wallet.Address{}) || walletPath != "" {
		t.Fatalf("unexpected provisioning result: addr=%+v walletPath=%q", addr, walletPath)
	}
}

func TestDefaultGenesisFixtureSupportsRegtestHard(t *testing.T) {
	if got := defaultGenesisFixture(types.RegtestHard); got != "fixtures/genesis/regtest_hard.json" {
		t.Fatalf("default genesis fixture = %q", got)
	}
}

func TestLoadGenesisFixtureSupportsRegtestHard(t *testing.T) {
	loaded, err := loadGenesisFixtureFromPath(filepath.Join("..", "..", "fixtures", "genesis", "regtest_hard.json"))
	if err != nil {
		t.Fatalf("loadGenesisFixture: %v", err)
	}
	if loaded.Fixture.Profile != string(types.RegtestHard) {
		t.Fatalf("fixture profile = %q, want %q", loaded.Fixture.Profile, types.RegtestHard)
	}
}
