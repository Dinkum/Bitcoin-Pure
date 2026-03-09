package config

import (
	"path/filepath"
	"testing"

	"bitcoin-pure/internal/consensus"
)

func TestDefaultConfigDisablesMiningWithoutDestination(t *testing.T) {
	cfg := Default()
	if cfg.MinerEnabled {
		t.Fatal("default config should keep mining disabled until a miner keyhash is configured")
	}
}

func TestDefaultMaxMessageBytesCoversConsensusFloor(t *testing.T) {
	cfg := Default()
	if cfg.MaxMessageBytes < int(consensus.MainnetParams().BlockSizeFloor) {
		t.Fatalf("max message bytes = %d, want at least %d", cfg.MaxMessageBytes, consensus.MainnetParams().BlockSizeFloor)
	}
}

func TestDefaultRPCWriteTimeoutAllowsLongAdminCalls(t *testing.T) {
	cfg := Default()
	if cfg.RPCWriteTimeoutMS < int((10 * 60 * 1000)) {
		t.Fatalf("rpc write timeout = %dms, want at least 10 minutes", cfg.RPCWriteTimeoutMS)
	}
}

func TestSaveRoundTripPersistsConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	cfg := Default()
	cfg.MinerEnabled = true
	cfg.MinerKeyHashHex = "abcd"
	if err := Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.MinerKeyHashHex != cfg.MinerKeyHashHex {
		t.Fatalf("miner keyhash = %q, want %q", loaded.MinerKeyHashHex, cfg.MinerKeyHashHex)
	}
	if !loaded.MinerEnabled {
		t.Fatal("expected miner_enabled to round-trip")
	}
}
