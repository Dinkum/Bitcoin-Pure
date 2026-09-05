package config

import (
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"bitcoin-pure/internal/consensus"
)

func TestDefaultConfigDisablesMiningWithoutDestination(t *testing.T) {
	cfg := Default()
	if cfg.TxIndexEnabled {
		t.Fatal("transaction index must be opt-in")
	}
	if cfg.MinerEnabled {
		t.Fatal("default config should keep mining disabled until a miner pubkey is configured")
	}
	if cfg.DandelionEnabled {
		t.Fatal("default config should keep dandelion relay disabled")
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
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := Default()
	cfg.MinerEnabled = true
	cfg.DandelionEnabled = true
	cfg.TxIndexEnabled = true
	cfg.MinerPubKeyHex = "abcd"
	cfg.MaxMempoolBytes = 123456
	if err := Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.MinerPubKeyHex != cfg.MinerPubKeyHex {
		t.Fatalf("miner pubkey = %q, want %q", loaded.MinerPubKeyHex, cfg.MinerPubKeyHex)
	}
	if !loaded.MinerEnabled {
		t.Fatal("expected miner_enabled to round-trip")
	}
	if !loaded.TxIndexEnabled {
		t.Fatal("expected tx_index_enabled to round-trip")
	}
	if !loaded.DandelionEnabled {
		t.Fatal("expected dandelion_enabled to round-trip")
	}
	if loaded.MaxMempoolBytes != cfg.MaxMempoolBytes {
		t.Fatalf("max mempool bytes = %d, want %d", loaded.MaxMempoolBytes, cfg.MaxMempoolBytes)
	}
	if sidecar, err := Load(filepath.Join(filepath.Dir(path), "config.json")); err != nil {
		t.Fatalf("Load sidecar json: %v", err)
	} else if !sidecar.TxIndexEnabled {
		t.Fatal("JSON sidecar lost tx_index_enabled")
	}
}

func TestCommentedYAMLCoversEveryConfigField(t *testing.T) {
	buf, err := renderCommentedYAML(Default())
	if err != nil {
		t.Fatalf("renderCommentedYAML: %v", err)
	}
	rendered := string(buf)
	cfgType := reflect.TypeOf(Config{})
	for i := 0; i < cfgType.NumField(); i++ {
		field := cfgType.Field(i)
		key := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if key == "" || key == "-" {
			continue
		}
		matches := regexp.MustCompile(`(?m)^`+regexp.QuoteMeta(key)+`:`).FindAllStringIndex(rendered, -1)
		if len(matches) != 1 {
			t.Fatalf("yaml key %q appears %d times, want exactly once", key, len(matches))
		}
	}
}

func TestLoadLegacyJSONConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	cfg := Default()
	cfg.MinerEnabled = true
	cfg.MinerPubKeyHex = "abcd"
	cfg.MaxMempoolBytes = 654321
	if err := Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !loaded.MinerEnabled {
		t.Fatal("expected miner_enabled to round-trip from json")
	}
	if loaded.MinerPubKeyHex != cfg.MinerPubKeyHex {
		t.Fatalf("miner pubkey = %q, want %q", loaded.MinerPubKeyHex, cfg.MinerPubKeyHex)
	}
	if loaded.MaxMempoolBytes != cfg.MaxMempoolBytes {
		t.Fatalf("max mempool bytes = %d, want %d", loaded.MaxMempoolBytes, cfg.MaxMempoolBytes)
	}
	if _, err := Load(filepath.Join(filepath.Dir(path), "config.yaml")); err != nil {
		t.Fatalf("Load canonical yaml: %v", err)
	}
}

func TestExampleConfigKeepsTransactionIndexOptional(t *testing.T) {
	cfg, err := Load("../../example.config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TxIndexEnabled {
		t.Fatal("example config enables transaction index by default")
	}
}
