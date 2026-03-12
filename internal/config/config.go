package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Config struct {
	Profile                     string   `json:"profile"`
	DBPath                      string   `json:"db_path"`
	LogPath                     string   `json:"log_path"`
	LogLevel                    string   `json:"log_level"`
	LogFormat                   string   `json:"log_format"`
	ThroughputSummaryIntervalMS int      `json:"throughput_summary_interval_ms"`
	PprofAddr                   string   `json:"pprof_addr"`
	RPCAddr                     string   `json:"rpc_addr"`
	RPCAuthToken                string   `json:"rpc_auth_token"`
	RPCReadTimeoutMS            int      `json:"rpc_read_timeout_ms"`
	RPCWriteTimeoutMS           int      `json:"rpc_write_timeout_ms"`
	RPCHeaderTimeoutMS          int      `json:"rpc_header_timeout_ms"`
	RPCIdleTimeoutMS            int      `json:"rpc_idle_timeout_ms"`
	RPCMaxHeaderBytes           int      `json:"rpc_max_header_bytes"`
	RPCMaxBodyBytes             int      `json:"rpc_max_body_bytes"`
	P2PAddr                     string   `json:"p2p_addr"`
	Peers                       []string `json:"peers"`
	MaxInboundPeers             int      `json:"max_inbound_peers"`
	MaxOutboundPeers            int      `json:"max_outbound_peers"`
	HandshakeTimeoutMS          int      `json:"handshake_timeout_ms"`
	StallTimeoutMS              int      `json:"stall_timeout_ms"`
	MaxMessageBytes             int      `json:"max_message_bytes"`
	MinRelayFeePerByte          uint64   `json:"min_relay_fee_per_byte"`
	MaxAncestors                int      `json:"max_ancestors"`
	MaxDescendants              int      `json:"max_descendants"`
	MaxOrphans                  int      `json:"max_orphans"`
	AvalancheMode               string   `json:"avalanche_mode"`
	AvalancheKSample            int      `json:"avalanche_k_sample"`
	AvalancheAlphaNumerator     int      `json:"avalanche_alpha_numerator"`
	AvalancheAlphaDenominator   int      `json:"avalanche_alpha_denominator"`
	AvalancheBeta               int      `json:"avalanche_beta"`
	AvalanchePollIntervalMS     int      `json:"avalanche_poll_interval_ms"`
	DandelionEnabled            bool     `json:"dandelion_enabled"`
	MinerEnabled                bool     `json:"miner_enabled"`
	MinerWorkers                int      `json:"miner_workers"`
	MinerPubKeyHex             string   `json:"miner_pubkey_hex"`
	GenesisFixture              string   `json:"genesis_fixture"`
}

func Default() Config {
	return Config{
		Profile:                     "regtest",
		DBPath:                      "data/chain",
		LogLevel:                    "info",
		LogFormat:                   "text",
		ThroughputSummaryIntervalMS: 60_000,
		RPCAddr:                     "127.0.0.1:18443",
		RPCReadTimeoutMS:            5000,
		// Mining and some wallet/history RPCs can legitimately run for much
		// longer than a few seconds on small nodes. Keep writes bounded, but
		// high enough that long admin calls do not get cut off mid-response.
		RPCWriteTimeoutMS:         15 * 60 * 1000,
		RPCHeaderTimeoutMS:        2000,
		RPCIdleTimeoutMS:          30000,
		RPCMaxHeaderBytes:         8 << 10,
		RPCMaxBodyBytes:           1 << 20,
		P2PAddr:                   "0.0.0.0:18444",
		MaxInboundPeers:           32,
		MaxOutboundPeers:          8,
		HandshakeTimeoutMS:        5000,
		StallTimeoutMS:            15000,
		MaxMessageBytes:           64_000_000,
		MinRelayFeePerByte:        1,
		MaxAncestors:              256,
		MaxDescendants:            256,
		MaxOrphans:                128,
		AvalancheMode:             "on",
		AvalancheKSample:          16,
		AvalancheAlphaNumerator:   3,
		AvalancheAlphaDenominator: 4,
		AvalancheBeta:             15,
		AvalanchePollIntervalMS:   200,
		DandelionEnabled:          false,
		MinerEnabled:              false,
		GenesisFixture:            "fixtures/genesis/regtest.json",
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return Config{}, err
	}
	if err := json.Unmarshal(buf, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func Save(path string, cfg Config) error {
	clean := filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(clean), 0o755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(clean), ".config-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, clean); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
