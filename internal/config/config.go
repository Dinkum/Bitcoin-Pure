package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Config struct {
	Profile            string   `json:"profile"`
	DBPath             string   `json:"db_path"`
	LogPath            string   `json:"log_path"`
	LogLevel           string   `json:"log_level"`
	RPCAddr            string   `json:"rpc_addr"`
	RPCAuthToken       string   `json:"rpc_auth_token"`
	RPCReadTimeoutMS   int      `json:"rpc_read_timeout_ms"`
	RPCWriteTimeoutMS  int      `json:"rpc_write_timeout_ms"`
	RPCHeaderTimeoutMS int      `json:"rpc_header_timeout_ms"`
	RPCMaxBodyBytes    int      `json:"rpc_max_body_bytes"`
	P2PAddr            string   `json:"p2p_addr"`
	Peers              []string `json:"peers"`
	MaxInboundPeers    int      `json:"max_inbound_peers"`
	MaxOutboundPeers   int      `json:"max_outbound_peers"`
	HandshakeTimeoutMS int      `json:"handshake_timeout_ms"`
	StallTimeoutMS     int      `json:"stall_timeout_ms"`
	MaxMessageBytes    int      `json:"max_message_bytes"`
	MinRelayFeePerByte uint64   `json:"min_relay_fee_per_byte"`
	MinerEnabled       bool     `json:"miner_enabled"`
	MinerKeyHashHex    string   `json:"miner_keyhash_hex"`
	MineIntervalMS     int      `json:"mine_interval_ms"`
	GenesisFixture     string   `json:"genesis_fixture"`
}

func Default() Config {
	return Config{
		Profile:            "regtest",
		DBPath:             "data/chain",
		LogLevel:           "info",
		RPCAddr:            "127.0.0.1:18443",
		RPCReadTimeoutMS:   5000,
		RPCWriteTimeoutMS:  5000,
		RPCHeaderTimeoutMS: 2000,
		RPCMaxBodyBytes:    1 << 20,
		P2PAddr:            "0.0.0.0:18444",
		MaxInboundPeers:    32,
		MaxOutboundPeers:   8,
		HandshakeTimeoutMS: 5000,
		StallTimeoutMS:     15000,
		MaxMessageBytes:    8 << 20,
		MinRelayFeePerByte: 1,
		MineIntervalMS:     0,
		GenesisFixture:     "fixtures/genesis/regtest.json",
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
