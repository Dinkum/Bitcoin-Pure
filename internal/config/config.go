package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultConfigPath = "/etc/bitcoin-pure/config.yaml"
	AltConfigPath     = "/etc/bitcoin-pure/config.yml"
	LegacyConfigPath  = "/etc/bitcoin-pure/config.json"
)

type Config struct {
	Profile                     string   `json:"profile" yaml:"profile"`
	DBPath                      string   `json:"db_path" yaml:"db_path"`
	LogPath                     string   `json:"log_path" yaml:"log_path"`
	LogLevel                    string   `json:"log_level" yaml:"log_level"`
	LogFormat                   string   `json:"log_format" yaml:"log_format"`
	ThroughputSummaryIntervalMS int      `json:"throughput_summary_interval_ms" yaml:"throughput_summary_interval_ms"`
	PprofAddr                   string   `json:"pprof_addr" yaml:"pprof_addr"`
	RPCAddr                     string   `json:"rpc_addr" yaml:"rpc_addr"`
	RPCAuthToken                string   `json:"rpc_auth_token" yaml:"rpc_auth_token"`
	RPCReadTimeoutMS            int      `json:"rpc_read_timeout_ms" yaml:"rpc_read_timeout_ms"`
	RPCWriteTimeoutMS           int      `json:"rpc_write_timeout_ms" yaml:"rpc_write_timeout_ms"`
	RPCHeaderTimeoutMS          int      `json:"rpc_header_timeout_ms" yaml:"rpc_header_timeout_ms"`
	RPCIdleTimeoutMS            int      `json:"rpc_idle_timeout_ms" yaml:"rpc_idle_timeout_ms"`
	RPCMaxHeaderBytes           int      `json:"rpc_max_header_bytes" yaml:"rpc_max_header_bytes"`
	RPCMaxBodyBytes             int      `json:"rpc_max_body_bytes" yaml:"rpc_max_body_bytes"`
	P2PAddr                     string   `json:"p2p_addr" yaml:"p2p_addr"`
	Peers                       []string `json:"peers" yaml:"peers"`
	MaxInboundPeers             int      `json:"max_inbound_peers" yaml:"max_inbound_peers"`
	MaxOutboundPeers            int      `json:"max_outbound_peers" yaml:"max_outbound_peers"`
	HandshakeTimeoutMS          int      `json:"handshake_timeout_ms" yaml:"handshake_timeout_ms"`
	StallTimeoutMS              int      `json:"stall_timeout_ms" yaml:"stall_timeout_ms"`
	MaxMessageBytes             int      `json:"max_message_bytes" yaml:"max_message_bytes"`
	MinRelayFeePerByte          uint64   `json:"min_relay_fee_per_byte" yaml:"min_relay_fee_per_byte"`
	MaxMempoolBytes             int      `json:"max_mempool_bytes" yaml:"max_mempool_bytes"`
	MaxAncestors                int      `json:"max_ancestors" yaml:"max_ancestors"`
	MaxDescendants              int      `json:"max_descendants" yaml:"max_descendants"`
	MaxOrphans                  int      `json:"max_orphans" yaml:"max_orphans"`
	AvalancheMode               string   `json:"avalanche_mode" yaml:"avalanche_mode"`
	AvalancheKSample            int      `json:"avalanche_k_sample" yaml:"avalanche_k_sample"`
	AvalancheAlphaNumerator     int      `json:"avalanche_alpha_numerator" yaml:"avalanche_alpha_numerator"`
	AvalancheAlphaDenominator   int      `json:"avalanche_alpha_denominator" yaml:"avalanche_alpha_denominator"`
	AvalancheBeta               int      `json:"avalanche_beta" yaml:"avalanche_beta"`
	AvalanchePollIntervalMS     int      `json:"avalanche_poll_interval_ms" yaml:"avalanche_poll_interval_ms"`
	DandelionEnabled            bool     `json:"dandelion_enabled" yaml:"dandelion_enabled"`
	MinerEnabled                bool     `json:"miner_enabled" yaml:"miner_enabled"`
	MinerWorkers                int      `json:"miner_workers" yaml:"miner_workers"`
	MinerPubKeyHex              string   `json:"miner_pubkey_hex" yaml:"miner_pubkey_hex"`
	GenesisFixture              string   `json:"genesis_fixture" yaml:"genesis_fixture"`
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
		MaxMempoolBytes:           64 << 20,
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
	switch configFormat(path) {
	case "yaml":
		if err := yaml.Unmarshal(buf, &cfg); err != nil {
			return Config{}, err
		}
	default:
		if err := json.Unmarshal(buf, &cfg); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

func DefaultPathCandidates() []string {
	return []string{DefaultConfigPath, AltConfigPath, LegacyConfigPath}
}

func Format(path string, cfg Config) ([]byte, error) {
	if configFormat(path) == "yaml" {
		return renderCommentedYAML(cfg)
	}
	buf, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(buf, '\n'), nil
}

func Save(path string, cfg Config) error {
	clean := filepath.Clean(path)
	if err := saveSingle(clean, cfg); err != nil {
		return err
	}
	sidecar := compatibilitySidecarPath(clean)
	if sidecar == "" || sidecar == clean {
		return nil
	}
	return saveSingle(sidecar, cfg)
}

func saveSingle(path string, cfg Config) error {
	clean := filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(clean), 0o755); err != nil {
		return err
	}
	buf, err := Format(clean, cfg)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(clean), ".config-*"+filepath.Ext(clean))
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

func compatibilitySidecarPath(path string) string {
	dir := filepath.Dir(path)
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return filepath.Join(dir, filepath.Base(LegacyConfigPath))
	case ".json":
		return filepath.Join(dir, filepath.Base(DefaultConfigPath))
	default:
		return ""
	}
}

func configFormat(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return "yaml"
	default:
		return "json"
	}
}

// The operator-facing config is rendered by hand so the saved YAML keeps stable
// section headers and explanatory comments instead of dropping them on rewrite.
func renderCommentedYAML(cfg Config) ([]byte, error) {
	defaults := Default()
	sections := []yamlSection{
		{
			title:       "Core Node",
			description: "Chain profile selection and bootstrap fixture wiring.",
			fields: []yamlField{
				{key: "profile", description: "Selects the chain profile and its network rules.", value: cfg.Profile, defaultValue: defaults.Profile},
				{key: "genesis_fixture", description: "Points at the genesis fixture used to bootstrap the selected profile.", value: cfg.GenesisFixture, defaultValue: defaults.GenesisFixture},
			},
		},
		{
			title:       "Storage Paths",
			description: "On-disk locations for durable chain state.",
			fields: []yamlField{
				{key: "db_path", description: "Sets the chain database directory.", value: cfg.DBPath, defaultValue: defaults.DBPath},
			},
		},
		{
			title:       "Logging and Profiling",
			description: "Operator-visible logs, summaries, and optional local profiling endpoints.",
			fields: []yamlField{
				{key: "log_path", description: "Writes daemon logs to a file when set; leave empty to log to stderr only.", value: cfg.LogPath, defaultValue: defaults.LogPath},
				{key: "log_level", description: "Controls log verbosity.", value: cfg.LogLevel, defaultValue: defaults.LogLevel},
				{key: "log_format", description: "Chooses structured json logs or human-readable text logs.", value: cfg.LogFormat, defaultValue: defaults.LogFormat},
				{key: "throughput_summary_interval_ms", description: "Emits periodic throughput summaries on this interval in milliseconds.", value: cfg.ThroughputSummaryIntervalMS, defaultValue: defaults.ThroughputSummaryIntervalMS},
				{key: "pprof_addr", description: "Exposes the local pprof server when set to a loopback listen address.", value: cfg.PprofAddr, defaultValue: defaults.PprofAddr},
			},
		},
		{
			title:       "RPC Server",
			description: "Operator RPC listen address, authentication, and HTTP safety limits.",
			fields: []yamlField{
				{key: "rpc_addr", description: "Listens for JSON-RPC requests on this address.", value: cfg.RPCAddr, defaultValue: defaults.RPCAddr},
				{key: "rpc_auth_token", description: "Requires this bearer token for non-loopback RPC access; leave empty to disable token auth on loopback-only setups.", value: cfg.RPCAuthToken, defaultValue: defaults.RPCAuthToken},
				{key: "rpc_read_timeout_ms", description: "Bounds RPC request body reads in milliseconds.", value: cfg.RPCReadTimeoutMS, defaultValue: defaults.RPCReadTimeoutMS},
				{key: "rpc_write_timeout_ms", description: "Bounds RPC response writes in milliseconds.", value: cfg.RPCWriteTimeoutMS, defaultValue: defaults.RPCWriteTimeoutMS},
				{key: "rpc_header_timeout_ms", description: "Bounds RPC header reads in milliseconds.", value: cfg.RPCHeaderTimeoutMS, defaultValue: defaults.RPCHeaderTimeoutMS},
				{key: "rpc_idle_timeout_ms", description: "Closes idle RPC keepalive connections after this many milliseconds.", value: cfg.RPCIdleTimeoutMS, defaultValue: defaults.RPCIdleTimeoutMS},
				{key: "rpc_max_header_bytes", description: "Rejects RPC requests with larger HTTP headers than this byte limit.", value: cfg.RPCMaxHeaderBytes, defaultValue: defaults.RPCMaxHeaderBytes},
				{key: "rpc_max_body_bytes", description: "Rejects RPC request bodies larger than this byte limit.", value: cfg.RPCMaxBodyBytes, defaultValue: defaults.RPCMaxBodyBytes},
			},
		},
		{
			title:       "P2P Networking",
			description: "Peer listen settings, outbound seeds, and message-level guardrails.",
			fields: []yamlField{
				{key: "p2p_addr", description: "Listens for P2P traffic on this address.", value: cfg.P2PAddr, defaultValue: defaults.P2PAddr},
				{key: "peers", description: "Pins outbound peers when you want explicit static connections; leave empty for normal discovery.", value: cfg.Peers, defaultValue: defaults.Peers},
				{key: "max_inbound_peers", description: "Caps inbound peer slots.", value: cfg.MaxInboundPeers, defaultValue: defaults.MaxInboundPeers},
				{key: "max_outbound_peers", description: "Caps outbound peer slots.", value: cfg.MaxOutboundPeers, defaultValue: defaults.MaxOutboundPeers},
				{key: "handshake_timeout_ms", description: "Drops peers that do not finish the handshake within this many milliseconds.", value: cfg.HandshakeTimeoutMS, defaultValue: defaults.HandshakeTimeoutMS},
				{key: "stall_timeout_ms", description: "Drops peers that stop making forward progress for this many milliseconds.", value: cfg.StallTimeoutMS, defaultValue: defaults.StallTimeoutMS},
				{key: "max_message_bytes", description: "Rejects P2P messages larger than this byte limit.", value: cfg.MaxMessageBytes, defaultValue: defaults.MaxMessageBytes},
			},
		},
		{
			title:       "Mempool Settings",
			description: "Admission policy for relayed transactions and dependency tracking limits.",
			fields: []yamlField{
				{key: "min_relay_fee_per_byte", description: "Rejects transactions whose fee rate falls below this minimum satoshi-per-byte policy floor.", value: cfg.MinRelayFeePerByte, defaultValue: defaults.MinRelayFeePerByte},
				{key: "max_mempool_bytes", description: "Caps total admitted mempool bytes; when full, lower-fee packages are evicted before higher-fee arrivals are admitted.", value: cfg.MaxMempoolBytes, defaultValue: defaults.MaxMempoolBytes},
				{key: "max_ancestors", description: "Caps in-mempool ancestors per transaction.", value: cfg.MaxAncestors, defaultValue: defaults.MaxAncestors},
				{key: "max_descendants", description: "Caps in-mempool descendants per transaction.", value: cfg.MaxDescendants, defaultValue: defaults.MaxDescendants},
				{key: "max_orphans", description: "Bounds the orphan pool for missing-input transactions.", value: cfg.MaxOrphans, defaultValue: defaults.MaxOrphans},
			},
		},
		{
			title:       "Avalanche",
			description: "Fast-finality polling and vote-threshold tuning.",
			fields: []yamlField{
				{key: "avalanche_mode", description: "Turns Avalanche polling on or off.", value: cfg.AvalancheMode, defaultValue: defaults.AvalancheMode},
				{key: "avalanche_k_sample", description: "Sets the Avalanche poll sample size.", value: cfg.AvalancheKSample, defaultValue: defaults.AvalancheKSample},
				{key: "avalanche_alpha_numerator", description: "Sets the accepted-vote threshold numerator.", value: cfg.AvalancheAlphaNumerator, defaultValue: defaults.AvalancheAlphaNumerator},
				{key: "avalanche_alpha_denominator", description: "Sets the accepted-vote threshold denominator.", value: cfg.AvalancheAlphaDenominator, defaultValue: defaults.AvalancheAlphaDenominator},
				{key: "avalanche_beta", description: "Sets the consecutive successful poll threshold before finalization.", value: cfg.AvalancheBeta, defaultValue: defaults.AvalancheBeta},
				{key: "avalanche_poll_interval_ms", description: "Schedules Avalanche polls on this millisecond interval.", value: cfg.AvalanchePollIntervalMS, defaultValue: defaults.AvalanchePollIntervalMS},
			},
		},
		{
			title:       "Beta Features",
			description: "Optional relay behavior that is useful for experimentation but not required for normal operation.",
			fields: []yamlField{
				{key: "dandelion_enabled", description: "Enables Dandelion transaction relay behavior.", value: cfg.DandelionEnabled, defaultValue: defaults.DandelionEnabled},
			},
		},
		{
			title:       "Mining",
			description: "Local block production and payout destination settings.",
			fields: []yamlField{
				{key: "miner_enabled", description: "Enables local mining workers.", value: cfg.MinerEnabled, defaultValue: defaults.MinerEnabled},
				{key: "miner_workers", description: "Sets the number of local mining workers; zero uses the node default.", value: cfg.MinerWorkers, defaultValue: defaults.MinerWorkers},
				{key: "miner_pubkey_hex", description: "Pays coinbase rewards to this compressed public key; leave empty to let wallet auto-provisioning fill it in.", value: cfg.MinerPubKeyHex, defaultValue: defaults.MinerPubKeyHex},
			},
		},
	}

	var b strings.Builder
	b.WriteString("# Bitcoin Pure configuration.\n")
	b.WriteString("# Command-line flags override values loaded from this file.\n")
	b.WriteString("# The daemon looks for /etc/bitcoin-pure/config.yaml by default.\n\n")
	for i, section := range sections {
		if i > 0 {
			b.WriteByte('\n')
		}
		if err := writeYAMLSection(&b, section); err != nil {
			return nil, err
		}
	}
	return []byte(b.String()), nil
}

type yamlSection struct {
	title       string
	description string
	fields      []yamlField
}

type yamlField struct {
	key          string
	description  string
	value        any
	defaultValue any
}

func writeYAMLSection(b *strings.Builder, section yamlSection) error {
	fmt.Fprintf(b, "# %s\n", strings.Repeat("=", 78))
	fmt.Fprintf(b, "# %s\n", strings.ToUpper(section.title))
	fmt.Fprintf(b, "# %s\n", strings.Repeat("=", 78))
	fmt.Fprintf(b, "# %s\n\n", section.description)
	for _, field := range section.fields {
		if err := writeYAMLField(b, field); err != nil {
			return err
		}
	}
	return nil
}

func writeYAMLField(b *strings.Builder, field yamlField) error {
	defaultValue, err := renderYAMLScalar(field.defaultValue)
	if err != nil {
		return err
	}
	fmt.Fprintf(b, "# %s\n", field.description)
	fmt.Fprintf(b, "# Default: %s\n", defaultValue)
	rendered, err := renderYAMLValue(field.value)
	if err != nil {
		return err
	}
	if !strings.Contains(rendered, "\n") {
		fmt.Fprintf(b, "%s: %s\n\n", field.key, rendered)
		return nil
	}
	fmt.Fprintf(b, "%s:\n", field.key)
	for _, line := range strings.Split(rendered, "\n") {
		fmt.Fprintf(b, "  %s\n", line)
	}
	b.WriteByte('\n')
	return nil
}

func renderYAMLScalar(value any) (string, error) {
	rendered, err := renderYAMLValue(value)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(rendered), nil
}

func renderYAMLValue(value any) (string, error) {
	buf, err := yaml.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("render yaml value: %w", err)
	}
	return strings.TrimSpace(string(buf)), nil
}
