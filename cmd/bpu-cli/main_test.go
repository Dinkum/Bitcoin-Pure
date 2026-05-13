package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/logging"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/wallet"
)

func TestEnsureMiningWalletProvisionedCreatesWalletAndPersistsPubKey(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
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
	if addr.PubKeyHex == "" {
		t.Fatal("expected generated miner pubkey")
	}
	if cfg.MinerPubKeyHex != addr.PubKeyHex {
		t.Fatalf("config miner pubkey = %q, want %q", cfg.MinerPubKeyHex, addr.PubKeyHex)
	}

	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.MinerPubKeyHex != addr.PubKeyHex {
		t.Fatalf("persisted miner pubkey = %q, want %q", loaded.MinerPubKeyHex, addr.PubKeyHex)
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
	if latest == nil || latest.PubKeyHex != addr.PubKeyHex {
		t.Fatalf("latest receive address = %+v, want pubkey %q", latest, addr.PubKeyHex)
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
	configPath := filepath.Join(root, "config.yaml")
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
	cfg.MinerPubKeyHex = ""
	second, _, err := ensureMiningWalletProvisioned(configPath, &cfg)
	if err != nil {
		t.Fatalf("second ensureMiningWalletProvisioned: %v", err)
	}
	if second.PubKeyHex != first.PubKeyHex {
		t.Fatalf("reused pubkey = %q, want %q", second.PubKeyHex, first.PubKeyHex)
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

func TestEnsureMiningWalletProvisionedNoopsWhenPubKeyConfigured(t *testing.T) {
	cfg := config.Default()
	cfg.MinerEnabled = true
	cfg.MinerPubKeyHex = "abcd"
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

func TestConfigNormalizeWritesCanonicalYAMLAndLegacyJSONSidecar(t *testing.T) {
	root := t.TempDir()
	inPath := filepath.Join(root, "input.json")
	outPath := filepath.Join(root, "config.yaml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	cfg.MaxMempoolBytes = 77 << 20
	if err := config.Save(inPath, cfg); err != nil {
		t.Fatalf("Save input: %v", err)
	}

	if err := run([]string{"config", "normalize", "--in", inPath, "--out", outPath}); err != nil {
		t.Fatalf("run config normalize: %v", err)
	}

	loadedYAML, err := config.Load(outPath)
	if err != nil {
		t.Fatalf("Load yaml: %v", err)
	}
	if loadedYAML.MaxMempoolBytes != cfg.MaxMempoolBytes {
		t.Fatalf("yaml max mempool bytes = %d, want %d", loadedYAML.MaxMempoolBytes, cfg.MaxMempoolBytes)
	}
	sidecarPath := filepath.Join(root, "config.json")
	if _, err := os.Stat(sidecarPath); err != nil {
		t.Fatalf("legacy sidecar missing: %v", err)
	}
	loadedJSON, err := config.Load(sidecarPath)
	if err != nil {
		t.Fatalf("Load json sidecar: %v", err)
	}
	if loadedJSON.MaxMempoolBytes != cfg.MaxMempoolBytes {
		t.Fatalf("json max mempool bytes = %d, want %d", loadedJSON.MaxMempoolBytes, cfg.MaxMempoolBytes)
	}
}

func TestPeerAddCallsAddPeerRPC(t *testing.T) {
	var gotAuth string
	var gotMethod string
	var gotAddr string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		var req struct {
			Method string `json:"method"`
			Params struct {
				Addr string `json:"addr"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod = req.Method
		gotAddr = req.Params.Addr
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"addr":"198.51.100.25:18444"}}`))
	}))
	defer server.Close()

	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	cfg.RPCAddr = server.URL
	cfg.RPCAuthToken = "secret-token"
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("Save config: %v", err)
	}

	if err := runPeer([]string{"add", "--config", configPath, "198.51.100.25:18444"}); err != nil {
		t.Fatalf("runPeer add: %v", err)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("Authorization = %q, want bearer token", gotAuth)
	}
	if gotMethod != "addpeer" {
		t.Fatalf("method = %q, want addpeer", gotMethod)
	}
	if gotAddr != "198.51.100.25:18444" {
		t.Fatalf("addr = %q, want peer address", gotAddr)
	}
}

func TestPeerAddRequiresAddress(t *testing.T) {
	err := runPeer([]string{"add"})
	if err == nil || !strings.Contains(err.Error(), "usage: bpu-cli peer add") {
		t.Fatalf("runPeer add err = %v, want usage error", err)
	}
}

func TestDefaultGenesisFixtureSupportsRegtestMediumAndHard(t *testing.T) {
	if got := defaultGenesisFixture(types.RegtestMedium); got != "fixtures/genesis/regtest_medium.json" {
		t.Fatalf("default medium genesis fixture = %q", got)
	}
	if got := defaultGenesisFixture(types.RegtestHard); got != "fixtures/genesis/regtest_hard.json" {
		t.Fatalf("default hard genesis fixture = %q", got)
	}
	if got := defaultGenesisFixture(types.BenchNet); got != "" {
		t.Fatalf("default benchnet genesis fixture = %q, want empty", got)
	}
}

func TestLoadGenesisFixtureSupportsRegtestMediumAndHard(t *testing.T) {
	loaded, err := loadGenesisFixtureFromPath(filepath.Join("..", "..", "fixtures", "genesis", "regtest_medium.json"))
	if err != nil {
		t.Fatalf("loadGenesisFixture(regtest_medium): %v", err)
	}
	if loaded.Fixture.Profile != string(types.RegtestMedium) {
		t.Fatalf("fixture profile = %q, want %q", loaded.Fixture.Profile, types.RegtestMedium)
	}

	loadedHard, err := loadGenesisFixtureFromPath(filepath.Join("..", "..", "fixtures", "genesis", "regtest_hard.json"))
	if err != nil {
		t.Fatalf("loadGenesisFixture(regtest_hard): %v", err)
	}
	if loadedHard.Fixture.Profile != string(types.RegtestHard) {
		t.Fatalf("fixture profile = %q, want %q", loadedHard.Fixture.Profile, types.RegtestHard)
	}
}

func TestLogsHelpersUseStandardJSONLPathAndOperationRender(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	logPath := resolveLogPath(cfg)
	if logPath != filepath.Join(root, "events.jsonl") {
		t.Fatalf("log path = %q, want events.jsonl beside chain dir", logPath)
	}

	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create log file: %v", err)
	}
	logger, err := logging.NewLogger(file, logging.Config{Format: "jsonl", Level: "info"})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	op := logging.StartOperation(logger, "sync", "sync.blocks", "Started block sync", slog.Uint64("from_height", 1))
	op.Step("headers", "Resolved locator", slog.Uint64("header_height", 3))
	op.Finish("Completed block sync", slog.Int("block_count", 2))
	if err := file.Close(); err != nil {
		t.Fatalf("close log file: %v", err)
	}

	lines, err := readLastLogLines(logPath, 2)
	if err != nil {
		t.Fatalf("readLastLogLines: %v", err)
	}
	if len(lines) != 2 || !strings.Contains(lines[1], "Completed block sync") {
		t.Fatalf("unexpected tail lines: %v", lines)
	}

	lastOpID, err := findLastOperationID(logPath)
	if err != nil {
		t.Fatalf("findLastOperationID: %v", err)
	}
	if lastOpID != op.ID() {
		t.Fatalf("last op id = %q, want %q", lastOpID, op.ID())
	}
	records, err := collectOperationRecords(logPath, op.ID())
	if err != nil {
		t.Fatalf("collectOperationRecords: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("operation record count = %d, want 3", len(records))
	}
	if !logRecordMatches(records[0], "sync", "sync.blocks", "info", op.ID(), "started") {
		t.Fatalf("root record did not match expected filters: %+v", records[0])
	}
	rendered := renderOperationRecords(records)
	for _, want := range []string{"Started block sync", ">> headers", "Completed block sync", "|="} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered log missing %q:\n%s", want, rendered)
		}
	}
}

func TestRenderOperationRecordsWrapsLongFields(t *testing.T) {
	records := []logRecord{{
		"ts":             "2026-05-13T12:00:00Z",
		"level":          "INFO",
		"category":       "service",
		"name":           "node.status",
		"message":        "node status",
		"seq":            json.Number("1"),
		"depth":          json.Number("0"),
		"op_id":          strings.Repeat("a", 48),
		"last_block_ago": strings.Repeat("9", 64),
		"detail":         "status " + strings.Repeat("x", 160),
	}}
	out := renderOperationRecords(records)
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		if len(line) > 100 {
			t.Fatalf("rendered log line length = %d, want <= 100:\n%s", len(line), out)
		}
	}
}

func TestWalletCreateDefaultsToMainWallet(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	configPath := filepath.Join(root, "config.yaml")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("Save config: %v", err)
	}
	walletDir := filepath.Join(root, "wallets")
	if err := runWalletCreate([]string{"--config", configPath, "--wallet-dir", walletDir}); err != nil {
		t.Fatalf("runWalletCreate: %v", err)
	}
	store, err := wallet.OpenWithProfile(wallet.StorePath(walletDir), types.Regtest)
	if err != nil {
		t.Fatalf("OpenWithProfile: %v", err)
	}
	if _, err := store.Wallet("main"); err != nil {
		t.Fatalf("default wallet not created: %v", err)
	}
}

func TestWalletFlagsMayFollowPositionals(t *testing.T) {
	walletDir := filepath.Join(t.TempDir(), "wallets")
	if err := runWalletCreate([]string{"main", "--wallet-dir", walletDir}); err != nil {
		t.Fatalf("runWalletCreate with trailing flag: %v", err)
	}
	if err := runWalletReceive([]string{"main", "--wallet-dir", walletDir}); err != nil {
		t.Fatalf("runWalletReceive with trailing flag: %v", err)
	}
}

func TestWalletSubcommandHelpIsHandled(t *testing.T) {
	if err := runWallet([]string{"--help"}); err != nil {
		t.Fatalf("wallet --help err = %v", err)
	}
	if err := runWallet([]string{"send", "--help"}); err != nil {
		t.Fatalf("wallet send --help err = %v", err)
	}
}

func TestWalletReceiveAndExportInferSingleWallet(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	configPath := filepath.Join(root, "config.yaml")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("Save config: %v", err)
	}
	walletDir := filepath.Join(root, "wallets")
	store, err := wallet.OpenWithProfile(wallet.StorePath(walletDir), types.Regtest)
	if err != nil {
		t.Fatalf("OpenWithProfile: %v", err)
	}
	if _, _, err := store.CreateWallet("main"); err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	if err := runWalletReceive([]string{"--config", configPath, "--wallet-dir", walletDir}); err != nil {
		t.Fatalf("runWalletReceive: %v", err)
	}
	reopened, err := wallet.OpenWithProfile(wallet.StorePath(walletDir), types.Regtest)
	if err != nil {
		t.Fatalf("reopen wallet store: %v", err)
	}
	loaded, err := reopened.Wallet("main")
	if err != nil {
		t.Fatalf("Wallet: %v", err)
	}
	if len(loaded.Addresses) != 2 {
		t.Fatalf("addresses = %d, want 2", len(loaded.Addresses))
	}
	if err := runWalletExport([]string{"--config", configPath, "--wallet-dir", walletDir}); err != nil {
		t.Fatalf("runWalletExport: %v", err)
	}
	if !fileExists(filepath.Join(walletDir, "main-wallet-export.json")) {
		t.Fatal("default export file was not written")
	}
}

func TestRejectInstalledMiningAutoProvision(t *testing.T) {
	cfg := config.Default()
	cfg.MinerEnabled = true
	if err := rejectInstalledMiningAutoProvision(config.DefaultConfigPath, cfg); err == nil || !strings.Contains(err.Error(), "install --mining on") {
		t.Fatalf("rejectInstalledMiningAutoProvision err = %v, want install guidance", err)
	}
	if err := rejectInstalledMiningAutoProvision(config.LegacyConfigPath, cfg); err == nil || !strings.Contains(err.Error(), "install --mining on") {
		t.Fatalf("legacy rejectInstalledMiningAutoProvision err = %v, want install guidance", err)
	}
	cfg.MinerPubKeyHex = "abcd"
	if err := rejectInstalledMiningAutoProvision(config.DefaultConfigPath, cfg); err != nil {
		t.Fatalf("configured pubkey should pass: %v", err)
	}
	cfg.MinerPubKeyHex = ""
	if err := rejectInstalledMiningAutoProvision(filepath.Join(t.TempDir(), "config.yaml"), cfg); err != nil {
		t.Fatalf("non-installed config should pass: %v", err)
	}
}

func TestWalletBackupRejectsEmptyStore(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(root, "chain")
	configPath := filepath.Join(root, "config.yaml")
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("Save config: %v", err)
	}
	err := runWalletBackup([]string{"--config", configPath, "--wallet-dir", filepath.Join(root, "wallets")})
	if err == nil || !strings.Contains(err.Error(), "no wallets found") {
		t.Fatalf("runWalletBackup err = %v, want empty-store refusal", err)
	}
}

func TestParseWalletFeePriority(t *testing.T) {
	tests := []struct {
		raw        string
		wantLabel  string
		wantBlocks int
	}{
		{raw: "now", wantLabel: "now", wantBlocks: 1},
		{raw: "soon", wantLabel: "soon", wantBlocks: 2},
		{raw: "relaxed", wantLabel: "relaxed", wantBlocks: 3},
		{raw: "cheap", wantLabel: "cheap", wantBlocks: 6},
		{raw: "normal", wantLabel: "soon", wantBlocks: 2},
		{raw: "slow", wantLabel: "cheap", wantBlocks: 6},
	}
	for _, test := range tests {
		label, blocks, err := parseWalletFeePriority(test.raw)
		if err != nil {
			t.Fatalf("parseWalletFeePriority(%q): %v", test.raw, err)
		}
		if label != test.wantLabel || blocks != test.wantBlocks {
			t.Fatalf("parseWalletFeePriority(%q) = (%q, %d), want (%q, %d)", test.raw, label, blocks, test.wantLabel, test.wantBlocks)
		}
	}
	if _, _, err := parseWalletFeePriority("mystery"); err == nil {
		t.Fatal("expected unknown priority to fail")
	}
}

func TestMinutesToTargetBlocksRoundsUp(t *testing.T) {
	tests := []struct {
		minutes int
		want    int
	}{
		{minutes: 1, want: 1},
		{minutes: 10, want: 1},
		{minutes: 11, want: 2},
		{minutes: 25, want: 3},
		{minutes: 60, want: 6},
	}
	for _, test := range tests {
		if got := minutesToTargetBlocks(test.minutes); got != test.want {
			t.Fatalf("minutesToTargetBlocks(%d) = %d, want %d", test.minutes, got, test.want)
		}
	}
}

func TestPromptWalletFeeQuoteInteractiveUsesRecommendedDefault(t *testing.T) {
	var out bytes.Buffer
	info := &node.MempoolInfo{}
	quote, err := promptWalletFeeQuoteInteractive(strings.NewReader("\n"), &out, info, func(targetBlocks int) (uint64, error) {
		return uint64(targetBlocks * 10), nil
	})
	if err != nil {
		t.Fatalf("promptWalletFeeQuoteInteractive: %v", err)
	}
	if quote.Label != "cheap" || quote.TargetBlocks != 6 || quote.FeeRate != 60 {
		t.Fatalf("default quote = %+v, want cheap / 6 blocks / 60 atoms-B", quote)
	}
	if !strings.Contains(out.String(), "recommended") {
		t.Fatalf("prompt output missing recommendation hint: %s", out.String())
	}
}

func TestPromptWalletFeeQuoteInteractiveSupportsCustomMinutes(t *testing.T) {
	var out bytes.Buffer
	quote, err := promptWalletFeeQuoteInteractive(strings.NewReader("6\n25\n"), &out, nil, func(targetBlocks int) (uint64, error) {
		return uint64(targetBlocks * 7), nil
	})
	if err != nil {
		t.Fatalf("promptWalletFeeQuoteInteractive custom minutes: %v", err)
	}
	if quote.Label != "custom" {
		t.Fatalf("quote label = %q, want custom", quote.Label)
	}
	if quote.TargetMinutes != 25 || quote.TargetBlocks != 3 {
		t.Fatalf("quote target = %d minutes / %d blocks, want 25 / 3", quote.TargetMinutes, quote.TargetBlocks)
	}
	if quote.FeeRate != 21 {
		t.Fatalf("quote fee rate = %d, want 21", quote.FeeRate)
	}
}

func TestRenderFanoutPreviewShowsBatchShape(t *testing.T) {
	view := renderFanoutPreview("miner", []string{"bpu:qone", "bpu:qtwo"}, 1_250_000_000, 3, 500, 0, nil)
	var out strings.Builder
	out.WriteString(view.title)
	for _, row := range view.rows {
		out.WriteString(row.label)
		out.WriteString(row.value)
	}
	text := out.String()
	for _, want := range []string{"fanout", "miner", "3", "2 addresses", "1.25 BPU", "3.75 BPU", "500 atoms each"} {
		if !strings.Contains(text, want) {
			t.Fatalf("fanout preview missing %q: %#v", want, view)
		}
	}
}

func TestRenderNodeStatusShowsOperatorSummary(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "chain")
	status := cliNodeStatus{
		Info: node.ServiceInfo{
			Profile:       "regtest",
			TipHeight:     7,
			HeaderHeight:  9,
			TipHeaderHash: strings.Repeat("a", 64),
			RPCAddr:       "127.0.0.1:18443",
		},
		Mempool: node.MempoolInfo{Count: 2, Bytes: 500, MinRelayFeePerByte: 1, MedianFee: 3, HighFee: 5},
		Mining:  node.MiningInfo{Enabled: true, Workers: 4},
		Peers:   []node.PeerInfo{{Addr: "127.0.0.1:18444"}},
	}
	out := renderNodeStatus(status, cfg)
	for _, want := range []string{"Bitcoin Pure status", "syncing", "blocks=7 headers=9", "1 peer", "2 tx / 500 bytes", "on (4 workers)", "events.jsonl"} {
		if !strings.Contains(out, want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
		}
	}
}

func TestRenderNodeStatusDoesNotCallIsolatedNodeSynced(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "chain")
	status := cliNodeStatus{
		Info: node.ServiceInfo{
			Profile:       "regtest",
			TipHeight:     7,
			HeaderHeight:  9,
			TipHeaderHash: strings.Repeat("a", 64),
			RPCAddr:       "127.0.0.1:18443",
		},
	}
	out := renderNodeStatus(status, cfg)
	if !strings.Contains(out, "waiting for peers") {
		t.Fatalf("status output did not flag isolated node:\n%s", out)
	}
	if strings.Contains(out, "health     synced") {
		t.Fatalf("status output called isolated node synced:\n%s", out)
	}
	if strings.Contains(out, "syncing") {
		t.Fatalf("status output masked zero peers with syncing:\n%s", out)
	}
}

func TestTerminalBoxWrapsToFixedWidth(t *testing.T) {
	out := renderTerminalBox("cpfp", []walletActionRow{
		{label: "parent", value: strings.Repeat("a", 64)},
		{label: "child", value: "3.9999995 BPU / 3999999500 atoms -> bpu:" + strings.Repeat("q", 80)},
	})
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		if len(line) != terminalBoxWidth {
			t.Fatalf("line length = %d, want %d:\n%s", len(line), terminalBoxWidth, out)
		}
	}
}

func TestLoadGenesisFixtureSupportsMainnet(t *testing.T) {
	loaded, err := loadGenesisFixtureFromPath(filepath.Join("..", "..", "fixtures", "genesis", "mainnet.json"))
	if err != nil {
		t.Fatalf("loadGenesisFixture: %v", err)
	}
	if loaded.Fixture.Profile != string(types.Mainnet) {
		t.Fatalf("fixture profile = %q, want %q", loaded.Fixture.Profile, types.Mainnet)
	}
}

func TestLoadGenesisFixtureRejectsBenchNet(t *testing.T) {
	if _, err := loadGenesisFixture(types.BenchNet); err == nil {
		t.Fatal("expected benchnet load to fail")
	}
}

func TestValidateLoopbackListenAddr(t *testing.T) {
	if err := validateLoopbackListenAddr("127.0.0.1:6060"); err != nil {
		t.Fatalf("validate loopback ipv4: %v", err)
	}
	if err := validateLoopbackListenAddr("[::1]:6060"); err != nil {
		t.Fatalf("validate loopback ipv6: %v", err)
	}
	if err := validateLoopbackListenAddr("localhost:6060"); err != nil {
		t.Fatalf("validate localhost: %v", err)
	}
	if err := validateLoopbackListenAddr(":6060"); err == nil {
		t.Fatal("expected wildcard pprof addr rejection")
	}
	if err := validateLoopbackListenAddr("0.0.0.0:6060"); err == nil {
		t.Fatal("expected non-loopback addr rejection")
	}
}

func TestNormalizePprofListenAddrUsesLoopbackShorthands(t *testing.T) {
	tests := map[string]string{
		"auto":  "127.0.0.1:6060",
		"6061":  "127.0.0.1:6061",
		":6062": "127.0.0.1:6062",
	}
	for raw, want := range tests {
		got, err := normalizePprofListenAddr(raw)
		if err != nil {
			t.Fatalf("normalizePprofListenAddr(%q): %v", raw, err)
		}
		if got != want {
			t.Fatalf("normalizePprofListenAddr(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestMaybeStartPprofServerServesLoopbackEndpoint(t *testing.T) {
	server, err := maybeStartPprofServer("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("maybeStartPprofServer: %v", err)
	}
	defer server.Close()

	resp, err := http.Get("http://" + server.ln.Addr().String() + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET /debug/pprof/: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
