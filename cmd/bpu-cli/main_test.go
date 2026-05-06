package main

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/wallet"
)

func TestBenchMicroWritesReports(t *testing.T) {
	root := t.TempDir()
	jsonPath := filepath.Join(root, "micro.json")
	markdownPath := filepath.Join(root, "micro.md")

	if err := run([]string{
		"bench", "micro",
		"--bench", "^BenchmarkTxAdmission$",
		"--count", "1",
		"--benchtime", "1x",
		"--report", jsonPath,
		"--markdown", markdownPath,
	}); err != nil {
		t.Fatalf("run bench micro: %v", err)
	}

	jsonData, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	if !strings.Contains(string(jsonData), "\"BenchmarkTxAdmission\"") {
		t.Fatalf("json report missing benchmark row: %s", jsonData)
	}

	markdownData, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatalf("read markdown: %v", err)
	}
	if !strings.Contains(string(markdownData), "MICRO BENCHMARKS") {
		t.Fatalf("markdown report missing banner")
	}
}

func TestBenchSimWritesReports(t *testing.T) {
	root := t.TempDir()
	jsonPath := filepath.Join(root, "sim.json")
	markdownPath := filepath.Join(root, "sim.md")

	if err := run([]string{
		"bench", "sim",
		"--nodes", "8",
		"--topology", "small-world",
		"--seed", "7",
		"--txs", "16",
		"--blocks", "4",
		"--base-latency", "5ms",
		"--latency-jitter", "15ms",
		"--churn-events", "2",
		"--churn-duration", "20ms",
		"--report", jsonPath,
		"--markdown", markdownPath,
	}); err != nil {
		t.Fatalf("run bench sim: %v", err)
	}

	jsonData, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	if !strings.Contains(string(jsonData), "\"topology\": \"small-world\"") {
		t.Fatalf("json report missing topology: %s", jsonData)
	}

	markdownData, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatalf("read markdown: %v", err)
	}
	if !strings.Contains(string(markdownData), "Network Simulation Report") {
		t.Fatalf("markdown report missing simulation title")
	}
}

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
	if err := validateLoopbackListenAddr("0.0.0.0:6060"); err == nil {
		t.Fatal("expected non-loopback addr rejection")
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
