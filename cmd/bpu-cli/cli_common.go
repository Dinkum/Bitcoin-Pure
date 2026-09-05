package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	osuser "os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/wallet"
)

type cliRPCClient struct {
	addr  string
	token string
	http  *http.Client
}

type cliRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error"`
}

type cliRPCRemoteError struct {
	Method  string
	Message string
}

func (e cliRPCRemoteError) Error() string {
	return e.Message
}

func newRPCClient(addr string, token string, timeout time.Duration) *cliRPCClient {
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	return &cliRPCClient{
		addr:  strings.TrimSpace(addr),
		token: strings.TrimSpace(token),
		http: &http.Client{
			Timeout: timeout,
			// RPC has one configured endpoint. Do not forward credentials or
			// process an untrusted redirect as another authenticated request.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func rpcClientTimeout(cfg config.Config) time.Duration {
	timeout := time.Duration(cfg.RPCWriteTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	timeout += 5 * time.Second
	if timeout < 15*time.Second {
		return 15 * time.Second
	}
	return timeout
}

func (c *cliRPCClient) Call(method string, params any, out any) error {
	return c.callContext(context.Background(), method, params, out)
}

func (c *cliRPCClient) callContext(ctx context.Context, method string, params any, out any) error {
	if c.addr == "" {
		return errors.New("rpc address is required")
	}
	body, err := json.Marshal(map[string]any{
		"method": method,
		"params": params,
	})
	if err != nil {
		return err
	}
	endpoint := c.addr
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	started := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("rpc %s to %s failed after %s: %w", method, endpoint, time.Since(started).Round(time.Millisecond), err)
	}
	defer resp.Body.Close()
	limit := rpcResponseLimit(method)
	if resp.StatusCode != http.StatusOK {
		limit = rpcErrorBodyLimit
	}
	// LimitReader also bounds chunked, compressed, and dishonestly sized bodies.
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return fmt.Errorf("rpc %s read response from %s after %s: %w", method, endpoint, time.Since(started).Round(time.Millisecond), err)
	}
	if resp.StatusCode != http.StatusOK {
		if int64(len(respBody)) > limit {
			respBody = append(respBody[:limit], []byte(" [truncated]")...)
		}
		return fmt.Errorf("rpc %s returned %s: %s", method, resp.Status, strings.TrimSpace(string(respBody)))
	}
	if int64(len(respBody)) > limit {
		return fmt.Errorf("rpc %s response exceeds %d-byte limit", method, limit)
	}
	var rpcResp cliRPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return err
	}
	if rpcResp.Error != "" {
		return cliRPCRemoteError{Method: method, Message: rpcResp.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(rpcResp.Result, out)
}

const rpcErrorBodyLimit int64 = 8 << 10

func rpcResponseLimit(method string) int64 {
	switch method {
	case "getblock":
		// Hex doubles the canonical block byte budget; leave room for JSON.
		return 2*int64(types.DefaultCodecLimits().MaxBlockBytes) + (1 << 20)
	case "getmempool", "getutxosbypubkeys", "getutxosbywatchitems", "getutxoproofbatch", "getcompactstatepackage", "getblockfilter", "getfilterheaders", "getfiltercheckpoint", "getwalletactivitybypubkeys", "getwalletactivitybywatchitems":
		return 64 << 20
	default:
		return 8 << 20
	}
}

func resolveCLIConfig(configPath string) (config.Config, string, error) {
	cfg := config.Default()
	resolved := strings.TrimSpace(configPath)
	if resolved == "" {
		for _, candidate := range config.DefaultPathCandidates() {
			if fileExists(candidate) {
				resolved = candidate
				break
			}
		}
	}
	if resolved == "" {
		return cfg, "", nil
	}
	loaded, err := config.Load(resolved)
	if err != nil {
		return config.Config{}, "", err
	}
	return loaded, resolved, nil
}

func openWalletStore(walletDir string, cfg config.Config) (*wallet.Store, string, error) {
	dir := strings.TrimSpace(walletDir)
	if dir == "" {
		dir = filepath.Join(filepath.Dir(filepath.Clean(cfg.DBPath)), "wallets")
	}
	path := wallet.StorePath(dir)
	profile, err := types.ParseChainProfile(cfg.Profile)
	if err != nil {
		return nil, "", err
	}
	store, err := wallet.OpenWithProfile(path, profile)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			return nil, "", fmt.Errorf("wallet store %s is not readable or writable; run as the wallet owner or pass --wallet-dir DIR", path)
		}
		return nil, "", err
	}
	return store, path, nil
}

func samePath(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	leftEval, leftEvalErr := filepath.EvalSymlinks(left)
	rightEval, rightEvalErr := filepath.EvalSymlinks(right)
	if leftEvalErr == nil && rightEvalErr == nil {
		return leftEval == rightEval
	}
	leftAbs, leftErr := filepath.Abs(filepath.Clean(left))
	rightAbs, rightErr := filepath.Abs(filepath.Clean(right))
	if leftErr == nil && rightErr == nil {
		return leftAbs == rightAbs
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func formatConfigFlag(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return " --config " + path
}

func ensureMiningWalletProvisioned(configPath string, cfg *config.Config) (wallet.Address, string, error) {
	if cfg == nil || !cfg.MinerEnabled || strings.TrimSpace(cfg.MinerPubKeyHex) != "" {
		return wallet.Address{}, "", nil
	}
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return wallet.Address{}, "", errors.New("mining auto-setup requires a config file path when miner_pubkey_hex is empty")
	}
	store, walletPath, err := openWalletStore("", *cfg)
	if err != nil {
		return wallet.Address{}, "", err
	}
	const minerWalletName = "miner"
	existing, err := store.Wallet(minerWalletName)
	var addr wallet.Address
	switch {
	case err == nil:
		if latest := latestReceiveAddressWithType(existing, types.OutputXOnlyP2PK); latest != nil {
			addr = *latest
		} else {
			addr, err = store.NewReceiveAddressWithType(minerWalletName, types.OutputXOnlyP2PK)
			if err != nil {
				return wallet.Address{}, "", err
			}
		}
	case errors.Is(err, wallet.ErrWalletNotFound):
		_, addr, err = store.CreateWallet(minerWalletName)
		if err != nil {
			return wallet.Address{}, "", err
		}
	default:
		return wallet.Address{}, "", err
	}
	if strings.TrimSpace(addr.PubKeyHex) == "" {
		return wallet.Address{}, "", errors.New("miner wallet did not produce an xonly mining pubkey")
	}
	cfg.MinerPubKeyHex = addr.PubKeyHex
	if err := saveCLIConfig(configPath, *cfg); err != nil {
		return wallet.Address{}, "", err
	}
	return addr, walletPath, nil
}

func saveCLIConfig(path string, cfg config.Config) error {
	if err := config.Save(path, cfg); err != nil {
		return err
	}
	if !isInstalledConfigPath(path) {
		return nil
	}
	group, err := osuser.LookupGroup("bitcoin-pure")
	if err != nil {
		return fmt.Errorf("lookup bitcoin-pure group: %w", err)
	}
	gid, err := strconv.Atoi(group.Gid)
	if err != nil {
		return fmt.Errorf("parse bitcoin-pure gid %q: %w", group.Gid, err)
	}
	for _, candidate := range []string{config.DefaultConfigPath, config.LegacyConfigPath} {
		if !fileExists(candidate) {
			continue
		}
		if err := os.Chown(candidate, 0, gid); err != nil {
			return fmt.Errorf("set owner on %s: %w", candidate, err)
		}
		if err := os.Chmod(candidate, 0o640); err != nil {
			return fmt.Errorf("set mode on %s: %w", candidate, err)
		}
	}
	return nil
}

func rejectInstalledMiningAutoProvision(configPath string, cfg config.Config) error {
	if !cfg.MinerEnabled || strings.TrimSpace(cfg.MinerPubKeyHex) != "" {
		return nil
	}
	cleanConfig := filepath.Clean(strings.TrimSpace(configPath))
	if cleanConfig == "" || !isInstalledConfigPath(cleanConfig) {
		return nil
	}
	return errors.New("installed service config has mining enabled without miner_pubkey_hex; run `sudo bpu-cli config mining on` to provision the miner wallet before service start")
}

func isInstalledConfigPath(path string) bool {
	clean := filepath.Clean(path)
	for _, candidate := range config.DefaultPathCandidates() {
		if clean == filepath.Clean(candidate) {
			return true
		}
	}
	return false
}

func latestReceiveAddressWithType(entry wallet.Wallet, outputType uint64) *wallet.Address {
	for i := len(entry.Addresses) - 1; i >= 0; i-- {
		if entry.Addresses[i].Change || entry.Addresses[i].OutputType() != outputType {
			continue
		}
		addr := entry.Addresses[i]
		return &addr
	}
	return nil
}

func resolveRPCAddr(cfg config.Config, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.TrimSpace(override)
	}
	return cfg.RPCAddr
}

func resolveRPCAuthToken(cfg config.Config, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.TrimSpace(override)
	}
	return cfg.RPCAuthToken
}

func walletRPCError(err error, cfg config.Config, override string) error {
	if err == nil {
		return nil
	}
	addr := resolveRPCAddr(cfg, override)
	if strings.TrimSpace(addr) == "" {
		addr = "(not configured)"
	}
	return fmt.Errorf("wallet cannot reach node RPC at %s\nnext: start bpu-cli serve or pass --rpc ADDR\ndetail: %w", addr, err)
}

func walletCommandError(walletName string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, wallet.ErrInsufficientFunds):
		return fmt.Errorf("%w\nwallet %q does not have enough spendable funds yet; run `bpu-cli wallet balance %s` to check available, pending, and immature funds", err, walletName, walletName)
	case errors.Is(err, wallet.ErrWalletNotFound):
		return fmt.Errorf("%w\nwallet %q was not found; run `bpu-cli wallet list`", err, walletName)
	default:
		return err
	}
}

func reconcileWalletPending(store *wallet.Store, client *cliRPCClient, walletName string, utxos []wallet.SpendableUTXO, confirmed map[[32]byte]struct{}) error {
	var txids []string
	if err := client.Call("getmempool", map[string]any{}, &txids); err != nil {
		return err
	}
	mempool := make(map[[32]byte]struct{}, len(txids))
	for _, raw := range txids {
		txid, err := decodeHex32(raw)
		if err != nil {
			continue
		}
		mempool[txid] = struct{}{}
	}
	_, err := store.ReconcilePendingWithStatus(walletName, mempool, utxos, confirmed)
	return err
}

func walletReconcileActivityLimit(store *wallet.Store, walletName string, fallback int) int {
	entry, err := store.Wallet(walletName)
	if err != nil {
		return fallback
	}
	return walletMinInt(walletActivityRPCLimitMax, walletMaxInt(fallback, len(entry.Pending)*2))
}

func confirmedWalletTxIDs(client *cliRPCClient, store *wallet.Store, walletName string, activity []walletActivityRPCItem) (map[[32]byte]struct{}, error) {
	out := make(map[[32]byte]struct{}, len(activity))
	for _, item := range activity {
		txid, err := decodeHex32(item.TxID)
		if err != nil {
			continue
		}
		out[txid] = struct{}{}
	}
	entry, err := store.Wallet(walletName)
	if err != nil {
		return nil, err
	}
	for _, pending := range entry.Pending {
		txid, err := decodeHex32(pending.TxID)
		if err != nil {
			continue
		}
		if _, ok := out[txid]; ok {
			continue
		}
		status, err := rpcTxStatus(client, pending.TxID)
		if err != nil {
			return nil, err
		}
		if status.Confirmed {
			out[txid] = struct{}{}
		}
	}
	return out, nil
}

func rpcUTXOsByPubKeys(client *cliRPCClient, pubKeys [][32]byte) ([]wallet.SpendableUTXO, error) {
	items := make([]wallet.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, wallet.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return rpcUTXOsByWatchItems(client, items)
}

func rpcUTXOsByWatchItems(client *cliRPCClient, items []wallet.WatchItem) ([]wallet.SpendableUTXO, error) {
	params := struct {
		WatchItems []struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		} `json:"watchitems"`
	}{WatchItems: make([]struct {
		Type      uint64 `json:"type"`
		Payload32 string `json:"payload32"`
	}, 0, len(items))}
	for _, item := range items {
		params.WatchItems = append(params.WatchItems, struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		}{
			Type:      item.Type,
			Payload32: hex.EncodeToString(item.Payload32[:]),
		})
	}
	var result struct {
		UTXOs []struct {
			TxID          string `json:"txid"`
			Vout          uint32 `json:"vout"`
			Value         uint64 `json:"value"`
			Type          uint64 `json:"type"`
			Payload32     string `json:"payload32"`
			Height        uint64 `json:"height"`
			Confirmations uint64 `json:"confirmations"`
			Coinbase      bool   `json:"coinbase"`
			Mature        bool   `json:"mature"`
		} `json:"utxos"`
	}
	if err := client.Call("getutxosbywatchitems", params, &result); err != nil {
		return nil, err
	}
	out := make([]wallet.SpendableUTXO, 0, len(result.UTXOs))
	for _, item := range result.UTXOs {
		txid, err := decodeHex32(item.TxID)
		if err != nil {
			return nil, err
		}
		payload32, err := decodeHex32(item.Payload32)
		if err != nil {
			return nil, err
		}
		out = append(out, wallet.SpendableUTXO{
			OutPoint:      types.OutPoint{TxID: txid, Vout: item.Vout},
			Value:         item.Value,
			Type:          item.Type,
			Payload32:     payload32,
			PubKey:        legacyWalletPubKey(item.Type, payload32),
			Height:        item.Height,
			Confirmations: item.Confirmations,
			Coinbase:      item.Coinbase,
			Mature:        item.Mature,
		})
	}
	return out, nil
}

type walletActivityRPCItem struct {
	TxID      string `json:"txid"`
	BlockHash string `json:"block_hash"`
	Height    uint64 `json:"height"`
	Timestamp string `json:"timestamp"`
	Coinbase  bool   `json:"coinbase"`
	Received  uint64 `json:"received"`
	Sent      uint64 `json:"sent"`
	Fee       uint64 `json:"fee"`
	Net       int64  `json:"net"`
}

type walletTxStatusRPCItem struct {
	TxID      string `json:"txid"`
	Confirmed bool   `json:"confirmed"`
	Mempool   bool   `json:"mempool"`
	BlockHash string `json:"block_hash"`
}

func rpcTxStatus(client *cliRPCClient, txid string) (walletTxStatusRPCItem, error) {
	var result walletTxStatusRPCItem
	if err := client.Call("gettxstatus", map[string]string{"txid": txid}, &result); err != nil {
		return walletTxStatusRPCItem{}, err
	}
	return result, nil
}

func rpcWalletActivityByPubKeys(client *cliRPCClient, pubKeys [][32]byte, limit int) ([]walletActivityRPCItem, error) {
	items := make([]wallet.WatchItem, 0, len(pubKeys))
	for _, pubKey := range pubKeys {
		items = append(items, wallet.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: pubKey})
	}
	return rpcWalletActivityByWatchItems(client, items, limit)
}

func rpcWalletActivityByWatchItems(client *cliRPCClient, items []wallet.WatchItem, limit int) ([]walletActivityRPCItem, error) {
	params := struct {
		WatchItems []struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		} `json:"watchitems"`
		Limit int `json:"limit"`
	}{WatchItems: make([]struct {
		Type      uint64 `json:"type"`
		Payload32 string `json:"payload32"`
	}, 0, len(items)), Limit: limit}
	for _, item := range items {
		params.WatchItems = append(params.WatchItems, struct {
			Type      uint64 `json:"type"`
			Payload32 string `json:"payload32"`
		}{
			Type:      item.Type,
			Payload32: hex.EncodeToString(item.Payload32[:]),
		})
	}
	var result struct {
		Activity []walletActivityRPCItem `json:"activity"`
	}
	if err := client.Call("getwalletactivitybywatchitems", params, &result); err != nil {
		return nil, err
	}
	return result.Activity, nil
}

func legacyWalletPubKey(outputType uint64, payload32 [32]byte) [32]byte {
	if outputType == types.OutputXOnlyP2PK {
		return payload32
	}
	return [32]byte{}
}

func rpcEstimateFee(client *cliRPCClient, targetBlocks int) (uint64, error) {
	var result struct {
		FeePerByte uint64 `json:"fee_per_byte"`
	}
	if err := client.Call("estimatefee", map[string]int{"target_blocks": targetBlocks}, &result); err != nil {
		return 0, err
	}
	return result.FeePerByte, nil
}

func rpcMempoolInfo(client *cliRPCClient) (*node.MempoolInfo, error) {
	var result node.MempoolInfo
	if err := client.Call("getmempoolinfo", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

type cliNodeStatus struct {
	Info    node.ServiceInfo
	Mempool node.MempoolInfo
	Mining  node.MiningInfo
	Peers   []node.PeerInfo
}

func fetchNodeStatus(client *cliRPCClient) (cliNodeStatus, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var status cliNodeStatus
	calls := []struct {
		method string
		out    any
	}{{"getinfo", &status.Info}, {"getmempoolinfo", &status.Mempool}, {"getmininginfo", &status.Mining}, {"getpeerinfo", &status.Peers}}
	// Each request owns one result field; cancel sibling requests on failure.
	var wg sync.WaitGroup
	errs := make([]error, len(calls))
	for i, call := range calls {
		wg.Go(func() {
			errs[i] = client.callContext(ctx, call.method, nil, call.out)
			if errs[i] != nil {
				cancel()
			}
		})
	}
	wg.Wait()
	// Report the original failure before any cancellation it caused.
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return cliNodeStatus{}, err
		}
	}
	for _, err := range errs {
		if err != nil {
			return cliNodeStatus{}, err
		}
	}
	return status, nil
}

func renderNodeStatus(status cliNodeStatus, cfg config.Config) string {
	peerWord := "peers"
	if len(status.Peers) == 1 {
		peerWord = "peer"
	}
	syncState := "synced"
	if len(status.Peers) == 0 {
		syncState = "waiting for peers"
	} else if status.Info.HeaderHeight > status.Info.TipHeight {
		syncState = fmt.Sprintf("syncing (%d headers ahead)", status.Info.HeaderHeight-status.Info.TipHeight)
	}
	mining := "off"
	if status.Mining.Enabled {
		if status.Mining.Workers > 0 {
			mining = fmt.Sprintf("on (%d workers)", status.Mining.Workers)
		} else {
			mining = "on (auto workers)"
		}
	}
	dashboard := status.Info.RPCAddr
	if dashboard == "" {
		dashboard = cfg.RPCAddr
	}
	if dashboard != "" && !strings.Contains(dashboard, "://") {
		dashboard = "http://" + dashboard + "/"
	}
	rows := []walletActionRow{
		{label: "health", value: syncState},
		{label: "profile", value: status.Info.Profile},
		{label: "height", value: fmt.Sprintf("blocks=%d headers=%d", status.Info.TipHeight, status.Info.HeaderHeight)},
		{label: "tip", value: shortenHex(status.Info.TipHeaderHash, 16)},
		{label: "peers", value: fmt.Sprintf("%d %s", len(status.Peers), peerWord)},
		{label: "mempool", value: fmt.Sprintf("%d tx / %d bytes / %d orphans", status.Mempool.Count, status.Mempool.Bytes, status.Mempool.Orphans)},
		{label: "fees", value: fmt.Sprintf("min=%d median=%d high=%d atoms/B", status.Mempool.MinRelayFeePerByte, status.Mempool.MedianFee, status.Mempool.HighFee)},
		{label: "mining", value: mining},
	}
	if dashboard != "" {
		rows = append(rows, walletActionRow{label: "monitor", value: dashboard})
	}
	rows = append(rows, walletActionRow{label: "logs", value: shortenDisplayPath(resolveLogPath(cfg), 56)})
	return renderTerminalBox("Bitcoin Pure status", rows)
}

func shortenHex(raw string, keep int) string {
	raw = strings.TrimSpace(raw)
	if keep <= 0 || len(raw) <= keep {
		return raw
	}
	return raw[:keep] + "..."
}

func shortenDisplayPath(path string, keep int) string {
	path = filepath.Clean(strings.TrimSpace(path))
	if keep <= 0 || len(path) <= keep {
		return path
	}
	base := filepath.Base(path)
	parent := filepath.Base(filepath.Dir(path))
	suffix := filepath.Join(parent, base)
	if len(suffix)+4 <= keep {
		return ".../" + suffix
	}
	if len(base)+4 <= keep {
		return ".../" + base
	}
	return "..." + base[len(base)-(keep-3):]
}

func flagWasPassed(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func decodeHex32(raw string) ([32]byte, error) {
	var out [32]byte
	buf, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil || len(buf) != 32 {
		return out, fmt.Errorf("invalid 32-byte hex: %q", raw)
	}
	copy(out[:], buf)
	return out, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func usageError() error {
	return errors.New("usage: bpu-cli <serve|status|peer|wallet|validate-tx|validate-block|chain|snapshot|config|logs>")
}

func defaultGenesisFixture(profile types.ChainProfile) string {
	switch profile {
	case types.Mainnet:
		return "fixtures/genesis/mainnet.json"
	case types.Regtest:
		return "fixtures/genesis/regtest.json"
	case types.RegtestMedium:
		return "fixtures/genesis/regtest_medium.json"
	case types.RegtestHard:
		return "fixtures/genesis/regtest_hard.json"
	case types.BenchNet:
		return ""
	default:
		return ""
	}
}

func deriveLogPath(dbPath string) string {
	dir := filepath.Dir(filepath.Clean(dbPath))
	if dir == "." || dir == "" {
		return "events.jsonl"
	}
	return filepath.Join(dir, "events.jsonl")
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
