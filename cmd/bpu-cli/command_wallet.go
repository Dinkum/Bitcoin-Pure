package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/wallet"
)

func runWallet(args []string) error {
	if len(args) == 0 {
		return errors.New(walletUsage())
	}
	if args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Print(walletUsage())
		return nil
	}
	command, ok := walletCommands[args[0]]
	if !ok {
		return fmt.Errorf("unknown wallet subcommand %q\n\n%s", args[0], walletUsage())
	}
	err := command(args[1:])
	if errors.Is(err, errFlagHelpHandled) {
		return nil
	}
	return err
}

var walletCommands = map[string]cliCommand{
	"create":  runWalletCreate,
	"list":    runWalletList,
	"balance": runWalletBalance,
	"history": runWalletHistory,
	"fee":     runWalletFee,
	"receive": runWalletReceive,
	"send":    runWalletSend,
	"fanout":  runWalletFanout,
	"backup":  runWalletBackup,
	"restore": runWalletRestore,
	"export":  runWalletExport,
	"import":  runWalletImport,
	"cpfp":    runWalletCPFP,
}

func walletUsage() string {
	return strings.TrimSpace(`wallet commands:
  bpu-cli wallet create main                 create your first wallet
  bpu-cli wallet receive [wallet]            get a fresh receive address
  bpu-cli wallet balance [wallet]            show spendable, pending, and immature funds
  bpu-cli wallet history [wallet]            show recent wallet activity
  bpu-cli wallet send ADDRESS AMOUNT         send BPU with guided fee selection
  bpu-cli wallet backup                      write a private local backup
  bpu-cli wallet list                        show wallets, profile, and store path

Advanced:
  fee, fanout, cpfp, export, import, restore
`) + "\n"
}

func setWalletFlagUsage(fs *flag.FlagSet, usage string) {
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s\n\nOptions:\n", usage)
		fs.PrintDefaults()
	}
}

var errFlagHelpHandled = errors.New("flag help handled")

func parseWalletFlags(fs *flag.FlagSet, args []string) error {
	reordered, err := reorderFlagsBeforePositionals(fs, args)
	if err != nil {
		return err
	}
	if err := fs.Parse(reordered); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return errFlagHelpHandled
		}
		return err
	}
	return nil
}

type boolFlagValue interface {
	IsBoolFlag() bool
}

func reorderFlagsBeforePositionals(fs *flag.FlagSet, args []string) ([]string, error) {
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			positionals = append(positionals, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if idx := strings.IndexByte(name, '='); idx >= 0 {
			name = name[:idx]
		}
		defined := fs.Lookup(name)
		if defined == nil {
			flags = append(flags, arg)
			continue
		}
		flags = append(flags, arg)
		if strings.Contains(arg, "=") {
			continue
		}
		if boolValue, ok := defined.Value.(boolFlagValue); ok && boolValue.IsBoolFlag() {
			continue
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("flag needs an argument: -%s", name)
		}
		i++
		flags = append(flags, args[i])
	}
	return append(flags, positionals...), nil
}

func runWalletCreate(args []string) error {
	fs := flag.NewFlagSet("wallet create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet create [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [name]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	family := fs.String("family", wallet.AddressFamilyXOnly, "receive address family: xonly or pq")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet create [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [name]")
	}
	walletName := "main"
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	}
	cfg, resolvedConfigPath, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	outputType, err := wallet.ParseAddressFamily(*family)
	if err != nil {
		return err
	}
	entry, addr, err := store.CreateWalletWithType(walletName, outputType)
	if err != nil {
		return err
	}
	fmt.Println("wallet created")
	fmt.Printf("wallet: %s\n", entry.Name)
	fmt.Printf("profile: %s\n", cfg.Profile)
	fmt.Printf("store: %s\n", walletPath)
	fmt.Printf("created_at: %s\n", entry.CreatedAt.Format(time.RFC3339))
	fmt.Printf("receive_address: %s\n", addr.Address)
	fmt.Printf("family: %s\n", wallet.AddressFamilyLabel(addr.OutputType()))
	printWalletAddressDetails(addr)
	fmt.Println("share only receive_address")
	fmt.Printf("next: after funds arrive, run bpu-cli wallet balance %s\n", entry.Name)
	fmt.Printf("backup: %s\n", walletBackupCommand(resolvedConfigPath, walletPath))
	return nil
}

func walletBackupCommand(configPath string, walletPath string) string {
	prefix := ""
	if isInstalledWalletContext(configPath, walletPath) {
		prefix = "sudo "
	}
	return fmt.Sprintf("%sbpu-cli wallet backup%s --wallet-dir %s", prefix, formatConfigFlag(configPath), filepath.Dir(walletPath))
}

func isInstalledWalletContext(configPath string, walletPath string) bool {
	configPath = filepath.Clean(strings.TrimSpace(configPath))
	walletPath = filepath.Clean(strings.TrimSpace(walletPath))
	return strings.HasPrefix(configPath, "/etc/bitcoin-pure/") ||
		configPath == "/etc/bitcoin-pure/config.yaml" ||
		strings.HasPrefix(walletPath, "/var/lib/bitcoin-pure/")
}

func runWalletList(args []string) error {
	fs := flag.NewFlagSet("wallet list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet list [--config PATH] [--wallet-dir DIR]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet list [--config PATH] [--wallet-dir DIR]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	wallets := store.List()
	fmt.Println("wallets")
	fmt.Printf("  profile  %s\n", cfg.Profile)
	fmt.Printf("  store    %s\n", walletPath)
	if len(wallets) == 0 {
		fmt.Println("  status   no wallets yet")
		fmt.Println("  next     bpu-cli wallet create main")
		return nil
	}
	if len(wallets) == 1 {
		fmt.Printf("  default  %s\n", wallets[0].Name)
	} else {
		fmt.Println("  default  none; pass a wallet name")
	}
	for _, entry := range wallets {
		receive := "-"
		if latest := entry.LatestReceiveAddress(); latest != nil {
			receive = latest.Address
		}
		fmt.Printf("  %-8s addresses=%d  pending=%d  receive=%s\n", entry.Name, len(entry.Addresses), len(entry.Pending), receive)
	}
	return nil
}

func runWalletBalance(args []string) error {
	fs := flag.NewFlagSet("wallet balance", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet balance [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet balance [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	if _, err := store.Wallet(walletName); err != nil {
		return walletCommandError(walletName, err)
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(walletName)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, walletName, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, walletName, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, walletName, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	balance, err := store.Balance(walletName, utxos)
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", walletName)
	fmt.Printf("profile: %s\n", cfg.Profile)
	fmt.Printf("store: %s\n", walletPath)
	fmt.Printf("confirmed: %s (%d atoms)\n", wallet.FormatAmount(balance.Confirmed), balance.Confirmed)
	fmt.Printf("mature: %s (%d atoms)\n", wallet.FormatAmount(balance.Mature), balance.Mature)
	fmt.Printf("available: %s (%d atoms)\n", wallet.FormatAmount(balance.Available), balance.Available)
	fmt.Printf("immature: %s (%d atoms)\n", wallet.FormatAmount(balance.Immature), balance.Immature)
	fmt.Printf("reserved: %s (%d atoms)\n", wallet.FormatAmount(balance.Reserved), balance.Reserved)
	fmt.Printf("pending_txs: %d\n", balance.PendingCount)
	fmt.Printf("addresses: %d\n", balance.AddressCount)
	if balance.Confirmed == 0 && balance.Available == 0 && balance.PendingCount == 0 {
		fmt.Printf("next: bpu-cli wallet receive %s\n", walletName)
	}
	return nil
}

func runWalletHistory(args []string) error {
	fs := flag.NewFlagSet("wallet history", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet history [--limit N] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	limit := fs.Int("limit", 20, "maximum activity rows to show")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet history [--limit N] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN] [wallet]")
	}
	if *limit <= 0 {
		return errors.New("--limit must be positive")
	}
	if *limit > walletActivityRPCLimitMax {
		return fmt.Errorf("--limit must be <= %d", walletActivityRPCLimitMax)
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	watchItems, err := store.SpendableWatchItems(walletName)
	if err != nil {
		return walletCommandError(walletName, err)
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	activityLimit := walletMaxInt(*limit, walletReconcileActivityLimit(store, walletName, 20))
	if activityLimit > walletActivityRPCLimitMax {
		activityLimit = walletActivityRPCLimitMax
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, activityLimit)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, walletName, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, walletName, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	entry, err := store.Wallet(walletName)
	if err != nil {
		return walletCommandError(walletName, err)
	}
	if len(activity) > *limit {
		activity = activity[:*limit]
	}
	if len(activity) == 0 && len(entry.Pending) == 0 {
		fmt.Printf("wallet: %s\n", walletName)
		fmt.Println("activity: none yet")
		fmt.Printf("next: bpu-cli wallet receive %s\n", walletName)
		return nil
	}
	fmt.Printf("wallet: %s\n", walletName)
	for _, pending := range entry.Pending {
		fmt.Printf("pending  %s  tx=%s\n", pending.CreatedAt.Format(time.RFC3339), pending.TxID)
	}
	for _, item := range activity {
		fmt.Printf("%d  %s  tx=%s  received=%s  sent=%s  fee=%s  net=%s  %s\n",
			item.Height,
			item.Timestamp,
			item.TxID,
			wallet.FormatAmount(item.Received),
			wallet.FormatAmount(item.Sent),
			wallet.FormatAmount(item.Fee),
			formatSignedWalletAmount(item.Net),
			item.BlockHash,
		)
	}
	return nil
}

func runWalletFee(args []string) error {
	fs := flag.NewFlagSet("wallet fee", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet fee [--target-blocks N] [--tx-bytes N] [--config PATH] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	txBytes := fs.Int("tx-bytes", 250, "estimated transaction size in bytes")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet fee [--target-blocks N] [--tx-bytes N] [--config PATH] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	if *targetBlocks <= 0 {
		return errors.New("--target-blocks must be positive")
	}
	if *txBytes < 0 {
		return errors.New("--tx-bytes must be non-negative")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	feePerByte, err := rpcEstimateFee(client, *targetBlocks)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	fmt.Println("fee estimate")
	fmt.Printf("target_blocks: %d\n", *targetBlocks)
	fmt.Printf("fee_rate: %d atoms/B\n", feePerByte)
	estimatedFee := feePerByte * uint64(*txBytes)
	fmt.Printf("estimated_fee: %s (%d atoms)\n", wallet.FormatAmount(estimatedFee), estimatedFee)
	return nil
}

func runWalletReceive(args []string) error {
	fs := flag.NewFlagSet("wallet receive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet receive [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	family := fs.String("family", wallet.AddressFamilyXOnly, "receive address family: xonly or pq")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet receive [--family xonly|pq] [--config PATH] [--wallet-dir DIR] [wallet]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	outputType, err := wallet.ParseAddressFamily(*family)
	if err != nil {
		return err
	}
	addr, err := store.NewReceiveAddressWithType(walletName, outputType)
	if err != nil {
		return walletCommandError(walletName, err)
	}
	fmt.Println("receive")
	fmt.Printf("wallet: %s\n", walletName)
	fmt.Printf("receive_address: %s\n", addr.Address)
	fmt.Printf("family: %s\n", wallet.AddressFamilyLabel(addr.OutputType()))
	printWalletAddressDetails(addr)
	fmt.Println("next: share only receive_address")
	return nil
}

func runWalletBackup(args []string) error {
	fs := flag.NewFlagSet("wallet backup", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet backup [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	out := fs.String("out", "", "backup output path")
	overwrite := fs.Bool("overwrite", false, "allow replacing an existing backup file")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet backup [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	if len(store.List()) == 0 {
		return errors.New("wallet backup refused: no wallets found; run bpu-cli wallet create first or pass the correct --config/--wallet-dir")
	}
	backupPath := strings.TrimSpace(*out)
	if backupPath == "" {
		stamp := time.Now().UTC().Format("20060102T150405Z")
		backupPath = filepath.Join(filepath.Dir(walletPath), "wallets-"+stamp+".backup.json")
	}
	if samePath(backupPath, walletPath) {
		return errors.New("backup output cannot be the live wallet store")
	}
	if err := store.BackupWithOptions(backupPath, *overwrite); err != nil {
		return err
	}
	fmt.Printf("wallet_store: %s\n", walletPath)
	fmt.Printf("backup: %s\n", backupPath)
	fmt.Println("keep this file private; it can spend these wallets")
	return nil
}

func runWalletRestore(args []string) error {
	fs := flag.NewFlagSet("wallet restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet restore --from PATH [--yes] [--force-profile-mismatch] [--config PATH] [--wallet-dir DIR]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	from := fs.String("from", "", "backup file to restore")
	yes := fs.Bool("yes", false, "restore without interactive confirmation")
	forceProfileMismatch := fs.Bool("force-profile-mismatch", false, "restore a backup from another chain profile")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() == 1 && *from == "" {
		*from = fs.Arg(0)
	}
	if *from == "" || fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet restore --from PATH [--yes] [--force-profile-mismatch] [--config PATH] [--wallet-dir DIR]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	backup, err := wallet.LoadBackupFile(*from)
	if err != nil {
		return err
	}
	if backup.Profile != "" && backup.Profile != types.ChainProfile(cfg.Profile) && !*forceProfileMismatch {
		return fmt.Errorf("backup profile %q does not match current profile %q; pass --force-profile-mismatch only if you are sure", backup.Profile, cfg.Profile)
	}
	if !*yes && !stdinLooksInteractive() {
		return errors.New("wallet restore requires --yes when stdin is not interactive")
	}
	existingWallets := store.List()
	if !*yes {
		fmt.Println("restore wallet backup")
		fmt.Printf("  source   %s\n", *from)
		fmt.Printf("  target   %s\n", walletPath)
		fmt.Printf("  profile  %s\n", cfg.Profile)
		fmt.Printf("  replace  %d wallet(s) with %d wallet(s)\n", len(existingWallets), len(backup.Wallets))
		fmt.Print("replace local wallet store? [y/N]: ")
		var response string
		if _, err := fmt.Fscanln(os.Stdin, &response); err != nil {
			return errors.New("restore cancelled")
		}
		if strings.ToLower(strings.TrimSpace(response)) != "y" && strings.ToLower(strings.TrimSpace(response)) != "yes" {
			return errors.New("restore cancelled")
		}
	}
	safetyBackup := ""
	if len(existingWallets) > 0 {
		stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
		safetyBackup = filepath.Join(filepath.Dir(walletPath), "pre-restore-"+stamp+".backup.json")
		if err := store.BackupWithOptions(safetyBackup, false); err != nil {
			return err
		}
	}
	if err := store.RestoreBackupWithOptions(*from, *forceProfileMismatch); err != nil {
		return err
	}
	fmt.Printf("restored: %s\n", walletPath)
	if safetyBackup != "" {
		fmt.Printf("previous_backup: %s\n", safetyBackup)
	}
	return nil
}

func runWalletExport(args []string) error {
	fs := flag.NewFlagSet("wallet export", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet export [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR] [wallet]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	out := fs.String("out", "", "export output path")
	overwrite := fs.Bool("overwrite", false, "allow replacing an existing export file")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet export [--out PATH] [--overwrite] [--config PATH] [--wallet-dir DIR] [wallet]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, walletPath, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	walletName := ""
	if fs.NArg() == 1 {
		walletName = fs.Arg(0)
	} else {
		walletName, err = defaultWalletName(store, stdinLooksInteractive())
		if err != nil {
			return err
		}
	}
	export, err := store.ExportWallet(walletName)
	if err != nil {
		return err
	}
	outPath := strings.TrimSpace(*out)
	if outPath == "" {
		outPath = filepath.Join(filepath.Dir(walletPath), safeWalletFileStem(walletName)+"-wallet-export.json")
	}
	if samePath(outPath, walletPath) {
		return errors.New("export output cannot be the live wallet store")
	}
	if err := wallet.SaveExportFileWithOptions(outPath, export, *overwrite); err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", walletName)
	fmt.Printf("export: %s\n", outPath)
	fmt.Println("keep this file private; it can spend this wallet")
	return nil
}

func runWalletImport(args []string) error {
	fs := flag.NewFlagSet("wallet import", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet import [--name NAME] [--force] [--config PATH] [--wallet-dir DIR] <export-file>")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	name := fs.String("name", "", "imported wallet name")
	force := fs.Bool("force", false, "import a wallet from another chain profile")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli wallet import [--name NAME] [--force] [--config PATH] [--wallet-dir DIR] <export-file>")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	export, err := wallet.LoadExportFile(fs.Arg(0))
	if err != nil {
		return err
	}
	imported, err := store.ImportWalletWithOptions(export, *name, *force)
	if err != nil {
		return err
	}
	fmt.Printf("wallet: %s\n", imported.Name)
	fmt.Printf("profile: %s\n", cfg.Profile)
	fmt.Printf("addresses: %d\n", len(imported.Addresses))
	return nil
}

func runWalletSend(args []string) error {
	fs := flag.NewFlagSet("wallet send", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet send [ADDRESS AMOUNT] [--from NAME] [--amount BPU | --amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	from := fs.String("from", "", "wallet to spend from")
	to := fs.String("to", "", "destination receive address")
	amountRaw := fs.String("amount", "", "amount in BPU")
	amountAtoms := fs.Uint64("amount-atoms", 0, "amount in atoms")
	fee := fs.Uint64("fee", 0, "exact fee in atoms")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	targetMinutes := fs.Int("target-minutes", 0, "rough confirmation target in minutes")
	priority := fs.String("priority", "", "fee target: now, soon, relaxed, or cheap")
	yes := fs.Bool("yes", false, "broadcast without interactive confirmation")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	switch fs.NArg() {
	case 0:
	case 2:
		if *to != "" || *amountRaw != "" || *amountAtoms != 0 {
			return errors.New("positional ADDRESS AMOUNT cannot be combined with --to, --amount, or --amount-atoms")
		}
		if *to == "" {
			*to = fs.Arg(0)
		}
		if *amountRaw == "" && *amountAtoms == 0 {
			*amountRaw = fs.Arg(1)
		}
	default:
		return errors.New("usage: bpu-cli wallet send [ADDRESS AMOUNT] [--from NAME] [--amount BPU | --amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	if err := completeWalletSendInputs(store, from, to, amountRaw, amountAtoms, *yes); err != nil {
		return err
	}
	amount, err := resolveWalletAmount(*amountRaw, *amountAtoms)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(*from)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, *from, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, *from, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, *from, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	plan := wallet.SendPlan{}
	var feeQuote *walletFeeQuote
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
		}
		plan, err = store.BuildSend(*from, *to, amount, *fee, utxos)
		if err != nil {
			return walletCommandError(*from, err)
		}
	} else {
		quote, err := resolveWalletFeeQuote(client, walletFeeRequest{
			TargetBlocks:          *targetBlocks,
			TargetBlocksExplicit:  flagWasPassed(fs, "target-blocks"),
			TargetMinutes:         *targetMinutes,
			TargetMinutesExplicit: flagWasPassed(fs, "target-minutes"),
			Priority:              *priority,
			PriorityExplicit:      flagWasPassed(fs, "priority"),
			AllowInteractive:      stdinLooksInteractive() && !*yes,
		})
		if err != nil {
			return err
		}
		feeQuote = &quote
		plan, err = store.BuildSendAuto(*from, *to, amount, quote.FeeRate, utxos)
		if err != nil {
			return walletCommandError(*from, err)
		}
	}
	if err := maybeConfirmWalletAction(withWalletContext(renderSendPreview(plan, feeQuote), cfg, resolveRPCAddr(cfg, *rpcAddr)), *yes); err != nil {
		return err
	}
	result, err := submitWalletSendPlan(store, client, *from, plan)
	if err != nil {
		return err
	}
	printWalletAction(renderSendResult(plan, result.TxID, feeQuote))
	return nil
}

func runWalletFanout(args []string) error {
	fs := flag.NewFlagSet("wallet fanout", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet fanout --to ADDRESS[,ADDRESS...] --amount BPU --count N [--from NAME] [--amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	from := fs.String("from", "", "wallet to spend from")
	toRaw := fs.String("to", "", "comma-separated destination addresses")
	amountRaw := fs.String("amount", "", "amount per transaction in BPU")
	amountAtoms := fs.Uint64("amount-atoms", 0, "amount per transaction in atoms")
	count := fs.Int("count", 0, "number of transactions to create")
	fee := fs.Uint64("fee", 0, "exact fee per transaction in atoms")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	targetMinutes := fs.Int("target-minutes", 0, "rough confirmation target in minutes")
	priority := fs.String("priority", "", "fee target: now, soon, relaxed, or cheap")
	yes := fs.Bool("yes", false, "broadcast without interactive confirmation")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli wallet fanout --to ADDRESS[,ADDRESS...] --amount BPU --count N [--from NAME] [--amount-atoms ATOMS] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	if strings.TrimSpace(*toRaw) == "" || *count <= 0 {
		return errors.New("wallet fanout requires --to ADDRESS[,ADDRESS...] and --count N")
	}
	amount, err := resolveWalletAmount(*amountRaw, *amountAtoms)
	if err != nil {
		return err
	}
	destinations := splitCSV(*toRaw)
	if len(destinations) == 0 {
		return errors.New("wallet fanout requires at least one destination")
	}
	if amount > ^uint64(0)/uint64(*count) {
		return errors.New("wallet fanout total amount overflows atoms")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*from) == "" {
		name, err := defaultWalletNameFromFlag(store, stdinLooksInteractive() && !*yes)
		if err != nil {
			return err
		}
		*from = name
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(*from)
	if err != nil {
		return err
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, *from, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, *from, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, *from, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	var feeQuote *walletFeeQuote
	feeRate := uint64(0)
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
		}
	} else {
		quote, err := resolveWalletFeeQuote(client, walletFeeRequest{
			TargetBlocks:          *targetBlocks,
			TargetBlocksExplicit:  flagWasPassed(fs, "target-blocks"),
			TargetMinutes:         *targetMinutes,
			TargetMinutesExplicit: flagWasPassed(fs, "target-minutes"),
			Priority:              *priority,
			PriorityExplicit:      flagWasPassed(fs, "priority"),
			AllowInteractive:      stdinLooksInteractive() && !*yes,
		})
		if err != nil {
			return err
		}
		feeQuote = &quote
		feeRate = quote.FeeRate
	}
	plans := make([]wallet.SendPlan, 0, *count)
	workingUTXOs := append([]wallet.SpendableUTXO(nil), utxos...)
	knownAddresses := make([]wallet.Address, 0, *count)
	for i := 0; i < *count; i++ {
		to := destinations[i%len(destinations)]
		var plan wallet.SendPlan
		if *fee > 0 {
			plan, err = store.BuildSendWithKnownAddresses(*from, to, amount, *fee, workingUTXOs, knownAddresses)
		} else {
			plan, err = store.BuildSendAutoWithKnownAddresses(*from, to, amount, feeRate, workingUTXOs, knownAddresses)
		}
		if err != nil {
			return walletCommandError(*from, fmt.Errorf("fanout plan %d/%d: %w", i+1, *count, err))
		}
		plans = append(plans, plan)
		if plan.ChangeAddress != nil {
			knownAddresses = append(knownAddresses, *plan.ChangeAddress)
		}
		workingUTXOs = applyFanoutPlanToUTXOs(workingUTXOs, plan)
	}
	preview := withWalletContext(renderFanoutPlansPreview(*from, destinations, plans, feeQuote), cfg, resolveRPCAddr(cfg, *rpcAddr))
	if err := maybeConfirmWalletAction(preview, *yes); err != nil {
		return err
	}
	results := make([]walletFanoutResult, 0, *count)
	for i, plan := range plans {
		result, err := submitWalletSendPlan(store, client, *from, plan)
		if err != nil {
			if len(results) > 0 {
				printWalletFanoutPartialResult(*from, results, len(plans), feeQuote)
			}
			return fmt.Errorf("fanout tx %d/%d: %w", i+1, *count, err)
		}
		results = append(results, walletFanoutResult{Plan: plan, TxID: result.TxID})
	}
	printWalletFanoutResult(*from, results, feeQuote)
	return nil
}

func runWalletCPFP(args []string) error {
	fs := flag.NewFlagSet("wallet cpfp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	setWalletFlagUsage(fs, "bpu-cli wallet cpfp [PARENT_TXID] [--from NAME] [--txid PARENT_TXID] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	configPath := fs.String("config", "", "config file path")
	walletDir := fs.String("wallet-dir", "", "wallet store directory")
	rpcAddr := fs.String("rpc", "", "node RPC address")
	rpcAuthToken := fs.String("rpc-auth-token", "", "node RPC bearer token")
	from := fs.String("from", "", "wallet to spend from")
	parent := fs.String("txid", "", "parent transaction id to accelerate")
	fee := fs.Uint64("fee", 0, "exact child fee in atoms")
	targetBlocks := fs.Int("target-blocks", 1, "confirmation target in blocks")
	targetMinutes := fs.Int("target-minutes", 0, "rough confirmation target in minutes")
	priority := fs.String("priority", "", "fee target: now, soon, relaxed, or cheap")
	yes := fs.Bool("yes", false, "broadcast without interactive confirmation")
	if err := parseWalletFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() == 1 && *parent == "" {
		*parent = fs.Arg(0)
	}
	if *parent == "" || fs.NArg() > 1 {
		return errors.New("usage: bpu-cli wallet cpfp [PARENT_TXID] [--from NAME] [--txid PARENT_TXID] [--fee ATOMS | --priority now|soon|relaxed|cheap | --target-blocks N | --target-minutes N] [--yes] [--config PATH] [--wallet-dir DIR] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	parentTxID, err := decodeHex32(*parent)
	if err != nil {
		return err
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	store, _, err := openWalletStore(*walletDir, cfg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*from) == "" {
		name, err := defaultWalletNameFromFlag(store, stdinLooksInteractive() && !*yes)
		if err != nil {
			return err
		}
		*from = name
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	watchItems, err := store.SpendableWatchItems(*from)
	if err != nil {
		return walletCommandError(*from, err)
	}
	utxos, err := rpcUTXOsByWatchItems(client, watchItems)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	activity, err := rpcWalletActivityByWatchItems(client, watchItems, walletReconcileActivityLimit(store, *from, 20))
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	confirmed, err := confirmedWalletTxIDs(client, store, *from, activity)
	if err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	if err := reconcileWalletPending(store, client, *from, utxos, confirmed); err != nil {
		return walletRPCError(err, cfg, *rpcAddr)
	}
	var plan wallet.CPFPPlan
	var feeQuote *walletFeeQuote
	if *fee > 0 {
		if flagWasPassed(fs, "target-blocks") || flagWasPassed(fs, "target-minutes") || flagWasPassed(fs, "priority") {
			return errors.New("--fee cannot be combined with --target-blocks, --target-minutes, or --priority")
		}
		plan, err = store.BuildCPFPWithExactFee(*from, parentTxID, *fee)
		if err != nil {
			return walletCommandError(*from, err)
		}
	} else {
		quote, err := resolveWalletFeeQuote(client, walletFeeRequest{
			TargetBlocks:          *targetBlocks,
			TargetBlocksExplicit:  flagWasPassed(fs, "target-blocks"),
			TargetMinutes:         *targetMinutes,
			TargetMinutesExplicit: flagWasPassed(fs, "target-minutes"),
			Priority:              *priority,
			PriorityExplicit:      flagWasPassed(fs, "priority"),
			AllowInteractive:      stdinLooksInteractive() && !*yes,
		})
		if err != nil {
			return err
		}
		feeQuote = &quote
		plan, err = store.BuildCPFP(*from, parentTxID, quote.FeeRate)
		if err != nil {
			return walletCommandError(*from, err)
		}
	}
	if err := maybeConfirmWalletAction(withWalletContext(renderCPFPPreview(plan, feeQuote), cfg, resolveRPCAddr(cfg, *rpcAddr)), *yes); err != nil {
		return err
	}
	if err := store.MarkSubmitted(*from, plan.TransactionID, plan.Transaction, []wallet.SelectedInput{plan.Input}, &plan.SweepAddress); err != nil {
		return err
	}
	var result struct {
		TxID     string `json:"txid"`
		Fee      uint64 `json:"fee"`
		Orphaned bool   `json:"orphaned"`
	}
	if err := client.Call("submittx", map[string]string{"hex": plan.TransactionHex}, &result); err != nil {
		var remoteErr cliRPCRemoteError
		if errors.As(err, &remoteErr) {
			_ = store.ForgetPending(*from, plan.TransactionID)
		}
		return err
	}
	reportedTxID, err := decodeHex32(result.TxID)
	if err != nil {
		_ = store.ForgetPending(*from, plan.TransactionID)
		return err
	}
	if reportedTxID != plan.TransactionID {
		_ = store.ForgetPending(*from, plan.TransactionID)
		return fmt.Errorf("submitted txid mismatch: planned %x, node returned %s", plan.TransactionID, result.TxID)
	}
	if result.Orphaned {
		_ = store.ForgetPending(*from, plan.TransactionID)
		return fmt.Errorf("node stored CPFP child %s as an orphan; parent %x is not currently spendable by the node", result.TxID, plan.ParentTxID)
	}
	printWalletAction(renderCPFPResult(plan, result.TxID, feeQuote))
	return nil
}
