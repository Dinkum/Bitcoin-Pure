package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/wallet"
)

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli status [--config PATH] [--rpc ADDR] [--rpc-auth-token TOKEN]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	status, err := fetchNodeStatus(client)
	if err != nil {
		return err
	}
	fmt.Print(renderNodeStatus(status, cfg))
	return nil
}

func runPeer(args []string) error {
	if len(args) == 0 {
		return errors.New("missing peer subcommand")
	}
	switch args[0] {
	case "add":
		return runPeerAdd(args[1:])
	default:
		return errors.New("unknown peer subcommand")
	}
}

func runPeerAdd(args []string) error {
	fs := flag.NewFlagSet("peer add", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	rpcAddr := fs.String("rpc", "", "")
	rpcAuthToken := fs.String("rpc-auth-token", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli peer add [--config PATH] [--rpc ADDR] [--rpc-auth-token TOKEN] HOST:PORT")
	}
	addr := strings.TrimSpace(fs.Arg(0))
	if addr == "" {
		return errors.New("peer address is required")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	client := newRPCClient(resolveRPCAddr(cfg, *rpcAddr), resolveRPCAuthToken(cfg, *rpcAuthToken), rpcClientTimeout(cfg))
	var result struct {
		Addr string `json:"addr"`
	}
	if err := client.Call("addpeer", map[string]string{"addr": addr}, &result); err != nil {
		return err
	}
	if result.Addr == "" {
		result.Addr = addr
	}
	fmt.Printf("peer add requested: %s\n", result.Addr)
	return nil
}

func runConfig(args []string) error {
	if len(args) == 0 {
		return errors.New("missing config subcommand")
	}
	switch args[0] {
	case "normalize":
		return runConfigNormalize(args[1:])
	case "mining":
		return runConfigMining(args[1:])
	default:
		return errors.New("unknown config subcommand")
	}
}

func runConfigNormalize(args []string) error {
	fs := flag.NewFlagSet("config normalize", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	inPath := fs.String("in", "", "")
	outPath := fs.String("out", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*outPath) == "" {
		return errors.New("usage: bpu-cli config normalize --out PATH [--in PATH]")
	}

	cfg := config.Default()
	if strings.TrimSpace(*inPath) != "" {
		loaded, err := config.Load(strings.TrimSpace(*inPath))
		if err != nil {
			return err
		}
		cfg = loaded
	}
	return config.Save(strings.TrimSpace(*outPath), cfg)
}

func runConfigMining(args []string) error {
	fs := flag.NewFlagSet("config mining", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	workers := fs.Int("workers", 0, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: bpu-cli config mining [--config PATH] [--workers N] on|off")
	}
	mode := strings.ToLower(strings.TrimSpace(fs.Arg(0)))
	if mode != "on" && mode != "off" {
		return fmt.Errorf("invalid mining mode %q: want on or off", fs.Arg(0))
	}
	if *workers < 0 {
		return errors.New("--workers must not be negative")
	}
	resolvedConfigPath := strings.TrimSpace(*configPath)
	if resolvedConfigPath == "" {
		for _, candidate := range config.DefaultPathCandidates() {
			if fileExists(candidate) {
				resolvedConfigPath = candidate
				break
			}
		}
	}
	if resolvedConfigPath == "" {
		return errors.New("config mining requires --config when no installed config exists")
	}
	cfg, err := config.Load(resolvedConfigPath)
	if err != nil {
		return err
	}
	cfg.MinerEnabled = mode == "on"
	if *workers > 0 {
		cfg.MinerWorkers = *workers
	}
	var addr wallet.Address
	var walletPath string
	if cfg.MinerEnabled {
		addr, walletPath, err = ensureMiningWalletProvisioned(resolvedConfigPath, &cfg)
		if err != nil {
			return err
		}
	}
	if err := saveCLIConfig(resolvedConfigPath, cfg); err != nil {
		return err
	}
	if cfg.MinerEnabled {
		fmt.Println("mining: on")
		if cfg.MinerWorkers > 0 {
			fmt.Printf("workers: %d\n", cfg.MinerWorkers)
		} else {
			fmt.Println("workers: auto")
		}
		if strings.TrimSpace(cfg.MinerPubKeyHex) != "" {
			fmt.Printf("miner_pubkey_hex: %s\n", cfg.MinerPubKeyHex)
		}
		if walletPath != "" {
			fmt.Printf("wallet_dir: %s\n", filepath.Dir(walletPath))
		}
		if addr.Address != "" {
			fmt.Printf("receive_address: %s\n", addr.Address)
		}
	} else {
		fmt.Println("mining: off")
	}
	return nil
}
