package main

type cliCommand func([]string) error

var cliCommands = map[string]cliCommand{
	"serve":          runServe,
	"wallet":         runWallet,
	"peer":           runPeer,
	"validate-tx":    runValidateTx,
	"validate-block": runValidateBlock,
	"chain":          runChain,
	"snapshot":       runSnapshot,
	"config":         runConfig,
	"logs":           runLogs,
	"status":         runStatus,
}
