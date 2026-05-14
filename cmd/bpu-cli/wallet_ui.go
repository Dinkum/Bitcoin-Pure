package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"bitcoin-pure/internal/config"
	"bitcoin-pure/internal/node"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/wallet"
)

func printWalletAddressDetails(addr wallet.Address) {
	switch addr.OutputType() {
	case types.OutputPQLock32:
		fmt.Printf("pq_lock: %s\n", addr.PayloadHex)
		fmt.Printf("alg: ml-dsa-65\n")
	default:
		fmt.Printf("pubkey: %s\n", addr.PubKeyHex)
	}
}

type walletSubmitResult struct {
	TxID     string `json:"txid"`
	Fee      uint64 `json:"fee"`
	Orphaned bool   `json:"orphaned"`
}

type walletFanoutResult struct {
	Plan wallet.SendPlan
	TxID string
}

func submitWalletSendPlan(store *wallet.Store, client *cliRPCClient, walletName string, plan wallet.SendPlan) (walletSubmitResult, error) {
	if err := store.MarkSubmitted(walletName, plan.TransactionID, plan.Transaction, plan.Inputs, plan.ChangeAddress); err != nil {
		return walletSubmitResult{}, err
	}
	var result walletSubmitResult
	if err := client.Call("submittx", map[string]string{"hex": plan.TransactionHex}, &result); err != nil {
		var remoteErr cliRPCRemoteError
		if errors.As(err, &remoteErr) {
			_ = store.ForgetPending(walletName, plan.TransactionID)
		}
		return walletSubmitResult{}, err
	}
	reportedTxID, err := decodeHex32(result.TxID)
	if err != nil {
		_ = store.ForgetPending(walletName, plan.TransactionID)
		return walletSubmitResult{}, err
	}
	if reportedTxID != plan.TransactionID {
		_ = store.ForgetPending(walletName, plan.TransactionID)
		return walletSubmitResult{}, fmt.Errorf("submitted txid mismatch: planned %x, node returned %s", plan.TransactionID, result.TxID)
	}
	if result.Orphaned {
		_ = store.ForgetPending(walletName, plan.TransactionID)
		return walletSubmitResult{}, fmt.Errorf("node stored transaction %s as an orphan; the node is missing at least one input", result.TxID)
	}
	return result, nil
}

func completeWalletSendInputs(store *wallet.Store, from *string, to *string, amountRaw *string, amountAtoms *uint64, yes bool) error {
	if strings.TrimSpace(*from) == "" {
		name, err := defaultWalletNameFromFlag(store, stdinLooksInteractive() && !yes)
		if err != nil {
			return err
		}
		*from = name
	}
	if strings.TrimSpace(*to) == "" || (strings.TrimSpace(*amountRaw) == "" && *amountAtoms == 0) {
		if yes || !stdinLooksInteractive() {
			return errors.New("usage: bpu-cli wallet send [ADDRESS AMOUNT] [--from NAME] [--amount BPU | --amount-atoms ATOMS]")
		}
		reader := bufio.NewReader(os.Stdin)
		if strings.TrimSpace(*to) == "" {
			line, err := promptLine(reader, "to: ")
			if err != nil {
				return err
			}
			*to = line
		}
		if strings.TrimSpace(*amountRaw) == "" && *amountAtoms == 0 {
			line, err := promptLine(reader, "amount (BPU, or append atoms): ")
			if err != nil {
				return err
			}
			*amountRaw = line
		}
	}
	if strings.TrimSpace(*to) == "" {
		return errors.New("destination address is required")
	}
	return nil
}

func defaultWalletName(store *wallet.Store, allowPrompt bool) (string, error) {
	return defaultWalletNameWithHint(store, allowPrompt, "pass a wallet name")
}

func defaultWalletNameFromFlag(store *wallet.Store, allowPrompt bool) (string, error) {
	return defaultWalletNameWithHint(store, allowPrompt, "pass --from NAME")
}

func defaultWalletNameWithHint(store *wallet.Store, allowPrompt bool, hint string) (string, error) {
	wallets := store.List()
	switch len(wallets) {
	case 0:
		return "", errors.New("no wallets yet\nnext: bpu-cli wallet create")
	case 1:
		return wallets[0].Name, nil
	}
	if !allowPrompt {
		names := make([]string, 0, len(wallets))
		for _, entry := range wallets {
			names = append(names, entry.Name)
		}
		return "", fmt.Errorf("multiple wallets found (%s); %s", strings.Join(names, ", "), hint)
	}
	fmt.Println("choose wallet")
	for i, entry := range wallets {
		fmt.Printf("  %d) %s\n", i+1, entry.Name)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := promptLine(reader, "choose wallet: ")
		if err != nil {
			return "", err
		}
		choice, err := strconv.Atoi(strings.TrimSpace(line))
		if err == nil && choice >= 1 && choice <= len(wallets) {
			return wallets[choice-1].Name, nil
		}
		for _, entry := range wallets {
			if strings.EqualFold(entry.Name, strings.TrimSpace(line)) {
				return entry.Name, nil
			}
		}
		fmt.Println("enter a wallet number or name")
	}
}

func promptLine(reader *bufio.Reader, prompt string) (string, error) {
	fmt.Print(prompt)
	raw, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	line := strings.TrimSpace(raw)
	if line == "" {
		return "", errors.New("input cancelled")
	}
	return line, nil
}

func resolveWalletAmount(raw string, atoms uint64) (uint64, error) {
	if strings.TrimSpace(raw) != "" && atoms != 0 {
		return 0, errors.New("--amount and --amount-atoms cannot be combined")
	}
	if atoms != 0 {
		return atoms, nil
	}
	return wallet.ParseAmount(raw)
}

func formatSignedWalletAmount(value int64) string {
	if value < 0 {
		return "-" + wallet.FormatAmount(uint64(-value))
	}
	return wallet.FormatAmount(uint64(value))
}

type walletActionRow struct {
	label string
	value string
}

type walletActionView struct {
	title string
	rows  []walletActionRow
}

func withWalletContext(view walletActionView, cfg config.Config, rpcAddr string) walletActionView {
	rows := make([]walletActionRow, 0, len(view.rows)+2)
	rows = append(rows,
		walletActionRow{label: "profile", value: cfg.Profile},
		walletActionRow{label: "rpc", value: rpcAddr},
	)
	rows = append(rows, view.rows...)
	view.rows = rows
	return view
}

type walletFeeRequest struct {
	TargetBlocks          int
	TargetBlocksExplicit  bool
	TargetMinutes         int
	TargetMinutesExplicit bool
	Priority              string
	PriorityExplicit      bool
	AllowInteractive      bool
}

type walletFeeQuote struct {
	Label         string
	TargetBlocks  int
	TargetMinutes int
	FeeRate       uint64
	Mempool       *node.MempoolInfo
}

func renderSendPreview(plan wallet.SendPlan, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "to", value: plan.ToAddress},
		{label: "amount", value: formatWalletAmountWithAtoms(plan.Amount)},
	}
	if pqInputs := countPQInputs(plan.Inputs); pqInputs > 0 {
		rows = append(rows, walletActionRow{
			label: "warning",
			value: fmt.Sprintf("%d PQ input(s); auth payloads are large and the fee reflects the larger transaction size", pqInputs),
		})
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
		walletActionRow{label: "inputs", value: fmt.Sprintf("%d (%s)", len(plan.Inputs), formatWalletAmountWithAtoms(plan.InputTotal))},
		walletActionRow{label: "txid", value: fmt.Sprintf("%x", plan.TransactionID)},
	)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		rows = append(rows, walletActionRow{label: "change", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Change), plan.ChangeAddress.Address)})
	}
	return walletActionView{title: "send", rows: rows}
}

func renderSendResult(plan wallet.SendPlan, txid string, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "txid", value: txid},
		{label: "amount", value: formatWalletAmountWithAtoms(plan.Amount)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
	)
	if plan.Change > 0 && plan.ChangeAddress != nil {
		rows = append(rows, walletActionRow{label: "change", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Change), plan.ChangeAddress.Address)})
	}
	return walletActionView{title: "submitted", rows: rows}
}

func renderCPFPPreview(plan wallet.CPFPPlan, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "parent", value: hex.EncodeToString(plan.ParentTxID[:])},
		{label: "scope", value: "child fee only; parent package rate not estimated"},
		{label: "source", value: fmt.Sprintf("%x:%d (%s)", plan.Input.OutPoint.TxID, plan.Input.OutPoint.Vout, formatWalletAmountWithAtoms(plan.Input.Value))},
		{label: "child", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Amount), plan.SweepAddress.Address)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows,
		walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)},
		walletActionRow{label: "txid", value: fmt.Sprintf("%x", plan.TransactionID)},
	)
	return walletActionView{title: "cpfp", rows: rows}
}

func renderCPFPResult(plan wallet.CPFPPlan, txid string, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: plan.WalletName},
		{label: "parent", value: hex.EncodeToString(plan.ParentTxID[:])},
		{label: "txid", value: txid},
		{label: "scope", value: "child fee only; parent package rate not estimated"},
		{label: "child", value: fmt.Sprintf("%s -> %s", formatWalletAmountWithAtoms(plan.Amount), plan.SweepAddress.Address)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	rows = append(rows, walletActionRow{label: "fee", value: formatWalletFee(plan.Fee, plan.FeeRate, plan.EstimatedBytes)})
	return walletActionView{title: "submitted", rows: rows}
}

func formatWalletFee(fee uint64, feeRate uint64, estimatedBytes int) string {
	if estimatedBytes == 0 {
		return formatWalletAmountWithAtoms(fee)
	}
	if feeRate == 0 {
		return fmt.Sprintf("%s (effective %s, %d B)", formatWalletAmountWithAtoms(fee), formatEffectiveWalletFeeRate(fee, estimatedBytes), estimatedBytes)
	}
	return fmt.Sprintf("%s (%d atoms/B, %d B)", formatWalletAmountWithAtoms(fee), feeRate, estimatedBytes)
}

func formatEffectiveWalletFeeRate(fee uint64, estimatedBytes int) string {
	if estimatedBytes <= 0 {
		return "0 atoms/B"
	}
	bytes := uint64(estimatedBytes)
	if fee%bytes == 0 {
		return fmt.Sprintf("%d atoms/B", fee/bytes)
	}
	return fmt.Sprintf("%.2f atoms/B", float64(fee)/float64(bytes))
}

func formatWalletAmountWithAtoms(atoms uint64) string {
	return fmt.Sprintf("%s / %d atoms", wallet.FormatAmount(atoms), atoms)
}

func countPQInputs(inputs []wallet.SelectedInput) int {
	count := 0
	for _, input := range inputs {
		if input.Address.OutputType() == types.OutputPQLock32 {
			count++
		}
	}
	return count
}

func renderFanoutPreview(walletName string, destinations []string, amount uint64, count int, fee uint64, feeRate uint64, feeQuote *walletFeeQuote) walletActionView {
	rows := []walletActionRow{
		{label: "wallet", value: walletName},
		{label: "txs", value: fmt.Sprintf("%d", count)},
		{label: "to", value: formatWalletDestinationSummary(destinations)},
		{label: "amount", value: formatWalletAmountWithAtoms(amount)},
		{label: "total", value: formatWalletAmountWithAtoms(amount * uint64(count))},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	switch {
	case fee > 0:
		rows = append(rows, walletActionRow{label: "fee", value: formatWalletAmountWithAtoms(fee) + " each"})
	case feeRate > 0:
		rows = append(rows, walletActionRow{label: "fee", value: fmt.Sprintf("%d atoms/B", feeRate)})
	}
	return walletActionView{title: "fanout", rows: rows}
}

func renderFanoutPlansPreview(walletName string, destinations []string, plans []wallet.SendPlan, feeQuote *walletFeeQuote) walletActionView {
	totalAmount := uint64(0)
	totalFee := uint64(0)
	totalInputs := 0
	plannedTxIDs := make(map[[32]byte]struct{}, len(plans))
	dependentTxs := 0
	for _, plan := range plans {
		dependsOnEarlier := false
		for _, input := range plan.Inputs {
			if _, ok := plannedTxIDs[input.OutPoint.TxID]; ok {
				dependsOnEarlier = true
			}
		}
		if dependsOnEarlier {
			dependentTxs++
		}
		plannedTxIDs[plan.TransactionID] = struct{}{}
		totalAmount += plan.Amount
		totalFee += plan.Fee
		totalInputs += len(plan.Inputs)
	}
	rows := []walletActionRow{
		{label: "wallet", value: walletName},
		{label: "txs", value: fmt.Sprintf("%d", len(plans))},
		{label: "to", value: formatWalletDestinationSummary(destinations)},
		{label: "total", value: formatWalletAmountWithAtoms(totalAmount)},
		{label: "fee", value: formatWalletAmountWithAtoms(totalFee)},
		{label: "inputs", value: fmt.Sprintf("%d total", totalInputs)},
	}
	if dependentTxs > 0 {
		rows = append(rows, walletActionRow{label: "chain", value: fmt.Sprintf("%d txs spend earlier fanout change", dependentTxs)})
	} else {
		rows = append(rows, walletActionRow{label: "chain", value: "independent inputs"})
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	return walletActionView{title: "fanout", rows: rows}
}

func printWalletFanoutResult(walletName string, results []walletFanoutResult, feeQuote *walletFeeQuote) {
	printWalletFanoutResultWithTitle("fanout submitted", walletName, results, len(results), feeQuote)
}

func printWalletFanoutPartialResult(walletName string, results []walletFanoutResult, plannedCount int, feeQuote *walletFeeQuote) {
	printWalletFanoutResultWithTitle("fanout partial failure", walletName, results, plannedCount, feeQuote)
}

func printWalletFanoutResultWithTitle(title string, walletName string, results []walletFanoutResult, plannedCount int, feeQuote *walletFeeQuote) {
	totalAmount := uint64(0)
	totalFee := uint64(0)
	for _, result := range results {
		totalAmount += result.Plan.Amount
		totalFee += result.Plan.Fee
	}
	rows := []walletActionRow{
		{label: "wallet", value: walletName},
		{label: "txs", value: fmt.Sprintf("%d/%d", len(results), plannedCount)},
		{label: "amount", value: formatWalletAmountWithAtoms(totalAmount)},
		{label: "fee", value: formatWalletAmountWithAtoms(totalFee)},
	}
	rows = append(rows, walletFeeQuoteRows(feeQuote)...)
	printWalletAction(walletActionView{title: title, rows: rows})
	for i, result := range results {
		fmt.Printf("  %03d  %s  %s\n", i+1, result.TxID, result.Plan.ToAddress)
	}
}

func applyFanoutPlanToUTXOs(utxos []wallet.SpendableUTXO, plan wallet.SendPlan) []wallet.SpendableUTXO {
	spent := make(map[types.OutPoint]struct{}, len(plan.Inputs))
	for _, input := range plan.Inputs {
		spent[input.OutPoint] = struct{}{}
	}
	next := make([]wallet.SpendableUTXO, 0, len(utxos)+1)
	for _, utxo := range utxos {
		if _, ok := spent[utxo.OutPoint]; ok {
			continue
		}
		next = append(next, utxo)
	}
	if plan.Change > 0 && plan.ChangeAddress != nil {
		item, err := plan.ChangeAddress.WatchItem()
		if err == nil {
			next = append(next, wallet.SpendableUTXO{
				OutPoint:  types.OutPoint{TxID: plan.TransactionID, Vout: 1},
				Value:     plan.Change,
				Type:      item.Type,
				Payload32: item.Payload32,
				PubKey:    legacyWalletPubKey(item.Type, item.Payload32),
			})
		}
	}
	return next
}

func formatWalletDestinationSummary(destinations []string) string {
	if len(destinations) == 0 {
		return "-"
	}
	if len(destinations) == 1 {
		return destinations[0]
	}
	return fmt.Sprintf("%d addresses, round-robin from %s", len(destinations), destinations[0])
}

func safeWalletFileStem(name string) string {
	var b strings.Builder
	for _, ch := range strings.TrimSpace(name) {
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			b.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			b.WriteRune(ch)
		case ch == '-' || ch == '_':
			b.WriteRune(ch)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "wallet"
	}
	return b.String()
}

func resolveWalletFeeQuote(client *cliRPCClient, req walletFeeRequest) (walletFeeQuote, error) {
	selectors := 0
	if req.TargetBlocksExplicit {
		selectors++
	}
	if req.TargetMinutesExplicit {
		selectors++
	}
	if req.PriorityExplicit && strings.TrimSpace(req.Priority) != "" {
		selectors++
	}
	if selectors > 1 {
		return walletFeeQuote{}, errors.New("choose only one of --priority, --target-blocks, or --target-minutes")
	}
	info, err := rpcMempoolInfo(client)
	if err != nil {
		info = nil
	}
	estimate := func(targetBlocks int) (uint64, error) {
		return rpcEstimateFee(client, targetBlocks)
	}
	switch {
	case req.PriorityExplicit && strings.TrimSpace(req.Priority) != "":
		label, blocks, err := parseWalletFeePriority(req.Priority)
		if err != nil {
			return walletFeeQuote{}, err
		}
		return buildWalletFeeQuote(label, blocks, blocks*10, info, estimate)
	case req.TargetMinutesExplicit:
		if req.TargetMinutes <= 0 {
			return walletFeeQuote{}, errors.New("--target-minutes must be positive")
		}
		return buildWalletFeeQuote("custom", minutesToTargetBlocks(req.TargetMinutes), req.TargetMinutes, info, estimate)
	case req.TargetBlocksExplicit:
		if req.TargetBlocks <= 0 {
			return walletFeeQuote{}, errors.New("--target-blocks must be positive")
		}
		return buildWalletFeeQuote("custom", req.TargetBlocks, req.TargetBlocks*10, info, estimate)
	case req.AllowInteractive:
		return promptWalletFeeQuoteInteractive(os.Stdin, os.Stdout, info, estimate)
	default:
		label, blocks, err := parseWalletFeePriority(recommendedWalletFeeLabel(info))
		if err != nil {
			return walletFeeQuote{}, err
		}
		return buildWalletFeeQuote(label, blocks, blocks*10, info, estimate)
	}
}

func buildWalletFeeQuote(label string, targetBlocks int, targetMinutes int, info *node.MempoolInfo, estimate func(int) (uint64, error)) (walletFeeQuote, error) {
	if targetBlocks <= 0 {
		return walletFeeQuote{}, errors.New("target blocks must be positive")
	}
	feeRate, err := estimate(targetBlocks)
	if err != nil {
		return walletFeeQuote{}, err
	}
	if targetMinutes <= 0 {
		targetMinutes = targetBlocks * 10
	}
	return walletFeeQuote{
		Label:         label,
		TargetBlocks:  targetBlocks,
		TargetMinutes: targetMinutes,
		FeeRate:       feeRate,
		Mempool:       info,
	}, nil
}

func promptWalletFeeQuoteInteractive(in io.Reader, out io.Writer, info *node.MempoolInfo, estimate func(int) (uint64, error)) (walletFeeQuote, error) {
	presets := []struct {
		label  string
		blocks int
	}{
		{label: "now", blocks: 1},
		{label: "soon", blocks: 2},
		{label: "relaxed", blocks: 3},
		{label: "cheap", blocks: 6},
	}
	quotes := make([]walletFeeQuote, 0, len(presets))
	for _, preset := range presets {
		quote, err := buildWalletFeeQuote(preset.label, preset.blocks, preset.blocks*10, info, estimate)
		if err != nil {
			return walletFeeQuote{}, err
		}
		quotes = append(quotes, quote)
	}
	defaultIndex := 1
	recommended := recommendedWalletFeeLabel(info)
	for i := range quotes {
		if quotes[i].Label == recommended {
			defaultIndex = i
			break
		}
	}
	reader := bufio.NewReader(in)
	fmt.Fprintln(out, "fee target")
	if info != nil {
		fmt.Fprintf(out, "  mempool  %s\n", formatWalletMempoolSummary(*info))
	}
	for i, quote := range quotes {
		suffix := ""
		if i == defaultIndex {
			suffix = " (recommended)"
		}
		fmt.Fprintf(out, "  %d) %-7s %s  %d atoms/B%s\n", i+1, quote.Label, formatWalletTargetSummary(&quote), quote.FeeRate, suffix)
	}
	fmt.Fprintln(out, "  5) blocks   custom block target")
	fmt.Fprintln(out, "  6) minutes  custom minute target")
	for {
		fmt.Fprintf(out, "choose fee target [%d]: ", defaultIndex+1)
		raw, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return walletFeeQuote{}, err
		}
		choice := strings.TrimSpace(raw)
		if choice == "" {
			return quotes[defaultIndex], nil
		}
		switch choice {
		case "1", "2", "3", "4":
			return quotes[int(choice[0]-'1')], nil
		case "5":
			blocks, err := promptPositiveInt(reader, out, "confirm in how many blocks? ")
			if err != nil {
				return walletFeeQuote{}, err
			}
			return buildWalletFeeQuote("custom", blocks, blocks*10, info, estimate)
		case "6":
			minutes, err := promptPositiveInt(reader, out, "confirm in roughly how many minutes? ")
			if err != nil {
				return walletFeeQuote{}, err
			}
			return buildWalletFeeQuote("custom", minutesToTargetBlocks(minutes), minutes, info, estimate)
		default:
			fmt.Fprintln(out, "choose 1-6, or press enter for the recommended target")
			if errors.Is(err, io.EOF) {
				return walletFeeQuote{}, errors.New("fee target selection cancelled")
			}
		}
	}
}

func promptPositiveInt(reader *bufio.Reader, out io.Writer, prompt string) (int, error) {
	for {
		fmt.Fprint(out, prompt)
		raw, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		value, convErr := strconv.Atoi(strings.TrimSpace(raw))
		if convErr == nil && value > 0 {
			return value, nil
		}
		fmt.Fprintln(out, "enter a positive integer")
		if errors.Is(err, io.EOF) {
			return 0, errors.New("fee target selection cancelled")
		}
	}
}

func parseWalletFeePriority(raw string) (string, int, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "now", "fast", "asap":
		return "now", 1, nil
	case "soon", "normal":
		return "soon", 2, nil
	case "relaxed", "standard":
		return "relaxed", 3, nil
	case "cheap", "slow":
		return "cheap", 6, nil
	default:
		return "", 0, errors.New("unknown --priority value (expected: now, soon, relaxed, cheap)")
	}
}

func minutesToTargetBlocks(minutes int) int {
	if minutes <= 0 {
		return 1
	}
	return (minutes + 9) / 10
}

func recommendedWalletFeeLabel(info *node.MempoolInfo) string {
	if info == nil {
		return "soon"
	}
	switch classifyWalletMempoolPressure(*info) {
	case "high":
		return "now"
	case "active":
		return "soon"
	case "idle":
		return "cheap"
	default:
		return "relaxed"
	}
}

func classifyWalletMempoolPressure(info node.MempoolInfo) string {
	maxBytes := info.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	switch {
	case info.Count == 0 && info.Orphans == 0 && info.Bytes == 0:
		return "idle"
	case info.Bytes >= (maxBytes*8)/10 || info.Count >= 10_000 || info.Orphans >= walletMaxInt(32, 128/2):
		return "high"
	case info.Bytes >= walletMaxInt(16<<20, maxBytes/4) || info.Count >= 1_000 || info.Orphans > 0:
		return "active"
	default:
		return "normal"
	}
}

func walletMaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func walletMinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func formatWalletTargetSummary(quote *walletFeeQuote) string {
	if quote == nil {
		return ""
	}
	blockWord := "blocks"
	if quote.TargetBlocks == 1 {
		blockWord = "block"
	}
	return fmt.Sprintf("~%d min / %d %s", quote.TargetMinutes, quote.TargetBlocks, blockWord)
}

func formatWalletMempoolSummary(info node.MempoolInfo) string {
	pressure := classifyWalletMempoolPressure(info)
	if info.Count == 0 && info.Orphans == 0 && info.Bytes == 0 {
		return fmt.Sprintf("%s, empty, min relay %d atoms/B", pressure, info.MinRelayFeePerByte)
	}
	return fmt.Sprintf("%s, %d tx, fee totals median %d atoms, range %d-%d", pressure, info.Count, info.MedianFee, info.LowFee, info.HighFee)
}

func walletFeeQuoteRows(quote *walletFeeQuote) []walletActionRow {
	if quote == nil {
		return nil
	}
	rows := []walletActionRow{
		{label: "target", value: fmt.Sprintf("%s (%s)", quote.Label, formatWalletTargetSummary(quote))},
	}
	if quote.Mempool != nil {
		rows = append(rows, walletActionRow{label: "market", value: formatWalletMempoolSummary(*quote.Mempool)})
	}
	return rows
}

func printWalletAction(view walletActionView) {
	title := strings.TrimSpace(view.title)
	if title == "" {
		title = "wallet"
	}
	fmt.Print(renderTerminalBox(title, view.rows))
}

const (
	terminalBoxWidth      = 72
	terminalBoxLabelWidth = 9
)

func renderTerminalBox(title string, rows []walletActionRow) string {
	title = strings.TrimSpace(title)
	if title == "" {
		title = "status"
	}
	innerWidth := terminalBoxWidth - 2
	contentWidth := innerWidth - 2
	var b strings.Builder
	border := "+" + strings.Repeat("-", innerWidth) + "+\n"
	b.WriteString(border)
	b.WriteString(formatTerminalBoxLine(title))
	b.WriteString(border)
	valueWidth := contentWidth - terminalBoxLabelWidth - 2
	if valueWidth < 16 {
		valueWidth = 16
	}
	for _, row := range rows {
		label := strings.TrimSpace(row.label)
		if len(label) > terminalBoxLabelWidth {
			label = label[:terminalBoxLabelWidth]
		}
		lines := wrapTerminalText(strings.TrimSpace(row.value), valueWidth)
		if len(lines) == 0 {
			lines = []string{""}
		}
		for i, line := range lines {
			rowLabel := label
			if i > 0 {
				rowLabel = ""
			}
			b.WriteString(formatTerminalBoxLine(fmt.Sprintf("%-*s  %s", terminalBoxLabelWidth, rowLabel, line)))
		}
	}
	b.WriteString(border)
	return b.String()
}

func formatTerminalBoxLine(text string) string {
	contentWidth := terminalBoxWidth - 4
	if len(text) > contentWidth {
		text = text[:contentWidth]
	}
	return fmt.Sprintf("| %-*s |\n", contentWidth, text)
}

func wrapTerminalText(text string, width int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}
	lines := []string{}
	current := ""
	flushCurrent := func() {
		if current != "" {
			lines = append(lines, current)
			current = ""
		}
	}
	for _, word := range words {
		for len(word) > width {
			if current != "" {
				flushCurrent()
			}
			lines = append(lines, word[:width])
			word = word[width:]
		}
		if current == "" {
			current = word
			continue
		}
		if len(current)+1+len(word) <= width {
			current += " " + word
			continue
		}
		flushCurrent()
		current = word
	}
	flushCurrent()
	return lines
}

func maybeConfirmWalletAction(view walletActionView, yes bool) error {
	if yes {
		return nil
	}
	if !stdinLooksInteractive() {
		return errors.New("wallet action requires --yes when stdin is not interactive")
	}
	printWalletAction(view)
	fmt.Print("broadcast transaction? [y/N]: ")
	var response string
	if _, err := fmt.Fscanln(os.Stdin, &response); err != nil {
		return errors.New("transaction cancelled")
	}
	switch strings.ToLower(strings.TrimSpace(response)) {
	case "y", "yes":
		return nil
	default:
		return errors.New("transaction cancelled")
	}
}

func stdinLooksInteractive() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
