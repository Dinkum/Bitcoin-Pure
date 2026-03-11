package wallet

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

const (
	StoreFileName = "wallets.json"
)

var (
	ErrWalletNotFound    = errors.New("wallet not found")
	ErrWalletExists      = errors.New("wallet already exists")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidAddress    = errors.New("invalid wallet address")
)

type Store struct {
	path    string
	wallets []Wallet
}

type Wallet struct {
	Name      string      `json:"name"`
	CreatedAt time.Time   `json:"created_at"`
	Addresses []Address   `json:"addresses"`
	Pending   []PendingTx `json:"pending"`
}

type Address struct {
	Index         int       `json:"index"`
	Change        bool      `json:"change"`
	CreatedAt     time.Time `json:"created_at"`
	Address       string    `json:"address"`
	KeyHashHex    string    `json:"keyhash_hex"`
	PublicKeyHex  string    `json:"public_key_hex"`
	PrivateKeyHex string    `json:"private_key_hex"`
}

type PendingTx struct {
	TxID      string            `json:"txid"`
	CreatedAt time.Time         `json:"created_at"`
	Spent     []PendingOutPoint `json:"spent"`
	Outputs   []PendingOutput   `json:"outputs,omitempty"`
}

type PendingOutput struct {
	Vout       uint32 `json:"vout"`
	Value      uint64 `json:"value"`
	Address    string `json:"address"`
	KeyHashHex string `json:"keyhash_hex"`
	Change     bool   `json:"change"`
}

type PendingOutPoint struct {
	TxID string `json:"txid"`
	Vout uint32 `json:"vout"`
}

type SpendableUTXO struct {
	OutPoint types.OutPoint
	Value    uint64
	KeyHash  [32]byte
}

type SelectedInput struct {
	OutPoint types.OutPoint
	Value    uint64
	Address  Address
}

type SendPlan struct {
	WalletName     string
	ToAddress      string
	Amount         uint64
	Fee            uint64
	FeeRate        uint64
	Change         uint64
	InputTotal     uint64
	EstimatedBytes int
	Inputs         []SelectedInput
	ChangeAddress  *Address
	Transaction    types.Transaction
	TransactionID  [32]byte
	TransactionHex string
}

type CPFPPlan struct {
	WalletName     string
	ParentTxID     [32]byte
	Amount         uint64
	Fee            uint64
	FeeRate        uint64
	InputTotal     uint64
	EstimatedBytes int
	Input          SelectedInput
	SweepAddress   Address
	Transaction    types.Transaction
	TransactionID  [32]byte
	TransactionHex string
}

type BalanceSummary struct {
	Confirmed    uint64
	Reserved     uint64
	Available    uint64
	PendingCount int
	AddressCount int
}

func Open(path string) (*Store, error) {
	store := &Store{path: filepath.Clean(path)}
	buf, err := os.ReadFile(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return store, nil
		}
		return nil, err
	}
	if len(buf) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(buf, &store.wallets); err != nil {
		return nil, err
	}
	// Legacy wallets may still have placeholder `bpu1<hex>` address strings in
	// storage. Recompute the display address from the canonical key hash so the
	// wallet surface hard-cuts to cashaddr without needing a separate migration.
	for wi := range store.wallets {
		for ai := range store.wallets[wi].Addresses {
			keyHash, err := decodeHash(store.wallets[wi].Addresses[ai].KeyHashHex)
			if err != nil {
				continue
			}
			store.wallets[wi].Addresses[ai].Address = EncodeAddress(keyHash)
		}
	}
	return store, nil
}

func StorePath(dir string) string {
	return filepath.Join(filepath.Clean(dir), StoreFileName)
}

func (s *Store) List() []Wallet {
	out := make([]Wallet, len(s.wallets))
	copy(out, s.wallets)
	return out
}

func (s *Store) CreateWallet(name string) (Wallet, Address, error) {
	name = normalizeWalletName(name)
	if name == "" {
		return Wallet{}, Address{}, errors.New("wallet name is required")
	}
	if _, _, ok := s.findWallet(name); ok {
		return Wallet{}, Address{}, ErrWalletExists
	}
	entry := Wallet{
		Name:      name,
		CreatedAt: time.Now().UTC(),
		Pending:   []PendingTx{},
	}
	addr, err := newAddress(0, false)
	if err != nil {
		return Wallet{}, Address{}, err
	}
	entry.Addresses = append(entry.Addresses, addr)
	s.wallets = append(s.wallets, entry)
	if err := s.save(); err != nil {
		return Wallet{}, Address{}, err
	}
	return entry, addr, nil
}

func (s *Store) Wallet(name string) (Wallet, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return Wallet{}, ErrWalletNotFound
	}
	return *wallet, nil
}

func (s *Store) NewReceiveAddress(name string) (Address, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return Address{}, ErrWalletNotFound
	}
	addr, err := newAddress(nextAddressIndex(*wallet), false)
	if err != nil {
		return Address{}, err
	}
	wallet.Addresses = append(wallet.Addresses, addr)
	if err := s.save(); err != nil {
		return Address{}, err
	}
	return addr, nil
}

func (s *Store) ReconcilePending(name string, mempool map[[32]byte]struct{}) (int, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return 0, ErrWalletNotFound
	}
	filtered := wallet.Pending[:0]
	removed := 0
	for _, pending := range wallet.Pending {
		txid, err := decodeHash(pending.TxID)
		if err != nil {
			removed++
			continue
		}
		if _, ok := mempool[txid]; ok {
			filtered = append(filtered, pending)
			continue
		}
		removed++
	}
	if removed == 0 {
		return 0, nil
	}
	wallet.Pending = append([]PendingTx{}, filtered...)
	return removed, s.save()
}

func (s *Store) BuildSend(name string, to string, amount, fee uint64, utxos []SpendableUTXO) (SendPlan, error) {
	if amount == 0 {
		return SendPlan{}, errors.New("amount must be positive")
	}
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return SendPlan{}, ErrWalletNotFound
	}
	destKeyHash, err := ParseAddress(to)
	if err != nil {
		return SendPlan{}, err
	}
	available, err := spendableCoins(*wallet, utxos)
	if err != nil {
		return SendPlan{}, err
	}
	required := amount + fee
	selected, total, err := selectCoins(available, required)
	if err != nil {
		return SendPlan{}, err
	}
	change := total - required
	plan, err := s.buildSignedPlan(wallet, selected, EncodeAddress(destKeyHash), destKeyHash, amount, fee, change)
	if err != nil {
		return SendPlan{}, err
	}
	plan.InputTotal = total
	plan.EstimatedBytes = len(plan.Transaction.Encode())
	return plan, nil
}

func (s *Store) BuildSendAuto(name string, to string, amount, feeRate uint64, utxos []SpendableUTXO) (SendPlan, error) {
	if feeRate == 0 {
		return SendPlan{}, errors.New("fee rate must be positive")
	}
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return SendPlan{}, ErrWalletNotFound
	}
	destKeyHash, err := ParseAddress(to)
	if err != nil {
		return SendPlan{}, err
	}
	available, err := spendableCoins(*wallet, utxos)
	if err != nil {
		return SendPlan{}, err
	}
	selected, total, fee, change, estimatedBytes, err := selectCoinsForAutoFee(available, amount, feeRate)
	if err != nil {
		return SendPlan{}, err
	}
	plan, err := s.buildSignedPlan(wallet, selected, EncodeAddress(destKeyHash), destKeyHash, amount, fee, change)
	if err != nil {
		return SendPlan{}, err
	}
	plan.InputTotal = total
	plan.FeeRate = feeRate
	plan.EstimatedBytes = estimatedBytes
	return plan, nil
}

func (s *Store) BuildCPFP(name string, parentTxID [32]byte, feeRate uint64) (CPFPPlan, error) {
	if feeRate == 0 {
		return CPFPPlan{}, errors.New("fee rate must be positive")
	}
	input, wallet, err := s.selectPendingCPFPInput(name, parentTxID)
	if err != nil {
		return CPFPPlan{}, err
	}
	estimatedBytes := estimateSignedTxBytes(1, 1)
	fee := feeRate * uint64(estimatedBytes)
	if fee == 0 {
		return CPFPPlan{}, ErrInsufficientFunds
	}
	return s.buildCPFPPlan(wallet, parentTxID, input, fee, feeRate, estimatedBytes)
}

func (s *Store) BuildCPFPWithExactFee(name string, parentTxID [32]byte, fee uint64) (CPFPPlan, error) {
	if fee == 0 {
		return CPFPPlan{}, errors.New("fee must be positive")
	}
	input, wallet, err := s.selectPendingCPFPInput(name, parentTxID)
	if err != nil {
		return CPFPPlan{}, err
	}
	estimatedBytes := estimateSignedTxBytes(1, 1)
	feeRate := fee / uint64(estimatedBytes)
	if fee%uint64(estimatedBytes) != 0 {
		feeRate++
	}
	return s.buildCPFPPlan(wallet, parentTxID, input, fee, feeRate, estimatedBytes)
}

func (s *Store) selectPendingCPFPInput(name string, parentTxID [32]byte) (SelectedInput, *Wallet, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return SelectedInput{}, nil, ErrWalletNotFound
	}
	pendingUTXOs, err := s.PendingSpendableUTXOs(name, &parentTxID)
	if err != nil {
		return SelectedInput{}, nil, err
	}
	available, err := spendableCoins(*wallet, pendingUTXOs)
	if err != nil {
		return SelectedInput{}, nil, err
	}
	if len(available) == 0 {
		return SelectedInput{}, nil, ErrInsufficientFunds
	}
	return available[0], wallet, nil
}

func (s *Store) buildCPFPPlan(wallet *Wallet, parentTxID [32]byte, input SelectedInput, fee uint64, feeRate uint64, estimatedBytes int) (CPFPPlan, error) {
	if fee == 0 || input.Value <= fee {
		return CPFPPlan{}, ErrInsufficientFunds
	}
	// Sweep the child output back to a fresh internal address so the bump path
	// stays wallet-owned and easy to follow in later balance/history views.
	addr, err := newAddress(nextAddressIndex(*wallet), true)
	if err != nil {
		return CPFPPlan{}, err
	}
	wallet.Addresses = append(wallet.Addresses, addr)
	if err := s.save(); err != nil {
		return CPFPPlan{}, err
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: input.OutPoint}},
			Outputs: []types.TxOutput{{
				ValueAtoms: input.Value - fee,
				KeyHash:    mustParseKeyHash(addr.KeyHashHex),
			}},
		},
		Auth: types.TxAuth{Entries: make([]types.TxAuthEntry, 1)},
	}
	msg, err := consensus.Sighash(&tx, 0, []uint64{input.Value})
	if err != nil {
		return CPFPPlan{}, err
	}
	pubKey, sig, err := signAddress(input.Address, &msg)
	if err != nil {
		return CPFPPlan{}, err
	}
	tx.Auth.Entries[0] = types.TxAuthEntry{PubKey: pubKey, Signature: sig}
	txid := consensus.TxID(&tx)
	return CPFPPlan{
		WalletName:     wallet.Name,
		ParentTxID:     parentTxID,
		Amount:         input.Value - fee,
		Fee:            fee,
		FeeRate:        feeRate,
		InputTotal:     input.Value,
		EstimatedBytes: estimatedBytes,
		Input:          input,
		SweepAddress:   addr,
		Transaction:    tx,
		TransactionID:  txid,
		TransactionHex: hex.EncodeToString(tx.Encode()),
	}, nil
}

func (s *Store) MarkSubmitted(name string, txid [32]byte, tx types.Transaction, inputs []SelectedInput) error {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return ErrWalletNotFound
	}
	entry := PendingTx{
		TxID:      hex.EncodeToString(txid[:]),
		CreatedAt: time.Now().UTC(),
		Spent:     make([]PendingOutPoint, 0, len(inputs)),
		Outputs:   walletOwnedOutputs(*wallet, tx),
	}
	for _, input := range inputs {
		outPoint := input.OutPoint
		entry.Spent = append(entry.Spent, PendingOutPoint{
			TxID: hex.EncodeToString(outPoint.TxID[:]),
			Vout: outPoint.Vout,
		})
	}
	for _, pending := range wallet.Pending {
		if pending.TxID == entry.TxID {
			return nil
		}
	}
	wallet.Pending = append(wallet.Pending, entry)
	return s.save()
}

func (s *Store) PendingSpendableUTXOs(name string, parentTxID *[32]byte) ([]SpendableUTXO, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return nil, ErrWalletNotFound
	}
	out := make([]SpendableUTXO, 0)
	for _, pending := range wallet.Pending {
		txid, err := decodeHash(pending.TxID)
		if err != nil {
			continue
		}
		if parentTxID != nil && txid != *parentTxID {
			continue
		}
		for _, output := range pending.Outputs {
			keyHash, err := decodeHash(output.KeyHashHex)
			if err != nil {
				continue
			}
			out = append(out, SpendableUTXO{
				OutPoint: types.OutPoint{TxID: txid, Vout: output.Vout},
				Value:    output.Value,
				KeyHash:  keyHash,
			})
		}
	}
	return out, nil
}

func (s *Store) SpendableKeyHashes(name string) ([][32]byte, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return nil, ErrWalletNotFound
	}
	out := make([][32]byte, 0, len(wallet.Addresses))
	seen := make(map[[32]byte]struct{}, len(wallet.Addresses))
	for _, addr := range wallet.Addresses {
		keyHash := mustParseKeyHash(addr.KeyHashHex)
		if _, ok := seen[keyHash]; ok {
			continue
		}
		seen[keyHash] = struct{}{}
		out = append(out, keyHash)
	}
	return out, nil
}

func (s *Store) Balance(name string, utxos []SpendableUTXO) (BalanceSummary, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return BalanceSummary{}, ErrWalletNotFound
	}
	confirmed := uint64(0)
	for _, utxo := range utxos {
		confirmed += utxo.Value
	}
	availableCoins, err := spendableCoins(*wallet, utxos)
	if err != nil {
		return BalanceSummary{}, err
	}
	available := uint64(0)
	for _, coin := range availableCoins {
		available += coin.Value
	}
	reserved := uint64(0)
	if confirmed > available {
		reserved = confirmed - available
	}
	return BalanceSummary{
		Confirmed:    confirmed,
		Reserved:     reserved,
		Available:    available,
		PendingCount: len(wallet.Pending),
		AddressCount: len(wallet.Addresses),
	}, nil
}

func (w Wallet) LatestReceiveAddress() *Address {
	for i := len(w.Addresses) - 1; i >= 0; i-- {
		if !w.Addresses[i].Change {
			addr := w.Addresses[i]
			return &addr
		}
	}
	return nil
}

func normalizeWalletName(name string) string {
	return strings.TrimSpace(name)
}

func nextAddressIndex(wallet Wallet) int {
	maxIndex := -1
	for _, addr := range wallet.Addresses {
		if addr.Index > maxIndex {
			maxIndex = addr.Index
		}
	}
	return maxIndex + 1
}

func newAddress(index int, change bool) (Address, error) {
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		return Address{}, err
	}
	pubKey := schnorr.SerializePubKey(privKey.PubKey())
	var xonly [32]byte
	copy(xonly[:], pubKey)
	keyHash := bpcrypto.KeyHash(&xonly)
	var secret [32]byte
	copy(secret[:], privKey.Serialize())
	return Address{
		Index:         index,
		Change:        change,
		CreatedAt:     time.Now().UTC(),
		Address:       EncodeAddress(keyHash),
		KeyHashHex:    hex.EncodeToString(keyHash[:]),
		PublicKeyHex:  hex.EncodeToString(xonly[:]),
		PrivateKeyHex: hex.EncodeToString(secret[:]),
	}, nil
}

func signAddress(addr Address, msg *[32]byte) ([32]byte, [64]byte, error) {
	var pubOut [32]byte
	var sigOut [64]byte
	secret, err := hex.DecodeString(addr.PrivateKeyHex)
	if err != nil || len(secret) != 32 {
		return pubOut, sigOut, errors.New("wallet private key is invalid")
	}
	privKey, _ := btcec.PrivKeyFromBytes(secret)
	pubKey := schnorr.SerializePubKey(privKey.PubKey())
	copy(pubOut[:], pubKey)
	sig, err := schnorr.Sign(privKey, msg[:])
	if err != nil {
		return pubOut, sigOut, err
	}
	copy(sigOut[:], sig.Serialize())
	return pubOut, sigOut, nil
}

func spendableCoins(wallet Wallet, utxos []SpendableUTXO) ([]SelectedInput, error) {
	addressesByKeyHash := make(map[[32]byte]Address, len(wallet.Addresses))
	for _, addr := range wallet.Addresses {
		addressesByKeyHash[mustParseKeyHash(addr.KeyHashHex)] = addr
	}
	pendingSpent := make(map[types.OutPoint]struct{})
	for _, pending := range wallet.Pending {
		for _, spent := range pending.Spent {
			txid, err := decodeHash(spent.TxID)
			if err != nil {
				continue
			}
			pendingSpent[types.OutPoint{TxID: txid, Vout: spent.Vout}] = struct{}{}
		}
	}
	out := make([]SelectedInput, 0, len(utxos))
	for _, utxo := range utxos {
		addr, ok := addressesByKeyHash[utxo.KeyHash]
		if !ok {
			continue
		}
		if _, reserved := pendingSpent[utxo.OutPoint]; reserved {
			continue
		}
		out = append(out, SelectedInput{
			OutPoint: utxo.OutPoint,
			Value:    utxo.Value,
			Address:  addr,
		})
	}
	slices.SortFunc(out, func(a, b SelectedInput) int {
		if a.Value != b.Value {
			if a.Value > b.Value {
				return -1
			}
			return 1
		}
		if cmp := strings.Compare(hex.EncodeToString(a.OutPoint.TxID[:]), hex.EncodeToString(b.OutPoint.TxID[:])); cmp != 0 {
			return cmp
		}
		switch {
		case a.OutPoint.Vout < b.OutPoint.Vout:
			return -1
		case a.OutPoint.Vout > b.OutPoint.Vout:
			return 1
		default:
			return 0
		}
	})
	return out, nil
}

func selectCoins(coins []SelectedInput, required uint64) ([]SelectedInput, uint64, error) {
	selected := make([]SelectedInput, 0, len(coins))
	var total uint64
	for _, coin := range coins {
		selected = append(selected, coin)
		total += coin.Value
		if total >= required {
			return selected, total, nil
		}
	}
	return nil, 0, ErrInsufficientFunds
}

func selectCoinsForAutoFee(coins []SelectedInput, amount uint64, feeRate uint64) ([]SelectedInput, uint64, uint64, uint64, int, error) {
	selected := make([]SelectedInput, 0, len(coins))
	var total uint64
	for _, coin := range coins {
		selected = append(selected, coin)
		total += coin.Value
		feeNoChange := feeRate * uint64(estimateSignedTxBytes(len(selected), 1))
		if total < amount+feeNoChange {
			continue
		}
		feeWithChange := feeRate * uint64(estimateSignedTxBytes(len(selected), 2))
		if total >= amount+feeWithChange {
			return selected, total, feeWithChange, total - amount - feeWithChange, estimateSignedTxBytes(len(selected), 2), nil
		}
		// If we can fund the payment at the requested fee rate but not a second
		// wallet-owned change output, collapse the remainder into fee instead of
		// manufacturing dust-like change that would immediately need another spend.
		return selected, total, total - amount, 0, estimateSignedTxBytes(len(selected), 1), nil
	}
	return nil, 0, 0, 0, 0, ErrInsufficientFunds
}

func EstimateSignedTxBytes(inputCount int, outputCount int) int {
	return estimateSignedTxBytes(inputCount, outputCount)
}

func estimateSignedTxBytes(inputCount int, outputCount int) int {
	return 4 + // version
		varIntLen(inputCount) +
		(inputCount * 36) +
		varIntLen(outputCount) +
		(outputCount * 40) +
		varIntLen(inputCount) +
		(inputCount * 96)
}

func varIntLen(count int) int {
	switch {
	case count < 0xfd:
		return 1
	case count <= 0xffff:
		return 3
	case uint64(count) <= 0xffff_ffff:
		return 5
	default:
		return 9
	}
}

func (s *Store) buildSignedPlan(wallet *Wallet, inputs []SelectedInput, toAddress string, destKeyHash [32]byte, amount uint64, fee uint64, change uint64) (SendPlan, error) {
	outputs := []types.TxOutput{{ValueAtoms: amount, KeyHash: destKeyHash}}
	var changeAddr *Address
	if change > 0 {
		// Persist change addresses before broadcast so a later-confirmed self output is still recoverable
		// even if the operator loses the pending-send context locally.
		addr, err := newAddress(nextAddressIndex(*wallet), true)
		if err != nil {
			return SendPlan{}, err
		}
		wallet.Addresses = append(wallet.Addresses, addr)
		if err := s.save(); err != nil {
			return SendPlan{}, err
		}
		changeAddr = &addr
		outputs = append(outputs, types.TxOutput{ValueAtoms: change, KeyHash: mustParseKeyHash(addr.KeyHashHex)})
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  make([]types.TxInput, 0, len(inputs)),
			Outputs: outputs,
		},
		Auth: types.TxAuth{Entries: make([]types.TxAuthEntry, len(inputs))},
	}
	inputAmounts := make([]uint64, len(inputs))
	inputTotal := uint64(0)
	for i, coin := range inputs {
		tx.Base.Inputs = append(tx.Base.Inputs, types.TxInput{PrevOut: coin.OutPoint})
		inputAmounts[i] = coin.Value
		inputTotal += coin.Value
	}
	for i, coin := range inputs {
		msg, err := consensus.Sighash(&tx, i, inputAmounts)
		if err != nil {
			return SendPlan{}, err
		}
		pubKey, sig, err := signAddress(coin.Address, &msg)
		if err != nil {
			return SendPlan{}, err
		}
		tx.Auth.Entries[i] = types.TxAuthEntry{PubKey: pubKey, Signature: sig}
	}
	txid := consensus.TxID(&tx)
	return SendPlan{
		WalletName:     wallet.Name,
		ToAddress:      toAddress,
		Amount:         amount,
		Fee:            fee,
		Change:         change,
		InputTotal:     inputTotal,
		Inputs:         inputs,
		ChangeAddress:  changeAddr,
		Transaction:    tx,
		TransactionID:  txid,
		TransactionHex: hex.EncodeToString(tx.Encode()),
	}, nil
}

func walletOwnedOutputs(wallet Wallet, tx types.Transaction) []PendingOutput {
	addressByKeyHash := make(map[[32]byte]Address, len(wallet.Addresses))
	for _, addr := range wallet.Addresses {
		addressByKeyHash[mustParseKeyHash(addr.KeyHashHex)] = addr
	}
	out := make([]PendingOutput, 0)
	for vout, output := range tx.Base.Outputs {
		addr, ok := addressByKeyHash[output.KeyHash]
		if !ok {
			continue
		}
		out = append(out, PendingOutput{
			Vout:       uint32(vout),
			Value:      output.ValueAtoms,
			Address:    addr.Address,
			KeyHashHex: addr.KeyHashHex,
			Change:     addr.Change,
		})
	}
	return out
}

func mustParseKeyHash(raw string) [32]byte {
	keyHash, err := decodeHash(raw)
	if err != nil {
		panic(fmt.Sprintf("invalid stored keyhash %q: %v", raw, err))
	}
	return keyHash
}

func decodeHash(raw string) ([32]byte, error) {
	var out [32]byte
	buf, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil || len(buf) != len(out) {
		return out, ErrInvalidAddress
	}
	copy(out[:], buf)
	return out, nil
}

func (s *Store) findWallet(name string) (*Wallet, int, bool) {
	name = normalizeWalletName(name)
	for i := range s.wallets {
		if strings.EqualFold(s.wallets[i].Name, name) {
			return &s.wallets[i], i, true
		}
	}
	return nil, -1, false
}

func (s *Store) save() error {
	if s.path == "" {
		return errors.New("wallet store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(s.wallets, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')
	tmp := s.path + ".tmp"
	// Replace the store atomically so wallet create/receive/send never leaves a truncated JSON file behind.
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
