package wallet

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"bitcoin-pure/internal/consensus"
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

const (
	StoreFileName = "wallets.json"

	AtomsPerBPU = 1_000_000_000

	AddressFamilyXOnly = "xonly"
	AddressFamilyPQ    = "pq"
)

var (
	ErrWalletNotFound    = errors.New("wallet not found")
	ErrWalletExists      = errors.New("wallet already exists")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidAddress    = errors.New("invalid wallet address")
)

type Store struct {
	profile types.ChainProfile
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
	Index              int       `json:"index"`
	Change             bool      `json:"change"`
	CreatedAt          time.Time `json:"created_at"`
	Address            string    `json:"address"`
	Type               uint64    `json:"type,omitempty"`
	PayloadHex         string    `json:"payload_hex,omitempty"`
	AlgID              uint64    `json:"alg_id,omitempty"`
	PubKeyHex          string    `json:"pubkey_hex"`
	SecretKeyHex       string    `json:"secret_key_hex,omitempty"`
	PrivateKeyHex      string    `json:"private_key_hex"`
	VerificationKeyHex string    `json:"verification_key_hex,omitempty"`
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
	Type       uint64 `json:"type,omitempty"`
	PayloadHex string `json:"payload_hex,omitempty"`
	PubKeyHex  string `json:"pubkey_hex"`
	Change     bool   `json:"change"`
}

type PendingOutPoint struct {
	TxID string `json:"txid"`
	Vout uint32 `json:"vout"`
}

type SpendableUTXO struct {
	OutPoint      types.OutPoint
	Value         uint64
	Type          uint64
	Payload32     [32]byte
	PubKey        [32]byte
	Height        uint64
	Confirmations uint64
	Coinbase      bool
	Mature        bool
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
	Mature       uint64
	Immature     uint64
	Reserved     uint64
	Available    uint64
	PendingCount int
	AddressCount int
}

type ExportFile struct {
	Version    int                `json:"version"`
	Profile    types.ChainProfile `json:"profile"`
	ExportedAt time.Time          `json:"exported_at"`
	Wallet     Wallet             `json:"wallet"`
}

type BackupFile struct {
	Version   int                `json:"version"`
	Profile   types.ChainProfile `json:"profile"`
	CreatedAt time.Time          `json:"created_at"`
	Wallets   []Wallet           `json:"wallets"`
}

func ParseAddressFamily(raw string) (uint64, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", AddressFamilyXOnly, "x-only":
		return types.OutputXOnlyP2PK, nil
	case AddressFamilyPQ, "pq-lock", "pq_lock", "pqlock":
		return types.OutputPQLock32, nil
	default:
		return 0, fmt.Errorf("unknown address family %q", raw)
	}
}

func AddressFamilyLabel(outputType uint64) string {
	switch outputType {
	case types.OutputXOnlyP2PK:
		return AddressFamilyXOnly
	case types.OutputPQLock32:
		return AddressFamilyPQ
	default:
		return fmt.Sprintf("type-%d", outputType)
	}
}

func (a Address) OutputType() uint64 {
	if a.Type == types.OutputPQLock32 {
		return types.OutputPQLock32
	}
	return types.OutputXOnlyP2PK
}

func (a Address) WatchItem() (WatchItem, error) {
	payload, err := a.payload32()
	if err != nil {
		return WatchItem{}, err
	}
	return WatchItem{Type: a.OutputType(), Payload32: payload}, nil
}

func (a Address) payload32() ([32]byte, error) {
	switch a.OutputType() {
	case types.OutputPQLock32:
		return decodeHex32(a.PayloadHex)
	default:
		if strings.TrimSpace(a.PayloadHex) != "" {
			return decodeHex32(a.PayloadHex)
		}
		return decodeHex32(a.PubKeyHex)
	}
}

func (a Address) signingSecretHex() string {
	if strings.TrimSpace(a.SecretKeyHex) != "" {
		return a.SecretKeyHex
	}
	return a.PrivateKeyHex
}

func (u SpendableUTXO) WatchItem() WatchItem {
	if u.Type == types.OutputXOnlyP2PK && u.Payload32 == ([32]byte{}) {
		return WatchItem{Type: types.OutputXOnlyP2PK, Payload32: u.PubKey}
	}
	return WatchItem{Type: u.Type, Payload32: u.Payload32}
}

func (u SpendableUTXO) SpendMature() bool {
	return !u.Coinbase || u.Mature
}

func Open(path string) (*Store, error) {
	return OpenWithProfile(path, types.Mainnet)
}

func OpenWithProfile(path string, profile types.ChainProfile) (*Store, error) {
	store := &Store{path: filepath.Clean(path), profile: profile}
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
	// Recompute the display address from the canonical stored pubkey so the
	// wallet surface stays consistent even if older wallet files only persisted
	// x-only keys and newer ones carry typed watch items.
	for wi := range store.wallets {
		for ai := range store.wallets[wi].Addresses {
			normalizeStoredAddress(&store.wallets[wi].Addresses[ai])
		}
	}
	return store, nil
}

func (s *Store) chainParams() consensus.ChainParams {
	return consensus.ParamsForProfile(s.profile)
}

func StorePath(dir string) string {
	return filepath.Join(filepath.Clean(dir), StoreFileName)
}

func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func FormatAmount(atoms uint64) string {
	whole := atoms / AtomsPerBPU
	frac := atoms % AtomsPerBPU
	if frac == 0 {
		return fmt.Sprintf("%d BPU", whole)
	}
	fracText := fmt.Sprintf("%09d", frac)
	fracText = strings.TrimRight(fracText, "0")
	return fmt.Sprintf("%d.%s BPU", whole, fracText)
}

func ParseAmount(raw string) (uint64, error) {
	text := strings.TrimSpace(strings.ToLower(raw))
	if text == "" {
		return 0, errors.New("amount is required")
	}
	switch {
	case strings.HasSuffix(text, "atoms"):
		return parseAtomsAmount(strings.TrimSpace(strings.TrimSuffix(text, "atoms")))
	case strings.HasSuffix(text, "atom"):
		return parseAtomsAmount(strings.TrimSpace(strings.TrimSuffix(text, "atom")))
	case strings.HasSuffix(text, "sat"):
		return parseAtomsAmount(strings.TrimSpace(strings.TrimSuffix(text, "sat")))
	case strings.HasSuffix(text, "bpu"):
		text = strings.TrimSpace(strings.TrimSuffix(text, "bpu"))
	}
	return parseBPUAmount(text)
}

func parseAtomsAmount(raw string) (uint64, error) {
	atoms, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid atom amount %q", raw)
	}
	if atoms == 0 {
		return 0, errors.New("amount must be positive")
	}
	return atoms, nil
}

func parseBPUAmount(raw string) (uint64, error) {
	if strings.HasPrefix(raw, "+") || strings.HasPrefix(raw, "-") {
		return 0, fmt.Errorf("invalid BPU amount %q", raw)
	}
	parts := strings.Split(raw, ".")
	if len(parts) > 2 || parts[0] == "" {
		return 0, fmt.Errorf("invalid BPU amount %q", raw)
	}
	whole, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid BPU amount %q", raw)
	}
	if whole > ^uint64(0)/AtomsPerBPU {
		return 0, fmt.Errorf("BPU amount overflows atoms: %q", raw)
	}
	atoms := whole * AtomsPerBPU
	if len(parts) == 2 {
		fracText := parts[1]
		if fracText == "" || len(fracText) > 9 {
			return 0, fmt.Errorf("BPU amount supports at most 9 decimal places: %q", raw)
		}
		for _, ch := range fracText {
			if ch < '0' || ch > '9' {
				return 0, fmt.Errorf("invalid BPU amount %q", raw)
			}
		}
		fracText += strings.Repeat("0", 9-len(fracText))
		frac, err := strconv.ParseUint(fracText, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid BPU amount %q", raw)
		}
		atoms, err = checkedAdd(atoms, frac)
		if err != nil {
			return 0, fmt.Errorf("BPU amount overflows atoms: %q", raw)
		}
	}
	if atoms == 0 {
		return 0, errors.New("amount must be positive")
	}
	return atoms, nil
}

func (s *Store) List() []Wallet {
	out := make([]Wallet, len(s.wallets))
	for i := range s.wallets {
		out[i] = cloneWallet(s.wallets[i])
	}
	return out
}

func (s *Store) Backup(path string) error {
	return s.BackupWithOptions(path, false)
}

func (s *Store) BackupWithOptions(path string, overwrite bool) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("backup path is required")
	}
	if sameCleanPath(path, s.path) {
		return errors.New("backup path cannot be the live wallet store")
	}
	backup := BackupFile{
		Version:   1,
		Profile:   s.profile,
		CreatedAt: time.Now().UTC(),
		Wallets:   cloneWallets(s.wallets),
	}
	buf, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Clean(path), append(buf, '\n'), 0o600, overwrite)
}

func (s *Store) RestoreBackup(path string) error {
	return s.RestoreBackupWithOptions(path, false)
}

func (s *Store) RestoreBackupWithOptions(path string, allowProfileMismatch bool) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("backup path is required")
	}
	backup, err := LoadBackupFile(path)
	if err != nil {
		return err
	}
	if backup.Profile != "" && backup.Profile != s.profile && !allowProfileMismatch {
		return fmt.Errorf("wallet backup profile %q does not match current profile %q", backup.Profile, s.profile)
	}
	wallets := backup.Wallets
	if len(wallets) == 0 {
		return errors.New("wallet backup contains no wallets")
	}
	for wi := range wallets {
		wallets[wi].Name = normalizeWalletName(wallets[wi].Name)
		if wallets[wi].Name == "" {
			return errors.New("backup contains a wallet with an empty name")
		}
		for ai := range wallets[wi].Addresses {
			normalizeStoredAddress(&wallets[wi].Addresses[ai])
			if err := validateWalletAddress(wallets[wi].Addresses[ai]); err != nil {
				return fmt.Errorf("backup wallet %q address %d: %w", wallets[wi].Name, ai, err)
			}
		}
	}
	seen := make(map[string]struct{}, len(wallets))
	for _, entry := range wallets {
		key := strings.ToLower(entry.Name)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("backup contains duplicate wallet %q", entry.Name)
		}
		seen[key] = struct{}{}
	}
	next := cloneWallets(wallets)
	if err := s.saveWallets(next); err != nil {
		return err
	}
	s.wallets = next
	return nil
}

func LoadBackupFile(path string) (BackupFile, error) {
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return BackupFile{}, err
	}
	var envelope struct {
		Version   int                `json:"version"`
		Profile   types.ChainProfile `json:"profile"`
		CreatedAt time.Time          `json:"created_at"`
		Wallets   *json.RawMessage   `json:"wallets"`
	}
	if err := json.Unmarshal(buf, &envelope); err == nil && (envelope.Version != 0 || envelope.Wallets != nil) {
		if envelope.Version != 1 {
			return BackupFile{}, fmt.Errorf("unsupported wallet backup version %d", envelope.Version)
		}
		if envelope.Wallets == nil {
			return BackupFile{}, errors.New("wallet backup is missing wallets")
		}
		if string(bytes.TrimSpace(*envelope.Wallets)) == "null" {
			return BackupFile{}, errors.New("wallet backup wallets must be an array")
		}
		var wallets []Wallet
		if err := json.Unmarshal(*envelope.Wallets, &wallets); err != nil {
			return BackupFile{}, err
		}
		return BackupFile{Version: envelope.Version, Profile: envelope.Profile, CreatedAt: envelope.CreatedAt, Wallets: wallets}, nil
	}
	var legacy []Wallet
	if err := json.Unmarshal(buf, &legacy); err != nil {
		return BackupFile{}, err
	}
	if legacy == nil && string(bytes.TrimSpace(buf)) == "null" {
		return BackupFile{}, errors.New("legacy wallet backup must be an array")
	}
	return BackupFile{Version: 0, Wallets: legacy}, nil
}

func (s *Store) ExportWallet(name string) (ExportFile, error) {
	entry, err := s.Wallet(name)
	if err != nil {
		return ExportFile{}, err
	}
	return ExportFile{
		Version:    1,
		Profile:    s.profile,
		ExportedAt: time.Now().UTC(),
		Wallet:     entry,
	}, nil
}

func (s *Store) ImportWallet(export ExportFile, nameOverride string) (Wallet, error) {
	return s.ImportWalletWithOptions(export, nameOverride, false)
}

func (s *Store) ImportWalletWithOptions(export ExportFile, nameOverride string, allowProfileMismatch bool) (Wallet, error) {
	if export.Profile != "" && export.Profile != s.profile && !allowProfileMismatch {
		return Wallet{}, fmt.Errorf("wallet export profile %q does not match current profile %q", export.Profile, s.profile)
	}
	entry := cloneWallet(export.Wallet)
	if strings.TrimSpace(nameOverride) != "" {
		entry.Name = strings.TrimSpace(nameOverride)
	}
	entry.Name = normalizeWalletName(entry.Name)
	if entry.Name == "" {
		return Wallet{}, errors.New("wallet name is required")
	}
	if _, _, ok := s.findWallet(entry.Name); ok {
		return Wallet{}, ErrWalletExists
	}
	for i := range entry.Addresses {
		normalizeStoredAddress(&entry.Addresses[i])
		if err := validateWalletAddress(entry.Addresses[i]); err != nil {
			return Wallet{}, fmt.Errorf("wallet %q address %d: %w", entry.Name, i, err)
		}
	}
	next := append(cloneWallets(s.wallets), cloneWallet(entry))
	if err := s.saveWallets(next); err != nil {
		return Wallet{}, err
	}
	s.wallets = next
	return cloneWallet(entry), nil
}

func validateWalletAddress(addr Address) error {
	payload, err := addr.payload32()
	if err != nil {
		return err
	}
	if payload == ([32]byte{}) {
		return errors.New("address payload is empty")
	}
	secretHex := strings.TrimSpace(addr.signingSecretHex())
	if secretHex == "" {
		return errors.New("signing secret is missing")
	}
	secret, err := hex.DecodeString(secretHex)
	if err != nil || len(secret) == 0 {
		return errors.New("signing secret is invalid")
	}
	if addr.OutputType() == types.OutputPQLock32 {
		verificationKey, err := hex.DecodeString(strings.TrimSpace(addr.VerificationKeyHex))
		if err != nil || len(verificationKey) != bpcrypto.MLDSA65VerificationKeySize {
			return errors.New("PQ verification key is invalid")
		}
		if len(secret) != bpcrypto.MLDSA65PrivateKeySize {
			return errors.New("PQ signing secret is invalid")
		}
		wantLock := consensus.PQLock(verificationKey)
		if wantLock != payload {
			return errors.New("PQ verification key does not match receive address")
		}
		msg := []byte("BPU wallet backup validation")
		sig, err := bpcrypto.SignMLDSA65(secret, msg)
		if err != nil || !bpcrypto.VerifyMLDSA65(verificationKey, sig, msg) {
			return errors.New("PQ signing secret does not match receive address")
		}
		return nil
	}
	if len(secret) != 32 {
		return errors.New("xonly signing secret must be 32 bytes")
	}
	priv, _ := btcec.PrivKeyFromBytes(secret)
	var derived [32]byte
	copy(derived[:], schnorr.SerializePubKey(priv.PubKey()))
	if derived != payload {
		return errors.New("private key does not match receive address")
	}
	if addr.Address != "" && addr.Address != EncodeAddress(payload) {
		return errors.New("receive address does not match private key")
	}
	return nil
}

func LoadExportFile(path string) (ExportFile, error) {
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return ExportFile{}, err
	}
	var export ExportFile
	if err := json.Unmarshal(buf, &export); err == nil && export.Wallet.Name != "" {
		return export, nil
	}
	var legacy Wallet
	if err := json.Unmarshal(buf, &legacy); err != nil {
		return ExportFile{}, err
	}
	return ExportFile{Version: 1, Wallet: legacy}, nil
}

func SaveExportFile(path string, export ExportFile) error {
	return SaveExportFileWithOptions(path, export, false)
}

func SaveExportFileWithOptions(path string, export ExportFile, overwrite bool) error {
	buf, err := json.MarshalIndent(export, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Clean(path), append(buf, '\n'), 0o600, overwrite)
}

func (s *Store) CreateWallet(name string) (Wallet, Address, error) {
	return s.CreateWalletWithType(name, types.OutputXOnlyP2PK)
}

func (s *Store) CreateWalletWithType(name string, outputType uint64) (Wallet, Address, error) {
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
	addr, err := newAddress(0, false, outputType)
	if err != nil {
		return Wallet{}, Address{}, err
	}
	entry.Addresses = append(entry.Addresses, addr)
	next := append(cloneWallets(s.wallets), cloneWallet(entry))
	if err := s.saveWallets(next); err != nil {
		return Wallet{}, Address{}, err
	}
	s.wallets = next
	return cloneWallet(entry), addr, nil
}

func (s *Store) Wallet(name string) (Wallet, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return Wallet{}, ErrWalletNotFound
	}
	return cloneWallet(*wallet), nil
}

func (s *Store) NewReceiveAddress(name string) (Address, error) {
	return s.NewReceiveAddressWithType(name, types.OutputXOnlyP2PK)
}

func (s *Store) NewReceiveAddressWithType(name string, outputType uint64) (Address, error) {
	wallet, index, ok := s.findWallet(name)
	if !ok {
		return Address{}, ErrWalletNotFound
	}
	addr, err := newAddress(nextAddressIndex(*wallet), false, outputType)
	if err != nil {
		return Address{}, err
	}
	next := cloneWallets(s.wallets)
	next[index].Addresses = append(next[index].Addresses, addr)
	if err := s.saveWallets(next); err != nil {
		return Address{}, err
	}
	s.wallets = next
	return addr, nil
}

func (s *Store) ReconcilePending(name string, mempool map[[32]byte]struct{}) (int, error) {
	wallet, index, ok := s.findWallet(name)
	if !ok {
		return 0, ErrWalletNotFound
	}
	_ = mempool
	filtered := wallet.Pending[:0]
	removed := 0
	for _, pending := range wallet.Pending {
		if _, err := decodeHex32(pending.TxID); err != nil {
			removed++
			continue
		}
		// Mempool absence is not a final wallet state: the tx may be confirmed,
		// evicted, not relayed to this node, or hidden by a restart. Keep unknown
		// spends reserved until a later explicit status signal can clear them.
		filtered = append(filtered, pending)
	}
	if removed == 0 {
		return 0, nil
	}
	next := cloneWallets(s.wallets)
	next[index].Pending = append([]PendingTx{}, filtered...)
	if err := s.saveWallets(next); err != nil {
		return 0, err
	}
	s.wallets = next
	return removed, nil
}

func (s *Store) ReconcilePendingWithStatus(name string, mempool map[[32]byte]struct{}, utxos []SpendableUTXO, confirmed map[[32]byte]struct{}) (int, error) {
	wallet, index, ok := s.findWallet(name)
	if !ok {
		return 0, ErrWalletNotFound
	}
	current := make(map[types.OutPoint]struct{}, len(utxos))
	for _, utxo := range utxos {
		current[utxo.OutPoint] = struct{}{}
	}
	filtered := wallet.Pending[:0]
	removed := 0
	for _, pending := range wallet.Pending {
		txid, err := decodeHex32(pending.TxID)
		if err != nil {
			removed++
			continue
		}
		if _, ok := mempool[txid]; ok {
			filtered = append(filtered, pending)
			continue
		}
		if _, ok := confirmed[txid]; ok {
			removed++
			continue
		}
		if pendingResolvedAgainstUTXOs(txid, pending, current) {
			removed++
			continue
		}
		filtered = append(filtered, pending)
	}
	if removed == 0 {
		return 0, nil
	}
	next := cloneWallets(s.wallets)
	next[index].Pending = append([]PendingTx{}, filtered...)
	if err := s.saveWallets(next); err != nil {
		return 0, err
	}
	s.wallets = next
	return removed, nil
}

func (s *Store) ReconcilePendingWithUTXOs(name string, mempool map[[32]byte]struct{}, utxos []SpendableUTXO) (int, error) {
	return s.ReconcilePendingWithStatus(name, mempool, utxos, nil)
}

func pendingResolvedAgainstUTXOs(txid [32]byte, pending PendingTx, current map[types.OutPoint]struct{}) bool {
	for _, spent := range pending.Spent {
		spentTxID, err := decodeHex32(spent.TxID)
		if err != nil {
			continue
		}
		if _, ok := current[types.OutPoint{TxID: spentTxID, Vout: spent.Vout}]; ok {
			return true
		}
	}
	for _, output := range pending.Outputs {
		if _, ok := current[types.OutPoint{TxID: txid, Vout: output.Vout}]; ok {
			return true
		}
	}
	return false
}

func (s *Store) BuildSend(name string, to string, amount, fee uint64, utxos []SpendableUTXO) (SendPlan, error) {
	return s.BuildSendWithKnownAddresses(name, to, amount, fee, utxos, nil)
}

func (s *Store) BuildSendWithKnownAddresses(name string, to string, amount, fee uint64, utxos []SpendableUTXO, knownAddresses []Address) (SendPlan, error) {
	if amount == 0 {
		return SendPlan{}, errors.New("amount must be positive")
	}
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return SendPlan{}, ErrWalletNotFound
	}
	destItem, err := ParseWatchAddress(to)
	if err != nil {
		return SendPlan{}, err
	}
	planningWallet := walletWithKnownAddresses(*wallet, knownAddresses)
	available, err := spendableCoins(planningWallet, utxos)
	if err != nil {
		return SendPlan{}, err
	}
	required, err := checkedAdd(amount, fee)
	if err != nil {
		return SendPlan{}, ErrInsufficientFunds
	}
	selected, total, err := selectCoins(available, required)
	if err != nil {
		return SendPlan{}, err
	}
	change := total - required
	plan, err := s.buildSignedPlan(&planningWallet, selected, to, destItem, amount, fee, change)
	if err != nil {
		return SendPlan{}, err
	}
	plan.InputTotal = total
	plan.EstimatedBytes = plan.Transaction.EncodedLen()
	return plan, nil
}

func (s *Store) BuildSendAuto(name string, to string, amount, feeRate uint64, utxos []SpendableUTXO) (SendPlan, error) {
	return s.BuildSendAutoWithKnownAddresses(name, to, amount, feeRate, utxos, nil)
}

func (s *Store) BuildSendAutoWithKnownAddresses(name string, to string, amount, feeRate uint64, utxos []SpendableUTXO, knownAddresses []Address) (SendPlan, error) {
	if feeRate == 0 {
		return SendPlan{}, errors.New("fee rate must be positive")
	}
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return SendPlan{}, ErrWalletNotFound
	}
	destItem, err := ParseWatchAddress(to)
	if err != nil {
		return SendPlan{}, err
	}
	planningWallet := walletWithKnownAddresses(*wallet, knownAddresses)
	available, err := spendableCoins(planningWallet, utxos)
	if err != nil {
		return SendPlan{}, err
	}
	if amount == 0 {
		return SendPlan{}, errors.New("amount must be positive")
	}
	selected, total, fee, change, estimatedBytes, err := selectCoinsForAutoFee(available, destItem, amount, feeRate)
	if err != nil {
		return SendPlan{}, err
	}
	plan, err := s.buildSignedPlan(&planningWallet, selected, to, destItem, amount, fee, change)
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
	estimatedBytes := estimateSignedTxBytesForFamilies([]SelectedInput{{Address: input.Address}}, []uint64{input.Address.OutputType()})
	fee, err := checkedMul(feeRate, uint64(estimatedBytes))
	if err != nil {
		return CPFPPlan{}, ErrInsufficientFunds
	}
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
	estimatedBytes := estimateSignedTxBytesForFamilies([]SelectedInput{{Address: input.Address}}, []uint64{input.Address.OutputType()})
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
	addr, err := newAddress(nextAddressIndex(*wallet), true, input.Address.OutputType())
	if err != nil {
		return CPFPPlan{}, err
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: input.OutPoint}},
			Outputs: []types.TxOutput{txOutputForWatchItem(input.Value-fee, mustWatchItem(addr))},
		},
		Auth: types.TxAuth{Entries: make([]types.TxAuthEntry, 1)},
	}
	spentWatchItem, err := input.Address.WatchItem()
	if err != nil {
		return CPFPPlan{}, err
	}
	msg, err := consensus.SighashWithParams(&tx, 0, []consensus.UtxoEntry{{
		Type:       spentWatchItem.Type,
		ValueAtoms: input.Value,
		Payload32:  spentWatchItem.Payload32,
		PubKey:     legacyPubKeyForWatchItem(spentWatchItem),
	}}, s.chainParams())
	if err != nil {
		return CPFPPlan{}, err
	}
	authEntry, err := signAddressAuthEntry(input.Address, &msg)
	if err != nil {
		return CPFPPlan{}, err
	}
	tx.Auth.Entries[0] = authEntry
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

func (s *Store) MarkSubmitted(name string, txid [32]byte, tx types.Transaction, inputs []SelectedInput, changeAddress *Address) error {
	wallet, index, ok := s.findWallet(name)
	if !ok {
		return ErrWalletNotFound
	}
	next := cloneWallets(s.wallets)
	wallet = &next[index]
	if changeAddress != nil && !walletHasAddress(*wallet, changeAddress.Address) {
		wallet.Addresses = append(wallet.Addresses, *changeAddress)
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
	if err := s.saveWallets(next); err != nil {
		return err
	}
	s.wallets = next
	return nil
}

func (s *Store) RememberAddress(name string, addr Address) error {
	_, index, ok := s.findWallet(name)
	if !ok {
		return ErrWalletNotFound
	}
	next := cloneWallets(s.wallets)
	wallet := &next[index]
	if walletHasAddress(*wallet, addr.Address) {
		return nil
	}
	wallet.Addresses = append(wallet.Addresses, addr)
	if err := s.saveWallets(next); err != nil {
		return err
	}
	s.wallets = next
	return nil
}

func (s *Store) ForgetPending(name string, txid [32]byte) error {
	_, index, ok := s.findWallet(name)
	if !ok {
		return ErrWalletNotFound
	}
	txidText := hex.EncodeToString(txid[:])
	next := cloneWallets(s.wallets)
	wallet := &next[index]
	filtered := wallet.Pending[:0]
	removed := false
	for _, pending := range wallet.Pending {
		if pending.TxID == txidText {
			removed = true
			continue
		}
		filtered = append(filtered, pending)
	}
	if !removed {
		return nil
	}
	wallet.Pending = append([]PendingTx{}, filtered...)
	if err := s.saveWallets(next); err != nil {
		return err
	}
	s.wallets = next
	return nil
}

func walletHasAddress(wallet Wallet, address string) bool {
	for _, candidate := range wallet.Addresses {
		if candidate.Address == address {
			return true
		}
	}
	return false
}

func walletWithKnownAddresses(wallet Wallet, knownAddresses []Address) Wallet {
	if len(knownAddresses) == 0 {
		return wallet
	}
	next := cloneWallet(wallet)
	for _, addr := range knownAddresses {
		if walletHasAddress(next, addr.Address) {
			continue
		}
		next.Addresses = append(next.Addresses, addr)
	}
	return next
}

func (s *Store) PendingSpendableUTXOs(name string, parentTxID *[32]byte) ([]SpendableUTXO, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return nil, ErrWalletNotFound
	}
	out := make([]SpendableUTXO, 0)
	for _, pending := range wallet.Pending {
		txid, err := decodeHex32(pending.TxID)
		if err != nil {
			continue
		}
		if parentTxID != nil && txid != *parentTxID {
			continue
		}
		for _, output := range pending.Outputs {
			item, err := pendingOutputWatchItem(output)
			if err != nil {
				continue
			}
			out = append(out, SpendableUTXO{
				OutPoint:  types.OutPoint{TxID: txid, Vout: output.Vout},
				Value:     output.Value,
				Type:      item.Type,
				Payload32: item.Payload32,
				PubKey:    legacyPubKeyForWatchItem(item),
			})
		}
	}
	return out, nil
}

func (s *Store) SpendablePubKeys(name string) ([][32]byte, error) {
	items, err := s.SpendableWatchItems(name)
	if err != nil {
		return nil, err
	}
	out := make([][32]byte, 0, len(items))
	for _, item := range items {
		if item.Type != types.OutputXOnlyP2PK {
			continue
		}
		out = append(out, item.Payload32)
	}
	return out, nil
}

func (s *Store) SpendableWatchItems(name string) ([]WatchItem, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return nil, ErrWalletNotFound
	}
	out := make([]WatchItem, 0, len(wallet.Addresses))
	seen := make(map[WatchItem]struct{}, len(wallet.Addresses))
	for _, addr := range wallet.Addresses {
		item, err := addr.WatchItem()
		if err != nil {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out, nil
}

func (s *Store) Balance(name string, utxos []SpendableUTXO) (BalanceSummary, error) {
	wallet, _, ok := s.findWallet(name)
	if !ok {
		return BalanceSummary{}, ErrWalletNotFound
	}
	confirmed := uint64(0)
	immature := uint64(0)
	for _, utxo := range utxos {
		nextConfirmed, err := checkedAdd(confirmed, utxo.Value)
		if err != nil {
			return BalanceSummary{}, errors.New("wallet balance overflow")
		}
		confirmed = nextConfirmed
		if !utxo.SpendMature() {
			nextImmature, err := checkedAdd(immature, utxo.Value)
			if err != nil {
				return BalanceSummary{}, errors.New("wallet balance overflow")
			}
			immature = nextImmature
		}
	}
	availableCoins, err := spendableCoins(*wallet, utxos)
	if err != nil {
		return BalanceSummary{}, err
	}
	available := uint64(0)
	for _, coin := range availableCoins {
		nextAvailable, err := checkedAdd(available, coin.Value)
		if err != nil {
			return BalanceSummary{}, errors.New("wallet balance overflow")
		}
		available = nextAvailable
	}
	reserved := uint64(0)
	mature := confirmed - immature
	if mature > available {
		reserved = mature - available
	}
	return BalanceSummary{
		Confirmed:    confirmed,
		Mature:       mature,
		Immature:     immature,
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

func cloneWallet(wallet Wallet) Wallet {
	wallet.Addresses = append([]Address(nil), wallet.Addresses...)
	wallet.Pending = clonePendingTxs(wallet.Pending)
	return wallet
}

func cloneWallets(wallets []Wallet) []Wallet {
	out := make([]Wallet, len(wallets))
	for i := range wallets {
		out[i] = cloneWallet(wallets[i])
	}
	return out
}

func clonePendingTxs(pending []PendingTx) []PendingTx {
	out := make([]PendingTx, len(pending))
	for i := range pending {
		out[i] = pending[i]
		out[i].Spent = append([]PendingOutPoint(nil), pending[i].Spent...)
		out[i].Outputs = append([]PendingOutput(nil), pending[i].Outputs...)
	}
	return out
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

func newAddress(index int, change bool, outputType uint64) (Address, error) {
	switch outputType {
	case types.OutputXOnlyP2PK:
		privKey, err := btcec.NewPrivateKey()
		if err != nil {
			return Address{}, err
		}
		pubKey := schnorr.SerializePubKey(privKey.PubKey())
		var xonly [32]byte
		copy(xonly[:], pubKey)
		var secret [32]byte
		copy(secret[:], privKey.Serialize())
		return Address{
			Index:         index,
			Change:        change,
			CreatedAt:     time.Now().UTC(),
			Address:       EncodeAddress(xonly),
			Type:          types.OutputXOnlyP2PK,
			PayloadHex:    hex.EncodeToString(xonly[:]),
			PubKeyHex:     hex.EncodeToString(xonly[:]),
			SecretKeyHex:  hex.EncodeToString(secret[:]),
			PrivateKeyHex: hex.EncodeToString(secret[:]),
		}, nil
	case types.OutputPQLock32:
		verificationKey, secretKey, err := bpcrypto.GenerateMLDSA65Key(rand.Reader)
		if err != nil {
			return Address{}, err
		}
		pqLock := consensus.PQLock(verificationKey)
		return Address{
			Index:              index,
			Change:             change,
			CreatedAt:          time.Now().UTC(),
			Address:            EncodeTypedAddress(WatchItem{Type: types.OutputPQLock32, Payload32: pqLock}),
			Type:               types.OutputPQLock32,
			PayloadHex:         hex.EncodeToString(pqLock[:]),
			AlgID:              types.AlgMLDSA65,
			SecretKeyHex:       hex.EncodeToString(secretKey),
			VerificationKeyHex: hex.EncodeToString(verificationKey),
		}, nil
	default:
		return Address{}, fmt.Errorf("unsupported wallet address type %d", outputType)
	}
}

func signAddressAuthEntry(addr Address, msg *[32]byte) (types.TxAuthEntry, error) {
	switch addr.OutputType() {
	case types.OutputXOnlyP2PK:
		secret, err := hex.DecodeString(addr.signingSecretHex())
		if err != nil || len(secret) != 32 {
			return types.TxAuthEntry{}, errors.New("wallet private key is invalid")
		}
		privKey, _ := btcec.PrivKeyFromBytes(secret)
		sig, err := schnorr.Sign(privKey, msg[:])
		if err != nil {
			return types.TxAuthEntry{}, err
		}
		var sigOut [64]byte
		copy(sigOut[:], sig.Serialize())
		return types.NewXOnlyAuthEntry(sigOut), nil
	case types.OutputPQLock32:
		secret, err := hex.DecodeString(addr.signingSecretHex())
		if err != nil {
			return types.TxAuthEntry{}, errors.New("wallet PQ secret key is invalid")
		}
		verificationKey, err := hex.DecodeString(strings.TrimSpace(addr.VerificationKeyHex))
		if err != nil || len(verificationKey) == 0 {
			return types.TxAuthEntry{}, errors.New("wallet PQ verification key is invalid")
		}
		signature, err := bpcrypto.SignMLDSA65(secret, msg[:])
		if err != nil {
			return types.TxAuthEntry{}, err
		}
		payload := buildPQAuthPayload(verificationKey, signature)
		return types.TxAuthEntry{AuthPayload: payload}, nil
	default:
		return types.TxAuthEntry{}, fmt.Errorf("unsupported wallet address type %d", addr.OutputType())
	}
}

func spendableCoins(wallet Wallet, utxos []SpendableUTXO) ([]SelectedInput, error) {
	addressesByWatchItem := make(map[WatchItem]Address, len(wallet.Addresses))
	for _, addr := range wallet.Addresses {
		item, err := addr.WatchItem()
		if err != nil {
			continue
		}
		addressesByWatchItem[item] = addr
	}
	pendingSpent := make(map[types.OutPoint]struct{})
	for _, pending := range wallet.Pending {
		for _, spent := range pending.Spent {
			txid, err := decodeHex32(spent.TxID)
			if err != nil {
				continue
			}
			pendingSpent[types.OutPoint{TxID: txid, Vout: spent.Vout}] = struct{}{}
		}
	}
	out := make([]SelectedInput, 0, len(utxos))
	for _, utxo := range utxos {
		if !utxo.SpendMature() {
			continue
		}
		addr, ok := addressesByWatchItem[utxo.WatchItem()]
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
		if cmp := bytes.Compare(a.OutPoint.TxID[:], b.OutPoint.TxID[:]); cmp != 0 {
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
		next, err := checkedAdd(total, coin.Value)
		if err != nil {
			return nil, 0, ErrInsufficientFunds
		}
		total = next
		if total >= required {
			return selected, total, nil
		}
	}
	return nil, 0, ErrInsufficientFunds
}

func checkedAdd(left, right uint64) (uint64, error) {
	if right > ^uint64(0)-left {
		return 0, errors.New("uint64 addition overflow")
	}
	return left + right, nil
}

func checkedMul(left, right uint64) (uint64, error) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, errors.New("uint64 multiplication overflow")
	}
	return left * right, nil
}

func selectCoinsForAutoFee(coins []SelectedInput, destItem WatchItem, amount uint64, feeRate uint64) ([]SelectedInput, uint64, uint64, uint64, int, error) {
	selected := make([]SelectedInput, 0, len(coins))
	var total uint64
	inputBytes := 0
	authBytes := 0
	for _, coin := range coins {
		selected = append(selected, coin)
		inputBytes += 36
		authBytes += estimatedAuthEntryBytes(coin.Address)
		nextTotal, err := checkedAdd(total, coin.Value)
		if err != nil {
			return nil, 0, 0, 0, 0, ErrInsufficientFunds
		}
		total = nextTotal
		noChangeBytes := estimateSignedTxBytesFromParts(len(selected), inputBytes, authBytes, []uint64{destItem.Type})
		feeNoChange, err := checkedMul(feeRate, uint64(noChangeBytes))
		if err != nil {
			return nil, 0, 0, 0, 0, ErrInsufficientFunds
		}
		requiredNoChange, err := checkedAdd(amount, feeNoChange)
		if err != nil || total < requiredNoChange {
			continue
		}
		changeBytes := estimateSignedTxBytesFromParts(len(selected), inputBytes, authBytes, []uint64{destItem.Type, selected[0].Address.OutputType()})
		feeWithChange, err := checkedMul(feeRate, uint64(changeBytes))
		if err != nil {
			return nil, 0, 0, 0, 0, ErrInsufficientFunds
		}
		requiredWithChange, err := checkedAdd(amount, feeWithChange)
		if err == nil && total >= requiredWithChange {
			return selected, total, feeWithChange, total - requiredWithChange, changeBytes, nil
		}
		// If we can fund the payment at the requested fee rate but not a second
		// wallet-owned change output, collapse the remainder into fee instead of
		// manufacturing dust-like change that would immediately need another spend.
		return selected, total, total - amount, 0, noChangeBytes, nil
	}
	return nil, 0, 0, 0, 0, ErrInsufficientFunds
}

func EstimateSignedTxBytes(inputCount int, outputCount int) int {
	return estimateSignedTxBytes(inputCount, outputCount)
}

func estimateSignedTxBytes(inputCount int, outputCount int) int {
	selected := make([]SelectedInput, inputCount)
	for i := range selected {
		selected[i] = SelectedInput{Address: Address{Type: types.OutputXOnlyP2PK}}
	}
	outputTypes := make([]uint64, outputCount)
	for i := range outputTypes {
		outputTypes[i] = types.OutputXOnlyP2PK
	}
	return estimateSignedTxBytesForFamilies(selected, outputTypes)
}

func estimateSignedTxBytesForFamilies(inputs []SelectedInput, outputTypes []uint64) int {
	authBytes := 0
	for _, input := range inputs {
		authBytes += estimatedAuthEntryBytes(input.Address)
	}
	return estimateSignedTxBytesFromParts(len(inputs), len(inputs)*36, authBytes, outputTypes)
}

func estimateSignedTxBytesFromParts(inputCount int, inputBytes int, authBytes int, outputTypes []uint64) int {
	size := 4 // version
	size += types.CanonicalVarIntLen(uint64(inputCount))
	size += inputBytes
	size += types.CanonicalVarIntLen(uint64(len(outputTypes)))
	for _, outputType := range outputTypes {
		size += estimatedOutputBytes(outputType)
	}
	size += types.CanonicalVarIntLen(uint64(inputCount))
	size += authBytes
	return size
}

func estimatedOutputBytes(outputType uint64) int {
	return types.CanonicalVarIntLen(outputType) + 8 + 32
}

func estimatedAuthEntryBytes(addr Address) int {
	payloadLen := 64
	if addr.OutputType() == types.OutputPQLock32 {
		vkLen := bpcrypto.MLDSA65VerificationKeySize
		if raw, err := hex.DecodeString(strings.TrimSpace(addr.VerificationKeyHex)); err == nil && len(raw) > 0 {
			vkLen = len(raw)
		}
		sigLen := bpcrypto.MLDSA65SignatureSize
		payloadLen = vkLen + sigLen
	}
	return types.CanonicalVarIntLen(uint64(payloadLen)) + payloadLen
}

func (s *Store) buildSignedPlan(wallet *Wallet, inputs []SelectedInput, toAddress string, destItem WatchItem, amount uint64, fee uint64, change uint64) (SendPlan, error) {
	outputs := []types.TxOutput{txOutputForWatchItem(amount, destItem)}
	var changeAddr *Address
	if change > 0 {
		// Persist change addresses before broadcast so a later-confirmed self output is still recoverable
		// even if the operator loses the pending-send context locally.
		changeType := inputs[0].Address.OutputType()
		addr, err := newAddress(nextAddressIndex(*wallet), true, changeType)
		if err != nil {
			return SendPlan{}, err
		}
		changeAddr = &addr
		outputs = append(outputs, txOutputForWatchItem(change, mustWatchItem(addr)))
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  make([]types.TxInput, 0, len(inputs)),
			Outputs: outputs,
		},
		Auth: types.TxAuth{Entries: make([]types.TxAuthEntry, len(inputs))},
	}
	spentCoins := make([]consensus.UtxoEntry, len(inputs))
	inputTotal := uint64(0)
	for i, coin := range inputs {
		tx.Base.Inputs = append(tx.Base.Inputs, types.TxInput{PrevOut: coin.OutPoint})
		item, err := coin.Address.WatchItem()
		if err != nil {
			return SendPlan{}, err
		}
		spentCoins[i] = consensus.UtxoEntry{
			Type:       item.Type,
			ValueAtoms: coin.Value,
			Payload32:  item.Payload32,
			PubKey:     legacyPubKeyForWatchItem(item),
		}
		inputTotal += coin.Value
	}
	msgs, err := consensus.SighashesWithParams(&tx, spentCoins, s.chainParams())
	if err != nil {
		return SendPlan{}, err
	}
	for i, coin := range inputs {
		msg := msgs[i]
		authEntry, err := signAddressAuthEntry(coin.Address, &msg)
		if err != nil {
			return SendPlan{}, err
		}
		tx.Auth.Entries[i] = authEntry
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
	addressByWatchItem := make(map[WatchItem]Address, len(wallet.Addresses))
	for _, addr := range wallet.Addresses {
		item, err := addr.WatchItem()
		if err != nil {
			continue
		}
		addressByWatchItem[item] = addr
	}
	out := make([]PendingOutput, 0)
	for vout, output := range tx.Base.Outputs {
		item := watchItemForOutput(output)
		addr, ok := addressByWatchItem[item]
		if !ok {
			continue
		}
		out = append(out, PendingOutput{
			Vout:       uint32(vout),
			Value:      output.ValueAtoms,
			Address:    addr.Address,
			Type:       item.Type,
			PayloadHex: hex.EncodeToString(item.Payload32[:]),
			PubKeyHex:  addr.PubKeyHex,
			Change:     addr.Change,
		})
	}
	return out
}

func normalizeStoredAddress(addr *Address) {
	if addr == nil {
		return
	}
	if addr.Type == types.OutputPQLock32 && strings.TrimSpace(addr.PayloadHex) == "" {
		if item, err := ParseWatchAddress(addr.Address); err == nil && item.Type == types.OutputPQLock32 {
			addr.PayloadHex = hex.EncodeToString(item.Payload32[:])
		}
	}
	if addr.Type != types.OutputPQLock32 {
		addr.Type = types.OutputXOnlyP2PK
		if strings.TrimSpace(addr.PayloadHex) == "" {
			addr.PayloadHex = addr.PubKeyHex
		}
		if strings.TrimSpace(addr.SecretKeyHex) == "" {
			addr.SecretKeyHex = addr.PrivateKeyHex
		}
	}
	if addr.Type == types.OutputPQLock32 && addr.AlgID == 0 {
		addr.AlgID = types.AlgMLDSA65
	}
	if item, err := addr.WatchItem(); err == nil {
		addr.Address = EncodeTypedAddress(item)
	}
}

func txOutputForWatchItem(value uint64, item WatchItem) types.TxOutput {
	switch item.Type {
	case types.OutputPQLock32:
		return types.NewPQLockOutput(value, item.Payload32)
	default:
		return types.NewXOnlyOutput(value, item.Payload32)
	}
}

func pendingOutputWatchItem(output PendingOutput) (WatchItem, error) {
	switch output.Type {
	case types.OutputPQLock32:
		payload32, err := decodeHex32(output.PayloadHex)
		if err != nil {
			return WatchItem{}, err
		}
		return WatchItem{Type: types.OutputPQLock32, Payload32: payload32}, nil
	default:
		if strings.TrimSpace(output.PayloadHex) != "" {
			payload32, err := decodeHex32(output.PayloadHex)
			if err != nil {
				return WatchItem{}, err
			}
			return WatchItem{Type: types.OutputXOnlyP2PK, Payload32: payload32}, nil
		}
		payload32, err := decodeHex32(output.PubKeyHex)
		if err != nil {
			return WatchItem{}, err
		}
		return WatchItem{Type: types.OutputXOnlyP2PK, Payload32: payload32}, nil
	}
}

func watchItemForOutput(output types.TxOutput) WatchItem {
	return WatchItem{Type: output.Type, Payload32: output.CanonicalPayload32()}
}

func legacyPubKeyForWatchItem(item WatchItem) [32]byte {
	if item.Type == types.OutputXOnlyP2PK {
		return item.Payload32
	}
	return [32]byte{}
}

func mustWatchItem(addr Address) WatchItem {
	item, err := addr.WatchItem()
	if err != nil {
		panic(fmt.Sprintf("invalid stored wallet address %q: %v", addr.Address, err))
	}
	return item
}

func buildPQAuthPayload(verificationKey []byte, signature []byte) []byte {
	payload := make([]byte, 0, len(verificationKey)+len(signature))
	payload = append(payload, verificationKey...)
	payload = append(payload, signature...)
	return payload
}

func mustParsePubKey(raw string) [32]byte {
	pubKey, err := decodeHex32(raw)
	if err != nil {
		panic(fmt.Sprintf("invalid stored pubkey %q: %v", raw, err))
	}
	return pubKey
}

func decodeHex32(raw string) ([32]byte, error) {
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
	return s.saveWallets(s.wallets)
}

func (s *Store) saveWallets(wallets []Wallet) error {
	if s.path == "" {
		return errors.New("wallet store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(wallets, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.path, append(buf, '\n'), 0o600, true)
}

func writeFileAtomic(path string, buf []byte, mode os.FileMode, overwrite bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	uid, gid, hasOwner := existingFileOwner(path)
	if !hasOwner && overwrite {
		uid, gid, hasOwner = existingFileOwner(filepath.Dir(path))
	}
	tmpFile, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := tmpFile.Name()
	// Replace wallet material atomically so a crash never leaves truncated JSON
	// in the live store, backup, or export path.
	if _, err := tmpFile.Write(buf); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if hasOwner {
		if err := os.Chown(tmp, uid, gid); err != nil && os.Geteuid() == 0 {
			_ = os.Remove(tmp)
			return err
		}
	}
	if !overwrite {
		if err := os.Link(tmp, path); err != nil {
			_ = os.Remove(tmp)
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("%s already exists; pass --overwrite to replace it", path)
			}
			return err
		}
		return os.Remove(tmp)
	}
	return os.Rename(tmp, path)
}

func existingFileOwner(path string) (int, int, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return int(stat.Uid), int(stat.Gid), true
}

func sameCleanPath(left string, right string) bool {
	if strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
		return false
	}
	leftAbs, leftErr := filepath.Abs(filepath.Clean(left))
	rightAbs, rightErr := filepath.Abs(filepath.Clean(right))
	if leftErr == nil && rightErr == nil {
		return leftAbs == rightAbs
	}
	return filepath.Clean(left) == filepath.Clean(right)
}
