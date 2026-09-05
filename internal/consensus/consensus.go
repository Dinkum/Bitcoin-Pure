package consensus

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"runtime"
	"slices"
	"sync"
	"time"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

const (
	MaxFutureBlockTimeSeconds = 7200
	atomsPerBPU               = 1_000_000_000
)

var (
	bigIntTwo            = big.NewInt(2)
	bigIntThree          = big.NewInt(3)
	bigIntOne            = big.NewInt(1)
	bigIntABLADelta      = new(big.Int).SetUint64(ablaDelta)
	bigIntABLAGammaTwice = new(big.Int).SetUint64(2 * ablaGammaDen)
	asertCoeffLinear     = big.NewInt(195766423245049)
	asertCoeffQuadratic  = big.NewInt(971821376)
	asertCoeffCubic      = big.NewInt(5127)
	asertPolyBias        = new(big.Int).Lsh(big.NewInt(1), 47)
	asertScaleFactor     = big.NewInt(1 << 16)
)

type ChainParams struct {
	Profile             types.ChainProfile
	ChainLineage        string
	TargetSpacingSecs   int64
	AsertHalfLifeSecs   int64
	HalvingInterval     uint64
	CoinbaseMaturity    uint64
	InitialSubsidyAtoms uint64
	TailSubsidyAtoms    uint64
	BlockSizeFloor      uint64
	PowLimitBits        uint32
	GenesisTimestamp    uint64
	GenesisBits         uint32
}

func MainnetParams() ChainParams {
	return ChainParams{
		Profile:             types.Mainnet,
		ChainLineage:        "c1",
		TargetSpacingSecs:   600,
		AsertHalfLifeSecs:   86_400,
		HalvingInterval:     525_600,
		CoinbaseMaturity:    100,
		InitialSubsidyAtoms: 5_000 * atomsPerBPU,
		TailSubsidyAtoms:    atomsPerBPU,
		BlockSizeFloor:      32_000_000,
		PowLimitBits:        0x1d0f930c,
		GenesisTimestamp:    1_700_000_000,
		// Retuned from the measured DEV1-S hash rate so genesis lands around a
		// practical ~10 minute solve on the test VPSes. The max target moves with
		// it so genesis remains valid under the chain's compact target bounds.
		GenesisBits: 0x1d0f930c,
	}
}

// SighashTag returns the consensus-critical tagged-hash domain used for input
// authorization on this chain profile and lineage.
func (p ChainParams) SighashTag() string {
	return fmt.Sprintf("BPU/%s/%s/SigHashV1", p.Profile, p.ChainLineage)
}

func RegtestParams() ChainParams {
	params := MainnetParams()
	params.Profile = types.Regtest
	params.PowLimitBits = 0x207fffff
	params.GenesisTimestamp = 1_700_000_600
	params.GenesisBits = 0x207fffff
	return params
}

func RegtestMediumParams() ChainParams {
	params := RegtestParams()
	params.Profile = types.RegtestMedium
	// Retuned upward after live two-miner medium-profile testing showed blocks
	// landing in roughly 1-2 seconds on the DEV1-S pair. This aims for a truer
	// medium fork-debug cadence closer to ~15 seconds before ASERT retargeting
	// takes over.
	params.PowLimitBits = 0x1d4ddf20
	params.GenesisBits = 0x1d4ddf20
	return params
}

func RegtestHardParams() ChainParams {
	params := RegtestParams()
	params.Profile = types.RegtestHard
	// Calibrated from the same measured DEV1-S hash rate so the starting cadence
	// lands near ~2 minutes for the slower TPS-focused live runs.
	params.PowLimitBits = 0x1d4ddf3d
	params.GenesisBits = 0x1d4ddf3d
	return params
}

var (
	benchNetParamsMu sync.RWMutex
	benchNetParams   = defaultBenchNetParams()
)

func defaultBenchNetParams() ChainParams {
	params := RegtestParams()
	params.Profile = types.BenchNet
	return params
}

// SetBenchNetParams installs the benchmark-local chain params used by the
// benchnet profile. Benchmarks start from genesis on every run, so they can
// safely retune the initial difficulty without dragging operator-facing
// regtest profiles into the benchmark UX.
func SetBenchNetParams(params ChainParams) {
	params.Profile = types.BenchNet
	benchNetParamsMu.Lock()
	benchNetParams = params
	benchNetParamsMu.Unlock()
}

func BenchNetParams() ChainParams {
	benchNetParamsMu.RLock()
	params := benchNetParams
	benchNetParamsMu.RUnlock()
	return params
}

func ParamsForProfile(profile types.ChainProfile) ChainParams {
	switch profile {
	case types.Mainnet:
		return MainnetParams()
	case types.Regtest:
		return RegtestParams()
	case types.RegtestMedium:
		return RegtestMediumParams()
	case types.RegtestHard:
		return RegtestHardParams()
	case types.BenchNet:
		return BenchNetParams()
	default:
		panic("unknown chain profile")
	}
}

type ConsensusRules struct {
	EnforceUTXORoot bool
	// SkipPow is benchmark/testing-only. It must stay off for real consensus
	// validation so header acceptance continues to depend on actual work.
	SkipPow bool
	// ValidatedAuthCache is an implementation cache hook, not a consensus rule.
	// It may return true only for exact txid/authid pairs whose authorizations
	// were already fully verified under the same chain parameters.
	ValidatedAuthCache func(txid, authid [32]byte, params ChainParams) bool
	// ValidatedTxCache is a stronger implementation cache hook for exact
	// txid/authid pairs that already passed full transaction validation under
	// the same chain parameters. Block validation still checks contextual state:
	// UTXO availability, duplicate spends, fees, ordering, and roots.
	ValidatedTxCache func(txid, authid [32]byte, params ChainParams) bool
}

func DefaultConsensusRules() ConsensusRules {
	return ConsensusRules{
		EnforceUTXORoot: true,
		SkipPow:         false,
	}
}

type UtxoEntry struct {
	Type          uint64
	ValueAtoms    uint64
	Payload32     [32]byte
	PubKey        [32]byte
	CreatedHeight uint64
	Coinbase      bool
}

// SpentUTXO is one coin consumed from the pre-block UTXO view. Outputs created
// and consumed inside the same atomic block patch are deliberately excluded.
type SpentUTXO struct {
	OutPoint types.OutPoint
	Entry    UtxoEntry
}

// ValidatedBlockDelta is the authoritative state transition produced by full
// consensus validation. Persistence and rollback code must use SpentPreBlock
// instead of attempting to rediscover undo entries in serialization order.
type ValidatedBlockDelta struct {
	Summary         BlockValidationSummary
	Overlay         *UtxoOverlay
	NextAccumulator *utreexo.Accumulator
	SpentPreBlock   []SpentUTXO
}

type UtxoSet map[types.OutPoint]UtxoEntry

const PQLockTag = "BPU/PQLOCK/MLDSA65/v1"

// PQLock derives the 32-byte consensus lock committed by PQ outputs.
func PQLock(verificationKey []byte) [32]byte {
	return crypto.TaggedHash(PQLockTag, verificationKey)
}

// UtxoEntryFromOutput materializes a transient, non-coinbase output. Committed
// chainstate creation must use UtxoEntryFromOutputAtHeight so origin metadata
// cannot be silently omitted.
func UtxoEntryFromOutput(output types.TxOutput) UtxoEntry {
	payload32 := output.CanonicalPayload32()
	if output.Type == types.OutputXOnlyP2PK {
		return UtxoEntry{
			Type:       output.Type,
			ValueAtoms: output.ValueAtoms,
			Payload32:  payload32,
			PubKey:     payload32,
		}
	}
	return UtxoEntry{
		Type:       output.Type,
		ValueAtoms: output.ValueAtoms,
		Payload32:  payload32,
		PubKey:     output.PubKey,
	}
}

// UtxoEntryFromOutputAtHeight materializes a committed output with the origin
// metadata required by contextual consensus rules. Origin is local validating
// state and is deliberately excluded from UTXO leaf and sighash commitments.
func UtxoEntryFromOutputAtHeight(output types.TxOutput, createdHeight uint64, coinbase bool) UtxoEntry {
	entry := UtxoEntryFromOutput(output)
	entry.CreatedHeight = createdHeight
	entry.Coinbase = coinbase
	return entry
}

// UtxoEntryFromLeaf converts accumulator leaves back to validation coins
// without losing the x-only compatibility alias.
func UtxoEntryFromLeaf(leaf utreexo.UtxoLeaf) UtxoEntry {
	return NormalizeUtxoEntry(UtxoEntry{
		Type:       leaf.Type,
		ValueAtoms: leaf.ValueAtoms,
		Payload32:  leaf.Payload32,
		PubKey:     leaf.PubKey,
	})
}

// UtxoLeafFromOutput materializes a newly-created output for accumulator
// updates using the same typed payload rules as transaction validation.
func UtxoLeafFromOutput(outPoint types.OutPoint, output types.TxOutput) utreexo.UtxoLeaf {
	return UtxoLeafFromEntry(outPoint, UtxoEntryFromOutput(output))
}

// UtxoLeafFromEntry prepares an existing validation coin for accumulator
// hashing after normalizing legacy x-only aliases.
func UtxoLeafFromEntry(outPoint types.OutPoint, entry UtxoEntry) utreexo.UtxoLeaf {
	entry = NormalizeUtxoEntry(entry)
	return utreexo.UtxoLeaf{
		OutPoint:   outPoint,
		Type:       entry.Type,
		ValueAtoms: entry.ValueAtoms,
		Payload32:  entry.Payload32,
		PubKey:     entry.PubKey,
	}
}

// NormalizeUtxoEntry repairs the x-only PubKey/Payload32 alias boundary while
// leaving typed PQ payloads untouched.
func NormalizeUtxoEntry(entry UtxoEntry) UtxoEntry {
	if entry.Type == types.OutputXOnlyP2PK {
		switch {
		case entry.Payload32 == ([32]byte{}) && entry.PubKey != ([32]byte{}):
			entry.Payload32 = entry.PubKey
		case entry.PubKey == ([32]byte{}) && entry.Payload32 != ([32]byte{}):
			entry.PubKey = entry.Payload32
		}
	}
	return entry
}

func UtxoLeaves(utxos UtxoSet) []utreexo.UtxoLeaf {
	leaves := make([]utreexo.UtxoLeaf, 0, len(utxos))
	for outPoint, coin := range utxos {
		leaves = append(leaves, UtxoLeafFromEntry(outPoint, coin))
	}
	return leaves
}

func utxoLeavesFromOverlay(overlay *UtxoOverlay) []utreexo.UtxoLeaf {
	if overlay == nil {
		return nil
	}
	leaves := make([]utreexo.UtxoLeaf, 0, len(overlay.base)+len(overlay.created))
	for outPoint, coin := range overlay.base {
		if _, deleted := overlay.deleted[outPoint]; deleted {
			continue
		}
		if _, replaced := overlay.created[outPoint]; replaced {
			continue
		}
		leaves = append(leaves, UtxoLeafFromEntry(outPoint, coin))
	}
	for outPoint, coin := range overlay.created {
		leaves = append(leaves, UtxoLeafFromEntry(outPoint, coin))
	}
	return leaves
}

func UtxoAccumulator(utxos UtxoSet) (*utreexo.Accumulator, error) {
	return utreexo.NewAccumulatorFromLeaves(UtxoLeaves(utxos))
}

type BlockSizeState struct {
	BlockSize uint64
	Epsilon   uint64
	Beta      uint64
}

func NewBlockSizeState(params ChainParams) BlockSizeState {
	return BlockSizeState{
		Epsilon: params.BlockSizeFloor / 2,
		Beta:    params.BlockSizeFloor / 2,
	}
}

func (s BlockSizeState) Limit() uint64 {
	return s.Epsilon + s.Beta
}

type PrevBlockContext struct {
	Height         uint64
	Header         types.BlockHeader
	MedianTimePast uint64
	CurrentTime    uint64
}

type AsertAnchor struct {
	Height     uint64
	ParentTime int64
	Bits       uint32
}

type TxValidationSummary struct {
	InputSum  uint64
	OutputSum uint64
	Fee       uint64
}

// TxValidationContext carries the chain position needed by contextual input
// rules. Requiring it prevents callers from accidentally omitting coinbase
// maturity when validating a transaction.
type TxValidationContext struct {
	Params      ChainParams
	SpendHeight uint64
}

type PQVerifyCheck struct {
	VerificationKey []byte
	Signature       []byte
	Msg             [32]byte
}

type PreparedTxValidation struct {
	Summary         TxValidationSummary
	SignatureChecks []crypto.SchnorrBatchItem
	PQChecks        []PQVerifyCheck
}

type UtxoLookup func(types.OutPoint) (UtxoEntry, bool)

// UtxoLookupWithErr preserves backend lookup failures for consensus-critical
// paths that must not silently treat disk I/O faults as "coin not found".
type UtxoLookupWithErr func(types.OutPoint) (UtxoEntry, bool, error)

type blockCreatedOutput struct {
	TxIndex int
	Entry   UtxoEntry
}

// LookupFromSet adapts a concrete UTXO map to the generic lookup surface used
// by validation and overlay-backed tentative state transitions.
func LookupFromSet(utxos UtxoSet) UtxoLookup {
	return func(out types.OutPoint) (UtxoEntry, bool) {
		utxo, ok := utxos[out]
		if !ok {
			return UtxoEntry{}, false
		}
		return NormalizeUtxoEntry(utxo), true
	}
}

// LookupWithErrFromSet adapts an in-memory UTXO map to the error-aware lookup
// interface used by disk-backed migration paths and tests.
func LookupWithErrFromSet(utxos UtxoSet) UtxoLookupWithErr {
	return func(out types.OutPoint) (UtxoEntry, bool, error) {
		utxo, ok := utxos[out]
		if !ok {
			return UtxoEntry{}, false, nil
		}
		return NormalizeUtxoEntry(utxo), true, nil
	}
}

// LookupWithErrFromLookup upgrades an error-free lookup to the error-aware
// interface for callers that only need API compatibility.
func LookupWithErrFromLookup(lookup UtxoLookup) UtxoLookupWithErr {
	return func(out types.OutPoint) (UtxoEntry, bool, error) {
		utxo, ok := lookup(out)
		if !ok {
			return UtxoEntry{}, false, nil
		}
		return NormalizeUtxoEntry(utxo), true, nil
	}
}

// UtxoOverlay records only the spent/created delta on top of an immutable base
// UTXO set. Hot paths can validate and tentatively apply state changes without
// cloning the whole live set up front.
type UtxoOverlay struct {
	base       UtxoSet
	baseLookup UtxoLookupWithErr
	firstErr   error
	created    map[types.OutPoint]UtxoEntry
	deleted    map[types.OutPoint]struct{}
}

func NewUtxoOverlay(base UtxoSet) *UtxoOverlay {
	return &UtxoOverlay{
		base:       base,
		baseLookup: LookupWithErrFromSet(base),
		created:    make(map[types.OutPoint]UtxoEntry),
		deleted:    make(map[types.OutPoint]struct{}),
	}
}

// NewUtxoOverlayWithBaseLookup preserves a materialized base set for callers
// that still need Materialize while sourcing reads from an arbitrary backend.
func NewUtxoOverlayWithBaseLookup(base UtxoSet, lookup UtxoLookupWithErr) *UtxoOverlay {
	return &UtxoOverlay{
		base:       base,
		baseLookup: lookup,
		created:    make(map[types.OutPoint]UtxoEntry),
		deleted:    make(map[types.OutPoint]struct{}),
	}
}

// NewUtxoOverlayWithLookup creates an overlay against an arbitrary lookup
// backend. It is the additive constructor needed for the disk-backed UTXO
// migration, while current callers that still need Materialize can continue to
// use NewUtxoOverlay with a concrete map.
func NewUtxoOverlayWithLookup(lookup UtxoLookupWithErr) *UtxoOverlay {
	return NewUtxoOverlayWithBaseLookup(nil, lookup)
}

func (o *UtxoOverlay) Lookup(out types.OutPoint) (UtxoEntry, bool) {
	if o == nil {
		return UtxoEntry{}, false
	}
	if entry, ok := o.created[out]; ok {
		return NormalizeUtxoEntry(entry), true
	}
	if _, ok := o.deleted[out]; ok {
		return UtxoEntry{}, false
	}
	if o.baseLookup == nil {
		return UtxoEntry{}, false
	}
	entry, ok, err := o.baseLookup(out)
	if err != nil && o.firstErr == nil {
		o.firstErr = err
	}
	if !ok {
		return UtxoEntry{}, false
	}
	return NormalizeUtxoEntry(entry), true
}

func (o *UtxoOverlay) Spend(out types.OutPoint) {
	if o == nil {
		return
	}
	delete(o.created, out)
	o.deleted[out] = struct{}{}
}

func (o *UtxoOverlay) Restore(out types.OutPoint, entry UtxoEntry) {
	if o == nil {
		return
	}
	delete(o.deleted, out)
	o.created[out] = NormalizeUtxoEntry(entry)
}

func (o *UtxoOverlay) Set(out types.OutPoint, entry UtxoEntry) {
	if o == nil {
		return
	}
	delete(o.deleted, out)
	o.created[out] = NormalizeUtxoEntry(entry)
}

func (o *UtxoOverlay) ApplyTx(tx types.Transaction, txid [32]byte) {
	for _, input := range tx.Base.Inputs {
		o.Spend(input.PrevOut)
	}
	for vout, output := range tx.Base.Outputs {
		o.Set(types.OutPoint{TxID: txid, Vout: uint32(vout)}, UtxoEntryFromOutput(output))
	}
}

func (o *UtxoOverlay) Materialize() UtxoSet {
	if o == nil {
		return nil
	}
	out := cloneUtxos(o.base)
	for outPoint := range o.deleted {
		delete(out, outPoint)
	}
	for outPoint, entry := range o.created {
		out[outPoint] = entry
	}
	return out
}

// ApplyToSet mutates an existing materialized UTXO map with the overlay delta
// instead of cloning the whole base set first. Callers must ensure the target
// map is an owned mutable instance, not a shared published view.
func (o *UtxoOverlay) ApplyToSet(dst UtxoSet) {
	if o == nil || dst == nil {
		return
	}
	for outPoint := range o.deleted {
		delete(dst, outPoint)
	}
	for outPoint, entry := range o.created {
		dst[outPoint] = entry
	}
}

// Err reports the first backend lookup failure observed through Lookup.
func (o *UtxoOverlay) Err() error {
	if o == nil {
		return nil
	}
	return o.firstErr
}

func (o *UtxoOverlay) CreatedEntriesClone() map[types.OutPoint]UtxoEntry {
	if o == nil || len(o.created) == 0 {
		return nil
	}
	out := make(map[types.OutPoint]UtxoEntry, len(o.created))
	for outPoint, entry := range o.created {
		out[outPoint] = entry
	}
	return out
}

// CreatedEntries exposes the created side of the overlay delta without copying.
// Callers must treat the returned map as read-only.
func (o *UtxoOverlay) CreatedEntries() map[types.OutPoint]UtxoEntry {
	if o == nil || len(o.created) == 0 {
		return nil
	}
	return o.created
}

// SpentOutPoints returns the deleted side of the overlay delta.
func (o *UtxoOverlay) SpentOutPoints() []types.OutPoint {
	if o == nil || len(o.deleted) == 0 {
		return nil
	}
	spent := make([]types.OutPoint, 0, len(o.deleted))
	for out := range o.deleted {
		spent = append(spent, out)
	}
	return spent
}

type BlockValidationSummary struct {
	Height                 uint64
	TotalFees              uint64
	CoinbaseValue          uint64
	SignatureChecks        int
	SignatureBatchFallback bool
	SignatureVerifyTime    time.Duration
	NextBlockSizeState     BlockSizeState
}

const (
	minParallelMerkleLeaves = 64
	minParallelBlockHashes  = 128
	minParallelSigChecks    = 256
	minSigChecksPerWorker   = 128
)

var (
	ErrEmptyBlock            = errors.New("empty block")
	ErrFirstTxNotCoinbase    = errors.New("first transaction must be coinbase")
	ErrTxOrderInvalid        = errors.New("non-coinbase transactions must be in ascending txid order")
	ErrCoinbaseHasAuth       = errors.New("coinbase must not have auth entries")
	ErrCoinbaseHeightInvalid = errors.New("coinbase height does not match block height")
	ErrCoinbaseExtraNonce    = errors.New("coinbase extra nonce is missing")
	ErrCoinbaseNoOutputs     = errors.New("coinbase has no outputs")
	ErrEmptyInputs           = errors.New("non-coinbase transaction has zero inputs")
	ErrEmptyOutputs          = errors.New("transaction has zero outputs")
	ErrAuthCountMismatch     = errors.New("auth count mismatch with input count")
	ErrDuplicateInput        = errors.New("duplicate input prevout")
	ErrMissingUTXO           = errors.New("missing UTXO")
	ErrImmatureCoinbase      = errors.New("immature coinbase spend")
	ErrZeroOutputValue       = errors.New("output value must be greater than zero")
	ErrInvalidOutputPubKey   = errors.New("invalid output pubkey")
	ErrOutputPayloadMismatch = errors.New("output payload alias mismatch")
	ErrUnsupportedOutputType = errors.New("unsupported output type")
	ErrInvalidAuthPayload    = errors.New("invalid auth payload")
	ErrInvalidPQLock         = errors.New("invalid PQ lock")
	ErrInvalidSignature      = errors.New("invalid signature")
	ErrAmountOverflow        = errors.New("integer overflow")
	ErrInputsLessThanOutputs = errors.New("inputs less than outputs")
	ErrCoinbaseOverpay       = errors.New("coinbase value exceeds subsidy + fees")
	ErrPrevHashMismatch      = errors.New("prev block hash mismatch")
	ErrMerkleTxIDMismatch    = errors.New("merkle txid root mismatch")
	ErrMerkleAuthMismatch    = errors.New("merkle auth root mismatch")
	ErrInvalidNBits          = errors.New("unexpected nbits")
	ErrInvalidCompactTarget  = errors.New("invalid compact target")
	ErrInvalidPow            = errors.New("pow check failed")
	ErrMiningNonceExhausted  = errors.New("mining header nonce space exhausted")
	ErrTimestampTooEarly     = errors.New("block timestamp must be greater than median time past")
	ErrTimestampTooFarFuture = errors.New("block timestamp must not exceed local system time plus 7200 seconds")
	ErrBlockTooLarge         = errors.New("block too large")
	ErrUTXORootMismatch      = errors.New("utxo root mismatch")
)

func TxID(tx *types.Transaction) [32]byte {
	encoded := tx.EncodeBase()
	return crypto.Sha256d(encoded)
}

func AuthID(tx *types.Transaction) [32]byte {
	encoded := tx.EncodeAuth()
	return crypto.Sha256d(encoded)
}

func HeaderHash(header *types.BlockHeader) [32]byte {
	encoded := encodeHeaderFixed(*header)
	return crypto.Sha256d(encoded[:])
}

func encodeHeaderFixed(header types.BlockHeader) [types.BlockHeaderEncodedLen]byte {
	var out [types.BlockHeaderEncodedLen]byte
	binary.LittleEndian.PutUint32(out[0:4], header.Version)
	copy(out[4:36], header.PrevBlockHash[:])
	copy(out[36:68], header.MerkleTxIDRoot[:])
	copy(out[68:100], header.MerkleAuthRoot[:])
	copy(out[100:132], header.UTXORoot[:])
	binary.LittleEndian.PutUint64(out[132:140], header.Timestamp)
	binary.LittleEndian.PutUint32(out[140:144], header.NBits)
	binary.LittleEndian.PutUint64(out[144:152], header.Nonce)
	return out
}

func MerkleRoot(items [][32]byte) [32]byte {
	return merkleRoot(items, false)
}

func MerkleRootParallel(items [][32]byte) [32]byte {
	return merkleRoot(items, true)
}

// Merkle commitments follow the BLOCK.md tagged construction:
// leaves are hashed as Leaf(x), interior pairs as Node(l, r), and odd carries
// as Solo(x). This avoids raw-leaf roots and duplicate-last ambiguity.
func merkleRoot(items [][32]byte, allowParallel bool) [32]byte {
	if len(items) == 0 {
		panic("merkle root requires a non-empty leaf set")
	}
	layer := make([][32]byte, len(items))
	if allowParallel && shouldParallelMerkleLayer(len(items)) {
		hashMerkleLeavesParallel(layer, items)
	} else {
		hashMerkleLeaves(layer, items)
	}
	for len(layer) > 1 {
		next := make([][32]byte, (len(layer)+1)/2)
		if allowParallel && shouldParallelMerkleLayer(len(layer)) {
			hashMerkleLayerParallel(next, layer)
		} else {
			hashMerkleLayer(next, layer)
		}
		layer = next
	}
	return layer[0]
}

func hashMerkleLeaves(next, items [][32]byte) {
	for i := range items {
		next[i] = hashMerkleLeaf(items[i])
	}
}

func hashMerkleLeavesParallel(next, items [][32]byte) {
	workers := parallelWorkers(len(next))
	if workers <= 1 {
		hashMerkleLeaves(next, items)
		return
	}
	chunk := (len(next) + workers - 1) / workers
	var wg sync.WaitGroup
	for start := 0; start < len(next); start += chunk {
		end := start + chunk
		if end > len(next) {
			end = len(next)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				next[i] = hashMerkleLeaf(items[i])
			}
		}(start, end)
	}
	wg.Wait()
}

func hashMerkleLayer(next, layer [][32]byte) {
	for i := range next {
		next[i] = hashMerklePair(layer, i*2)
	}
}

func hashMerkleLayerParallel(next, layer [][32]byte) {
	workers := parallelWorkers(len(next))
	if workers <= 1 {
		hashMerkleLayer(next, layer)
		return
	}
	chunk := (len(next) + workers - 1) / workers
	var wg sync.WaitGroup
	for start := 0; start < len(next); start += chunk {
		end := start + chunk
		if end > len(next) {
			end = len(next)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				next[i] = hashMerklePair(layer, i*2)
			}
		}(start, end)
	}
	wg.Wait()
}

func hashMerklePair(layer [][32]byte, leftIndex int) [32]byte {
	left := layer[leftIndex]
	if leftIndex+1 >= len(layer) {
		var buf [33]byte
		buf[0] = 0x02
		copy(buf[1:], left[:])
		return crypto.Sha256d(buf[:])
	}
	right := layer[leftIndex+1]
	var buf [65]byte
	buf[0] = 0x01
	copy(buf[1:33], left[:])
	copy(buf[33:], right[:])
	return crypto.Sha256d(buf[:])
}

func hashMerkleLeaf(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x00
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

func BuildBlockRoots(txs []types.Transaction) ([][32]byte, [][32]byte, [32]byte, [32]byte) {
	txids := make([][32]byte, len(txs))
	authids := make([][32]byte, len(txs))
	if shouldParallelBlockHashes(len(txs)) {
		workers := parallelWorkers(len(txs))
		chunk := (len(txs) + workers - 1) / workers
		var wg sync.WaitGroup
		for start := 0; start < len(txs); start += chunk {
			end := start + chunk
			if end > len(txs) {
				end = len(txs)
			}
			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				for i := start; i < end; i++ {
					txids[i] = TxID(&txs[i])
					authids[i] = AuthID(&txs[i])
				}
			}(start, end)
		}
		wg.Wait()
	} else {
		for i := range txs {
			txids[i] = TxID(&txs[i])
			authids[i] = AuthID(&txs[i])
		}
	}
	txRoot, authRoot := BuildBlockRootsFromIDs(txids, authids)
	return txids, authids, txRoot, authRoot
}

// BuildBlockRootsFromIDs reuses precomputed txid/authid vectors when callers
// already have them, which avoids a full re-hash pass during block template
// assembly and other hot paths that carry immutable tx snapshots around.
func BuildBlockRootsFromIDs(txids, authids [][32]byte) ([32]byte, [32]byte) {
	return MerkleRootParallel(txids), MerkleRootParallel(authids)
}

func shouldParallelMerkleLayer(layerLen int) bool {
	return layerLen >= minParallelMerkleLeaves && runtime.GOMAXPROCS(0) > 1
}

func shouldParallelBlockHashes(txCount int) bool {
	return txCount >= minParallelBlockHashes && runtime.GOMAXPROCS(0) > 1
}

func parallelWorkers(units int) int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}
	if workers > units {
		return units
	}
	return workers
}

func SubsidyAtoms(height uint64, params ChainParams) uint64 {
	halvings := height / params.HalvingInterval
	if halvings >= 64 {
		return params.TailSubsidyAtoms
	}
	subsidy := params.InitialSubsidyAtoms >> halvings
	if subsidy < params.TailSubsidyAtoms {
		return params.TailSubsidyAtoms
	}
	return subsidy
}

type sighashContext struct {
	tag            string
	version        uint32
	prevoutsHash   [32]byte
	outputsHash    [32]byte
	spentCoinsHash [32]byte
}

type pqAuthV1 struct {
	VerificationKey []byte
	Signature       []byte
}

// newSighashContext precomputes the tx-wide hashes shared by every input so
// multi-input validation does not rebuild identical prevout/output/spent-coin
// commitments for each signature check.
func newSighashContextWithParams(tx *types.Transaction, spentCoins []UtxoEntry, params ChainParams) (sighashContext, error) {
	if len(spentCoins) != len(tx.Base.Inputs) {
		return sighashContext{}, fmt.Errorf("invalid sighash: spent coins length mismatch")
	}

	// These serializations are rebuilt for every transaction validation, so
	// reserve the exact payload size up front and avoid repeated growth.
	prevouts := make([]byte, 0, types.CanonicalVarIntLen(uint64(len(tx.Base.Inputs)))+len(tx.Base.Inputs)*36)
	prevouts = types.AppendCanonicalVarInt(prevouts, uint64(len(tx.Base.Inputs)))
	for _, input := range tx.Base.Inputs {
		prevouts = append(prevouts, input.PrevOut.TxID[:]...)
		prevouts = append(prevouts,
			byte(input.PrevOut.Vout),
			byte(input.PrevOut.Vout>>8),
			byte(input.PrevOut.Vout>>16),
			byte(input.PrevOut.Vout>>24),
		)
	}

	outputs := make([]byte, 0, types.CanonicalVarIntLen(uint64(len(tx.Base.Outputs)))+len(tx.Base.Outputs)*49)
	outputs = types.AppendCanonicalVarInt(outputs, uint64(len(tx.Base.Outputs)))
	for _, output := range tx.Base.Outputs {
		outputs = appendTypedCoinEncoding(outputs, output.Type, output.ValueAtoms, canonicalOutputPayload32(output))
	}

	// Sighash commits to the full spent-coin encoding, not just amounts, so the
	// authorization domain stays aligned with the canonical UTXO object layout.
	spentCoinPayload := make([]byte, 0, types.CanonicalVarIntLen(uint64(len(spentCoins)))+len(spentCoins)*49)
	spentCoinPayload = types.AppendCanonicalVarInt(spentCoinPayload, uint64(len(spentCoins)))
	for _, coin := range spentCoins {
		coin = NormalizeUtxoEntry(coin)
		spentCoinPayload = appendTypedCoinEncoding(spentCoinPayload, coin.Type, coin.ValueAtoms, canonicalUtxoPayload32(coin))
	}

	return sighashContext{
		tag:            params.SighashTag(),
		version:        tx.Base.Version,
		prevoutsHash:   crypto.Sha256d(prevouts),
		outputsHash:    crypto.Sha256d(outputs),
		spentCoinsHash: crypto.Sha256d(spentCoinPayload),
	}, nil
}

func (ctx sighashContext) hash(inputIndex int, inputCount int) ([32]byte, error) {
	if inputIndex < 0 || inputIndex >= inputCount {
		return [32]byte{}, fmt.Errorf("invalid sighash: input index out of range")
	}
	preimage := make([]byte, 0, 108)
	preimage = append(preimage,
		byte(ctx.version),
		byte(ctx.version>>8),
		byte(ctx.version>>16),
		byte(ctx.version>>24),
	)
	index := uint64(inputIndex)
	preimage = append(preimage,
		byte(index),
		byte(index>>8),
		byte(index>>16),
		byte(index>>24),
		byte(index>>32),
		byte(index>>40),
		byte(index>>48),
		byte(index>>56),
	)
	preimage = append(preimage, ctx.prevoutsHash[:]...)
	preimage = append(preimage, ctx.outputsHash[:]...)
	preimage = append(preimage, ctx.spentCoinsHash[:]...)
	return crypto.TaggedHash(ctx.tag, preimage), nil
}

// SighashWithParams computes the consensus sighash for the supplied chain
// profile and lineage.
func SighashWithParams(tx *types.Transaction, inputIndex int, spentCoins []UtxoEntry, params ChainParams) ([32]byte, error) {
	ctx, err := newSighashContextWithParams(tx, spentCoins, params)
	if err != nil {
		return [32]byte{}, err
	}
	return ctx.hash(inputIndex, len(tx.Base.Inputs))
}

// SighashesWithParams computes every input sighash while sharing the tx-wide
// prevout/output/spent-coin commitments.
func SighashesWithParams(tx *types.Transaction, spentCoins []UtxoEntry, params ChainParams) ([][32]byte, error) {
	ctx, err := newSighashContextWithParams(tx, spentCoins, params)
	if err != nil {
		return nil, err
	}
	hashes := make([][32]byte, len(tx.Base.Inputs))
	for i := range tx.Base.Inputs {
		hash, err := ctx.hash(i, len(tx.Base.Inputs))
		if err != nil {
			return nil, err
		}
		hashes[i] = hash
	}
	return hashes, nil
}

// Sighash preserves the legacy mainnet-default helper for callers that have
// not yet threaded explicit chain params.
func Sighash(tx *types.Transaction, inputIndex int, spentCoins []UtxoEntry) ([32]byte, error) {
	return SighashWithParams(tx, inputIndex, spentCoins, MainnetParams())
}

func appendTypedCoinEncoding(dst []byte, outputType uint64, valueAtoms uint64, payload32 [32]byte) []byte {
	dst = types.AppendCanonicalVarInt(dst, outputType)
	dst = append(dst,
		byte(valueAtoms),
		byte(valueAtoms>>8),
		byte(valueAtoms>>16),
		byte(valueAtoms>>24),
		byte(valueAtoms>>32),
		byte(valueAtoms>>40),
		byte(valueAtoms>>48),
		byte(valueAtoms>>56),
	)
	return append(dst, payload32[:]...)
}

// appendValuePubKeyEncoding is a compatibility shim for x-only test helpers
// while the codebase migrates to typed committed coin payloads.
func appendValuePubKeyEncoding(dst []byte, valueAtoms uint64, pubKey [32]byte) []byte {
	return appendTypedCoinEncoding(dst, types.OutputXOnlyP2PK, valueAtoms, pubKey)
}

func canonicalOutputPayload32(output types.TxOutput) [32]byte {
	return output.CanonicalPayload32()
}

func canonicalUtxoPayload32(entry UtxoEntry) [32]byte {
	entry = NormalizeUtxoEntry(entry)
	return entry.Payload32
}

func validateOutputPayload(output types.TxOutput) error {
	return validateOutputPayloadWithKeyCache(output, nil)
}

func parsePQAuthPayload(payload []byte) (pqAuthV1, error) {
	const payloadLen = crypto.MLDSA65VerificationKeySize + crypto.MLDSA65SignatureSize
	if len(payload) != payloadLen {
		return pqAuthV1{}, ErrInvalidAuthPayload
	}
	verificationKey := append([]byte(nil), payload[:crypto.MLDSA65VerificationKeySize]...)
	signature := append([]byte(nil), payload[crypto.MLDSA65VerificationKeySize:]...)
	return pqAuthV1{
		VerificationKey: verificationKey,
		Signature:       signature,
	}, nil
}

func ValidateTx(tx *types.Transaction, utxos UtxoSet, context TxValidationContext, rules ConsensusRules) (TxValidationSummary, error) {
	return ValidateTxWithLookup(tx, func(out types.OutPoint) (UtxoEntry, bool) {
		utxo, ok := utxos[out]
		return utxo, ok
	}, context, rules)
}

// ValidateTxWithParams is a fail-closed convenience for detached non-coinbase
// coin views. Chain-aware callers must provide an explicit spending height.
func ValidateTxWithParams(tx *types.Transaction, utxos UtxoSet, params ChainParams, rules ConsensusRules) (TxValidationSummary, error) {
	return ValidateTx(tx, utxos, TxValidationContext{Params: params}, rules)
}

// PrepareTxValidationWithLookup resolves the tx against a lookup view and
// computes all signature statements without actually verifying them. Consensus
// callers should still verify each statement exactly before acceptance, while
// non-consensus callers may batch the prepared checks as an optimization.
func PrepareTxValidationWithLookup(tx *types.Transaction, lookup UtxoLookup, context TxValidationContext, _ ConsensusRules) (PreparedTxValidation, error) {
	summary, signatureChecks, pqChecks, err := prepareTxValidationWithLookupAndParams(tx, lookup, context, nil, nil, false, false)
	if err != nil {
		return PreparedTxValidation{}, err
	}
	return PreparedTxValidation{
		Summary:         summary,
		SignatureChecks: signatureChecks,
		PQChecks:        pqChecks,
	}, nil
}

func prepareTxValidationWithLookupAndParams(tx *types.Transaction, lookup UtxoLookup, context TxValidationContext, signatureChecks []crypto.SchnorrBatchItem, pqChecks []PQVerifyCheck, skipAuthChecks bool, skipOutputPayloadChecks bool) (TxValidationSummary, []crypto.SchnorrBatchItem, []PQVerifyCheck, error) {
	if len(tx.Base.Inputs) == 0 {
		return TxValidationSummary{}, signatureChecks, pqChecks, ErrEmptyInputs
	}
	if len(tx.Base.Outputs) == 0 {
		return TxValidationSummary{}, signatureChecks, pqChecks, ErrEmptyOutputs
	}
	if len(tx.Auth.Entries) != len(tx.Base.Inputs) {
		return TxValidationSummary{}, signatureChecks, pqChecks, ErrAuthCountMismatch
	}

	var seen map[types.OutPoint]struct{}
	if len(tx.Base.Inputs) > 1 {
		seen = make(map[types.OutPoint]struct{}, len(tx.Base.Inputs))
	}
	var singleResolved [1]UtxoEntry
	resolvedInputs := singleResolved[:0]
	if len(tx.Base.Inputs) > 1 {
		resolvedInputs = make([]UtxoEntry, len(tx.Base.Inputs))
	} else {
		resolvedInputs = singleResolved[:1]
	}
	var inputSum uint64
	var outputSum uint64

	for i, input := range tx.Base.Inputs {
		if seen != nil {
			if _, ok := seen[input.PrevOut]; ok {
				return TxValidationSummary{}, signatureChecks, pqChecks, ErrDuplicateInput
			}
			seen[input.PrevOut] = struct{}{}
		}
		utxo, ok := lookup(input.PrevOut)
		if !ok {
			return TxValidationSummary{}, signatureChecks, pqChecks, ErrMissingUTXO
		}
		utxo = NormalizeUtxoEntry(utxo)
		if utxo.Coinbase && (context.SpendHeight < utxo.CreatedHeight || context.SpendHeight-utxo.CreatedHeight < context.Params.CoinbaseMaturity) {
			return TxValidationSummary{}, signatureChecks, pqChecks, fmt.Errorf(
				"%w: outpoint=%v created_height=%d spend_height=%d required=%d",
				ErrImmatureCoinbase,
				input.PrevOut,
				utxo.CreatedHeight,
				context.SpendHeight,
				context.Params.CoinbaseMaturity,
			)
		}
		next := inputSum + utxo.ValueAtoms
		if next < inputSum {
			return TxValidationSummary{}, signatureChecks, pqChecks, ErrAmountOverflow
		}
		inputSum = next
		resolvedInputs[i] = utxo
	}

	for _, output := range tx.Base.Outputs {
		if !skipOutputPayloadChecks {
			if err := validateOutputPayload(output); err != nil {
				return TxValidationSummary{}, signatureChecks, pqChecks, err
			}
		}
		next := outputSum + output.ValueAtoms
		if next < outputSum {
			return TxValidationSummary{}, signatureChecks, pqChecks, ErrAmountOverflow
		}
		outputSum = next
	}
	if inputSum < outputSum {
		return TxValidationSummary{}, signatureChecks, pqChecks, ErrInputsLessThanOutputs
	}
	summary := TxValidationSummary{
		InputSum:  inputSum,
		OutputSum: outputSum,
		Fee:       inputSum - outputSum,
	}
	if skipAuthChecks {
		return summary, signatureChecks, pqChecks, nil
	}

	sighashCtx, err := newSighashContextWithParams(tx, resolvedInputs, context.Params)
	if err != nil {
		return TxValidationSummary{}, signatureChecks, pqChecks, err
	}
	for i := range tx.Base.Inputs {
		utxo := resolvedInputs[i]
		auth := tx.Auth.Entries[i]
		msg, err := sighashCtx.hash(i, len(tx.Base.Inputs))
		if err != nil {
			return TxValidationSummary{}, signatureChecks, pqChecks, err
		}
		switch utxo.Type {
		case types.OutputXOnlyP2PK:
			signature, ok := auth.XOnlySignature()
			if !ok {
				return TxValidationSummary{}, signatureChecks, pqChecks, ErrInvalidAuthPayload
			}
			signatureChecks = append(signatureChecks, crypto.SchnorrBatchItem{
				PubKey:    canonicalUtxoPayload32(utxo),
				Signature: signature,
				Msg:       msg,
			})
		case types.OutputPQLock32:
			parsedAuth, err := parsePQAuthPayload(auth.AuthPayload)
			if err != nil {
				return TxValidationSummary{}, signatureChecks, pqChecks, err
			}
			if len(parsedAuth.VerificationKey) != crypto.MLDSA65VerificationKeySize || len(parsedAuth.Signature) != crypto.MLDSA65SignatureSize {
				return TxValidationSummary{}, signatureChecks, pqChecks, ErrInvalidAuthPayload
			}
			if PQLock(parsedAuth.VerificationKey) != canonicalUtxoPayload32(utxo) {
				return TxValidationSummary{}, signatureChecks, pqChecks, ErrInvalidPQLock
			}
			pqChecks = append(pqChecks, PQVerifyCheck{
				VerificationKey: parsedAuth.VerificationKey,
				Signature:       parsedAuth.Signature,
				Msg:             msg,
			})
		default:
			return TxValidationSummary{}, signatureChecks, pqChecks, ErrUnsupportedOutputType
		}
	}

	return summary, signatureChecks, pqChecks, nil
}

// ValidatePreparedTx reuses a previously prepared validation bundle and verifies
// every signature independently. Consensus paths must not use probabilistic
// batch verification for acceptance decisions.
func ValidatePreparedTx(prepared PreparedTxValidation) (TxValidationSummary, error) {
	return validatePreparedTxWithSchnorrVerifier(prepared, crypto.VerifySchnorrXOnlyItems)
}

// ValidatePreparedTxBatchOptimized preserves the batch accelerator for policy
// paths such as mempool admission. Do not use it for block consensus.
func ValidatePreparedTxBatchOptimized(prepared PreparedTxValidation) (TxValidationSummary, error) {
	return validatePreparedTxWithSchnorrVerifier(prepared, crypto.VerifySchnorrBatchXOnlyWithFallback)
}

func validatePreparedTxWithSchnorrVerifier(prepared PreparedTxValidation, verify func([]crypto.SchnorrBatchItem) bool) (TxValidationSummary, error) {
	if !verify(prepared.SignatureChecks) {
		return TxValidationSummary{}, ErrInvalidSignature
	}
	for _, check := range prepared.PQChecks {
		if !crypto.VerifyMLDSA65(check.VerificationKey, check.Signature, check.Msg[:]) {
			return TxValidationSummary{}, ErrInvalidSignature
		}
	}
	return prepared.Summary, nil
}

func ValidateTxWithLookup(tx *types.Transaction, lookup UtxoLookup, context TxValidationContext, rules ConsensusRules) (TxValidationSummary, error) {
	prepared, err := PrepareTxValidationWithLookup(tx, lookup, context, rules)
	if err != nil {
		return TxValidationSummary{}, err
	}
	return ValidatePreparedTx(prepared)
}

func ComputedUTXORoot(utxos UtxoSet) [32]byte {
	return utreexo.UtxoRoot(UtxoLeaves(utxos))
}

func computedUTXORootFromOverlay(overlay *UtxoOverlay) [32]byte {
	return utreexo.UtxoRoot(utxoLeavesFromOverlay(overlay))
}

const (
	ablaGammaDen   = uint64(37_938)
	ablaThetaDen   = uint64(37_938)
	ablaDelta      = uint64(10)
	ablaEpsilonMax = uint64(2_837_960_626_724_546_304)
	ablaBetaMax    = uint64(9_459_868_755_748_488_064)
)

func NextBlockSizeLimit(prev BlockSizeState, params ChainParams) uint64 {
	return ablaNextStep(prev, params).Limit()
}

func AdvanceBlockSizeState(prev BlockSizeState, blockSize uint64, params ChainParams) BlockSizeState {
	next := ablaNextStep(prev, params)
	next.BlockSize = blockSize
	return next
}

func ablaNextStep(prev BlockSizeState, params ChainParams) BlockSizeState {
	e := max(prev.Epsilon, params.BlockSizeFloor/2)
	b := max(prev.Beta, params.BlockSizeFloor/2)
	y := e + b
	x := min(prev.BlockSize, y)

	nextE := e
	nextB := b
	decay := b / ablaThetaDen

	// Reuse local big.Int scratch values here instead of building a fresh tree of
	// temporaries every block. The math remains identical, but the hot path stops
	// manufacturing a pile of one-shot heap objects.
	var xInt, eInt, bInt big.Int
	var threeX, twoE, diff big.Int
	var dENum, denInner, threeB, dEDen, dEInt big.Int
	var nextEInt, nextBInt, baseBInt, deltaTerm big.Int

	xInt.SetUint64(x)
	eInt.SetUint64(e)
	bInt.SetUint64(b)
	threeX.Mul(&xInt, bigIntThree)
	twoE.Mul(&eInt, bigIntTwo)

	if threeX.Cmp(&twoE) > 0 {
		diff.Sub(&threeX, &twoE)
		dENum.Mul(&eInt, &diff)
		threeB.Mul(&bInt, bigIntThree)
		denInner.Add(&eInt, &threeB)
		dEDen.Mul(&denInner, bigIntABLAGammaTwice)
		dEInt.Div(&dENum, &dEDen)
		nextEInt.Add(&eInt, &dEInt)
		nextE = bigIntToUint64(&nextEInt)
		baseBInt.SetUint64(b - decay)
		deltaTerm.Mul(&dEInt, bigIntABLADelta)
		nextBInt.Add(&baseBInt, &deltaTerm)
		nextB = bigIntToUint64(&nextBInt)
	} else {
		diff.Sub(&twoE, &threeX)
		shrinkNum := bigIntToUint64(&diff)
		shrinkDen := 2 * ablaGammaDen
		dE := ceilDivUint64(shrinkNum, shrinkDen)
		nextE = saturatingSub(e, dE)
		nextB = b - decay
	}

	nextE = clampUint64(nextE, params.BlockSizeFloor/2, ablaEpsilonMax)
	nextB = clampUint64(nextB, params.BlockSizeFloor/2, ablaBetaMax)
	return BlockSizeState{
		Epsilon: nextE,
		Beta:    nextB,
	}
}

func bigIntToUint64(v *big.Int) uint64 {
	if v == nil || v.Sign() <= 0 {
		return 0
	}
	if !v.IsUint64() {
		return ^uint64(0)
	}
	return v.Uint64()
}

func ceilDivUint64(num uint64, den uint64) uint64 {
	if den == 0 {
		return 0
	}
	q := num / den
	if num%den == 0 {
		return q
	}
	return q + 1
}

func clampUint64(v uint64, floor uint64, ceil uint64) uint64 {
	if v < floor {
		return floor
	}
	if v > ceil {
		return ceil
	}
	return v
}

func compactToTarget(compact uint32) (*big.Int, error) {
	size := byte(compact >> 24)
	mantissa := compact & 0x007fffff
	negative := compact&0x00800000 != 0
	if mantissa == 0 || negative {
		return nil, ErrInvalidCompactTarget
	}
	target := new(big.Int).SetUint64(uint64(mantissa))
	if size <= 3 {
		target.Rsh(target, uint(8*(3-int(size))))
	} else {
		target.Lsh(target, uint(8*(int(size)-3)))
	}
	if target.Sign() == 0 {
		return nil, ErrInvalidCompactTarget
	}
	return target, nil
}

func targetToCompact(target *big.Int) (uint32, error) {
	if target.Sign() == 0 {
		return 0, ErrInvalidCompactTarget
	}
	bytes := target.Bytes()
	size := uint32(len(bytes))
	if bytes[0] >= 0x80 {
		bytes = append([]byte{0}, bytes...)
		size++
	}
	var mantissa uint32
	if size <= 3 {
		for _, b := range bytes {
			mantissa = (mantissa << 8) | uint32(b)
		}
		mantissa <<= 8 * (3 - size)
	} else {
		mantissa = uint32(bytes[0])<<16 | uint32(bytes[1])<<8 | uint32(bytes[2])
	}
	return (size << 24) | mantissa, nil
}

func clampTarget(target *big.Int, params ChainParams) (*big.Int, error) {
	powLimit, err := compactToTarget(params.PowLimitBits)
	if err != nil {
		return nil, err
	}
	if target.Sign() <= 0 {
		return big.NewInt(1), nil
	}
	if target.Cmp(powLimit) > 0 {
		return powLimit, nil
	}
	return target, nil
}

func GenesisAsertAnchor(params ChainParams) AsertAnchor {
	return AsertAnchor{
		Height:     0,
		ParentTime: int64(params.GenesisTimestamp) - params.TargetSpacingSecs,
		Bits:       params.GenesisBits,
	}
}

func NextWorkRequiredASERT(anchor AsertAnchor, prev PrevBlockContext, params ChainParams) (uint32, error) {
	if prev.Height < anchor.Height {
		return 0, fmt.Errorf("asert eval height %d before anchor %d", prev.Height, anchor.Height)
	}
	anchorTarget, err := compactToTarget(anchor.Bits)
	if err != nil {
		return 0, err
	}
	powLimit, err := compactToTarget(params.PowLimitBits)
	if err != nil {
		return 0, err
	}

	timeDelta := int64(prev.Header.Timestamp) - anchor.ParentTime
	heightDelta := int64(prev.Height - anchor.Height)
	exponent := ((timeDelta - params.TargetSpacingSecs*(heightDelta+1)) << 16) / params.AsertHalfLifeSecs
	numShifts := exponent >> 16
	frac := exponent - (numShifts << 16)

	fracSquared := frac * frac
	fracCubed := fracSquared * frac

	// ASERT runs for every block/header validation, so reuse shared constants and
	// local scratch big.Int values instead of constructing fresh temporaries on
	// every polynomial term.
	var fracInt, fracSquaredInt, fracCubedInt big.Int
	var poly, term, nextTarget big.Int
	fracInt.SetInt64(frac)
	fracSquaredInt.SetInt64(fracSquared)
	fracCubedInt.SetInt64(fracCubed)

	poly.Mul(asertCoeffLinear, &fracInt)
	term.Mul(asertCoeffQuadratic, &fracSquaredInt)
	poly.Add(&poly, &term)
	term.Mul(asertCoeffCubic, &fracCubedInt)
	poly.Add(&poly, &term)
	poly.Add(&poly, asertPolyBias)
	poly.Rsh(&poly, 48)
	poly.Add(&poly, asertScaleFactor)

	nextTarget.Mul(anchorTarget, &poly)
	if numShifts < 0 {
		nextTarget.Rsh(&nextTarget, uint(-numShifts))
	} else if numShifts > 0 {
		nextTarget.Lsh(&nextTarget, uint(numShifts))
	}
	nextTarget.Rsh(&nextTarget, 16)

	if nextTarget.Sign() <= 0 {
		return targetToCompact(bigIntOne)
	}
	if nextTarget.Cmp(powLimit) > 0 {
		return params.PowLimitBits, nil
	}
	return targetToCompact(&nextTarget)
}

func NextWorkRequiredBitcoinLegacy(firstHeader *types.BlockHeader, prevHeader *types.BlockHeader, params ChainParams) (uint32, error) {
	if firstHeader == nil || prevHeader == nil {
		return 0, fmt.Errorf("bitcoin legacy daa requires first and previous headers")
	}
	prevTarget, err := compactToTarget(prevHeader.NBits)
	if err != nil {
		return 0, err
	}
	targetTimespan := params.TargetSpacingSecs * 2016
	actualTimespan := int64(prevHeader.Timestamp) - int64(firstHeader.Timestamp)
	minTimespan := targetTimespan / 4
	maxTimespan := targetTimespan * 4
	if actualTimespan < minTimespan {
		actualTimespan = minTimespan
	}
	if actualTimespan > maxTimespan {
		actualTimespan = maxTimespan
	}
	nextTarget := new(big.Int).Mul(new(big.Int).Set(prevTarget), big.NewInt(actualTimespan))
	nextTarget.Quo(nextTarget, big.NewInt(targetTimespan))
	clamped, err := clampTarget(nextTarget, params)
	if err != nil {
		return 0, err
	}
	return targetToCompact(clamped)
}

func NextWorkRequired(prev PrevBlockContext, params ChainParams) (uint32, error) {
	return NextWorkRequiredASERT(GenesisAsertAnchor(params), prev, params)
}

func checkPow(header *types.BlockHeader, params ChainParams) error {
	target, err := compactToTarget(header.NBits)
	if err != nil {
		return err
	}
	powLimit, err := compactToTarget(params.PowLimitBits)
	if err != nil {
		return err
	}
	if target.Cmp(powLimit) > 0 {
		return ErrInvalidPow
	}
	targetBytes := targetToHashBytes(target)
	hash := HeaderHash(header)
	if bytes.Compare(hash[:], targetBytes[:]) > 0 {
		return ErrInvalidPow
	}
	return nil
}

func targetToHashBytes(target *big.Int) [32]byte {
	var out [32]byte
	if target == nil || target.Sign() <= 0 {
		return out
	}
	target.FillBytes(out[:])
	return out
}

func BlockWork(nBits uint32) ([32]byte, error) {
	target, err := compactToTarget(nBits)
	if err != nil {
		return [32]byte{}, err
	}
	if target.Sign() <= 0 {
		return [32]byte{}, ErrInvalidCompactTarget
	}
	denom := new(big.Int).Add(target, big.NewInt(1))
	numerator := new(big.Int).Lsh(big.NewInt(1), 256)
	work := new(big.Int).Quo(numerator, denom)
	if work.Sign() <= 0 {
		return [32]byte{}, ErrInvalidCompactTarget
	}
	return BigIntTo32(work), nil
}

func BigIntTo32(value *big.Int) [32]byte {
	var out [32]byte
	if value == nil {
		return out
	}
	buf := value.Bytes()
	if len(buf) > len(out) {
		buf = buf[len(buf)-len(out):]
	}
	copy(out[len(out)-len(buf):], buf)
	return out
}

func BigIntFrom32(buf [32]byte) *big.Int {
	return new(big.Int).SetBytes(buf[:])
}

func AddChainWork(left, right [32]byte) [32]byte {
	sum := new(big.Int).Add(BigIntFrom32(left), BigIntFrom32(right))
	return BigIntTo32(sum)
}

func CompareChainWork(left, right [32]byte) int {
	return BigIntFrom32(left).Cmp(BigIntFrom32(right))
}

func EncodeChainWork(value [32]byte) []byte {
	return append([]byte(nil), value[:]...)
}

func DecodeChainWork(buf []byte) ([32]byte, error) {
	var out [32]byte
	if len(buf) != len(out) {
		return out, fmt.Errorf("invalid chainwork length: %d", len(buf))
	}
	copy(out[:], buf)
	return out, nil
}

func EncodeBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func DecodeBool(buf []byte) (bool, error) {
	if len(buf) != 1 {
		return false, fmt.Errorf("invalid bool length: %d", len(buf))
	}
	return buf[0] != 0, nil
}

func EncodeLenPrefixed(data []byte) []byte {
	out := make([]byte, 4, 4+len(data))
	binary.LittleEndian.PutUint32(out, uint32(len(data)))
	out = append(out, data...)
	return out
}

func DecodeLenPrefixed(buf []byte) ([]byte, []byte, error) {
	if len(buf) < 4 {
		return nil, nil, errors.New("missing length prefix")
	}
	n := int(binary.LittleEndian.Uint32(buf[:4]))
	if len(buf) < 4+n {
		return nil, nil, errors.New("truncated length-prefixed payload")
	}
	return buf[4 : 4+n], buf[4+n:], nil
}

func MedianTimePast(timestamps []uint64) uint64 {
	if len(timestamps) == 0 {
		return 0
	}
	if len(timestamps) <= 11 {
		// MTP uses the last 11 header timestamps, so keep the hot path entirely on
		// the stack and use insertion sort for the tiny fixed window.
		var sorted [11]uint64
		n := copy(sorted[:], timestamps)
		for i := 1; i < n; i++ {
			value := sorted[i]
			j := i - 1
			for ; j >= 0 && sorted[j] > value; j-- {
				sorted[j+1] = sorted[j]
			}
			sorted[j+1] = value
		}
		return sorted[n/2]
	}

	sorted := append([]uint64(nil), timestamps...)
	slices.Sort(sorted)
	return sorted[len(sorted)/2]
}

func validateHeaderWithRules(header *types.BlockHeader, prev PrevBlockContext, params ChainParams, rules ConsensusRules) error {
	if header.PrevBlockHash != HeaderHash(&prev.Header) {
		return ErrPrevHashMismatch
	}
	medianTimePast := prev.MedianTimePast
	if medianTimePast == 0 {
		medianTimePast = prev.Header.Timestamp
	}
	if header.Timestamp <= medianTimePast {
		return ErrTimestampTooEarly
	}
	if prev.CurrentTime != 0 && header.Timestamp > prev.CurrentTime+MaxFutureBlockTimeSeconds {
		return ErrTimestampTooFarFuture
	}
	expectedNBits, err := NextWorkRequired(prev, params)
	if err != nil {
		return err
	}
	if header.NBits != expectedNBits {
		return ErrInvalidNBits
	}
	if !rules.SkipPow {
		if err := checkPow(header, params); err != nil {
			return err
		}
	}
	return nil
}

func ValidateHeaderWithRules(header *types.BlockHeader, prev PrevBlockContext, params ChainParams, rules ConsensusRules) error {
	if err := validateHeaderWithRules(header, prev, params, rules); err != nil {
		return err
	}
	return nil
}

func ValidateHeader(header *types.BlockHeader, prev PrevBlockContext, params ChainParams) error {
	return ValidateHeaderWithRules(header, prev, params, DefaultConsensusRules())
}

func MineHeader(header types.BlockHeader, params ChainParams) (types.BlockHeader, error) {
	mined, ok, err := MineHeaderInterruptible(header, params, func(uint64) bool { return true })
	if err != nil {
		return types.BlockHeader{}, err
	}
	if !ok {
		return types.BlockHeader{}, ErrInvalidPow
	}
	return mined, nil
}

func MineHeaderInterruptible(header types.BlockHeader, params ChainParams, shouldContinue func(uint64) bool) (types.BlockHeader, bool, error) {
	target, err := compactToTarget(header.NBits)
	if err != nil {
		return types.BlockHeader{}, false, err
	}
	powLimit, err := compactToTarget(params.PowLimitBits)
	if err != nil {
		return types.BlockHeader{}, false, err
	}
	if target.Cmp(powLimit) > 0 {
		return types.BlockHeader{}, false, ErrInvalidPow
	}
	targetBytes := targetToHashBytes(target)
	encoded := encodeHeaderFixed(header)
	for nonce := header.Nonce; ; nonce++ {
		if shouldContinue != nil && nonce&0x0fff == 0 && !shouldContinue(nonce) {
			return types.BlockHeader{}, false, nil
		}
		binary.LittleEndian.PutUint64(encoded[144:152], nonce)
		hash := crypto.Sha256d(encoded[:])
		if bytes.Compare(hash[:], targetBytes[:]) <= 0 {
			header.Nonce = nonce
			return header, true, nil
		}
		if nonce == math.MaxUint64 {
			break
		}
	}
	return types.BlockHeader{}, false, ErrMiningNonceExhausted
}

func sumCoinbaseOutputs(tx *types.Transaction) (uint64, error) {
	var sum uint64
	for _, output := range tx.Base.Outputs {
		next := sum + output.ValueAtoms
		if next < sum {
			return 0, ErrAmountOverflow
		}
		sum = next
	}
	return sum, nil
}

func cloneUtxos(utxos UtxoSet) UtxoSet {
	out := make(UtxoSet, len(utxos))
	for k, v := range utxos {
		out[k] = v
	}
	return out
}

func blockUtxoDelta(block *types.Block) ([]types.OutPoint, []utreexo.UtxoLeaf) {
	txids := make([][32]byte, len(block.Txs))
	for i := range block.Txs {
		txids[i] = TxID(&block.Txs[i])
	}
	return blockUtxoDeltaFromIDs(block, txids)
}

func blockUtxoDeltaFromIDs(block *types.Block, txids [][32]byte) ([]types.OutPoint, []utreexo.UtxoLeaf) {
	if block == nil || len(block.Txs) == 0 {
		return nil, nil
	}
	spentCap := max(0, len(block.Txs)-1)
	outputCap := len(block.Txs)
	for _, tx := range block.Txs {
		if len(tx.Base.Inputs) > 1 {
			spentCap += len(tx.Base.Inputs) - 1
		}
		if len(tx.Base.Outputs) > 1 {
			outputCap += len(tx.Base.Outputs) - 1
		}
	}
	spent := make([]types.OutPoint, 0, spentCap)
	createdByOutPoint, createdOrder := blockCreatedOutputsFromIDs(block, txids, outputCap)
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		for _, input := range tx.Base.Inputs {
			// Outputs created and spent within the same block never exist in the
			// pre-block accumulator, so they cancel out of the accumulator delta.
			if _, ok := createdByOutPoint[input.PrevOut]; ok {
				delete(createdByOutPoint, input.PrevOut)
				continue
			}
			spent = append(spent, input.PrevOut)
		}
	}
	coinbase := &block.Txs[0]
	coinbaseTxID := txids[0]
	for vout, output := range coinbase.Base.Outputs {
		outPoint := types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}
		createdByOutPoint[outPoint] = UtxoLeafFromOutput(outPoint, output)
		createdOrder = append(createdOrder, outPoint)
	}
	created := make([]utreexo.UtxoLeaf, 0, len(createdByOutPoint))
	for _, outPoint := range createdOrder {
		if leaf, ok := createdByOutPoint[outPoint]; ok {
			created = append(created, leaf)
		}
	}
	return spent, created
}

func blockCreatedOutputsFromIDs(block *types.Block, txids [][32]byte, outputCap int) (map[types.OutPoint]utreexo.UtxoLeaf, []types.OutPoint) {
	createdByOutPoint := make(map[types.OutPoint]utreexo.UtxoLeaf, outputCap)
	createdOrder := make([]types.OutPoint, 0, outputCap)
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		txid := txids[i]
		for vout, output := range tx.Base.Outputs {
			outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			createdByOutPoint[outPoint] = UtxoLeafFromOutput(outPoint, output)
			createdOrder = append(createdOrder, outPoint)
		}
	}
	return createdByOutPoint, createdOrder
}

// ValidateAndApplyBlockOverlayWithLookup validates against an explicit lookup
// backend while optionally keeping a materialized base set for callers that
// need to materialize the full post-block view.
func ValidateAndApplyBlockOverlayWithLookup(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, base UtxoSet, lookup UtxoLookupWithErr, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules) (BlockValidationSummary, *UtxoOverlay, *utreexo.Accumulator, error) {
	return validateAndApplyBlockOverlayWithLookup(block, prev, blockSizeState, base, lookup, accumulator, params, rules, nil)
}

// ValidateAndApplyBlockDeltaWithLookup returns the exact pre-block spend set
// observed during validation. It is the only authoritative undo source.
func ValidateAndApplyBlockDeltaWithLookup(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, base UtxoSet, lookup UtxoLookupWithErr, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules) (ValidatedBlockDelta, error) {
	var spentPreBlock []SpentUTXO
	summary, overlay, nextAccumulator, err := validateAndApplyBlockOverlayWithLookup(
		block, prev, blockSizeState, base, lookup, accumulator, params, rules, &spentPreBlock,
	)
	if err != nil {
		return ValidatedBlockDelta{}, err
	}
	return ValidatedBlockDelta{
		Summary:         summary,
		Overlay:         overlay,
		NextAccumulator: nextAccumulator,
		SpentPreBlock:   spentPreBlock,
	}, nil
}

func validateAndApplyBlockOverlayWithLookup(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, base UtxoSet, lookup UtxoLookupWithErr, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules, spentPreBlockOut *[]SpentUTXO) (BlockValidationSummary, *UtxoOverlay, *utreexo.Accumulator, error) {
	if len(block.Txs) == 0 {
		return BlockValidationSummary{}, nil, nil, ErrEmptyBlock
	}
	blockSize := uint64(block.EncodedLen())
	if blockSize > uint64(NextBlockSizeLimit(blockSizeState, params)) {
		return BlockValidationSummary{}, nil, nil, ErrBlockTooLarge
	}

	txids, authids, txRoot, authRoot := BuildBlockRoots(block.Txs)
	if txRoot != block.Header.MerkleTxIDRoot {
		return BlockValidationSummary{}, nil, nil, ErrMerkleTxIDMismatch
	}
	if authRoot != block.Header.MerkleAuthRoot {
		return BlockValidationSummary{}, nil, nil, ErrMerkleAuthMismatch
	}

	if err := ValidateHeaderWithRules(&block.Header, prev, params, rules); err != nil {
		return BlockValidationSummary{}, nil, nil, err
	}

	var outputKeys outputKeyCache
	coinbase := &block.Txs[0]
	if len(coinbase.Base.Inputs) != 0 {
		return BlockValidationSummary{}, nil, nil, ErrFirstTxNotCoinbase
	}
	if coinbase.Base.CoinbaseHeight == nil || *coinbase.Base.CoinbaseHeight != prev.Height+1 {
		return BlockValidationSummary{}, nil, nil, ErrCoinbaseHeightInvalid
	}
	if coinbase.Base.CoinbaseExtraNonce == nil {
		return BlockValidationSummary{}, nil, nil, ErrCoinbaseExtraNonce
	}
	if len(coinbase.Auth.Entries) != 0 {
		return BlockValidationSummary{}, nil, nil, ErrCoinbaseHasAuth
	}
	if len(coinbase.Base.Outputs) == 0 {
		return BlockValidationSummary{}, nil, nil, ErrCoinbaseNoOutputs
	}
	for _, output := range coinbase.Base.Outputs {
		if err := validateOutputPayloadWithKeyCache(output, &outputKeys); err != nil {
			return BlockValidationSummary{}, nil, nil, err
		}
	}

	tempUtxos := NewUtxoOverlayWithBaseLookup(base, lookup)
	claimedInputs := make(map[types.OutPoint]struct{}, max(0, len(block.Txs)-1))
	sameBlockCreated := make(map[types.OutPoint]blockCreatedOutput)
	createdOrder := make([]types.OutPoint, 0)
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		if i > 1 && bytes.Compare(txids[i-1][:], txids[i][:]) >= 0 {
			return BlockValidationSummary{}, nil, nil, ErrTxOrderInvalid
		}
		for _, input := range tx.Base.Inputs {
			if _, ok := claimedInputs[input.PrevOut]; ok {
				return BlockValidationSummary{}, nil, nil, ErrDuplicateInput
			}
			claimedInputs[input.PrevOut] = struct{}{}
		}
		for vout, output := range tx.Base.Outputs {
			if err := validateOutputPayloadWithKeyCache(output, &outputKeys); err != nil {
				return BlockValidationSummary{}, nil, nil, err
			}
			outPoint := types.OutPoint{TxID: txids[i], Vout: uint32(vout)}
			sameBlockCreated[outPoint] = blockCreatedOutput{
				TxIndex: i,
				Entry:   UtxoEntryFromOutputAtHeight(output, prev.Height+1, false),
			}
			createdOrder = append(createdOrder, outPoint)
		}
	}
	signatureChecks := make([]crypto.SchnorrBatchItem, 0, max(0, len(block.Txs)-1))
	pqChecks := make([]PQVerifyCheck, 0)
	sameBlockSpent := make(map[types.OutPoint]struct{})
	spentPreBlock := make(map[types.OutPoint]UtxoEntry, len(claimedInputs))
	var totalFees uint64
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		// LTOR is only the serialization order. Consensus spends are resolved
		// against the atomic block patch, so a transaction may spend an output
		// created by any other non-coinbase transaction in this block.
		blockLookup := func(out types.OutPoint) (UtxoEntry, bool) {
			if entry, ok := tempUtxos.Lookup(out); ok {
				spentPreBlock[out] = entry
				return entry, true
			}
			created, ok := sameBlockCreated[out]
			if !ok || created.TxIndex == i {
				return UtxoEntry{}, false
			}
			sameBlockSpent[out] = struct{}{}
			return created.Entry, true
		}
		validatedTx := rules.ValidatedTxCache != nil && rules.ValidatedTxCache(txids[i], authids[i], params)
		skipAuthChecks := validatedTx || (rules.ValidatedAuthCache != nil && rules.ValidatedAuthCache(txids[i], authids[i], params))
		summary, nextSignatureChecks, nextPQChecks, err := prepareTxValidationWithLookupAndParams(tx, blockLookup, TxValidationContext{
			Params:      params,
			SpendHeight: prev.Height + 1,
		}, signatureChecks, pqChecks, skipAuthChecks, true)
		if err != nil {
			if lookupErr := tempUtxos.Err(); lookupErr != nil {
				return BlockValidationSummary{}, nil, nil, fmt.Errorf("utxo lookup failed during block validation: %w", lookupErr)
			}
			return BlockValidationSummary{}, nil, nil, err
		}
		nextFees := totalFees + summary.Fee
		if nextFees < totalFees {
			return BlockValidationSummary{}, nil, nil, ErrAmountOverflow
		}
		totalFees = nextFees
		signatureChecks = nextSignatureChecks
		pqChecks = nextPQChecks
	}
	verifyStartedAt := time.Now()
	schnorrResult := verifyBlockSignatureChecks(signatureChecks)
	verifyDuration := time.Since(verifyStartedAt)
	if !schnorrResult.Valid {
		return BlockValidationSummary{}, nil, nil, ErrInvalidSignature
	}
	for _, check := range pqChecks {
		if !crypto.VerifyMLDSA65(check.VerificationKey, check.Signature, check.Msg[:]) {
			return BlockValidationSummary{}, nil, nil, ErrInvalidSignature
		}
	}

	coinbaseValue, err := sumCoinbaseOutputs(coinbase)
	if err != nil {
		return BlockValidationSummary{}, nil, nil, err
	}
	subsidy := SubsidyAtoms(prev.Height+1, params)
	if coinbaseValue > subsidy+totalFees {
		return BlockValidationSummary{}, nil, nil, ErrCoinbaseOverpay
	}

	coinbaseTxID := TxID(coinbase)
	for outPoint := range claimedInputs {
		if _, ok := sameBlockSpent[outPoint]; ok {
			delete(sameBlockCreated, outPoint)
			continue
		}
		tempUtxos.Spend(outPoint)
	}
	for _, outPoint := range createdOrder {
		created, ok := sameBlockCreated[outPoint]
		if !ok {
			continue
		}
		tempUtxos.Set(outPoint, created.Entry)
	}
	for vout, output := range coinbase.Base.Outputs {
		tempUtxos.Set(types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}, UtxoEntryFromOutputAtHeight(output, prev.Height+1, true))
	}
	var nextAccumulator *utreexo.Accumulator
	if accumulator != nil {
		spent, created := blockUtxoDeltaFromIDs(block, txids)
		nextAccumulator, err = accumulator.Apply(spent, created)
		if err != nil {
			return BlockValidationSummary{}, nil, nil, err
		}
	}
	if rules.EnforceUTXORoot {
		root := block.Header.UTXORoot
		if nextAccumulator != nil {
			if nextAccumulator.Root() != root {
				return BlockValidationSummary{}, nil, nil, ErrUTXORootMismatch
			}
		} else {
			if computedUTXORootFromOverlay(tempUtxos) != root {
				return BlockValidationSummary{}, nil, nil, ErrUTXORootMismatch
			}
		}
	}
	if err := tempUtxos.Err(); err != nil {
		return BlockValidationSummary{}, nil, nil, fmt.Errorf("utxo lookup failed during block validation: %w", err)
	}
	if spentPreBlockOut != nil {
		spent := make([]SpentUTXO, 0, len(spentPreBlock))
		for i := 1; i < len(block.Txs); i++ {
			for _, input := range block.Txs[i].Base.Inputs {
				if entry, ok := spentPreBlock[input.PrevOut]; ok {
					spent = append(spent, SpentUTXO{OutPoint: input.PrevOut, Entry: entry})
				}
			}
		}
		*spentPreBlockOut = spent
	}

	nextState := AdvanceBlockSizeState(blockSizeState, blockSize, params)
	return BlockValidationSummary{
		Height:                 prev.Height + 1,
		TotalFees:              totalFees,
		CoinbaseValue:          coinbaseValue,
		SignatureChecks:        len(signatureChecks),
		SignatureBatchFallback: schnorrResult.Fallback,
		SignatureVerifyTime:    verifyDuration,
		NextBlockSizeState:     nextState,
	}, tempUtxos, nextAccumulator, nil
}

// ResolveBlockInputEntries validates an undo record against the block's atomic
// LTOR dependency graph and returns the coin consumed by every input. The undo
// record must contain exactly the pre-block spends and no intra-block spends.
func ResolveBlockInputEntries(block *types.Block, spentPreBlock []SpentUTXO) (map[types.OutPoint]UtxoEntry, error) {
	if block == nil {
		return nil, errors.New("block is required")
	}
	undo := make(map[types.OutPoint]UtxoEntry, len(spentPreBlock))
	for _, spent := range spentPreBlock {
		if _, duplicate := undo[spent.OutPoint]; duplicate {
			return nil, fmt.Errorf("duplicate undo outpoint %v", spent.OutPoint)
		}
		undo[spent.OutPoint] = NormalizeUtxoEntry(spent.Entry)
	}

	type createdOutput struct {
		txIndex int
		entry   UtxoEntry
	}
	created := make(map[types.OutPoint]createdOutput)
	for txIndex := 1; txIndex < len(block.Txs); txIndex++ {
		txid := TxID(&block.Txs[txIndex])
		for vout, output := range block.Txs[txIndex].Base.Outputs {
			outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			created[outPoint] = createdOutput{txIndex: txIndex, entry: UtxoEntryFromOutput(output)}
		}
	}

	resolved := make(map[types.OutPoint]UtxoEntry)
	usedUndo := make(map[types.OutPoint]struct{}, len(undo))
	for txIndex := 1; txIndex < len(block.Txs); txIndex++ {
		for _, input := range block.Txs[txIndex].Base.Inputs {
			if _, duplicate := resolved[input.PrevOut]; duplicate {
				return nil, fmt.Errorf("duplicate block input %v", input.PrevOut)
			}
			if entry, ok := undo[input.PrevOut]; ok {
				resolved[input.PrevOut] = entry
				usedUndo[input.PrevOut] = struct{}{}
				continue
			}
			output, ok := created[input.PrevOut]
			if !ok {
				return nil, fmt.Errorf("missing input and undo entry for %v", input.PrevOut)
			}
			if output.txIndex == txIndex {
				return nil, fmt.Errorf("transaction %d spends its own output %v", txIndex, input.PrevOut)
			}
			resolved[input.PrevOut] = output.entry
		}
	}
	if len(usedUndo) != len(undo) {
		return nil, fmt.Errorf("block undo mismatch: %d unused entries", len(undo)-len(usedUndo))
	}
	return resolved, nil
}

func verifyBlockSignatureChecks(items []crypto.SchnorrBatchItem) crypto.SchnorrBatchResult {
	if len(items) < minParallelSigChecks {
		return exactSchnorrResult(items)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		return exactSchnorrResult(items)
	}
	maxWorkers := (len(items) + minSigChecksPerWorker - 1) / minSigChecksPerWorker
	if workers > maxWorkers {
		workers = maxWorkers
	}
	if workers < 2 {
		return exactSchnorrResult(items)
	}

	chunkSize := (len(items) + workers - 1) / workers
	results := make(chan crypto.SchnorrBatchResult, workers)
	for start := 0; start < len(items); start += chunkSize {
		end := min(start+chunkSize, len(items))
		chunk := items[start:end]
		go func() {
			results <- exactSchnorrResult(chunk)
		}()
	}

	out := crypto.SchnorrBatchResult{Valid: true}
	for start := 0; start < len(items); start += chunkSize {
		result := <-results
		if !result.Valid {
			return result
		}
		out.Fallback = out.Fallback || result.Fallback
	}
	return out
}

func exactSchnorrResult(items []crypto.SchnorrBatchItem) crypto.SchnorrBatchResult {
	return crypto.SchnorrBatchResult{Valid: crypto.VerifySchnorrXOnlyItems(items)}
}

func DecodeTxHex(raw string, limits types.CodecLimits) (types.Transaction, error) {
	return types.DecodeTransactionHex(raw, limits)
}

func DecodeBlockHex(raw string, limits types.CodecLimits) (types.Block, error) {
	return types.DecodeBlockHex(raw, limits)
}

func saturatingSub(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}
