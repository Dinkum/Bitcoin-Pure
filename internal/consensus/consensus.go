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

const SighashTag = "BPU/SigHashV1"

type ChainParams struct {
	Profile             types.ChainProfile
	TargetSpacingSecs   int64
	AsertHalfLifeSecs   int64
	HalvingInterval     uint64
	InitialSubsidyAtoms uint64
	BlockSizeFloor      uint64
	PowLimitBits        uint32
	GenesisTimestamp    uint64
	GenesisBits         uint32
}

func MainnetParams() ChainParams {
	return ChainParams{
		Profile:             types.Mainnet,
		TargetSpacingSecs:   600,
		AsertHalfLifeSecs:   86_400,
		HalvingInterval:     2_500_000,
		InitialSubsidyAtoms: 1_000_000_000_000,
		BlockSizeFloor:      32_000_000,
		PowLimitBits:        0x1d0f930c,
		GenesisTimestamp:    1_700_000_000,
		// Retuned from the measured DEV1-S hash rate so genesis lands around a
		// practical ~10 minute solve on the test VPSes. The max target moves with
		// it so genesis remains valid under the chain's compact target bounds.
		GenesisBits: 0x1d0f930c,
	}
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
	default:
		panic("unknown chain profile")
	}
}

type ConsensusRules struct {
	CodecLimits     types.CodecLimits
	EnforceUTXORoot bool
}

func DefaultConsensusRules() ConsensusRules {
	return ConsensusRules{
		CodecLimits:     types.DefaultCodecLimits(),
		EnforceUTXORoot: true,
	}
}

type UtxoEntry struct {
	ValueAtoms uint64
	PubKey     [32]byte
}

type UtxoSet map[types.OutPoint]UtxoEntry

func UtxoLeaves(utxos UtxoSet) []utreexo.UtxoLeaf {
	leaves := make([]utreexo.UtxoLeaf, 0, len(utxos))
	for outPoint, coin := range utxos {
		leaves = append(leaves, utreexo.UtxoLeaf{
			OutPoint:   outPoint,
			ValueAtoms: coin.ValueAtoms,
			PubKey:     coin.PubKey,
		})
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

type PreparedTxValidation struct {
	Summary         TxValidationSummary
	SignatureChecks []crypto.SchnorrBatchItem
}

type UtxoLookup func(types.OutPoint) (UtxoEntry, bool)

// LookupFromSet adapts a concrete UTXO map to the generic lookup surface used
// by validation and overlay-backed tentative state transitions.
func LookupFromSet(utxos UtxoSet) UtxoLookup {
	return func(out types.OutPoint) (UtxoEntry, bool) {
		utxo, ok := utxos[out]
		return utxo, ok
	}
}

// UtxoOverlay records only the spent/created delta on top of an immutable base
// UTXO set. Hot paths can validate and tentatively apply state changes without
// cloning the whole live set up front.
type UtxoOverlay struct {
	base    UtxoSet
	lookup  UtxoLookup
	created map[types.OutPoint]UtxoEntry
	deleted map[types.OutPoint]struct{}
}

func NewUtxoOverlay(base UtxoSet) *UtxoOverlay {
	return &UtxoOverlay{
		base:    base,
		lookup:  LookupFromSet(base),
		created: make(map[types.OutPoint]UtxoEntry),
		deleted: make(map[types.OutPoint]struct{}),
	}
}

func (o *UtxoOverlay) Lookup(out types.OutPoint) (UtxoEntry, bool) {
	if o == nil {
		return UtxoEntry{}, false
	}
	if entry, ok := o.created[out]; ok {
		return entry, true
	}
	if _, ok := o.deleted[out]; ok {
		return UtxoEntry{}, false
	}
	return o.lookup(out)
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
	o.created[out] = entry
}

func (o *UtxoOverlay) Set(out types.OutPoint, entry UtxoEntry) {
	if o == nil {
		return
	}
	delete(o.deleted, out)
	o.created[out] = entry
}

func (o *UtxoOverlay) ApplyTx(tx types.Transaction, txid [32]byte) {
	for _, input := range tx.Base.Inputs {
		o.Spend(input.PrevOut)
	}
	for vout, output := range tx.Base.Outputs {
		o.Set(types.OutPoint{TxID: txid, Vout: uint32(vout)}, UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			PubKey:     output.PubKey,
		})
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
	minParallelMerkleLeaves = 256
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
	ErrInvalidOutputPubKey   = errors.New("invalid output pubkey")
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
	encoded := header.Encode()
	return crypto.Sha256d(encoded)
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
	return txids, authids, MerkleRootParallel(txids), MerkleRootParallel(authids)
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
		return 1
	}
	subsidy := params.InitialSubsidyAtoms >> halvings
	if subsidy == 0 {
		return 1
	}
	return subsidy
}

func writeVarInt(out *[]byte, v uint64) {
	switch {
	case v <= 0xfc:
		*out = append(*out, byte(v))
	case v <= 0xffff:
		*out = append(*out, 0xfd, byte(v), byte(v>>8))
	case v <= 0xffff_ffff:
		*out = append(*out, 0xfe, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	default:
		*out = append(*out, 0xff,
			byte(v),
			byte(v>>8),
			byte(v>>16),
			byte(v>>24),
			byte(v>>32),
			byte(v>>40),
			byte(v>>48),
			byte(v>>56),
		)
	}
}

func Sighash(tx *types.Transaction, inputIndex int, inputAmounts []uint64) ([32]byte, error) {
	if inputIndex < 0 || inputIndex >= len(tx.Base.Inputs) {
		return [32]byte{}, fmt.Errorf("invalid sighash: input index out of range")
	}
	if len(inputAmounts) != len(tx.Base.Inputs) {
		return [32]byte{}, fmt.Errorf("invalid sighash: input amounts length mismatch")
	}

	prevouts := make([]byte, 0)
	writeVarInt(&prevouts, uint64(len(tx.Base.Inputs)))
	for _, input := range tx.Base.Inputs {
		prevouts = append(prevouts, input.PrevOut.TxID[:]...)
		prevouts = append(prevouts,
			byte(input.PrevOut.Vout),
			byte(input.PrevOut.Vout>>8),
			byte(input.PrevOut.Vout>>16),
			byte(input.PrevOut.Vout>>24),
		)
	}
	prevoutsHash := crypto.Sha256d(prevouts)

	outputs := make([]byte, 0)
	writeVarInt(&outputs, uint64(len(tx.Base.Outputs)))
	for _, output := range tx.Base.Outputs {
		outputs = append(outputs,
			byte(output.ValueAtoms),
			byte(output.ValueAtoms>>8),
			byte(output.ValueAtoms>>16),
			byte(output.ValueAtoms>>24),
			byte(output.ValueAtoms>>32),
			byte(output.ValueAtoms>>40),
			byte(output.ValueAtoms>>48),
			byte(output.ValueAtoms>>56),
		)
		outputs = append(outputs, output.PubKey[:]...)
	}
	outputsHash := crypto.Sha256d(outputs)

	amounts := make([]byte, 0)
	writeVarInt(&amounts, uint64(len(inputAmounts)))
	for _, amount := range inputAmounts {
		amounts = append(amounts,
			byte(amount),
			byte(amount>>8),
			byte(amount>>16),
			byte(amount>>24),
			byte(amount>>32),
			byte(amount>>40),
			byte(amount>>48),
			byte(amount>>56),
		)
	}
	amountsHash := crypto.Sha256d(amounts)

	preimage := make([]byte, 0, 108)
	preimage = append(preimage,
		byte(tx.Base.Version),
		byte(tx.Base.Version>>8),
		byte(tx.Base.Version>>16),
		byte(tx.Base.Version>>24),
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
	preimage = append(preimage, prevoutsHash[:]...)
	preimage = append(preimage, outputsHash[:]...)
	preimage = append(preimage, amountsHash[:]...)
	return crypto.TaggedHash(SighashTag, preimage), nil
}

func ValidateTx(tx *types.Transaction, utxos UtxoSet, rules ConsensusRules) (TxValidationSummary, error) {
	return ValidateTxWithLookup(tx, func(out types.OutPoint) (UtxoEntry, bool) {
		utxo, ok := utxos[out]
		return utxo, ok
	}, rules)
}

// PrepareTxValidationWithLookup resolves the tx against a lookup view and
// computes all signature statements without actually verifying them. Consensus
// callers should still verify each statement exactly before acceptance, while
// non-consensus callers may batch the prepared checks as an optimization.
func PrepareTxValidationWithLookup(tx *types.Transaction, lookup UtxoLookup, _ ConsensusRules) (PreparedTxValidation, error) {
	if len(tx.Base.Inputs) == 0 {
		return PreparedTxValidation{}, ErrEmptyInputs
	}
	if len(tx.Base.Outputs) == 0 {
		return PreparedTxValidation{}, ErrEmptyOutputs
	}
	if len(tx.Auth.Entries) != len(tx.Base.Inputs) {
		return PreparedTxValidation{}, ErrAuthCountMismatch
	}

	seen := make(map[types.OutPoint]struct{}, len(tx.Base.Inputs))
	inputAmounts := make([]uint64, 0, len(tx.Base.Inputs))
	resolvedInputs := make([]UtxoEntry, 0, len(tx.Base.Inputs))
	signatureChecks := make([]crypto.SchnorrBatchItem, 0, len(tx.Base.Inputs))
	var inputSum uint64
	var outputSum uint64

	for _, input := range tx.Base.Inputs {
		if _, ok := seen[input.PrevOut]; ok {
			return PreparedTxValidation{}, ErrDuplicateInput
		}
		seen[input.PrevOut] = struct{}{}
		utxo, ok := lookup(input.PrevOut)
		if !ok {
			return PreparedTxValidation{}, ErrMissingUTXO
		}
		next := inputSum + utxo.ValueAtoms
		if next < inputSum {
			return PreparedTxValidation{}, ErrAmountOverflow
		}
		inputSum = next
		inputAmounts = append(inputAmounts, utxo.ValueAtoms)
		resolvedInputs = append(resolvedInputs, utxo)
	}

	for _, output := range tx.Base.Outputs {
		if !crypto.IsValidXOnlyPubKey(&output.PubKey) {
			return PreparedTxValidation{}, ErrInvalidOutputPubKey
		}
		next := outputSum + output.ValueAtoms
		if next < outputSum {
			return PreparedTxValidation{}, ErrAmountOverflow
		}
		outputSum = next
	}
	if inputSum < outputSum {
		return PreparedTxValidation{}, ErrInputsLessThanOutputs
	}

	for i := range tx.Base.Inputs {
		utxo := resolvedInputs[i]
		auth := tx.Auth.Entries[i]
		msg, err := Sighash(tx, i, inputAmounts)
		if err != nil {
			return PreparedTxValidation{}, err
		}
		signatureChecks = append(signatureChecks, crypto.SchnorrBatchItem{
			PubKey:    utxo.PubKey,
			Signature: auth.Signature,
			Msg:       msg,
		})
	}

	return PreparedTxValidation{
		Summary: TxValidationSummary{
			InputSum:  inputSum,
			OutputSum: outputSum,
			Fee:       inputSum - outputSum,
		},
		SignatureChecks: signatureChecks,
	}, nil
}

func ValidateTxWithLookup(tx *types.Transaction, lookup UtxoLookup, rules ConsensusRules) (TxValidationSummary, error) {
	prepared, err := PrepareTxValidationWithLookup(tx, lookup, rules)
	if err != nil {
		return TxValidationSummary{}, err
	}
	for i := range prepared.SignatureChecks {
		check := prepared.SignatureChecks[i]
		if !crypto.VerifySchnorrXOnly(&check.PubKey, &check.Signature, &check.Msg) {
			return TxValidationSummary{}, ErrInvalidSignature
		}
	}
	return prepared.Summary, nil
}

func ComputedUTXORoot(utxos UtxoSet) [32]byte {
	return utreexo.UtxoRoot(UtxoLeaves(utxos))
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
	e := max64(prev.Epsilon, params.BlockSizeFloor/2)
	b := max64(prev.Beta, params.BlockSizeFloor/2)
	y := e + b
	x := min64(prev.BlockSize, y)

	nextE := e
	nextB := b
	decay := b / ablaThetaDen
	threeX := new(big.Int).Mul(new(big.Int).SetUint64(x), big.NewInt(3))
	twoE := new(big.Int).Mul(new(big.Int).SetUint64(e), big.NewInt(2))
	if threeX.Cmp(twoE) > 0 {
		diff := new(big.Int).Sub(threeX, twoE)
		dENum := new(big.Int).Mul(new(big.Int).SetUint64(e), diff)
		denInner := new(big.Int).Add(
			new(big.Int).SetUint64(e),
			new(big.Int).Mul(new(big.Int).SetUint64(b), big.NewInt(3)),
		)
		dEDen := new(big.Int).Mul(denInner, big.NewInt(int64(2*ablaGammaDen)))
		dE := bigIntToUint64(new(big.Int).Div(dENum, dEDen))
		nextE = bigIntToUint64(new(big.Int).Add(new(big.Int).SetUint64(e), new(big.Int).SetUint64(dE)))
		nextB = bigIntToUint64(new(big.Int).Add(
			new(big.Int).SetUint64(b-decay),
			new(big.Int).Mul(new(big.Int).SetUint64(dE), new(big.Int).SetUint64(ablaDelta)),
		))
	} else {
		shrinkNum := bigIntToUint64(new(big.Int).Sub(twoE, threeX))
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

	poly := new(big.Int).Mul(big.NewInt(195766423245049), big.NewInt(frac))
	fracSquared := frac * frac
	fracCubed := fracSquared * frac
	poly.Add(poly, new(big.Int).Mul(big.NewInt(971821376), big.NewInt(fracSquared)))
	poly.Add(poly, new(big.Int).Mul(big.NewInt(5127), big.NewInt(fracCubed)))
	poly.Add(poly, new(big.Int).Lsh(big.NewInt(1), 47))
	poly.Rsh(poly, 48)
	poly.Add(poly, big.NewInt(1<<16))

	nextTarget := new(big.Int).Mul(new(big.Int).Set(anchorTarget), poly)
	if numShifts < 0 {
		nextTarget.Rsh(nextTarget, uint(-numShifts))
	} else if numShifts > 0 {
		nextTarget.Lsh(nextTarget, uint(numShifts))
	}
	nextTarget.Rsh(nextTarget, 16)

	if nextTarget.Sign() <= 0 {
		return targetToCompact(big.NewInt(1))
	}
	if nextTarget.Cmp(powLimit) > 0 {
		return params.PowLimitBits, nil
	}
	return targetToCompact(nextTarget)
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
	hash := HeaderHash(header)
	value := new(big.Int).SetBytes(hash[:])
	if value.Cmp(target) > 0 {
		return ErrInvalidPow
	}
	return nil
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
	sorted := append([]uint64(nil), timestamps...)
	slices.Sort(sorted)
	return sorted[len(sorted)/2]
}

func ValidateHeader(header *types.BlockHeader, prev PrevBlockContext, params ChainParams) error {
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
	expectedNBits, err := NextWorkRequired(prev, params)
	if err != nil {
		return err
	}
	if header.NBits != expectedNBits {
		return ErrInvalidNBits
	}
	if err := checkPow(header, params); err != nil {
		return err
	}
	return nil
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
	for nonce := header.Nonce; ; nonce++ {
		if shouldContinue != nil && nonce&0x0fff == 0 && !shouldContinue(nonce) {
			return types.BlockHeader{}, false, nil
		}
		header.Nonce = nonce
		hash := HeaderHash(&header)
		if new(big.Int).SetBytes(hash[:]).Cmp(target) <= 0 {
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
	spent := make([]types.OutPoint, 0)
	createdByOutPoint := make(map[types.OutPoint]utreexo.UtxoLeaf)
	createdOrder := make([]types.OutPoint, 0)
	for i := 1; i < len(block.Txs); i++ {
		tx := &block.Txs[i]
		txid := TxID(tx)
		for _, input := range tx.Base.Inputs {
			// Outputs created and spent within the same block never exist in the
			// pre-block accumulator, so they cancel out of the accumulator delta.
			if _, ok := createdByOutPoint[input.PrevOut]; ok {
				delete(createdByOutPoint, input.PrevOut)
				continue
			}
			spent = append(spent, input.PrevOut)
		}
		for vout, output := range tx.Base.Outputs {
			outPoint := types.OutPoint{TxID: txid, Vout: uint32(vout)}
			createdByOutPoint[outPoint] = utreexo.UtxoLeaf{
				OutPoint:   outPoint,
				ValueAtoms: output.ValueAtoms,
				PubKey:     output.PubKey,
			}
			createdOrder = append(createdOrder, outPoint)
		}
	}
	coinbase := &block.Txs[0]
	coinbaseTxID := TxID(coinbase)
	for vout, output := range coinbase.Base.Outputs {
		outPoint := types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}
		createdByOutPoint[outPoint] = utreexo.UtxoLeaf{
			OutPoint:   outPoint,
			ValueAtoms: output.ValueAtoms,
			PubKey:     output.PubKey,
		}
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

func ValidateAndApplyBlock(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, utxos UtxoSet, params ChainParams, rules ConsensusRules) (BlockValidationSummary, error) {
	summary, overlay, _, err := validateAndApplyBlockOverlay(block, prev, blockSizeState, utxos, nil, params, rules)
	if err != nil {
		return BlockValidationSummary{}, err
	}
	replaceUtxoSet(utxos, overlay.Materialize())
	return summary, nil
}

func ValidateAndApplyBlockWithAccumulator(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, utxos UtxoSet, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules) (BlockValidationSummary, *utreexo.Accumulator, error) {
	summary, overlay, nextAcc, err := validateAndApplyBlockOverlay(block, prev, blockSizeState, utxos, accumulator, params, rules)
	if err != nil {
		return BlockValidationSummary{}, nil, err
	}
	replaceUtxoSet(utxos, overlay.Materialize())
	return summary, nextAcc, nil
}

func ValidateAndApplyBlockOverlayWithAccumulator(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, utxos UtxoSet, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules) (BlockValidationSummary, *UtxoOverlay, *utreexo.Accumulator, error) {
	return validateAndApplyBlockOverlay(block, prev, blockSizeState, utxos, accumulator, params, rules)
}

func ValidateAndApplyBlockViewWithAccumulator(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, utxos UtxoSet, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules) (BlockValidationSummary, UtxoSet, *utreexo.Accumulator, error) {
	summary, overlay, nextAcc, err := validateAndApplyBlockOverlay(block, prev, blockSizeState, utxos, accumulator, params, rules)
	if err != nil {
		return BlockValidationSummary{}, nil, nil, err
	}
	return summary, overlay.Materialize(), nextAcc, nil
}

func validateAndApplyBlockOverlay(block *types.Block, prev PrevBlockContext, blockSizeState BlockSizeState, utxos UtxoSet, accumulator *utreexo.Accumulator, params ChainParams, rules ConsensusRules) (BlockValidationSummary, *UtxoOverlay, *utreexo.Accumulator, error) {
	if len(block.Txs) == 0 {
		return BlockValidationSummary{}, nil, nil, ErrEmptyBlock
	}
	if len(block.Encode()) > int(NextBlockSizeLimit(blockSizeState, params)) {
		return BlockValidationSummary{}, nil, nil, ErrBlockTooLarge
	}

	txids, _, txRoot, authRoot := BuildBlockRoots(block.Txs)
	if txRoot != block.Header.MerkleTxIDRoot {
		return BlockValidationSummary{}, nil, nil, ErrMerkleTxIDMismatch
	}
	if authRoot != block.Header.MerkleAuthRoot {
		return BlockValidationSummary{}, nil, nil, ErrMerkleAuthMismatch
	}

	if err := ValidateHeader(&block.Header, prev, params); err != nil {
		return BlockValidationSummary{}, nil, nil, err
	}

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
		if !crypto.IsValidXOnlyPubKey(&output.PubKey) {
			return BlockValidationSummary{}, nil, nil, ErrInvalidOutputPubKey
		}
	}

	tempUtxos := NewUtxoOverlay(utxos)
	claimedInputs := make(map[types.OutPoint]struct{})
	signatureChecks := make([]crypto.SchnorrBatchItem, 0)
	var totalFees uint64
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
		// Blocks are validated as an atomic patch to the pre-block UTXO set, so
		// later transactions must see outputs created by earlier non-coinbase
		// transactions in the same block.
		prepared, err := PrepareTxValidationWithLookup(tx, tempUtxos.Lookup, rules)
		if err != nil {
			return BlockValidationSummary{}, nil, nil, err
		}
		nextFees := totalFees + prepared.Summary.Fee
		if nextFees < totalFees {
			return BlockValidationSummary{}, nil, nil, ErrAmountOverflow
		}
		totalFees = nextFees
		signatureChecks = append(signatureChecks, prepared.SignatureChecks...)
		tempUtxos.ApplyTx(*tx, txids[i])
	}
	verifyStartedAt := time.Now()
	batchResult := verifyBlockSignatureChecks(signatureChecks)
	verifyDuration := time.Since(verifyStartedAt)
	if !batchResult.Valid {
		return BlockValidationSummary{}, nil, nil, ErrInvalidSignature
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
	for vout, output := range coinbase.Base.Outputs {
		tempUtxos.Set(types.OutPoint{TxID: coinbaseTxID, Vout: uint32(vout)}, UtxoEntry{
			ValueAtoms: output.ValueAtoms,
			PubKey:     output.PubKey,
		})
	}
	var nextAccumulator *utreexo.Accumulator
	if accumulator != nil {
		spent, created := blockUtxoDelta(block)
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
			finalUtxos := tempUtxos.Materialize()
			if ComputedUTXORoot(finalUtxos) != root {
				return BlockValidationSummary{}, nil, nil, ErrUTXORootMismatch
			}
		}
	}

	nextState := AdvanceBlockSizeState(blockSizeState, uint64(len(block.Encode())), params)
	return BlockValidationSummary{
		Height:                 prev.Height + 1,
		TotalFees:              totalFees,
		CoinbaseValue:          coinbaseValue,
		SignatureChecks:        len(signatureChecks),
		SignatureBatchFallback: batchResult.Fallback,
		SignatureVerifyTime:    verifyDuration,
		NextBlockSizeState:     nextState,
	}, tempUtxos, nextAccumulator, nil
}

func verifyBlockSignatureChecks(items []crypto.SchnorrBatchItem) crypto.SchnorrBatchResult {
	if len(items) < minParallelSigChecks {
		return crypto.VerifySchnorrBatchXOnlyResult(items)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		return crypto.VerifySchnorrBatchXOnlyResult(items)
	}
	maxWorkers := (len(items) + minSigChecksPerWorker - 1) / minSigChecksPerWorker
	if workers > maxWorkers {
		workers = maxWorkers
	}
	if workers < 2 {
		return crypto.VerifySchnorrBatchXOnlyResult(items)
	}

	chunkSize := (len(items) + workers - 1) / workers
	results := make(chan crypto.SchnorrBatchResult, workers)
	for start := 0; start < len(items); start += chunkSize {
		end := minInt(start+chunkSize, len(items))
		chunk := items[start:end]
		go func() {
			results <- crypto.VerifySchnorrBatchXOnlyResult(chunk)
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

func replaceUtxoSet(dst UtxoSet, src UtxoSet) {
	for k := range dst {
		delete(dst, k)
	}
	for k, v := range src {
		dst[k] = v
	}
}

func DecodeTxHex(raw string, limits types.CodecLimits) (types.Transaction, error) {
	return types.DecodeTransactionHex(raw, limits)
}

func DecodeBlockHex(raw string, limits types.CodecLimits) (types.Block, error) {
	return types.DecodeBlockHex(raw, limits)
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func saturatingSub(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}
