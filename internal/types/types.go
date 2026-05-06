package types

import (
	"encoding/hex"
	"errors"
	"fmt"
)

type OutPoint struct {
	TxID [32]byte
	Vout uint32
}

type TxInput struct {
	PrevOut OutPoint
}

type TxOutput struct {
	Type       uint64
	ValueAtoms uint64
	Payload32  [32]byte
	// PubKey is a compatibility alias for x-only outputs while the codebase
	// finishes migrating to typed payloads. PQ outputs leave this zeroed.
	PubKey [32]byte
}

type TxAuthEntry struct {
	AuthPayload []byte
	// Signature is a compatibility alias for fixed-width x-only auth entries.
	Signature [64]byte
}

type TxBase struct {
	Version            uint32
	CoinbaseHeight     *uint64
	CoinbaseExtraNonce *[CoinbaseExtraNonceLen]byte
	Inputs             []TxInput
	Outputs            []TxOutput
}

type TxAuth struct {
	Entries []TxAuthEntry
}

type Transaction struct {
	Base TxBase
	Auth TxAuth
}

type BlockHeader struct {
	Version        uint32
	PrevBlockHash  [32]byte
	MerkleTxIDRoot [32]byte
	MerkleAuthRoot [32]byte
	UTXORoot       [32]byte
	Timestamp      uint64
	NBits          uint32
	Nonce          uint64
}

type Block struct {
	Header BlockHeader
	Txs    []Transaction
}

type ChainProfile string

const (
	Mainnet       ChainProfile = "mainnet"
	Regtest       ChainProfile = "regtest"
	RegtestMedium ChainProfile = "regtest_medium"
	RegtestHard   ChainProfile = "regtest_hard"
	BenchNet      ChainProfile = "benchnet"
)

const (
	CoinbaseExtraNonceLen = 16
	BlockHeaderEncodedLen = 152

	OutputXOnlyP2PK = uint64(0x00)
	OutputPQLock32  = uint64(0x01)

	AlgMLDSA65 = uint64(0x01)
)

func ParseChainProfile(raw string) (ChainProfile, error) {
	switch ChainProfile(raw) {
	case Mainnet:
		return Mainnet, nil
	case Regtest:
		return Regtest, nil
	case RegtestMedium:
		return RegtestMedium, nil
	case RegtestHard:
		return RegtestHard, nil
	case BenchNet:
		return BenchNet, nil
	default:
		return "", fmt.Errorf("unknown chain profile: %s", raw)
	}
}

func (p ChainProfile) String() string {
	return string(p)
}

func (p ChainProfile) IsRegtestLike() bool {
	return p == Regtest || p == RegtestMedium || p == RegtestHard || p == BenchNet
}

type CodecLimits struct {
	MaxInputs      int
	MaxOutputs     int
	MaxTxsPerBlock int
	MaxBlockBytes  int
}

func DefaultCodecLimits() CodecLimits {
	return CodecLimits{
		MaxInputs:      100_000,
		MaxOutputs:     100_000,
		MaxTxsPerBlock: 200_000,
		MaxBlockBytes:  64_000_000,
	}
}

var (
	ErrUnexpectedEOF      = errors.New("unexpected EOF")
	ErrNonCanonicalVarInt = errors.New("non-canonical varint")
	ErrTrailingBytes      = errors.New("trailing bytes")
)

type LimitExceededError struct {
	Field string
}

func (e LimitExceededError) Error() string {
	return fmt.Sprintf("limit exceeded: %s", e.Field)
}

type InvalidFormatError struct {
	Reason string
}

func (e InvalidFormatError) Error() string {
	return fmt.Sprintf("invalid format: %s", e.Reason)
}

type reader struct {
	buf []byte
	pos int
}

func newReader(buf []byte) *reader {
	return &reader{buf: buf}
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrUnexpectedEOF
	}
	end := r.pos + n
	if end < r.pos || end > len(r.buf) {
		return nil, ErrUnexpectedEOF
	}
	out := r.buf[r.pos:end]
	r.pos = end
	return out, nil
}

func (r *reader) readU8() (uint8, error) {
	buf, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *reader) readU16LE() (uint16, error) {
	var out uint16
	buf, err := r.take(2)
	if err != nil {
		return 0, err
	}
	out = uint16(buf[0]) | uint16(buf[1])<<8
	return out, nil
}

func (r *reader) readU32LE() (uint32, error) {
	buf, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24, nil
}

func (r *reader) readU64LE() (uint64, error) {
	buf, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return uint64(buf[0]) |
		uint64(buf[1])<<8 |
		uint64(buf[2])<<16 |
		uint64(buf[3])<<24 |
		uint64(buf[4])<<32 |
		uint64(buf[5])<<40 |
		uint64(buf[6])<<48 |
		uint64(buf[7])<<56, nil
}

func (r *reader) readArray32() ([32]byte, error) {
	var out [32]byte
	buf, err := r.take(32)
	if err != nil {
		return out, err
	}
	copy(out[:], buf)
	return out, nil
}

func (r *reader) readArray16() ([CoinbaseExtraNonceLen]byte, error) {
	var out [CoinbaseExtraNonceLen]byte
	buf, err := r.take(CoinbaseExtraNonceLen)
	if err != nil {
		return out, err
	}
	copy(out[:], buf)
	return out, nil
}

func (r *reader) readArray64() ([64]byte, error) {
	var out [64]byte
	buf, err := r.take(64)
	if err != nil {
		return out, err
	}
	copy(out[:], buf)
	return out, nil
}

func (r *reader) readVarInt() (uint64, error) {
	first, err := r.readU8()
	if err != nil {
		return 0, err
	}
	switch {
	case first <= 0xfc:
		return uint64(first), nil
	case first == 0xfd:
		v, err := r.readU16LE()
		if err != nil {
			return 0, err
		}
		if v <= 0xfc {
			return 0, ErrNonCanonicalVarInt
		}
		return uint64(v), nil
	case first == 0xfe:
		v, err := r.readU32LE()
		if err != nil {
			return 0, err
		}
		if v <= 0xffff {
			return 0, ErrNonCanonicalVarInt
		}
		return uint64(v), nil
	default:
		v, err := r.readU64LE()
		if err != nil {
			return 0, err
		}
		if v <= 0xffff_ffff {
			return 0, ErrNonCanonicalVarInt
		}
		return v, nil
	}
}

func writeU32LE(out *[]byte, v uint32) {
	*out = append(*out, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func writeU64LE(out *[]byte, v uint64) {
	*out = append(*out,
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

func writeVarInt(out *[]byte, v uint64) {
	switch {
	case v <= 0xfc:
		*out = append(*out, byte(v))
	case v <= 0xffff:
		*out = append(*out, 0xfd, byte(v), byte(v>>8))
	case v <= 0xffff_ffff:
		*out = append(*out, 0xfe, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	default:
		*out = append(*out, 0xff)
		writeU64LE(out, v)
	}
}

func varIntEncodedLen(v uint64) int {
	switch {
	case v <= 0xfc:
		return 1
	case v <= 0xffff:
		return 3
	case v <= 0xffff_ffff:
		return 5
	default:
		return 9
	}
}

func CanonicalVarIntLen(v uint64) int {
	return varIntEncodedLen(v)
}

func AppendCanonicalVarInt(dst []byte, v uint64) []byte {
	writeVarInt(&dst, v)
	return dst
}

func CanonicalVarIntBytes(v uint64) []byte {
	return AppendCanonicalVarInt(nil, v)
}

func readCount(r *reader, max int, field string) (int, error) {
	raw, err := r.readVarInt()
	if err != nil {
		return 0, err
	}
	if raw > uint64(max) {
		return 0, LimitExceededError{Field: field}
	}
	return int(raw), nil
}

func (o OutPoint) Encode(out *[]byte) {
	*out = append(*out, o.TxID[:]...)
	writeU32LE(out, o.Vout)
}

func (o OutPoint) EncodedLen() int {
	return 36
}

func decodeOutPoint(r *reader) (OutPoint, error) {
	txid, err := r.readArray32()
	if err != nil {
		return OutPoint{}, err
	}
	vout, err := r.readU32LE()
	if err != nil {
		return OutPoint{}, err
	}
	return OutPoint{TxID: txid, Vout: vout}, nil
}

func (i TxInput) Encode(out *[]byte) {
	i.PrevOut.Encode(out)
}

func (i TxInput) EncodedLen() int {
	return i.PrevOut.EncodedLen()
}

func decodeTxInput(r *reader) (TxInput, error) {
	prevOut, err := decodeOutPoint(r)
	if err != nil {
		return TxInput{}, err
	}
	return TxInput{PrevOut: prevOut}, nil
}

func (o TxOutput) Encode(out *[]byte) {
	writeVarInt(out, o.Type)
	writeU64LE(out, o.ValueAtoms)
	payload32 := o.Payload32
	if payload32 == ([32]byte{}) && o.Type == OutputXOnlyP2PK {
		payload32 = o.PubKey
	}
	*out = append(*out, payload32[:]...)
}

func (o TxOutput) EncodedLen() int {
	return varIntEncodedLen(o.Type) + 8 + 32
}

func decodeTxOutput(r *reader) (TxOutput, error) {
	outputType, err := r.readVarInt()
	if err != nil {
		return TxOutput{}, err
	}
	value, err := r.readU64LE()
	if err != nil {
		return TxOutput{}, err
	}
	payload32, err := r.readArray32()
	if err != nil {
		return TxOutput{}, err
	}
	output := TxOutput{Type: outputType, ValueAtoms: value}
	if outputType == OutputXOnlyP2PK {
		output.PubKey = payload32
	} else {
		output.Payload32 = payload32
	}
	return output, nil
}

func (e TxAuthEntry) Encode(out *[]byte) {
	authPayload := e.AuthPayload
	if len(authPayload) == 0 && e.Signature != ([64]byte{}) {
		authPayload = e.Signature[:]
	}
	writeVarInt(out, uint64(len(authPayload)))
	*out = append(*out, authPayload...)
}

func (e TxAuthEntry) EncodedLen() int {
	authLen := len(e.AuthPayload)
	if authLen == 0 && e.Signature != ([64]byte{}) {
		authLen = len(e.Signature)
	}
	return varIntEncodedLen(uint64(authLen)) + authLen
}

func decodeTxAuthEntry(r *reader) (TxAuthEntry, error) {
	authLen, err := r.readVarInt()
	if err != nil {
		return TxAuthEntry{}, err
	}
	authPayload, err := r.take(int(authLen))
	if err != nil {
		return TxAuthEntry{}, err
	}
	entry := TxAuthEntry{}
	if len(authPayload) == len(entry.Signature) {
		copy(entry.Signature[:], authPayload)
	} else {
		entry.AuthPayload = append([]byte(nil), authPayload...)
	}
	return entry, nil
}

func (b TxBase) Encode(out *[]byte) {
	writeU32LE(out, b.Version)
	writeVarInt(out, uint64(len(b.Inputs)))
	if len(b.Inputs) == 0 {
		// Coinbase transactions structurally commit the block height inside the
		// base encoding. The fixed-width extra nonce extends miner search space
		// without introducing an unbounded arbitrary script region.
		writeVarInt(out, *b.CoinbaseHeight)
		*out = append(*out, (*b.CoinbaseExtraNonce)[:]...)
	}
	for _, input := range b.Inputs {
		input.Encode(out)
	}
	writeVarInt(out, uint64(len(b.Outputs)))
	for _, output := range b.Outputs {
		output.Encode(out)
	}
}

func (b TxBase) EncodedLen() int {
	size := 4 + varIntEncodedLen(uint64(len(b.Inputs)))
	if len(b.Inputs) == 0 {
		if b.CoinbaseHeight != nil {
			size += varIntEncodedLen(*b.CoinbaseHeight)
		}
		size += CoinbaseExtraNonceLen
	}
	for _, input := range b.Inputs {
		size += input.EncodedLen()
	}
	size += varIntEncodedLen(uint64(len(b.Outputs)))
	for _, output := range b.Outputs {
		size += output.EncodedLen()
	}
	return size
}

func decodeTxBase(r *reader, limits CodecLimits) (TxBase, error) {
	version, err := r.readU32LE()
	if err != nil {
		return TxBase{}, err
	}
	inputCount, err := readCount(r, limits.MaxInputs, "tx.inputs")
	if err != nil {
		return TxBase{}, err
	}
	var coinbaseHeight *uint64
	var coinbaseExtraNonce *[CoinbaseExtraNonceLen]byte
	if inputCount == 0 {
		height, err := r.readVarInt()
		if err != nil {
			return TxBase{}, err
		}
		coinbaseHeight = new(uint64)
		*coinbaseHeight = height
		extraNonce, err := r.readArray16()
		if err != nil {
			return TxBase{}, err
		}
		coinbaseExtraNonce = new([CoinbaseExtraNonceLen]byte)
		*coinbaseExtraNonce = extraNonce
	}
	inputs := make([]TxInput, 0, inputCount)
	for range inputCount {
		input, err := decodeTxInput(r)
		if err != nil {
			return TxBase{}, err
		}
		inputs = append(inputs, input)
	}
	outputCount, err := readCount(r, limits.MaxOutputs, "tx.outputs")
	if err != nil {
		return TxBase{}, err
	}
	outputs := make([]TxOutput, 0, outputCount)
	for range outputCount {
		output, err := decodeTxOutput(r)
		if err != nil {
			return TxBase{}, err
		}
		outputs = append(outputs, output)
	}
	return TxBase{
		Version:            version,
		CoinbaseHeight:     coinbaseHeight,
		CoinbaseExtraNonce: coinbaseExtraNonce,
		Inputs:             inputs,
		Outputs:            outputs,
	}, nil
}

func (a TxAuth) Encode(out *[]byte) {
	writeVarInt(out, uint64(len(a.Entries)))
	for _, entry := range a.Entries {
		entry.Encode(out)
	}
}

func (a TxAuth) EncodedLen() int {
	size := varIntEncodedLen(uint64(len(a.Entries)))
	for _, entry := range a.Entries {
		size += entry.EncodedLen()
	}
	return size
}

func decodeTxAuth(r *reader, maxEntries int) (TxAuth, error) {
	count, err := readCount(r, maxEntries, "tx.auth")
	if err != nil {
		return TxAuth{}, err
	}
	entries := make([]TxAuthEntry, 0, count)
	for range count {
		entry, err := decodeTxAuthEntry(r)
		if err != nil {
			return TxAuth{}, err
		}
		entries = append(entries, entry)
	}
	return TxAuth{Entries: entries}, nil
}

func (t Transaction) IsCoinbase() bool {
	return len(t.Base.Inputs) == 0 && t.Base.CoinbaseHeight != nil && t.Base.CoinbaseExtraNonce != nil
}

func (t Transaction) EncodeBase() []byte {
	out := make([]byte, 0, t.Base.EncodedLen())
	t.Base.Encode(&out)
	return out
}

func (t Transaction) EncodeAuth() []byte {
	out := make([]byte, 0, t.Auth.EncodedLen())
	t.Auth.Encode(&out)
	return out
}

func (t Transaction) Encode() []byte {
	out := make([]byte, 0, t.EncodedLen())
	t.Base.Encode(&out)
	t.Auth.Encode(&out)
	return out
}

func (t Transaction) EncodedLen() int {
	return t.Base.EncodedLen() + t.Auth.EncodedLen()
}

func decodeTransactionFromReader(r *reader, limits CodecLimits) (Transaction, error) {
	base, err := decodeTxBase(r, limits)
	if err != nil {
		return Transaction{}, err
	}
	auth, err := decodeTxAuth(r, limits.MaxInputs)
	if err != nil {
		return Transaction{}, err
	}
	if len(base.Inputs) == 0 {
		if base.CoinbaseHeight == nil {
			return Transaction{}, InvalidFormatError{Reason: "coinbase tx must include coinbase_height"}
		}
		if base.CoinbaseExtraNonce == nil {
			return Transaction{}, InvalidFormatError{Reason: "coinbase tx must include coinbase_extra_nonce"}
		}
		if len(auth.Entries) != 0 {
			return Transaction{}, InvalidFormatError{Reason: "coinbase tx must have empty auth"}
		}
	} else {
		if base.CoinbaseHeight != nil {
			return Transaction{}, InvalidFormatError{Reason: "non-coinbase tx must not include coinbase_height"}
		}
		if base.CoinbaseExtraNonce != nil {
			return Transaction{}, InvalidFormatError{Reason: "non-coinbase tx must not include coinbase_extra_nonce"}
		}
		if len(auth.Entries) != len(base.Inputs) {
			return Transaction{}, InvalidFormatError{Reason: "auth entry count must equal input count"}
		}
	}
	return Transaction{Base: base, Auth: auth}, nil
}

func DecodeTransactionWithLimits(buf []byte, limits CodecLimits) (Transaction, error) {
	r := newReader(buf)
	tx, err := decodeTransactionFromReader(r, limits)
	if err != nil {
		return Transaction{}, err
	}
	if r.pos != len(buf) {
		return Transaction{}, ErrTrailingBytes
	}
	return tx, nil
}

func DecodeTransactionHex(raw string, limits CodecLimits) (Transaction, error) {
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return Transaction{}, err
	}
	return DecodeTransactionWithLimits(buf, limits)
}

func (h BlockHeader) Encode() []byte {
	out := make([]byte, 0, BlockHeaderEncodedLen)
	writeU32LE(&out, h.Version)
	out = append(out, h.PrevBlockHash[:]...)
	out = append(out, h.MerkleTxIDRoot[:]...)
	out = append(out, h.MerkleAuthRoot[:]...)
	out = append(out, h.UTXORoot[:]...)
	writeU64LE(&out, h.Timestamp)
	writeU32LE(&out, h.NBits)
	writeU64LE(&out, h.Nonce)
	return out
}

func decodeBlockHeader(r *reader) (BlockHeader, error) {
	version, err := r.readU32LE()
	if err != nil {
		return BlockHeader{}, err
	}
	prev, err := r.readArray32()
	if err != nil {
		return BlockHeader{}, err
	}
	txRoot, err := r.readArray32()
	if err != nil {
		return BlockHeader{}, err
	}
	authRoot, err := r.readArray32()
	if err != nil {
		return BlockHeader{}, err
	}
	utxoRoot, err := r.readArray32()
	if err != nil {
		return BlockHeader{}, err
	}
	timestamp, err := r.readU64LE()
	if err != nil {
		return BlockHeader{}, err
	}
	nbits, err := r.readU32LE()
	if err != nil {
		return BlockHeader{}, err
	}
	nonce, err := r.readU64LE()
	if err != nil {
		return BlockHeader{}, err
	}
	return BlockHeader{
		Version:        version,
		PrevBlockHash:  prev,
		MerkleTxIDRoot: txRoot,
		MerkleAuthRoot: authRoot,
		UTXORoot:       utxoRoot,
		Timestamp:      timestamp,
		NBits:          nbits,
		Nonce:          nonce,
	}, nil
}

func DecodeBlockHeader(buf []byte) (BlockHeader, error) {
	r := newReader(buf)
	header, err := decodeBlockHeader(r)
	if err != nil {
		return BlockHeader{}, err
	}
	if r.pos != len(buf) {
		return BlockHeader{}, ErrTrailingBytes
	}
	return header, nil
}

func (b Block) Encode() []byte {
	out := b.Header.Encode()
	writeVarInt(&out, uint64(len(b.Txs)))
	for _, tx := range b.Txs {
		bb := tx.EncodeBase()
		out = append(out, bb...)
		tx.Auth.Encode(&out)
	}
	return out
}

func (b Block) EncodedLen() int {
	size := BlockHeaderEncodedLen + varIntEncodedLen(uint64(len(b.Txs)))
	for _, tx := range b.Txs {
		size += tx.EncodedLen()
	}
	return size
}

func DecodeBlockWithLimits(buf []byte, limits CodecLimits) (Block, error) {
	r := newReader(buf)
	header, err := decodeBlockHeader(r)
	if err != nil {
		return Block{}, err
	}
	txCount, err := readCount(r, limits.MaxTxsPerBlock, "block.txs")
	if err != nil {
		return Block{}, err
	}
	txs := make([]Transaction, 0, txCount)
	for range txCount {
		tx, err := decodeTransactionFromReader(r, limits)
		if err != nil {
			return Block{}, err
		}
		txs = append(txs, tx)
	}
	if r.pos != len(buf) {
		return Block{}, ErrTrailingBytes
	}
	if len(buf) > limits.MaxBlockBytes {
		return Block{}, LimitExceededError{Field: "block.bytes"}
	}
	return Block{Header: header, Txs: txs}, nil
}

func DecodeBlockHex(raw string, limits CodecLimits) (Block, error) {
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return Block{}, err
	}
	return DecodeBlockWithLimits(buf, limits)
}

func NewXOnlyOutput(valueAtoms uint64, pubKey [32]byte) TxOutput {
	return TxOutput{
		Type:       OutputXOnlyP2PK,
		ValueAtoms: valueAtoms,
		Payload32:  pubKey,
		PubKey:     pubKey,
	}
}

func NewPQLockOutput(valueAtoms uint64, pqLock [32]byte) TxOutput {
	return TxOutput{
		Type:       OutputPQLock32,
		ValueAtoms: valueAtoms,
		Payload32:  pqLock,
	}
}

func NewXOnlyAuthEntry(signature [64]byte) TxAuthEntry {
	payload := make([]byte, len(signature))
	copy(payload, signature[:])
	return TxAuthEntry{AuthPayload: payload, Signature: signature}
}

func (o TxOutput) XOnlyPubKey() ([32]byte, bool) {
	if o.Type != OutputXOnlyP2PK {
		return [32]byte{}, false
	}
	return o.Payload32, true
}

func (o TxOutput) PQLock() ([32]byte, bool) {
	if o.Type != OutputPQLock32 {
		return [32]byte{}, false
	}
	return o.Payload32, true
}

func (e TxAuthEntry) XOnlySignature() ([64]byte, bool) {
	var signature [64]byte
	if len(e.AuthPayload) == 0 && e.Signature != ([64]byte{}) {
		return e.Signature, true
	}
	if len(e.AuthPayload) != len(signature) {
		return signature, false
	}
	copy(signature[:], e.AuthPayload)
	return signature, true
}
