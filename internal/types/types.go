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
	ValueAtoms uint64
	KeyHash    [32]byte
}

type TxAuthEntry struct {
	PubKey    [32]byte
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
)

const (
	CoinbaseExtraNonceLen = 16
	BlockHeaderEncodedLen = 152
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
	default:
		return "", fmt.Errorf("unknown chain profile: %s", raw)
	}
}

func (p ChainProfile) String() string {
	return string(p)
}

func (p ChainProfile) IsRegtestLike() bool {
	return p == Regtest || p == RegtestMedium || p == RegtestHard
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

func decodeTxInput(r *reader) (TxInput, error) {
	prevOut, err := decodeOutPoint(r)
	if err != nil {
		return TxInput{}, err
	}
	return TxInput{PrevOut: prevOut}, nil
}

func (o TxOutput) Encode(out *[]byte) {
	writeU64LE(out, o.ValueAtoms)
	*out = append(*out, o.KeyHash[:]...)
}

func decodeTxOutput(r *reader) (TxOutput, error) {
	value, err := r.readU64LE()
	if err != nil {
		return TxOutput{}, err
	}
	keyHash, err := r.readArray32()
	if err != nil {
		return TxOutput{}, err
	}
	return TxOutput{ValueAtoms: value, KeyHash: keyHash}, nil
}

func (e TxAuthEntry) Encode(out *[]byte) {
	*out = append(*out, e.PubKey[:]...)
	*out = append(*out, e.Signature[:]...)
}

func decodeTxAuthEntry(r *reader) (TxAuthEntry, error) {
	pubKey, err := r.readArray32()
	if err != nil {
		return TxAuthEntry{}, err
	}
	signature, err := r.readArray64()
	if err != nil {
		return TxAuthEntry{}, err
	}
	return TxAuthEntry{PubKey: pubKey, Signature: signature}, nil
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
	out := make([]byte, 0)
	t.Base.Encode(&out)
	return out
}

func (t Transaction) EncodeAuth() []byte {
	out := make([]byte, 0)
	t.Auth.Encode(&out)
	return out
}

func (t Transaction) Encode() []byte {
	out := t.EncodeBase()
	t.Auth.Encode(&out)
	return out
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
