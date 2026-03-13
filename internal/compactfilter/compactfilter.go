package compactfilter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
)

const (
	itemTag    = "BPU/CompactFilterItemV1"
	filterType = "basic"
)

type Filter struct {
	BlockHash [32]byte
	Entries   int
	Encoded   []byte
	Hash      [32]byte
}

func Type() string { return filterType }

// Build deterministically encodes the block's created output pubkeys and the
// consumed prevout pubkeys captured in undo data into a compact probabilistic
// filter. The filter is non-consensus and keyed by block hash.
func Build(blockHash [32]byte, block *types.Block, undo []storage.BlockUndoEntry) Filter {
	fingerprints := collectFingerprints(blockHash, block, undo)
	encoded := encodeFingerprints(fingerprints)
	return Filter{
		BlockHash: blockHash,
		Entries:   len(fingerprints),
		Encoded:   encoded,
		Hash:      crypto.Sha256d(encoded),
	}
}

// Header chains filter hashes the same way BIP157-style filter headers do:
// current filter hash committed against the previous filter header.
func Header(filterHash [32]byte, prevHeader [32]byte) [32]byte {
	var buf [64]byte
	copy(buf[:32], filterHash[:])
	copy(buf[32:], prevHeader[:])
	return crypto.Sha256d(buf[:])
}

func Match(blockHash [32]byte, encoded []byte, pubKey [32]byte) (bool, error) {
	fingerprints, err := decodeFingerprints(encoded)
	if err != nil {
		return false, err
	}
	target := fingerprintForPubKey(blockHash, pubKey)
	index := sort.Search(len(fingerprints), func(i int) bool { return fingerprints[i] >= target })
	return index < len(fingerprints) && fingerprints[index] == target, nil
}

func collectFingerprints(blockHash [32]byte, block *types.Block, undo []storage.BlockUndoEntry) []uint64 {
	if block == nil {
		return nil
	}
	unique := make(map[[32]byte]struct{})
	for i := range block.Txs {
		for _, output := range block.Txs[i].Base.Outputs {
			unique[output.PubKey] = struct{}{}
		}
	}
	for _, spent := range undo {
		unique[spent.Entry.PubKey] = struct{}{}
	}
	values := make([]uint64, 0, len(unique))
	for pubKey := range unique {
		values = append(values, fingerprintForPubKey(blockHash, pubKey))
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func fingerprintForPubKey(blockHash [32]byte, pubKey [32]byte) uint64 {
	var payload [64]byte
	copy(payload[:32], blockHash[:])
	copy(payload[32:], pubKey[:])
	hash := crypto.TaggedHash(itemTag, payload[:])
	return binary.BigEndian.Uint64(hash[:8])
}

func encodeFingerprints(values []uint64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 10+len(values)*10))
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(values)))
	buf.Write(scratch[:n])
	var prev uint64
	for _, value := range values {
		delta := value - prev
		n = binary.PutUvarint(scratch[:], delta)
		buf.Write(scratch[:n])
		prev = value
	}
	return buf.Bytes()
}

func decodeFingerprints(encoded []byte) ([]uint64, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	count, n := binary.Uvarint(encoded)
	if n <= 0 {
		return nil, errors.New("invalid compact filter count")
	}
	values := make([]uint64, 0, int(count))
	offset := n
	var prev uint64
	for len(values) < int(count) {
		if offset >= len(encoded) {
			return nil, errors.New("truncated compact filter")
		}
		delta, read := binary.Uvarint(encoded[offset:])
		if read <= 0 {
			return nil, errors.New("invalid compact filter delta")
		}
		value := prev + delta
		values = append(values, value)
		prev = value
		offset += read
	}
	if offset != len(encoded) {
		return nil, errors.New("unexpected trailing compact filter data")
	}
	return values, nil
}
