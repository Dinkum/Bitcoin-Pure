package compactfilter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"bitcoin-pure/internal/crypto"
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

type WatchItem struct {
	Type      uint64
	Payload32 [32]byte
}

func Type() string { return filterType }

// Build deterministically encodes the block's created typed watch items and the
// caller-supplied consumed prevout watch items into a compact probabilistic
// filter. The filter is non-consensus and keyed by block hash.
func Build(blockHash [32]byte, block *types.Block, spent []WatchItem) Filter {
	fingerprints := collectFingerprints(blockHash, block, spent)
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
	return MatchWatchItem(blockHash, encoded, WatchItem{
		Type:      types.OutputXOnlyP2PK,
		Payload32: pubKey,
	})
}

func MatchWatchItem(blockHash [32]byte, encoded []byte, item WatchItem) (bool, error) {
	fingerprints, err := decodeFingerprints(encoded)
	if err != nil {
		return false, err
	}
	return matchesFingerprint(fingerprints, fingerprintForWatchItem(blockHash, item)), nil
}

// MatchWatchItems validates and decodes a filter once for all requested watch
// items. Results preserve request order, including duplicate items. Even an
// empty request validates the complete filter before returning an empty result.
func MatchWatchItems(blockHash [32]byte, encoded []byte, items []WatchItem) ([]bool, error) {
	fingerprints, err := decodeFingerprints(encoded)
	if err != nil {
		return nil, err
	}
	matches := make([]bool, len(items))
	for i, item := range items {
		matches[i] = matchesFingerprint(fingerprints, fingerprintForWatchItem(blockHash, item))
	}
	return matches, nil
}

func matchesFingerprint(fingerprints []uint64, target uint64) bool {
	index := sort.Search(len(fingerprints), func(i int) bool { return fingerprints[i] >= target })
	return index < len(fingerprints) && fingerprints[index] == target
}

func WatchItemForOutput(output types.TxOutput) WatchItem {
	return WatchItem{Type: output.Type, Payload32: output.CanonicalPayload32()}
}

func collectFingerprints(blockHash [32]byte, block *types.Block, spent []WatchItem) []uint64 {
	if block == nil {
		return nil
	}
	unique := make(map[WatchItem]struct{})
	for i := range block.Txs {
		for _, output := range block.Txs[i].Base.Outputs {
			unique[WatchItemForOutput(output)] = struct{}{}
		}
	}
	for _, item := range spent {
		unique[item] = struct{}{}
	}
	values := make([]uint64, 0, len(unique))
	for item := range unique {
		values = append(values, fingerprintForWatchItem(blockHash, item))
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func fingerprintForWatchItem(blockHash [32]byte, item WatchItem) uint64 {
	payload := make([]byte, 0, 73)
	payload = append(payload, blockHash[:]...)
	payload = appendCanonicalVarInt(payload, item.Type)
	payload = append(payload, item.Payload32[:]...)
	hash := crypto.TaggedHash(itemTag, payload)
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
		return nil, errors.New("missing compact filter count")
	}
	count, n, ok := readCanonicalUvarint(encoded)
	if !ok {
		return nil, errors.New("invalid compact filter count")
	}
	if count > uint64(len(encoded)-n) {
		return nil, errors.New("compact filter count exceeds encoded deltas")
	}
	values := make([]uint64, 0, int(count))
	offset := n
	var prev uint64
	for len(values) < int(count) {
		if offset >= len(encoded) {
			return nil, errors.New("truncated compact filter")
		}
		delta, read, ok := readCanonicalUvarint(encoded[offset:])
		if !ok {
			return nil, errors.New("invalid compact filter delta")
		}
		if delta > ^uint64(0)-prev {
			return nil, errors.New("compact filter delta overflows fingerprint")
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

func readCanonicalUvarint(buf []byte) (uint64, int, bool) {
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, 0, false
	}
	var scratch [binary.MaxVarintLen64]byte
	if canonicalLen := binary.PutUvarint(scratch[:], value); canonicalLen != n {
		return 0, 0, false
	} else if !bytes.Equal(buf[:n], scratch[:canonicalLen]) {
		return 0, 0, false
	}
	return value, n, true
}

func appendCanonicalVarInt(dst []byte, v uint64) []byte {
	return types.AppendCanonicalVarInt(dst, v)
}
