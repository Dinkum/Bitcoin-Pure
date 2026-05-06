package wallet

import (
	"errors"
	"fmt"
	"strings"

	"bitcoin-pure/internal/types"
)

const (
	AddressPrefix      = "bpu"
	cashAddrSeparator  = ':'
	cashAddrTypeP2PK   = 0
	cashAddrTypePQLock = 1
	cashAddrChecksumSz = 8
)

const cashAddrCharset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

var (
	errInvalidCashAddr    = errors.New("invalid cashaddr")
	cashAddrDecodeMap     = buildCashAddrDecodeMap()
	cashAddrSizeBitsByLen = map[int]byte{20: 0, 24: 1, 28: 2, 32: 3, 40: 4, 48: 5, 56: 6, 64: 7}
	cashAddrLenBySizeBits = map[byte]int{0: 20, 1: 24, 2: 28, 3: 32, 4: 40, 5: 48, 6: 56, 7: 64}
	cashAddrPolymodGen    = [5]uint64{0x98f2bc8e61, 0x79b76d99e2, 0xf33e5fb3c4, 0xae2eabe2a8, 0x1e4f43e470}
)

type WatchItem struct {
	Type      uint64
	Payload32 [32]byte
}

func EncodeAddress(pubKey [32]byte) string {
	return EncodeTypedAddress(WatchItem{
		Type:      types.OutputXOnlyP2PK,
		Payload32: pubKey,
	})
}

func EncodeTypedAddress(item WatchItem) string {
	addrType, err := cashAddrTypeForOutputType(item.Type)
	if err != nil {
		panic(fmt.Sprintf("encode wallet address: %v", err))
	}
	encoded, err := encodeCashAddress(AddressPrefix, addrType, item.Payload32[:])
	if err != nil {
		panic(fmt.Sprintf("encode wallet address: %v", err))
	}
	return encoded
}

func ParseAddress(raw string) ([32]byte, error) {
	var pubKey [32]byte
	item, err := ParseWatchAddress(raw)
	if err != nil {
		return pubKey, err
	}
	if item.Type != types.OutputXOnlyP2PK {
		return pubKey, ErrInvalidAddress
	}
	return item.Payload32, nil
}

func ParseWatchAddress(raw string) (WatchItem, error) {
	var item WatchItem
	_, addrType, payload, err := decodeCashAddress(raw, AddressPrefix)
	if err != nil {
		return item, ErrInvalidAddress
	}
	outputType, err := outputTypeForCashAddrType(addrType)
	if err != nil || len(payload) != len(item.Payload32) {
		return item, ErrInvalidAddress
	}
	item.Type = outputType
	copy(item.Payload32[:], payload)
	return item, nil
}

func cashAddrTypeForOutputType(outputType uint64) (byte, error) {
	switch outputType {
	case types.OutputXOnlyP2PK:
		return cashAddrTypeP2PK, nil
	case types.OutputPQLock32:
		return cashAddrTypePQLock, nil
	default:
		return 0, errInvalidCashAddr
	}
}

func outputTypeForCashAddrType(addrType byte) (uint64, error) {
	switch addrType {
	case cashAddrTypeP2PK:
		return types.OutputXOnlyP2PK, nil
	case cashAddrTypePQLock:
		return types.OutputPQLock32, nil
	default:
		return 0, errInvalidCashAddr
	}
}

func encodeCashAddress(prefix string, addrType byte, payload []byte) (string, error) {
	version, err := cashAddrVersionByte(addrType, len(payload))
	if err != nil {
		return "", err
	}
	words, err := convertBits(append([]byte{version}, payload...), 8, 5, true)
	if err != nil {
		return "", err
	}
	checksum := cashAddrChecksum(prefix, words)
	var out strings.Builder
	out.Grow(len(prefix) + 1 + len(words) + len(checksum))
	out.WriteString(prefix)
	out.WriteByte(cashAddrSeparator)
	for _, value := range words {
		out.WriteByte(cashAddrCharset[value])
	}
	for _, value := range checksum {
		out.WriteByte(cashAddrCharset[value])
	}
	return out.String(), nil
}

func decodeCashAddress(raw string, defaultPrefix string) (string, byte, []byte, error) {
	if hasMixedAddressCase(raw) {
		return "", 0, nil, errInvalidCashAddr
	}
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return "", 0, nil, errInvalidCashAddr
	}
	prefix := defaultPrefix
	payloadPart := raw
	if idx := strings.LastIndexByte(raw, cashAddrSeparator); idx >= 0 {
		prefix = raw[:idx]
		payloadPart = raw[idx+1:]
	}
	if prefix == "" || payloadPart == "" {
		return "", 0, nil, errInvalidCashAddr
	}
	words := make([]byte, 0, len(payloadPart))
	for _, ch := range payloadPart {
		if ch > 127 {
			return "", 0, nil, errInvalidCashAddr
		}
		value := cashAddrDecodeMap[ch]
		if value == 0xff {
			return "", 0, nil, errInvalidCashAddr
		}
		words = append(words, value)
	}
	if len(words) <= cashAddrChecksumSz {
		return "", 0, nil, errInvalidCashAddr
	}
	if !cashAddrVerifyChecksum(prefix, words) {
		return "", 0, nil, errInvalidCashAddr
	}
	payloadWords := words[:len(words)-cashAddrChecksumSz]
	decoded, err := convertBits(payloadWords, 5, 8, false)
	if err != nil || len(decoded) < 1 {
		return "", 0, nil, errInvalidCashAddr
	}
	version := decoded[0]
	if version&0x80 != 0 {
		return "", 0, nil, errInvalidCashAddr
	}
	addrType := version >> 3
	expectedLen, ok := cashAddrLenBySizeBits[version&0x07]
	if !ok {
		return "", 0, nil, errInvalidCashAddr
	}
	payload := decoded[1:]
	if len(payload) != expectedLen {
		return "", 0, nil, errInvalidCashAddr
	}
	return prefix, addrType, payload, nil
}

func cashAddrVersionByte(addrType byte, payloadLen int) (byte, error) {
	sizeBits, ok := cashAddrSizeBitsByLen[payloadLen]
	if !ok || addrType > 0x0f {
		return 0, errInvalidCashAddr
	}
	return (addrType << 3) | sizeBits, nil
}

func cashAddrChecksum(prefix string, payload []byte) []byte {
	values := append(cashAddrPrefixExpand(prefix), payload...)
	values = append(values, make([]byte, cashAddrChecksumSz)...)
	mod := cashAddrPolymod(values)
	checksum := make([]byte, cashAddrChecksumSz)
	for i := range checksum {
		shift := uint(5 * (cashAddrChecksumSz - 1 - i))
		checksum[i] = byte((mod >> shift) & 0x1f)
	}
	return checksum
}

func cashAddrVerifyChecksum(prefix string, payload []byte) bool {
	return cashAddrPolymod(append(cashAddrPrefixExpand(prefix), payload...)) == 0
}

func cashAddrPrefixExpand(prefix string) []byte {
	expanded := make([]byte, 0, len(prefix)+1)
	for i := range prefix {
		expanded = append(expanded, prefix[i]&0x1f)
	}
	expanded = append(expanded, 0)
	return expanded
}

func cashAddrPolymod(values []byte) uint64 {
	c := uint64(1)
	for _, value := range values {
		c0 := c >> 35
		c = ((c & 0x07ffffffff) << 5) ^ uint64(value)
		for i, gen := range cashAddrPolymodGen {
			if c0&(1<<i) != 0 {
				c ^= gen
			}
		}
	}
	return c ^ 1
}

func convertBits(data []byte, fromBits, toBits uint, pad bool) ([]byte, error) {
	if fromBits == 0 || toBits == 0 || fromBits > 8 || toBits > 8 {
		return nil, errInvalidCashAddr
	}
	acc := uint(0)
	bits := uint(0)
	maxv := uint((1 << toBits) - 1)
	maxAcc := uint((1 << (fromBits + toBits - 1)) - 1)
	out := make([]byte, 0, (len(data)*int(fromBits)+int(toBits)-1)/int(toBits))
	for _, value := range data {
		if uint(value)>>fromBits != 0 {
			return nil, errInvalidCashAddr
		}
		acc = ((acc << fromBits) | uint(value)) & maxAcc
		bits += fromBits
		for bits >= toBits {
			bits -= toBits
			out = append(out, byte((acc>>bits)&maxv))
		}
	}
	if pad {
		if bits > 0 {
			out = append(out, byte((acc<<(toBits-bits))&maxv))
		}
		return out, nil
	}
	if bits >= fromBits || ((acc<<(toBits-bits))&maxv) != 0 {
		return nil, errInvalidCashAddr
	}
	return out, nil
}

func hasMixedAddressCase(raw string) bool {
	hasLower := false
	hasUpper := false
	for _, ch := range raw {
		switch {
		case ch >= 'a' && ch <= 'z':
			hasLower = true
		case ch >= 'A' && ch <= 'Z':
			hasUpper = true
		}
		if hasLower && hasUpper {
			return true
		}
	}
	return false
}

func buildCashAddrDecodeMap() [128]byte {
	var out [128]byte
	for i := range out {
		out[i] = 0xff
	}
	for i := range cashAddrCharset {
		out[cashAddrCharset[i]] = byte(i)
	}
	return out
}
