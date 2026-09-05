package consensus

import (
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

// outputKeyCache remembers only successful X-only key parsing within one block.
// Fixed storage prevents hostile output diversity from growing the cache. A
// collision merely evicts an entry; hits always compare the entire public key.
type outputKeyCache struct {
	keys [256][32]byte
	used [256]bool
}

func validateOutputPayloadWithKeyCache(output types.TxOutput, cache *outputKeyCache) error {
	if output.ValueAtoms == 0 {
		return ErrZeroOutputValue
	}
	switch output.Type {
	case types.OutputXOnlyP2PK:
		if output.Payload32 != ([32]byte{}) && output.PubKey != ([32]byte{}) && output.Payload32 != output.PubKey {
			return ErrOutputPayloadMismatch
		}
		payload32 := canonicalOutputPayload32(output)
		// Amount, type, and alias checks above apply even to cached keys.
		slot := payload32[0]
		if cache != nil && cache.used[slot] && cache.keys[slot] == payload32 {
			return nil
		}
		if !crypto.IsValidXOnlyPubKey(&payload32) {
			return ErrInvalidOutputPubKey
		}
		if cache != nil {
			cache.keys[slot] = payload32
			cache.used[slot] = true
		}
		return nil
	case types.OutputPQLock32:
		return nil
	default:
		return ErrUnsupportedOutputType
	}
}
