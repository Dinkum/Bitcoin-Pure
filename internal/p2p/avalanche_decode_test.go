package p2p

import (
	"encoding/binary"
	"strings"
	"testing"

	"bitcoin-pure/internal/types"
)

func TestDecodeAvaVoteRejectsNonCanonicalFlag(t *testing.T) {
	payload := make([]byte, 8+4+1+32)
	binary.LittleEndian.PutUint32(payload[8:12], 1)
	payload[12] = 2
	_, err := decodeMessage(CmdAvaVote, payload, types.DefaultCodecLimits())
	if err == nil || !strings.Contains(err.Error(), "avalanche vote flag") {
		t.Fatalf("decodeMessage err = %v, want avalanche vote flag error", err)
	}
}

func TestDecodeAvaVoteRejectsNoOpinionWithTxID(t *testing.T) {
	payload := make([]byte, 8+4+1+32)
	binary.LittleEndian.PutUint32(payload[8:12], 1)
	payload[13] = 1
	_, err := decodeMessage(CmdAvaVote, payload, types.DefaultCodecLimits())
	if err == nil || !strings.Contains(err.Error(), "zero txid") {
		t.Fatalf("decodeMessage err = %v, want zero txid error", err)
	}
}
