package node

import (
	"encoding/hex"
	"fmt"

	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/types"
	"bitcoin-pure/internal/utreexo"
)

type AnchoredUTXOProof struct {
	Height     uint64
	HeaderHash [32]byte
	UTXORoot   [32]byte
	Proof      utreexo.OutPointProof
}

type UTXOProofVerification struct {
	Valid              bool
	AnchorMatchesLocal bool
}

type RPCAnchoredUTXOProof struct {
	Height     uint64           `json:"height"`
	HeaderHash string           `json:"header_hash"`
	UTXORoot   string           `json:"utxo_root"`
	Proof      RPCOutPointProof `json:"proof"`
}

type RPCOutPointProof struct {
	TxID       string         `json:"txid"`
	Vout       uint32         `json:"vout"`
	Exists     bool           `json:"exists"`
	ValueAtoms uint64         `json:"value_atoms,omitempty"`
	PubKey     string         `json:"pubkey,omitempty"`
	Steps      []RPCProofStep `json:"steps"`
}

type RPCProofStep struct {
	HasSibling  bool   `json:"has_sibling"`
	SiblingHash string `json:"sibling_hash,omitempty"`
}

func (s *Service) UTXOProof(outPoint types.OutPoint) (AnchoredUTXOProof, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	if !ok {
		return AnchoredUTXOProof{}, ErrNoTip
	}
	if view.UTXOAcc == nil {
		return AnchoredUTXOProof{}, fmt.Errorf("missing committed utxo accumulator")
	}
	proof, err := view.UTXOAcc.Prove(outPoint)
	if err != nil {
		return AnchoredUTXOProof{}, err
	}
	return AnchoredUTXOProof{
		Height:     view.Height,
		HeaderHash: view.TipHash,
		UTXORoot:   view.UTXORoot,
		Proof:      proof,
	}, nil
}

// VerifyAnchoredUTXOProof performs the cryptographic proof check against the
// proof's published `utxo_root`. Local chain anchoring is handled separately.
func VerifyAnchoredUTXOProof(proof AnchoredUTXOProof) bool {
	return utreexo.VerifyProof(proof.UTXORoot, proof.Proof)
}

func (s *Service) VerifyAnchoredUTXOProof(proof AnchoredUTXOProof) (UTXOProofVerification, error) {
	result := UTXOProofVerification{
		Valid: VerifyAnchoredUTXOProof(proof),
	}
	entry, err := s.blockIndexByHeight(proof.Height)
	if err != nil {
		return result, nil
	}
	headerHash := consensus.HeaderHash(&entry.Header)
	result.AnchorMatchesLocal = headerHash == proof.HeaderHash && entry.Header.UTXORoot == proof.UTXORoot
	return result, nil
}

func EncodeRPCUTXOProof(proof AnchoredUTXOProof) RPCAnchoredUTXOProof {
	steps := make([]RPCProofStep, 0, len(proof.Proof.Steps))
	for _, step := range proof.Proof.Steps {
		item := RPCProofStep{HasSibling: step.HasSibling}
		if step.HasSibling {
			item.SiblingHash = hex.EncodeToString(step.SiblingHash[:])
		}
		steps = append(steps, item)
	}
	out := RPCAnchoredUTXOProof{
		Height:     proof.Height,
		HeaderHash: hex.EncodeToString(proof.HeaderHash[:]),
		UTXORoot:   hex.EncodeToString(proof.UTXORoot[:]),
		Proof: RPCOutPointProof{
			TxID:   hex.EncodeToString(proof.Proof.OutPoint.TxID[:]),
			Vout:   proof.Proof.OutPoint.Vout,
			Exists: proof.Proof.Exists,
			Steps:  steps,
		},
	}
	if proof.Proof.Exists {
		out.Proof.ValueAtoms = proof.Proof.ValueAtoms
		out.Proof.PubKey = hex.EncodeToString(proof.Proof.PubKey[:])
	}
	return out
}

func DecodeRPCUTXOProof(raw RPCAnchoredUTXOProof) (AnchoredUTXOProof, error) {
	headerHash, err := decodeProofHex32(raw.HeaderHash, "header_hash")
	if err != nil {
		return AnchoredUTXOProof{}, err
	}
	utxoRoot, err := decodeProofHex32(raw.UTXORoot, "utxo_root")
	if err != nil {
		return AnchoredUTXOProof{}, err
	}
	txid, err := decodeProofHex32(raw.Proof.TxID, "proof.txid")
	if err != nil {
		return AnchoredUTXOProof{}, err
	}
	steps := make([]utreexo.ProofStep, 0, len(raw.Proof.Steps))
	for i, step := range raw.Proof.Steps {
		item := utreexo.ProofStep{HasSibling: step.HasSibling}
		if step.HasSibling {
			hash, err := decodeProofHex32(step.SiblingHash, fmt.Sprintf("proof.steps[%d].sibling_hash", i))
			if err != nil {
				return AnchoredUTXOProof{}, err
			}
			item.SiblingHash = hash
		}
		steps = append(steps, item)
	}
	out := AnchoredUTXOProof{
		Height:     raw.Height,
		HeaderHash: headerHash,
		UTXORoot:   utxoRoot,
		Proof: utreexo.OutPointProof{
			OutPoint: types.OutPoint{
				TxID: txid,
				Vout: raw.Proof.Vout,
			},
			Exists: raw.Proof.Exists,
			Steps:  steps,
		},
	}
	if raw.Proof.Exists {
		pubKey, err := decodeProofHex32(raw.Proof.PubKey, "proof.pubkey")
		if err != nil {
			return AnchoredUTXOProof{}, err
		}
		out.Proof.ValueAtoms = raw.Proof.ValueAtoms
		out.Proof.PubKey = pubKey
	}
	return out, nil
}

func decodeProofHex32(raw, field string) ([32]byte, error) {
	var out [32]byte
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return out, fmt.Errorf("%s: %w", field, err)
	}
	if len(buf) != len(out) {
		return out, fmt.Errorf("%s: expected %d-byte hex", field, len(out))
	}
	copy(out[:], buf)
	return out, nil
}
