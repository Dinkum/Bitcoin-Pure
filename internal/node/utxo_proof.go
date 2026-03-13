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

type AnchoredUTXOProofBatch struct {
	Height     uint64
	HeaderHash [32]byte
	UTXORoot   [32]byte
	Proofs     []utreexo.OutPointProof
}

type CompactStatePackage struct {
	Height          uint64
	HeaderHash      [32]byte
	UTXORoot        [32]byte
	LocalityOrdered bool
	Proofs          []utreexo.OutPointProof
}

type UTXOProofVerification struct {
	Valid              bool
	AnchorMatchesLocal bool
}

type UTXOProofBatchVerification struct {
	AllValid           bool
	ValidCount         int
	AnchorMatchesLocal bool
}

type RPCAnchoredUTXOProof struct {
	Height     uint64           `json:"height"`
	HeaderHash string           `json:"header_hash"`
	UTXORoot   string           `json:"utxo_root"`
	Proof      RPCOutPointProof `json:"proof"`
}

type RPCAnchoredUTXOProofBatch struct {
	Height     uint64             `json:"height"`
	HeaderHash string             `json:"header_hash"`
	UTXORoot   string             `json:"utxo_root"`
	Proofs     []RPCOutPointProof `json:"proofs"`
}

type RPCCompactStatePackage struct {
	Height          uint64             `json:"height"`
	HeaderHash      string             `json:"header_hash"`
	UTXORoot        string             `json:"utxo_root"`
	LocalityOrdered bool               `json:"locality_ordered"`
	Proofs          []RPCOutPointProof `json:"proofs"`
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

func (s *Service) UTXOProofBatch(outPoints []types.OutPoint) (AnchoredUTXOProofBatch, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	view, ok := s.chainState.CommittedView()
	if !ok {
		return AnchoredUTXOProofBatch{}, ErrNoTip
	}
	if view.UTXOAcc == nil {
		return AnchoredUTXOProofBatch{}, fmt.Errorf("missing committed utxo accumulator")
	}
	proofs := make([]utreexo.OutPointProof, 0, len(outPoints))
	for _, outPoint := range outPoints {
		proof, err := view.UTXOAcc.Prove(outPoint)
		if err != nil {
			return AnchoredUTXOProofBatch{}, err
		}
		proofs = append(proofs, proof)
	}
	return AnchoredUTXOProofBatch{
		Height:     view.Height,
		HeaderHash: view.TipHash,
		UTXORoot:   view.UTXORoot,
		Proofs:     proofs,
	}, nil
}

// CompactStatePackageForOutPoints serves a shared-anchor proof package that is
// reordered by locality sequence to improve downstream packing/compression for
// proof-serving nodes without affecting proof validity.
func (s *Service) CompactStatePackageForOutPoints(outPoints []types.OutPoint) (CompactStatePackage, error) {
	ordered, err := s.PlanUTXOProofBatch(outPoints)
	if err != nil {
		return CompactStatePackage{}, err
	}
	batch, err := s.UTXOProofBatch(ordered)
	if err != nil {
		return CompactStatePackage{}, err
	}
	return CompactStatePackage{
		Height:          batch.Height,
		HeaderHash:      batch.HeaderHash,
		UTXORoot:        batch.UTXORoot,
		LocalityOrdered: true,
		Proofs:          batch.Proofs,
	}, nil
}

// VerifyAnchoredUTXOProof performs the cryptographic proof check against the
// proof's published `utxo_root`. Local chain anchoring is handled separately.
func VerifyAnchoredUTXOProof(proof AnchoredUTXOProof) bool {
	return utreexo.VerifyProof(proof.UTXORoot, proof.Proof)
}

func VerifyAnchoredUTXOProofBatch(batch AnchoredUTXOProofBatch) UTXOProofBatchVerification {
	result := UTXOProofBatchVerification{
		AllValid: true,
	}
	for _, proof := range batch.Proofs {
		if !utreexo.VerifyProof(batch.UTXORoot, proof) {
			result.AllValid = false
			continue
		}
		result.ValidCount++
	}
	return result
}

func VerifyCompactStatePackage(pkg CompactStatePackage) UTXOProofBatchVerification {
	return VerifyAnchoredUTXOProofBatch(AnchoredUTXOProofBatch{
		Height:     pkg.Height,
		HeaderHash: pkg.HeaderHash,
		UTXORoot:   pkg.UTXORoot,
		Proofs:     pkg.Proofs,
	})
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

func (s *Service) VerifyAnchoredUTXOProofBatch(batch AnchoredUTXOProofBatch) (UTXOProofBatchVerification, error) {
	result := VerifyAnchoredUTXOProofBatch(batch)
	entry, err := s.blockIndexByHeight(batch.Height)
	if err != nil {
		return result, nil
	}
	headerHash := consensus.HeaderHash(&entry.Header)
	result.AnchorMatchesLocal = headerHash == batch.HeaderHash && entry.Header.UTXORoot == batch.UTXORoot
	return result, nil
}

func (s *Service) VerifyCompactStatePackage(pkg CompactStatePackage) (UTXOProofBatchVerification, error) {
	result := VerifyCompactStatePackage(pkg)
	entry, err := s.blockIndexByHeight(pkg.Height)
	if err != nil {
		return result, nil
	}
	headerHash := consensus.HeaderHash(&entry.Header)
	result.AnchorMatchesLocal = headerHash == pkg.HeaderHash && entry.Header.UTXORoot == pkg.UTXORoot
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

func EncodeRPCUTXOProofBatch(batch AnchoredUTXOProofBatch) RPCAnchoredUTXOProofBatch {
	proofs := make([]RPCOutPointProof, 0, len(batch.Proofs))
	for _, proof := range batch.Proofs {
		proofs = append(proofs, encodeRPCOutPointProof(proof))
	}
	return RPCAnchoredUTXOProofBatch{
		Height:     batch.Height,
		HeaderHash: hex.EncodeToString(batch.HeaderHash[:]),
		UTXORoot:   hex.EncodeToString(batch.UTXORoot[:]),
		Proofs:     proofs,
	}
}

func EncodeRPCCompactStatePackage(pkg CompactStatePackage) RPCCompactStatePackage {
	proofs := make([]RPCOutPointProof, 0, len(pkg.Proofs))
	for _, proof := range pkg.Proofs {
		proofs = append(proofs, encodeRPCOutPointProof(proof))
	}
	return RPCCompactStatePackage{
		Height:          pkg.Height,
		HeaderHash:      hex.EncodeToString(pkg.HeaderHash[:]),
		UTXORoot:        hex.EncodeToString(pkg.UTXORoot[:]),
		LocalityOrdered: pkg.LocalityOrdered,
		Proofs:          proofs,
	}
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

func DecodeRPCUTXOProofBatch(raw RPCAnchoredUTXOProofBatch) (AnchoredUTXOProofBatch, error) {
	headerHash, err := decodeProofHex32(raw.HeaderHash, "header_hash")
	if err != nil {
		return AnchoredUTXOProofBatch{}, err
	}
	utxoRoot, err := decodeProofHex32(raw.UTXORoot, "utxo_root")
	if err != nil {
		return AnchoredUTXOProofBatch{}, err
	}
	proofs := make([]utreexo.OutPointProof, 0, len(raw.Proofs))
	for i, proof := range raw.Proofs {
		decoded, err := decodeRPCOutPointProof(proof, fmt.Sprintf("proofs[%d]", i))
		if err != nil {
			return AnchoredUTXOProofBatch{}, err
		}
		proofs = append(proofs, decoded)
	}
	return AnchoredUTXOProofBatch{
		Height:     raw.Height,
		HeaderHash: headerHash,
		UTXORoot:   utxoRoot,
		Proofs:     proofs,
	}, nil
}

func DecodeRPCCompactStatePackage(raw RPCCompactStatePackage) (CompactStatePackage, error) {
	headerHash, err := decodeProofHex32(raw.HeaderHash, "header_hash")
	if err != nil {
		return CompactStatePackage{}, err
	}
	utxoRoot, err := decodeProofHex32(raw.UTXORoot, "utxo_root")
	if err != nil {
		return CompactStatePackage{}, err
	}
	proofs := make([]utreexo.OutPointProof, 0, len(raw.Proofs))
	for i, proof := range raw.Proofs {
		decoded, err := decodeRPCOutPointProof(proof, fmt.Sprintf("proofs[%d]", i))
		if err != nil {
			return CompactStatePackage{}, err
		}
		proofs = append(proofs, decoded)
	}
	return CompactStatePackage{
		Height:          raw.Height,
		HeaderHash:      headerHash,
		UTXORoot:        utxoRoot,
		LocalityOrdered: raw.LocalityOrdered,
		Proofs:          proofs,
	}, nil
}

func encodeRPCOutPointProof(proof utreexo.OutPointProof) RPCOutPointProof {
	steps := make([]RPCProofStep, 0, len(proof.Steps))
	for _, step := range proof.Steps {
		item := RPCProofStep{HasSibling: step.HasSibling}
		if step.HasSibling {
			item.SiblingHash = hex.EncodeToString(step.SiblingHash[:])
		}
		steps = append(steps, item)
	}
	out := RPCOutPointProof{
		TxID:   hex.EncodeToString(proof.OutPoint.TxID[:]),
		Vout:   proof.OutPoint.Vout,
		Exists: proof.Exists,
		Steps:  steps,
	}
	if proof.Exists {
		out.ValueAtoms = proof.ValueAtoms
		out.PubKey = hex.EncodeToString(proof.PubKey[:])
	}
	return out
}

func decodeRPCOutPointProof(raw RPCOutPointProof, field string) (utreexo.OutPointProof, error) {
	txid, err := decodeProofHex32(raw.TxID, field+".txid")
	if err != nil {
		return utreexo.OutPointProof{}, err
	}
	steps := make([]utreexo.ProofStep, 0, len(raw.Steps))
	for i, step := range raw.Steps {
		item := utreexo.ProofStep{HasSibling: step.HasSibling}
		if step.HasSibling {
			hash, err := decodeProofHex32(step.SiblingHash, fmt.Sprintf("%s.steps[%d].sibling_hash", field, i))
			if err != nil {
				return utreexo.OutPointProof{}, err
			}
			item.SiblingHash = hash
		}
		steps = append(steps, item)
	}
	proof := utreexo.OutPointProof{
		OutPoint: types.OutPoint{
			TxID: txid,
			Vout: raw.Vout,
		},
		Exists: raw.Exists,
		Steps:  steps,
	}
	if raw.Exists {
		pubKey, err := decodeProofHex32(raw.PubKey, field+".pubkey")
		if err != nil {
			return utreexo.OutPointProof{}, err
		}
		proof.ValueAtoms = raw.ValueAtoms
		proof.PubKey = pubKey
	}
	return proof, nil
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
