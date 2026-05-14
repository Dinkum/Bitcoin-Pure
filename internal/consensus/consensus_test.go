package consensus

import (
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"testing"
)

type bootstrapVectorProfile struct {
	GenesisAuthIDHex        string `json:"genesis_authid_hex"`
	GenesisHeaderHashHex    string `json:"genesis_header_hash_hex"`
	GenesisTxIDHex          string `json:"genesis_txid_hex"`
	UTXORootAfterGenesisHex string `json:"utxo_root_after_genesis_hex"`
}

type bootstrapVectors struct {
	VectorsVersion   int                    `json:"vectors_version"`
	SighashTags      map[string]string      `json:"sighash_tags"`
	UTXORootTag      string                 `json:"utxo_root_tag"`
	UTXOLeafTag      string                 `json:"utxo_leaf_tag"`
	UTXOBranchTag    string                 `json:"utxo_branch_tag"`
	EmptyUTXORootHex string                 `json:"empty_utxo_root_hex"`
	Mainnet          bootstrapVectorProfile `json:"mainnet"`
	Regtest          bootstrapVectorProfile `json:"regtest"`
}

type genesisFixtureForVectorTest struct {
	Profile                      string `json:"profile"`
	ExpectedHeaderHashHex        string `json:"expected_header_hash_hex"`
	ExpectedTxIDHex              string `json:"expected_txid_hex"`
	ExpectedAuthIDHex            string `json:"expected_authid_hex"`
	ExpectedUTXORootAfterGenesis string `json:"expected_utxo_root_after_genesis_hex"`
	BlockHex                     string `json:"block_hex"`
}

func consensusTestPubKey(seed byte) [32]byte {
	return crypto.XOnlyPubKeyFromSecret([32]byte{seed})
}

func coinbaseTxForConsensusTest(height uint64, outputs []types.TxOutput) types.Transaction {
	var extraNonce [types.CoinbaseExtraNonceLen]byte
	return types.Transaction{
		Base: types.TxBase{
			Version:            1,
			CoinbaseHeight:     &height,
			CoinbaseExtraNonce: &extraNonce,
			Outputs:            outputs,
		},
	}
}

func assertGenesisVector(t *testing.T, path string, vector bootstrapVectorProfile) {
	t.Helper()
	var fixture genesisFixtureForVectorTest
	readJSONForConsensusTest(t, path, &fixture)
	if fixture.ExpectedTxIDHex != vector.GenesisTxIDHex {
		t.Fatalf("%s fixture txid vector = %s, want %s", path, fixture.ExpectedTxIDHex, vector.GenesisTxIDHex)
	}
	if fixture.ExpectedAuthIDHex != vector.GenesisAuthIDHex {
		t.Fatalf("%s fixture authid vector = %s, want %s", path, fixture.ExpectedAuthIDHex, vector.GenesisAuthIDHex)
	}
	if fixture.ExpectedHeaderHashHex != vector.GenesisHeaderHashHex {
		t.Fatalf("%s fixture header vector = %s, want %s", path, fixture.ExpectedHeaderHashHex, vector.GenesisHeaderHashHex)
	}
	if fixture.ExpectedUTXORootAfterGenesis != vector.UTXORootAfterGenesisHex {
		t.Fatalf("%s fixture utxo root vector = %s, want %s", path, fixture.ExpectedUTXORootAfterGenesis, vector.UTXORootAfterGenesisHex)
	}
	block, err := types.DecodeBlockHex(fixture.BlockHex, types.DefaultCodecLimits())
	if err != nil {
		t.Fatalf("DecodeBlockHex(%s): %v", path, err)
	}
	if len(block.Txs) != 1 {
		t.Fatalf("%s tx count = %d, want 1", path, len(block.Txs))
	}
	txid := TxID(&block.Txs[0])
	authid := AuthID(&block.Txs[0])
	headerHash := HeaderHash(&block.Header)
	if got := hex.EncodeToString(txid[:]); got != vector.GenesisTxIDHex {
		t.Fatalf("%s txid = %s, want %s", path, got, vector.GenesisTxIDHex)
	}
	if got := hex.EncodeToString(authid[:]); got != vector.GenesisAuthIDHex {
		t.Fatalf("%s authid = %s, want %s", path, got, vector.GenesisAuthIDHex)
	}
	if got := hex.EncodeToString(headerHash[:]); got != vector.GenesisHeaderHashHex {
		t.Fatalf("%s header hash = %s, want %s", path, got, vector.GenesisHeaderHashHex)
	}
	if got := hex.EncodeToString(block.Header.UTXORoot[:]); got != vector.UTXORootAfterGenesisHex {
		t.Fatalf("%s utxo root = %s, want %s", path, got, vector.UTXORootAfterGenesisHex)
	}
}

func readJSONForConsensusTest(t *testing.T, path string, out any) {
	t.Helper()
	buf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(buf))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		t.Fatalf("Unmarshal(%s): %v", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("Unmarshal(%s) trailing data err = %v, want EOF", path, err)
	}
}

func signedSpendTxForConsensusTest(t *testing.T, spenderSeed byte, prevOut types.OutPoint, value uint64, recipientSeed byte, fee uint64) types.Transaction {
	t.Helper()
	if fee >= value {
		t.Fatalf("fee %d must be less than value %d", fee, value)
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs:  []types.TxInput{{PrevOut: prevOut}},
			Outputs: []types.TxOutput{{ValueAtoms: value - fee, PubKey: consensusTestPubKey(recipientSeed)}},
		},
	}
	msg, err := SighashWithParams(&tx, 0, []UtxoEntry{{ValueAtoms: value, PubKey: consensusTestPubKey(spenderSeed)}}, RegtestParams())
	if err != nil {
		t.Fatal(err)
	}
	_, sig := crypto.SignSchnorrForTest([32]byte{spenderSeed}, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	return tx
}

func specSighashForTest(tx *types.Transaction, inputIndex int, spentCoins []UtxoEntry, params ChainParams) ([32]byte, error) {
	if inputIndex < 0 || inputIndex >= len(tx.Base.Inputs) {
		return [32]byte{}, errors.New("input index out of range")
	}
	if len(spentCoins) != len(tx.Base.Inputs) {
		return [32]byte{}, errors.New("spent coins length mismatch")
	}

	prevouts := make([]byte, 0)
	prevouts = types.AppendCanonicalVarInt(prevouts, uint64(len(tx.Base.Inputs)))
	for _, input := range tx.Base.Inputs {
		prevouts = append(prevouts, input.PrevOut.TxID[:]...)
		prevouts = append(prevouts,
			byte(input.PrevOut.Vout),
			byte(input.PrevOut.Vout>>8),
			byte(input.PrevOut.Vout>>16),
			byte(input.PrevOut.Vout>>24),
		)
	}

	outputs := make([]byte, 0)
	outputs = types.AppendCanonicalVarInt(outputs, uint64(len(tx.Base.Outputs)))
	for _, output := range tx.Base.Outputs {
		outputs = appendValuePubKeyEncoding(outputs, output.ValueAtoms, output.PubKey)
	}

	spentCoinPayload := make([]byte, 0)
	spentCoinPayload = types.AppendCanonicalVarInt(spentCoinPayload, uint64(len(spentCoins)))
	for _, coin := range spentCoins {
		spentCoinPayload = appendValuePubKeyEncoding(spentCoinPayload, coin.ValueAtoms, coin.PubKey)
	}

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
	prevoutsHash := crypto.Sha256d(prevouts)
	outputsHash := crypto.Sha256d(outputs)
	spentCoinsHash := crypto.Sha256d(spentCoinPayload)
	preimage = append(preimage, prevoutsHash[:]...)
	preimage = append(preimage, outputsHash[:]...)
	preimage = append(preimage, spentCoinsHash[:]...)
	return crypto.TaggedHash(params.SighashTag(), preimage), nil
}

func taggedMerkleLeafForTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x00
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

func taggedMerkleNodeForTest(left, right [32]byte) [32]byte {
	var buf [65]byte
	buf[0] = 0x01
	copy(buf[1:33], left[:])
	copy(buf[33:], right[:])
	return crypto.Sha256d(buf[:])
}

func taggedMerkleSoloForTest(item [32]byte) [32]byte {
	var buf [33]byte
	buf[0] = 0x02
	copy(buf[1:], item[:])
	return crypto.Sha256d(buf[:])
}

// specMerkleRootForTest mirrors the spec's leaf/node/solo rules so these
// tests don't derive their expectations from the production MerkleRoot code.
func specMerkleRootForTest(items [][32]byte) [32]byte {
	if len(items) == 0 {
		panic("specMerkleRootForTest requires at least one item")
	}
	level := make([][32]byte, len(items))
	for i, item := range items {
		level[i] = taggedMerkleLeafForTest(item)
	}
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, taggedMerkleSoloForTest(level[i]))
				continue
			}
			next = append(next, taggedMerkleNodeForTest(level[i], level[i+1]))
		}
		level = next
	}
	return level[0]
}

func equalUtxoSets(left, right UtxoSet) bool {
	if len(left) != len(right) {
		return false
	}
	for outPoint, entry := range left {
		if other, ok := right[outPoint]; !ok || other != entry {
			return false
		}
	}
	return true
}

func compactTargetForTest(compact uint32) *big.Int {
	target, err := compactToTarget(compact)
	if err != nil {
		panic(err)
	}
	return target
}

func referenceAsertBits(t *testing.T, anchor AsertAnchor, prev PrevBlockContext, params ChainParams) uint32 {
	t.Helper()
	anchorTarget, err := compactToTarget(anchor.Bits)
	if err != nil {
		t.Fatal(err)
	}
	powLimit, err := compactToTarget(params.PowLimitBits)
	if err != nil {
		t.Fatal(err)
	}

	timeDelta := int64(prev.Header.Timestamp) - anchor.ParentTime
	heightDelta := int64(prev.Height - anchor.Height)
	exponentFP := ((timeDelta - params.TargetSpacingSecs*(heightDelta+1)) * (1 << 16)) / params.AsertHalfLifeSecs

	// Implement the required arithmetic shift explicitly so this helper stays
	// independent from the production fixed-point decomposition.
	numShifts := exponentFP / (1 << 16)
	if exponentFP < 0 && exponentFP%(1<<16) != 0 {
		numShifts--
	}
	frac := exponentFP - numShifts*(1<<16)

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
		bits, err := targetToCompact(big.NewInt(1))
		if err != nil {
			t.Fatal(err)
		}
		return bits
	}
	if nextTarget.Cmp(powLimit) > 0 {
		return params.PowLimitBits
	}
	bits, err := targetToCompact(nextTarget)
	if err != nil {
		t.Fatal(err)
	}
	return bits
}

func mineHeaderForTest(header types.BlockHeader) types.BlockHeader {
	target := compactTargetForTest(header.NBits)
	for nonce := uint64(0); ; nonce++ {
		header.Nonce = nonce
		hash := HeaderHash(&header)
		if new(big.Int).SetBytes(hash[:]).Cmp(target) <= 0 {
			return header
		}
		if nonce == math.MaxUint64 {
			break
		}
	}
	panic("unable to mine header")
}
