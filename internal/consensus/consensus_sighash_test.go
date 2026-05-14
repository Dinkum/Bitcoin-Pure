package consensus

import (
	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
	"testing"
)

func TestValidateSignedSpend(t *testing.T) {
	seed := [32]byte{7}
	msgPub, _ := crypto.SignSchnorrForTest(seed, &[32]byte{})
	utxos := UtxoSet{
		types.OutPoint{TxID: [32]byte{2}, Vout: 0}: {
			ValueAtoms: 50,
			PubKey:     msgPub,
		},
	}
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{{
				PrevOut: types.OutPoint{TxID: [32]byte{2}, Vout: 0},
			}},
			Outputs: []types.TxOutput{{
				ValueAtoms: 40,
				PubKey:     consensusTestPubKey(8),
			}},
		},
	}
	msg, err := SighashWithParams(&tx, 0, []UtxoEntry{{ValueAtoms: 50, PubKey: msgPub}}, MainnetParams())
	if err != nil {
		t.Fatal(err)
	}
	_, sig := crypto.SignSchnorrForTest(seed, &msg)
	tx.Auth = types.TxAuth{Entries: []types.TxAuthEntry{{Signature: sig}}}
	summary, err := ValidateTx(&tx, utxos, DefaultConsensusRules())
	if err != nil {
		t.Fatalf("validate tx: %v", err)
	}
	if summary.Fee != 10 {
		t.Fatalf("unexpected fee: %d", summary.Fee)
	}
}

func TestSighashMatchesSpecForMultiInputSpend(t *testing.T) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{
				{PrevOut: types.OutPoint{TxID: [32]byte{1}, Vout: 0}},
				{PrevOut: types.OutPoint{TxID: [32]byte{2}, Vout: 1}},
			},
			Outputs: []types.TxOutput{
				{ValueAtoms: 60, PubKey: consensusTestPubKey(8)},
				{ValueAtoms: 15, PubKey: consensusTestPubKey(9)},
			},
		},
	}
	spentCoins := []UtxoEntry{
		{ValueAtoms: 50, PubKey: consensusTestPubKey(1)},
		{ValueAtoms: 30, PubKey: consensusTestPubKey(2)},
	}

	batched, err := SighashesWithParams(&tx, spentCoins, MainnetParams())
	if err != nil {
		t.Fatalf("batched sighashes: %v", err)
	}
	for i := range tx.Base.Inputs {
		got, err := SighashWithParams(&tx, i, spentCoins, MainnetParams())
		if err != nil {
			t.Fatalf("sighash input %d: %v", i, err)
		}
		if batched[i] != got {
			t.Fatalf("batched sighash input %d = %x, want %x", i, batched[i], got)
		}
		want, err := specSighashForTest(&tx, i, spentCoins, MainnetParams())
		if err != nil {
			t.Fatalf("spec sighash input %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("sighash input %d = %x, want %x", i, got, want)
		}
	}
}

func TestSighashCommitsToSpentCoinPubKey(t *testing.T) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{{
				PrevOut: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
			}},
			Outputs: []types.TxOutput{{
				ValueAtoms: 49,
				PubKey:     consensusTestPubKey(9),
			}},
		},
	}
	left, err := SighashWithParams(&tx, 0, []UtxoEntry{{ValueAtoms: 50, PubKey: consensusTestPubKey(1)}}, MainnetParams())
	if err != nil {
		t.Fatal(err)
	}
	right, err := SighashWithParams(&tx, 0, []UtxoEntry{{ValueAtoms: 50, PubKey: consensusTestPubKey(2)}}, MainnetParams())
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("expected sighash to change when spent coin pubkey changes")
	}
}

func TestSighashChangesAcrossProfiles(t *testing.T) {
	tx := types.Transaction{
		Base: types.TxBase{
			Version: 1,
			Inputs: []types.TxInput{{
				PrevOut: types.OutPoint{TxID: [32]byte{1}, Vout: 0},
			}},
			Outputs: []types.TxOutput{{
				ValueAtoms: 49,
				PubKey:     consensusTestPubKey(9),
			}},
		},
	}
	spent := []UtxoEntry{{ValueAtoms: 50, PubKey: consensusTestPubKey(1)}}
	mainnet, err := SighashWithParams(&tx, 0, spent, MainnetParams())
	if err != nil {
		t.Fatal(err)
	}
	regtest, err := SighashWithParams(&tx, 0, spent, RegtestParams())
	if err != nil {
		t.Fatal(err)
	}
	if mainnet == regtest {
		t.Fatal("expected profile-specific sighash domains to differ")
	}
}
