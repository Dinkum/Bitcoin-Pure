package p2p

import (
	"errors"
	"net"
	"testing"
	"time"

	"bitcoin-pure/internal/types"
)

func TestConnMessageRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(HeadersMessage{Headers: []types.BlockHeader{{Version: 1}, {Version: 2}}})
	}()

	msg, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	headers, ok := msg.(HeadersMessage)
	if !ok {
		t.Fatalf("message type = %T, want HeadersMessage", msg)
	}
	if len(headers.Headers) != 2 || headers.Headers[0].Version != 1 || headers.Headers[1].Version != 2 {
		t.Fatalf("unexpected headers payload: %+v", headers.Headers)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestNewConnDefaultPayloadCeilingCoversConsensusFloor(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	conn := NewConn(left, MagicForProfile(types.Regtest), 0)
	if conn.maxPayload < 32_000_000 {
		t.Fatalf("max payload = %d, want at least 32000000", conn.maxPayload)
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 2)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		leftConn := NewConn(conn, MagicForProfile(types.Regtest), 1<<20)
		remote, err := Handshake(leftConn, VersionMessage{Protocol: 1, Height: 5, Nonce: 1, UserAgent: "left"}, time.Second)
		if err != nil {
			errCh <- err
			return
		}
		if remote.UserAgent != "right" {
			errCh <- errors.New("left saw wrong remote user agent")
			return
		}
		errCh <- nil
	}()
	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		rightConn := NewConn(conn, MagicForProfile(types.Regtest), 1<<20)
		remote, err := Handshake(rightConn, VersionMessage{Protocol: 1, Height: 6, Nonce: 2, UserAgent: "right"}, time.Second)
		if err != nil {
			errCh <- err
			return
		}
		if remote.UserAgent != "left" {
			errCh <- errors.New("right saw wrong remote user agent")
			return
		}
		errCh <- nil
	}()
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func TestConnTxBatchRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := TxBatchMessage{
		Txs: []types.Transaction{
			{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 1}}}},
			{Base: types.TxBase{Version: 2, Outputs: []types.TxOutput{{ValueAtoms: 2}}}},
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	batch, ok := received.(TxBatchMessage)
	if !ok {
		t.Fatalf("message type = %T, want TxBatchMessage", received)
	}
	if len(batch.Txs) != 2 || batch.Txs[0].Base.Version != 1 || batch.Txs[1].Base.Version != 2 {
		t.Fatalf("unexpected tx batch payload: %+v", batch.Txs)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestConnTxReconRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := TxReconMessage{TxIDs: [][32]byte{{1}, {2}, {3}}}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	recon, ok := received.(TxReconMessage)
	if !ok {
		t.Fatalf("message type = %T, want TxReconMessage", received)
	}
	if len(recon.TxIDs) != 3 || recon.TxIDs[0] != ([32]byte{1}) || recon.TxIDs[2] != ([32]byte{3}) {
		t.Fatalf("unexpected recon payload: %+v", recon.TxIDs)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestConnXThinRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := XThinBlockMessage{
		Header:   types.BlockHeader{Version: 3, Timestamp: 42},
		Nonce:    99,
		Coinbase: types.Transaction{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 50}}}},
		ShortIDs: []uint64{11, 22, 33},
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	thin, ok := received.(XThinBlockMessage)
	if !ok {
		t.Fatalf("message type = %T, want XThinBlockMessage", received)
	}
	if thin.Header.Version != 3 || thin.Nonce != 99 || len(thin.ShortIDs) != 3 || thin.ShortIDs[2] != 33 {
		t.Fatalf("unexpected thin block payload: %+v", thin)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestConnXBlockTxRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewConn(left, MagicForProfile(types.Regtest), 1<<20)
	receiver := NewConn(right, MagicForProfile(types.Regtest), 1<<20)
	msg := XBlockTxMessage{
		BlockHash: [32]byte{9},
		Indexes:   []uint32{1, 3},
		Txs: []types.Transaction{
			{Base: types.TxBase{Version: 1, Outputs: []types.TxOutput{{ValueAtoms: 1}}}},
			{Base: types.TxBase{Version: 2, Outputs: []types.TxOutput{{ValueAtoms: 2}}}},
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- sender.WriteMessage(msg)
	}()

	received, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	resp, ok := received.(XBlockTxMessage)
	if !ok {
		t.Fatalf("message type = %T, want XBlockTxMessage", received)
	}
	if len(resp.Indexes) != 2 || resp.Indexes[1] != 3 || len(resp.Txs) != 2 || resp.Txs[1].Base.Version != 2 {
		t.Fatalf("unexpected xblocktx payload: %+v", resp)
	}
	if err := <-done; err != nil {
		t.Fatalf("write message: %v", err)
	}
}

func TestMagicForProfileDistinguishesRegtestHard(t *testing.T) {
	if got := MagicForProfile(types.RegtestHard); got == 0 {
		t.Fatal("regtest_hard magic should be non-zero")
	} else if got == MagicForProfile(types.Regtest) {
		t.Fatal("regtest_hard should use distinct network magic")
	}
}
