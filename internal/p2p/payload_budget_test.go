package p2p

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"bitcoin-pure/internal/types"
)

func TestReadMessageSharesPayloadBudgetAcrossConnections(t *testing.T) {
	const (
		magic       = uint32(0x11223344)
		payloadSize = 64
	)
	budget := NewPayloadBudget(payloadSize)
	firstClient, firstServer := net.Pipe()
	t.Cleanup(func() {
		_ = firstClient.Close()
		_ = firstServer.Close()
	})
	first := NewConnWithLimitsAndBudget(firstClient, magic, payloadSize, types.DefaultCodecLimits(), budget)

	firstResult := make(chan error, 1)
	go func() {
		_, err := first.ReadMessage()
		firstResult <- err
	}()
	writeAdvertisedPayloadHeader(t, firstServer, magic, CmdBlock, payloadSize)
	waitForPayloadBudgetUsage(t, budget, payloadSize)

	secondClient, secondServer := net.Pipe()
	t.Cleanup(func() {
		_ = secondClient.Close()
		_ = secondServer.Close()
	})
	second := NewConnWithLimitsAndBudget(secondClient, magic, payloadSize, types.DefaultCodecLimits(), budget)
	go writeAdvertisedPayloadHeader(t, secondServer, magic, CmdBlock, 1)
	if _, err := second.ReadMessage(); !errors.Is(err, ErrPayloadBudget) {
		t.Fatalf("second ReadMessage error = %v, want %v", err, ErrPayloadBudget)
	}

	if err := firstServer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstResult:
	case <-time.After(time.Second):
		t.Fatal("first ReadMessage did not return after connection close")
	}
	if got := budget.Stats().Used; got != 0 {
		t.Fatalf("payload budget used = %d, want 0", got)
	}
}

func writeAdvertisedPayloadHeader(t *testing.T, conn net.Conn, magic uint32, command Command, payloadSize int) {
	t.Helper()
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[:4], magic)
	header[4] = byte(command)
	binary.LittleEndian.PutUint32(header[8:12], uint32(payloadSize))
	if _, err := conn.Write(header); err != nil {
		// Closing the paired endpoint is how the test releases a blocked reader.
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("write header: %v", err)
		}
	}
}

func waitForPayloadBudgetUsage(t *testing.T, budget *PayloadBudget, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if budget.Stats().Used == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("payload budget used = %d, want %d", budget.Stats().Used, want)
}

func TestCompactBlockPrefilledTransactionsAreStrictlyBounded(t *testing.T) {
	msg := CompactBlockMessage{Prefilled: make([]PrefilledTx, maxCompactPrefilled+1)}
	if _, err := encodeMessage(msg); err == nil {
		t.Fatal("encoded compact block above prefilled transaction limit")
	}
}
