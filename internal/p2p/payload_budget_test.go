package p2p

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"bitcoin-pure/internal/crypto"
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

func readBudgetedMessageForTest(t *testing.T, budget *PayloadBudget, msg Message) (*ReceivedMessage, error) {
	t.Helper()
	a, b := net.Pipe()
	reader := NewConnWithLimitsAndBudget(a, 123, 2<<20, types.DefaultCodecLimits(), budget)
	writer := NewConn(b, 123, 2<<20)
	done := make(chan error, 1)
	go func() { done <- writer.WriteMessage(msg) }()
	received, err := reader.ReadMessage()
	a.Close()
	b.Close()
	<-done
	return received, err
}

func TestPayloadReservationLivesThroughDecodedMessageOwnership(t *testing.T) {
	msg := BlockChunkMessage{Data: make([]byte, 1<<20)}
	wire, err := encodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	budget := NewPayloadBudget(int64(2 * len(wire)))
	first, err := readBudgetedMessageForTest(t, budget, msg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := readBudgetedMessageForTest(t, budget, msg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if budget.Stats().Used != int64(2*len(wire)) {
		t.Fatalf("unaccounted decoded chunks: %+v", budget.Stats())
	}
	if len(first.Message.(BlockChunkMessage).Data) != 1<<20 {
		t.Fatal("decoded chunk lost its payload")
	}
	if _, err := readBudgetedMessageForTest(t, budget, msg); !errors.Is(err, ErrPayloadBudget) {
		t.Fatalf("third message bypassed budget: %v", err)
	}
	first.Release()
	first.Release()
	third, err := readBudgetedMessageForTest(t, budget, msg)
	if err != nil {
		t.Fatalf("release did not restore capacity: %v", err)
	}
	third.Release()
	second.Release()
	if stats := budget.Stats(); stats.Used != 0 || stats.Peak > stats.Limit {
		t.Fatalf("reservation leaked or exceeded cap: %+v", stats)
	}
}
func TestPayloadBudgetIncludesDecodedAllocations(t *testing.T) {
	msg := TxBatchMessage{Txs: []types.Transaction{testCoinbaseTx(1, 50), testCoinbaseTx(2, 50)}}
	wire, err := encodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	tight := NewPayloadBudget(int64(len(wire)))
	if _, err := readBudgetedMessageForTest(t, tight, msg); !errors.Is(err, ErrPayloadBudget) {
		t.Fatalf("decoded allocation bypassed wire-only budget: %v", err)
	}
	if tight.Stats().Used != 0 {
		t.Fatal("failed decode leaked reservation")
	}
	roomy := NewPayloadBudget(int64(10 * len(wire)))
	received, err := readBudgetedMessageForTest(t, roomy, msg)
	if err != nil {
		t.Fatal(err)
	}
	if roomy.Stats().Used <= int64(len(wire)) {
		t.Fatal("decoded arrays were not accounted")
	}
	received.Release()
	if roomy.Stats().Used != 0 {
		t.Fatal("decoded allocation reservation leaked")
	}
}
func TestPayloadBudgetReleasesMalformedDecodedMessage(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	budget := NewPayloadBudget(1 << 20)
	reader := NewConnWithLimitsAndBudget(a, 123, 1<<20, types.DefaultCodecLimits(), budget)
	payload := []byte{1, 0, 0, 0}
	checksum := crypto.Sha256d(payload)
	go func() { writeRawMessageForTest(b, 123, CmdTxBatch, payload, checksum[:4]) }()
	if _, err := reader.ReadMessage(); err == nil {
		t.Fatal("malformed transaction batch accepted")
	}
	if budget.Stats().Used != 0 {
		t.Fatal("partial decoded allocation leaked")
	}
}
