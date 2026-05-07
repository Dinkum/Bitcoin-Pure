package main

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/p2p"
)

func TestFrameForCaseEncodesHeaderAndChecksum(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	frame := frameForCase(0x4250554d, probeCase{
		Command: p2p.CmdPing,
		Payload: payload,
	})
	if len(frame) != wireHeaderSize+len(payload) {
		t.Fatalf("frame len = %d, want %d", len(frame), wireHeaderSize+len(payload))
	}
	if got := binary.LittleEndian.Uint32(frame[:4]); got != 0x4250554d {
		t.Fatalf("magic = %#x", got)
	}
	if got := p2p.Command(frame[4]); got != p2p.CmdPing {
		t.Fatalf("command = %v", got)
	}
	if got := binary.LittleEndian.Uint32(frame[8:12]); got != uint32(len(payload)) {
		t.Fatalf("payload len = %d", got)
	}
	checksum := crypto.Sha256d(payload)
	if string(frame[12:16]) != string(checksum[:4]) {
		t.Fatalf("checksum mismatch")
	}
}

func TestFrameForCaseCanSendOversizedHeaderOnly(t *testing.T) {
	frame := frameForCase(0x4250554d, probeCase{
		Command:     p2p.CmdPing,
		Length:      64_000_001,
		OmitPayload: true,
	})
	if len(frame) != wireHeaderSize {
		t.Fatalf("frame len = %d, want header only", len(frame))
	}
	if got := binary.LittleEndian.Uint32(frame[8:12]); got != 64_000_001 {
		t.Fatalf("payload len = %d", got)
	}
}

func TestRunSlowHandshakeProbeTreatsClosedConnectionsAsOK(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				time.Sleep(10 * time.Millisecond)
				_ = conn.Close()
			}(conn)
		}
	}()

	result := runSlowHandshakeProbe(ln.Addr().String(), 3, time.Second)
	if !result.OK {
		t.Fatalf("slow handshake probe failed: %+v", result)
	}
}
