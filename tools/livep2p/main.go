package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
)

const (
	wireHeaderSize = 16
	defaultMaxRead = 8
)

type probeCase struct {
	Name        string
	Command     p2p.Command
	Payload     []byte
	MagicOffset uint32
	Checksum    []byte
	Length      uint32
	OmitPayload bool
}

type probeResult struct {
	Host string
	Case string
	OK   bool
	Note string
}

func main() {
	nodesRaw := flag.String("nodes", "", "comma-separated host[:port] list")
	profileRaw := flag.String("profile", "mainnet", "chain profile for network magic")
	timeout := flag.Duration("timeout", 8*time.Second, "per-probe timeout")
	slowHandshakes := flag.Int("slow-handshakes", 0, "optional number of idle handshake connections to open per node")
	flag.Parse()

	if strings.TrimSpace(*nodesRaw) == "" {
		fatalf("-nodes is required")
	}
	profile, err := types.ParseChainProfile(*profileRaw)
	if err != nil {
		fatalf("profile: %v", err)
	}
	magic := p2p.MagicForProfile(profile)
	if magic == 0 {
		fatalf("profile %q has no p2p magic", profile)
	}
	if *slowHandshakes < 0 || *slowHandshakes > 512 {
		fatalf("-slow-handshakes must be between 0 and 512")
	}

	failures := 0
	for _, host := range splitCSV(*nodesRaw) {
		addr := host
		if _, _, err := net.SplitHostPort(host); err != nil {
			addr = net.JoinHostPort(host, "18444")
		}
		for _, tc := range probeCases() {
			result := runProbe(addr, magic, tc, *timeout)
			fmt.Printf("%s %-18s ok=%t %s\n", result.Host, result.Case, result.OK, result.Note)
			if !result.OK {
				failures++
			}
		}
		if *slowHandshakes > 0 {
			result := runSlowHandshakeProbe(addr, *slowHandshakes, *timeout)
			fmt.Printf("%s %-18s ok=%t %s\n", result.Host, result.Case, result.OK, result.Note)
			if !result.OK {
				failures++
			}
		}
	}
	if failures > 0 {
		os.Exit(1)
	}
}

func probeCases() []probeCase {
	return []probeCase{
		{Name: "bad-magic", Command: p2p.CmdPing, Payload: make([]byte, 8), MagicOffset: 1},
		{Name: "bad-checksum", Command: p2p.CmdPing, Payload: make([]byte, 8), Checksum: []byte{0xff, 0xff, 0xff, 0xff}},
		{Name: "unknown-command", Command: p2p.Command(0xff), Payload: nil},
		{Name: "short-version", Command: p2p.CmdVersion, Payload: []byte{1, 2, 3}},
		{Name: "oversized-header", Command: p2p.CmdPing, Length: 64_000_001, OmitPayload: true},
	}
}

func runProbe(addr string, magic uint32, tc probeCase, timeout time.Duration) probeResult {
	deadline := time.Now().Add(timeout)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return probeResult{Host: addr, Case: tc.Name, Note: "dial: " + err.Error()}
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline)
	if _, err := conn.Write(frameForCase(magic, tc)); err != nil {
		return probeResult{Host: addr, Case: tc.Name, Note: "write: " + err.Error()}
	}
	buf := make([]byte, defaultMaxRead)
	total := 0
	for {
		n, err := conn.Read(buf)
		total += n
		if err == nil {
			continue
		}
		if isExpectedDisconnect(err) {
			note := err.Error()
			if total > 0 {
				note = fmt.Sprintf("closed after %d initial bytes: %s", total, note)
			}
			return probeResult{Host: addr, Case: tc.Name, OK: true, Note: note}
		}
		return probeResult{Host: addr, Case: tc.Name, Note: err.Error()}
	}
}

func frameForCase(magic uint32, tc probeCase) []byte {
	header := make([]byte, wireHeaderSize)
	binary.LittleEndian.PutUint32(header[:4], magic+tc.MagicOffset)
	header[4] = byte(tc.Command)
	length := uint32(len(tc.Payload))
	if tc.Length != 0 {
		length = tc.Length
	}
	binary.LittleEndian.PutUint32(header[8:12], length)
	checksum := crypto.Sha256d(tc.Payload)
	copy(header[12:16], checksum[:4])
	if len(tc.Checksum) != 0 {
		copy(header[12:16], tc.Checksum)
	}
	if tc.OmitPayload {
		return header
	}
	return append(header, tc.Payload...)
}

func runSlowHandshakeProbe(addr string, count int, timeout time.Duration) probeResult {
	if count <= 0 {
		return probeResult{Host: addr, Case: "slow-handshake", OK: true, Note: "skipped"}
	}
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	type closeResult struct {
		bytes int
		err   error
	}
	results := make(chan closeResult, count)
	var wg sync.WaitGroup
	opened := 0
	for i := 0; i < count; i++ {
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			return probeResult{Host: addr, Case: fmt.Sprintf("slow-handshake-%d", count), Note: fmt.Sprintf("dial %d/%d: %v", i+1, count, err)}
		}
		opened++
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(timeout + 2*time.Second))
			buf := make([]byte, defaultMaxRead)
			total := 0
			for {
				n, err := conn.Read(buf)
				total += n
				if err == nil {
					continue
				}
				results <- closeResult{bytes: total, err: err}
				return
			}
		}(conn)
	}
	wg.Wait()
	close(results)
	closed := 0
	bytes := 0
	var lastErr error
	for result := range results {
		bytes += result.bytes
		lastErr = result.err
		if isExpectedDisconnect(result.err) {
			closed++
		}
	}
	ok := opened == count && closed == count
	note := fmt.Sprintf("opened=%d closed=%d initial_bytes=%d", opened, closed, bytes)
	if !ok && lastErr != nil {
		note = fmt.Sprintf("%s last_error=%v", note, lastErr)
	}
	return probeResult{Host: addr, Case: fmt.Sprintf("slow-handshake-%d", count), OK: ok, Note: note}
}

func isExpectedDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "use of closed network connection")
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
