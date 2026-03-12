package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunSnapshotRoot(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	output := captureStdout(t, func() {
		if err := runSnapshot([]string{"root", "--file", fixturePath}); err != nil {
			t.Fatalf("runSnapshot root: %v", err)
		}
	})
	if !strings.Contains(output, "utxo_root: 89937c242d821c308952134a0822e1f5ddc94cb5022ad49f8f35448e09a45b16") {
		t.Fatalf("snapshot root output missing expected root:\n%s", output)
	}
}

func TestRunSnapshotVerify(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	output := captureStdout(t, func() {
		if err := runSnapshot([]string{"verify", "--file", fixturePath}); err != nil {
			t.Fatalf("runSnapshot verify: %v", err)
		}
	})
	if !strings.Contains(output, "height: 2") {
		t.Fatalf("snapshot verify output missing height:\n%s", output)
	}
	if !strings.Contains(output, "utxo_count: 3") {
		t.Fatalf("snapshot verify output missing utxo count:\n%s", output)
	}
}

func TestRunSnapshotVerifyRejectsMismatchedExpectation(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "fixtures", "snapshots", "regtest_bootstrap_tip.json")
	buf, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	bad := strings.Replace(string(buf), "89937c242d821c308952134a0822e1f5ddc94cb5022ad49f8f35448e09a45b16", "0000000000000000000000000000000000000000000000000000000000000000", 1)
	path := t.TempDir() + "/bad_snapshot.json"
	if err := os.WriteFile(path, []byte(bad), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := runSnapshot([]string{"verify", "--file", path}); err == nil {
		t.Fatal("expected mismatched snapshot verify to fail")
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = old
	}()
	fn()
	if err := w.Close(); err != nil {
		t.Fatalf("Close stdout writer: %v", err)
	}
	buf, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return string(buf)
}
