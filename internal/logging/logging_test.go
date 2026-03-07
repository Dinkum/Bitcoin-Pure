package logging

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotatingFileRotatesAtCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "node.log")
	writer, err := openRotatingFile(path, 64)
	if err != nil {
		t.Fatalf("openRotatingFile() error = %v", err)
	}
	t.Cleanup(func() {
		_ = writer.Close()
	})

	first := bytes.Repeat([]byte("a"), 40)
	second := bytes.Repeat([]byte("b"), 40)
	if _, err := writer.Write(first); err != nil {
		t.Fatalf("writer.Write(first) error = %v", err)
	}
	if _, err := writer.Write(second); err != nil {
		t.Fatalf("writer.Write(second) error = %v", err)
	}

	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(current) error = %v", err)
	}
	backup, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatalf("ReadFile(backup) error = %v", err)
	}
	if got, want := string(current), string(second); got != want {
		t.Fatalf("current log mismatch: got %q want %q", got, want)
	}
	if got, want := string(backup), string(first); got != want {
		t.Fatalf("backup log mismatch: got %q want %q", got, want)
	}
}

func TestParseLevel(t *testing.T) {
	t.Parallel()

	level, err := parseLevel("debug")
	if err != nil {
		t.Fatalf("parseLevel(debug) error = %v", err)
	}
	if got, want := level.Level(), slog.LevelDebug; got != want {
		t.Fatalf("level = %v want %v", got, want)
	}
	if _, err := parseLevel("bogus"); err == nil {
		t.Fatal("parseLevel(bogus) unexpectedly succeeded")
	}
}

func TestSetupWritesLogLine(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "node.log")
	logger, closer, err := Setup(Config{Path: path, Level: "info", MaxSizeBytes: 1024})
	if err != nil {
		t.Fatalf("Setup() error = %v", err)
	}
	t.Cleanup(func() {
		_ = closer.Close()
	})

	logger.Info("hello", "component", "test")

	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	text := string(buf)
	if !strings.Contains(text, "hello") {
		t.Fatalf("log line missing message: %q", text)
	}
	if !strings.Contains(text, "component=test") {
		t.Fatalf("log line missing component: %q", text)
	}
}
