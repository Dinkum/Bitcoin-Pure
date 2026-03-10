package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLoggerJSONAndTextShareFields(t *testing.T) {
	var textBuf bytes.Buffer
	textLogger, err := NewLogger(&textBuf, Config{Format: "text", Level: "info"})
	if err != nil {
		t.Fatalf("NewLogger(text): %v", err)
	}
	textLogger.With("component", "service").Info("event", slog.String("foo", "bar"), slog.Int("count", 2))

	var jsonBuf bytes.Buffer
	jsonLogger, err := NewLogger(&jsonBuf, Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("NewLogger(json): %v", err)
	}
	jsonLogger.With("component", "service").Info("event", slog.String("foo", "bar"), slog.Int("count", 2))

	textOutput := textBuf.String()
	for _, want := range []string{"level=INFO", "msg=event", "component=service", "foo=bar", "count=2", "source="} {
		if !strings.Contains(textOutput, want) {
			t.Fatalf("text output missing %q: %q", want, textOutput)
		}
	}

	var payload map[string]any
	line := strings.TrimSpace(jsonBuf.String())
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%q): %v", line, err)
	}
	if got := payload["level"]; got != "INFO" {
		t.Fatalf("json level = %v, want INFO", got)
	}
	if got := payload["msg"]; got != "event" {
		t.Fatalf("json msg = %v, want event", got)
	}
	if got := payload["component"]; got != "service" {
		t.Fatalf("json component = %v, want service", got)
	}
	if got := payload["foo"]; got != "bar" {
		t.Fatalf("json foo = %v, want bar", got)
	}
	if got := payload["count"]; got != float64(2) {
		t.Fatalf("json count = %v, want 2", got)
	}
	if _, ok := payload["source"]; !ok {
		t.Fatalf("json output missing source: %v", payload)
	}
}

func TestNewHandlerRejectsUnsupportedFormat(t *testing.T) {
	if _, err := NewHandler(&bytes.Buffer{}, Config{Format: "yaml"}); err == nil {
		t.Fatal("expected unsupported format error")
	}
}
