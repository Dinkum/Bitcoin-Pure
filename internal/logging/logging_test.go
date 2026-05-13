package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLoggerJSONLAndTextShareFields(t *testing.T) {
	var textBuf bytes.Buffer
	textLogger, err := NewLogger(&textBuf, Config{Format: "text", Level: "info"})
	if err != nil {
		t.Fatalf("NewLogger(text): %v", err)
	}
	textLogger.With("component", "service").Info("event", slog.String("foo", "bar"), slog.Int("count", 2))

	var jsonBuf bytes.Buffer
	jsonLogger, err := NewLogger(&jsonBuf, Config{Format: "jsonl", Level: "info"})
	if err != nil {
		t.Fatalf("NewLogger(jsonl): %v", err)
	}
	jsonLogger.With("component", "service").Info("event", slog.String("foo", "bar"), slog.Int("count", 2))

	textOutput := textBuf.String()
	for _, want := range []string{"level=INFO", "message=event", "category=service", "name=event", "foo=bar", "count=2", "source="} {
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
	if got := payload["message"]; got != "event" {
		t.Fatalf("json message = %v, want event", got)
	}
	if got := payload["category"]; got != "service" {
		t.Fatalf("json category = %v, want service", got)
	}
	if got := payload["name"]; got != "event" {
		t.Fatalf("json name = %v, want event", got)
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
	if _, ok := payload["ts"]; !ok {
		t.Fatalf("json output missing ts: %v", payload)
	}
}

func TestNewLoggerJSONAliasUsesJSONLKeys(t *testing.T) {
	var buf bytes.Buffer
	logger, err := NewLogger(&buf, Config{Format: "json", Level: "info"})
	if err != nil {
		t.Fatalf("NewLogger(json): %v", err)
	}
	logger.Info("node service configured")

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &payload); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got := payload["category"]; got != defaultCategory {
		t.Fatalf("category = %v, want %s", got, defaultCategory)
	}
	if got := payload["name"]; got != "node.service.configured" {
		t.Fatalf("name = %v, want derived name", got)
	}
	if _, ok := payload["msg"]; ok {
		t.Fatalf("unexpected legacy msg key: %v", payload)
	}
}

func TestOperationEmitsSequencedRecords(t *testing.T) {
	var buf bytes.Buffer
	logger, err := NewLogger(&buf, Config{Format: "jsonl", Level: "info"})
	if err != nil {
		t.Fatalf("NewLogger(jsonl): %v", err)
	}

	op := StartOperation(logger, "sync", "sync.blocks", "Started block sync", slog.Uint64("from_height", 1))
	op.Step("headers", "Resolved locator", slog.Uint64("header_height", 3))
	op.Finish("Completed block sync", slog.Int("block_count", 2))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("line count = %d, want 3: %q", len(lines), buf.String())
	}
	var first, second, third map[string]any
	for i, dst := range []*map[string]any{&first, &second, &third} {
		if err := json.Unmarshal([]byte(lines[i]), dst); err != nil {
			t.Fatalf("line %d json.Unmarshal: %v", i, err)
		}
	}
	if first["op_id"] == "" || first["op_id"] != second["op_id"] || first["op_id"] != third["op_id"] {
		t.Fatalf("operation IDs not shared: %v %v %v", first["op_id"], second["op_id"], third["op_id"])
	}
	if first["seq"] != float64(0) || second["seq"] != float64(1) || third["seq"] != float64(2) {
		t.Fatalf("unexpected seq values: %v %v %v", first["seq"], second["seq"], third["seq"])
	}
	if second["depth"] != float64(1) {
		t.Fatalf("step depth = %v, want 1", second["depth"])
	}
	if _, ok := third["latency_ms"]; !ok {
		t.Fatalf("finish record missing latency_ms: %v", third)
	}
}

func TestNewHandlerRejectsUnsupportedFormat(t *testing.T) {
	if _, err := NewHandler(&bytes.Buffer{}, Config{Format: "yaml"}); err == nil {
		t.Fatal("expected unsupported format error")
	}
}
