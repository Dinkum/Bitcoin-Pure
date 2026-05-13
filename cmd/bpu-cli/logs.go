package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"bitcoin-pure/internal/config"
)

type logRecord map[string]any

func runLogs(args []string) error {
	if len(args) == 0 {
		return errors.New("missing logs subcommand")
	}
	switch args[0] {
	case "tail":
		return runLogsTail(args[1:])
	case "filter":
		return runLogsFilter(args[1:])
	case "render":
		return runLogsRender(args[1:])
	default:
		return errors.New("unknown logs subcommand")
	}
}

// Log views resolve through config so local runs and installed services use the
// same discovery rules as the daemon.
func runLogsTail(args []string) error {
	fs := flag.NewFlagSet("logs tail", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	lineCount := fs.Int("lines", 50, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli logs tail [--config PATH] [--lines N]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	lines, err := readLastLogLines(resolveLogPath(cfg), *lineCount)
	if err != nil {
		return err
	}
	for _, line := range lines {
		fmt.Println(line)
	}
	return nil
}

func runLogsFilter(args []string) error {
	fs := flag.NewFlagSet("logs filter", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	category := fs.String("category", "", "")
	name := fs.String("name", "", "")
	level := fs.String("level", "", "")
	opID := fs.String("op-id", "", "")
	contains := fs.String("contains", "", "")
	limit := fs.Int("limit", 100, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli logs filter [--config PATH] [--category NAME] [--name NAME] [--level LEVEL] [--op-id ID] [--contains TEXT] [--limit N]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	if !isStructuredLogFormat(cfg.LogFormat) {
		return fmt.Errorf("logs filter requires jsonl log_format, got %q", cfg.LogFormat)
	}
	printed := 0
	return scanLogRecords(resolveLogPath(cfg), func(line string, record logRecord) bool {
		if !logRecordMatches(record, *category, *name, *level, *opID, *contains) {
			return true
		}
		fmt.Println(line)
		printed++
		return *limit <= 0 || printed < *limit
	})
}

func runLogsRender(args []string) error {
	fs := flag.NewFlagSet("logs render", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "")
	opID := fs.String("op-id", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: bpu-cli logs render [--config PATH] [--op-id ID]")
	}
	cfg, _, err := resolveCLIConfig(*configPath)
	if err != nil {
		return err
	}
	if !isStructuredLogFormat(cfg.LogFormat) {
		return fmt.Errorf("logs render requires jsonl log_format, got %q", cfg.LogFormat)
	}
	path := resolveLogPath(cfg)
	resolvedOpID := strings.TrimSpace(*opID)
	if resolvedOpID == "" {
		resolvedOpID, err = findLastOperationID(path)
		if err != nil {
			return err
		}
		if resolvedOpID == "" {
			record, ok, err := findLastLogRecord(path)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("no log records found")
			}
			fmt.Print(renderOperationRecords([]logRecord{record}))
			return nil
		}
	}
	records, err := collectOperationRecords(path, resolvedOpID)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return fmt.Errorf("no records found for op_id %s", resolvedOpID)
	}
	fmt.Print(renderOperationRecords(records))
	return nil
}

func resolveLogPath(cfg config.Config) string {
	if strings.TrimSpace(cfg.LogPath) != "" {
		return filepath.Clean(strings.TrimSpace(cfg.LogPath))
	}
	return deriveLogPath(cfg.DBPath)
}

func isStructuredLogFormat(format string) bool {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "json", "jsonl":
		return true
	default:
		return false
	}
}

func readLastLogLines(path string, count int) ([]string, error) {
	if count <= 0 {
		return nil, errors.New("--lines must be positive")
	}
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lines := make([]string, 0, count)
	for scanner.Scan() {
		line := scanner.Text()
		if len(lines) < count {
			lines = append(lines, line)
			continue
		}
		copy(lines, lines[1:])
		lines[len(lines)-1] = line
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func scanLogRecords(path string, visit func(line string, record logRecord) bool) error {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lineNo := 0
	var pendingLine string
	var pendingLineNo int
	flushPending := func(final bool) error {
		if strings.TrimSpace(pendingLine) == "" {
			return nil
		}
		record, err := decodeLogRecord(pendingLine)
		if err != nil {
			if final {
				return nil
			}
			return fmt.Errorf("parse log JSONL %s:%d: %w", path, pendingLineNo, err)
		}
		if !visit(pendingLine, record) {
			return errStopLogScan
		}
		return nil
	}
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if pendingLineNo != 0 {
			if err := flushPending(false); err != nil {
				if errors.Is(err, errStopLogScan) {
					return nil
				}
				return err
			}
		}
		pendingLine = line
		pendingLineNo = lineNo
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if pendingLineNo != 0 {
		if err := flushPending(true); err != nil && !errors.Is(err, errStopLogScan) {
			return err
		}
	}
	return nil
}

var errStopLogScan = errors.New("stop log scan")

func decodeLogRecord(line string) (logRecord, error) {
	decoder := json.NewDecoder(strings.NewReader(line))
	decoder.UseNumber()
	var record logRecord
	if err := decoder.Decode(&record); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing data after log record")
		}
		return nil, err
	}
	return record, nil
}

func logRecordMatches(record logRecord, category string, name string, level string, opID string, contains string) bool {
	category = strings.TrimSpace(category)
	name = strings.TrimSpace(name)
	level = strings.ToUpper(strings.TrimSpace(level))
	opID = strings.TrimSpace(opID)
	contains = strings.ToLower(strings.TrimSpace(contains))
	if category != "" && recordString(record, "category") != category {
		return false
	}
	if name != "" && recordString(record, "name") != name {
		return false
	}
	if level != "" && normalizeLogLevel(recordString(record, "level")) != normalizeLogLevel(level) {
		return false
	}
	if opID != "" && recordString(record, "op_id") != opID {
		return false
	}
	if contains != "" && !strings.Contains(strings.ToLower(recordString(record, "message")), contains) {
		return false
	}
	return true
}

func normalizeLogLevel(level string) string {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "WARNING":
		return "WARN"
	default:
		return strings.ToUpper(strings.TrimSpace(level))
	}
}

func findLastOperationID(path string) (string, error) {
	var opID string
	err := scanLogRecords(path, func(_ string, record logRecord) bool {
		if current := recordString(record, "op_id"); current != "" {
			opID = current
		}
		return true
	})
	return opID, err
}

func findLastLogRecord(path string) (logRecord, bool, error) {
	var last logRecord
	err := scanLogRecords(path, func(_ string, record logRecord) bool {
		last = record
		return true
	})
	return last, last != nil, err
}

func collectOperationRecords(path string, opID string) ([]logRecord, error) {
	records := []logRecord{}
	err := scanLogRecords(path, func(_ string, record logRecord) bool {
		if recordString(record, "op_id") == opID {
			records = append(records, record)
		}
		return true
	})
	return records, err
}

func renderOperationRecords(records []logRecord) string {
	sort.SliceStable(records, func(i, j int) bool {
		return recordInt(records[i], "seq") < recordInt(records[j], "seq")
	})

	root := records[0]
	headerTime := formatLogHeaderTime(recordString(root, "ts"))
	headerLevel := maxLogLevel(records)
	category := recordString(root, "category")
	if category == "" {
		category = "app"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s | %-7s | %s -----\n", headerTime, headerLevel, category)
	var totalMS *float64
	for _, record := range records {
		if ms, ok := recordFloat(record, "latency_ms"); ok {
			totalMS = &ms
		}
		depth := recordInt(record, "depth")
		level := normalizeLogLevel(recordString(record, "level"))
		symbol := logLevelSymbol(level)
		message := recordString(record, "message")
		name := recordString(record, "name")
		fields := renderLogFields(record)
		if depth <= 0 {
			fmt.Fprintf(&b, "%s %s", symbol, message)
		} else {
			label := strings.Repeat(">> ", depth) + name
			fmt.Fprintf(&b, "%-26s | %s %s", label, symbol, message)
		}
		if fields != "" {
			fmt.Fprintf(&b, " | %s", fields)
		}
		b.WriteByte('\n')
	}
	if totalMS != nil {
		fmt.Fprintf(&b, "|= %.1fms total\n", *totalMS)
	}
	return b.String()
}

func renderLogFields(record logRecord) string {
	skip := map[string]struct{}{
		"ts":         {},
		"level":      {},
		"category":   {},
		"name":       {},
		"message":    {},
		"seq":        {},
		"depth":      {},
		"source":     {},
		"latency_ms": {},
	}
	preferred := []string{
		"op_id",
		"request_id",
		"run_id",
		"user_id",
		"peer_id",
		"addr",
		"method",
		"profile",
		"height",
		"header_height",
		"block_hash",
		"txid",
		"authid",
		"status_code",
		"error_type",
		"error",
	}
	used := make(map[string]struct{}, len(record))
	parts := []string{}
	for _, key := range preferred {
		if _, skipped := skip[key]; skipped {
			continue
		}
		value, ok := record[key]
		if !ok {
			continue
		}
		parts = append(parts, key+": "+formatLogValue(value))
		used[key] = struct{}{}
	}
	keys := make([]string, 0, len(record))
	for key := range record {
		if _, skipped := skip[key]; skipped {
			continue
		}
		if _, already := used[key]; already {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key+": "+formatLogValue(record[key]))
	}
	return strings.Join(parts, ", ")
}

func formatLogValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return typed
	case json.Number:
		return typed.String()
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		buf, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(buf)
	}
}

func recordString(record logRecord, key string) string {
	value, ok := record[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(typed)
	}
}

func recordInt(record logRecord, key string) int {
	value, ok := record[key]
	if !ok {
		return 0
	}
	switch typed := value.(type) {
	case json.Number:
		n, _ := typed.Int64()
		return int(n)
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		n, _ := strconv.Atoi(fmt.Sprint(typed))
		return n
	}
}

func recordFloat(record logRecord, key string) (float64, bool) {
	value, ok := record[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case json.Number:
		n, err := typed.Float64()
		return n, err == nil
	case float64:
		return typed, true
	default:
		n, err := strconv.ParseFloat(fmt.Sprint(typed), 64)
		return n, err == nil
	}
}

func formatLogHeaderTime(raw string) string {
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		if raw == "" {
			return "[unknown] ----- unknown"
		}
		return "[" + raw + "]"
	}
	return ts.Format("[2006-01-02] ----- 15:04:05.000")
}

func maxLogLevel(records []logRecord) string {
	max := "DEBUG"
	for _, record := range records {
		level := normalizeLogLevel(recordString(record, "level"))
		if logLevelRank(level) > logLevelRank(max) {
			max = level
		}
	}
	return max
}

func logLevelRank(level string) int {
	switch normalizeLogLevel(level) {
	case "ERROR":
		return 3
	case "WARN":
		return 2
	case "INFO":
		return 1
	default:
		return 0
	}
}

func logLevelSymbol(level string) string {
	switch normalizeLogLevel(level) {
	case "ERROR":
		return "(x)"
	case "WARN":
		return "(!)"
	case "INFO":
		return "(*)"
	default:
		return "(?)"
	}
}
