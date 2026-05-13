package logging

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultMaxSizeBytes int64 = 20 * 1024 * 1024

const (
	defaultCategory = "app"
	formatJSONL     = "jsonl"
	formatText      = "text"
)

var fallbackOperationID uint64

type Config struct {
	Path         string
	Level        string
	Format       string
	MaxSizeBytes int64
}

type rotatingFile struct {
	path         string
	maxSizeBytes int64

	mu   sync.Mutex
	file *os.File
	size int64
}

// standardHandler keeps old slog call sites schema-compatible by filling the
// stable fields that JSONL tooling depends on.
type standardHandler struct {
	inner slog.Handler
	attrs []slog.Attr
}

// Operation emits a flat sequence of correlated records; the CLI renderer is a
// view over these fields rather than a second daemon-side log format.
type Operation struct {
	logger   *slog.Logger
	opID     string
	category string
	started  time.Time

	mu       sync.Mutex
	seq      uint64
	finished bool
}

func Setup(cfg Config) (*slog.Logger, io.Closer, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, nil, errors.New("log path is required")
	}
	if cfg.MaxSizeBytes <= 0 {
		cfg.MaxSizeBytes = DefaultMaxSizeBytes
	}
	writer, err := openRotatingFile(cfg.Path, cfg.MaxSizeBytes)
	if err != nil {
		return nil, nil, err
	}
	logger, err := NewLogger(io.MultiWriter(os.Stderr, writer), Config{
		Level:  cfg.Level,
		Format: cfg.Format,
	})
	if err != nil {
		_ = writer.Close()
		return nil, nil, err
	}
	slog.SetDefault(logger)
	return logger, writer, nil
}

func Component(name string) *slog.Logger {
	return slog.Default().With("category", name)
}

func ComponentWith(base *slog.Logger, name string) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	return base.With("category", name)
}

func EventName(name string) slog.Attr {
	return slog.String("name", name)
}

func NewLogger(dst io.Writer, cfg Config) (*slog.Logger, error) {
	handler, err := NewHandler(dst, Config{
		Level:  cfg.Level,
		Format: cfg.Format,
	})
	if err != nil {
		return nil, err
	}
	return slog.New(handler), nil
}

func NewHandler(dst io.Writer, cfg Config) (slog.Handler, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	format, err := parseFormat(cfg.Format)
	if err != nil {
		return nil, err
	}
	options := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			return normalizeAttr(groups, attr)
		},
	}
	switch format {
	case formatJSONL:
		return newStandardHandler(slog.NewJSONHandler(dst, options)), nil
	default:
		return newStandardHandler(slog.NewTextHandler(dst, options)), nil
	}
}

func parseLevel(raw string) (slog.Leveler, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return nil, errors.New("unsupported log level: " + raw)
	}
}

func parseFormat(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", formatJSONL, "json":
		return formatJSONL, nil
	case formatText:
		return formatText, nil
	default:
		return "", errors.New("unsupported log format: " + raw)
	}
}

func newStandardHandler(inner slog.Handler) slog.Handler {
	return &standardHandler{inner: inner}
}

func (h *standardHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *standardHandler) Handle(ctx context.Context, record slog.Record) error {
	if !hasAttr(h.attrs, &record, "name") {
		record.AddAttrs(slog.String("name", defaultName(record.Message)))
	}
	if !hasCategory(h.attrs, &record) {
		record.AddAttrs(slog.String("category", defaultCategory))
	}
	return h.inner.Handle(ctx, record)
}

func (h *standardHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copied := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	copied = append(copied, h.attrs...)
	copied = append(copied, attrs...)
	return &standardHandler{
		inner: h.inner.WithAttrs(attrs),
		attrs: copied,
	}
}

func (h *standardHandler) WithGroup(name string) slog.Handler {
	return &standardHandler{
		inner: h.inner.WithGroup(name),
		attrs: append([]slog.Attr(nil), h.attrs...),
	}
}

func normalizeAttr(groups []string, attr slog.Attr) slog.Attr {
	if len(groups) != 0 {
		return attr
	}
	switch attr.Key {
	case slog.TimeKey:
		attr.Key = "ts"
	case slog.MessageKey:
		attr.Key = "message"
	case "component":
		attr.Key = "category"
	}
	return attr
}

func hasAttr(attrs []slog.Attr, record *slog.Record, key string) bool {
	for _, attr := range attrs {
		if attr.Key == key {
			return true
		}
	}
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == key {
			found = true
			return false
		}
		return true
	})
	return found
}

func hasCategory(attrs []slog.Attr, record *slog.Record) bool {
	for _, attr := range attrs {
		if attr.Key == "category" || attr.Key == "component" {
			return true
		}
	}
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "category" || attr.Key == "component" {
			found = true
			return false
		}
		return true
	})
	return found
}

func defaultName(message string) string {
	message = strings.ToLower(strings.TrimSpace(message))
	var b strings.Builder
	lastDot := false
	for _, ch := range message {
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteRune(ch)
			lastDot = false
		case ch >= '0' && ch <= '9':
			b.WriteRune(ch)
			lastDot = false
		default:
			if b.Len() > 0 && !lastDot {
				b.WriteByte('.')
				lastDot = true
			}
		}
	}
	out := strings.Trim(b.String(), ".")
	if out == "" {
		return "event"
	}
	return out
}

func StartOperation(logger *slog.Logger, category string, name string, message string, attrs ...slog.Attr) *Operation {
	return StartOperationContext(context.Background(), logger, category, name, message, attrs...)
}

func StartOperationContext(ctx context.Context, logger *slog.Logger, category string, name string, message string, attrs ...slog.Attr) *Operation {
	if logger == nil {
		logger = slog.Default()
	}
	category = strings.TrimSpace(category)
	if category == "" {
		category = defaultCategory
	}
	op := &Operation{
		logger:   logger,
		opID:     newOperationID(),
		category: category,
		started:  time.Now(),
	}
	op.log(ctx, slog.LevelInfo, 0, name, message, attrs...)
	return op
}

func (op *Operation) ID() string {
	if op == nil {
		return ""
	}
	return op.opID
}

func (op *Operation) Step(name string, message string, attrs ...slog.Attr) {
	op.StepContext(context.Background(), name, message, attrs...)
}

func (op *Operation) StepContext(ctx context.Context, name string, message string, attrs ...slog.Attr) {
	op.log(ctx, slog.LevelInfo, 1, name, message, attrs...)
}

func (op *Operation) Warn(name string, message string, attrs ...slog.Attr) {
	op.WarnContext(context.Background(), name, message, attrs...)
}

func (op *Operation) WarnContext(ctx context.Context, name string, message string, attrs ...slog.Attr) {
	op.log(ctx, slog.LevelWarn, 1, name, message, attrs...)
}

func (op *Operation) Error(name string, message string, err error, attrs ...slog.Attr) {
	op.ErrorContext(context.Background(), name, message, err, attrs...)
}

func (op *Operation) ErrorContext(ctx context.Context, name string, message string, err error, attrs ...slog.Attr) {
	if err != nil {
		attrs = append(attrs, slog.String("error_type", errorType(err)), slog.Any("error", err))
	}
	op.log(ctx, slog.LevelError, 1, name, message, attrs...)
}

func (op *Operation) Finish(message string, attrs ...slog.Attr) {
	op.FinishContext(context.Background(), message, attrs...)
}

func (op *Operation) FinishContext(ctx context.Context, message string, attrs ...slog.Attr) {
	if !op.markFinished() {
		return
	}
	attrs = append(attrs, slog.Float64("latency_ms", roundMilliseconds(time.Since(op.started))))
	op.log(ctx, slog.LevelInfo, 1, "result", message, attrs...)
}

func (op *Operation) Fail(message string, err error, attrs ...slog.Attr) {
	op.FailContext(context.Background(), message, err, attrs...)
}

func (op *Operation) FailContext(ctx context.Context, message string, err error, attrs ...slog.Attr) {
	if !op.markFinished() {
		return
	}
	if err != nil {
		attrs = append(attrs, slog.String("error_type", errorType(err)), slog.Any("error", err))
	}
	attrs = append(attrs, slog.Float64("latency_ms", roundMilliseconds(time.Since(op.started))))
	op.log(ctx, slog.LevelError, 1, "result", message, attrs...)
}

func (op *Operation) log(ctx context.Context, level slog.Level, depth int, name string, message string, attrs ...slog.Attr) {
	if op == nil || op.logger == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = defaultName(message)
	}
	op.mu.Lock()
	seq := op.seq
	op.seq++
	op.mu.Unlock()
	base := []slog.Attr{
		slog.String("category", op.category),
		slog.String("name", name),
		slog.String("op_id", op.opID),
		slog.Uint64("seq", seq),
		slog.Int("depth", depth),
	}
	base = append(base, attrs...)
	op.logger.LogAttrs(ctx, level, message, base...)
}

func (op *Operation) markFinished() bool {
	if op == nil {
		return false
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.finished {
		return false
	}
	op.finished = true
	return true
}

func newOperationID() string {
	var buf [8]byte
	if _, err := crand.Read(buf[:]); err == nil {
		return "op_" + hex.EncodeToString(buf[:])
	}
	seq := atomic.AddUint64(&fallbackOperationID, 1)
	return "op_" + hex.EncodeToString([]byte{
		byte(seq >> 56),
		byte(seq >> 48),
		byte(seq >> 40),
		byte(seq >> 32),
		byte(seq >> 24),
		byte(seq >> 16),
		byte(seq >> 8),
		byte(seq),
	})
}

func roundMilliseconds(d time.Duration) float64 {
	ms := float64(d) / float64(time.Millisecond)
	return math.Round(ms*10) / 10
}

func errorType(err error) string {
	if err == nil {
		return ""
	}
	name := defaultName(err.Error())
	if name == "" {
		return "error"
	}
	return name
}

func openRotatingFile(path string, maxSizeBytes int64) (*rotatingFile, error) {
	if err := os.MkdirAll(filepath.Dir(filepath.Clean(path)), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Clean(path), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &rotatingFile{
		path:         filepath.Clean(path),
		maxSizeBytes: maxSizeBytes,
		file:         file,
		size:         info.Size(),
	}, nil
}

func (r *rotatingFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil {
		return 0, os.ErrClosed
	}
	if r.size+int64(len(p)) > r.maxSizeBytes {
		if err := r.rotateLocked(); err != nil {
			return 0, err
		}
	}
	n, err := r.file.Write(p)
	r.size += int64(n)
	return n, err
}

func (r *rotatingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func (r *rotatingFile) rotateLocked() error {
	if err := r.file.Close(); err != nil {
		return err
	}
	backupPath := r.path + ".1"
	if err := os.Remove(backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = r.reopenLocked()
		return err
	}
	if err := os.Rename(r.path, backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = r.reopenLocked()
		return err
	}
	file, err := os.OpenFile(r.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		if restoreErr := os.Rename(backupPath, r.path); restoreErr != nil && !errors.Is(restoreErr, os.ErrNotExist) {
			_, _ = fmt.Fprintf(os.Stderr, "restore rotated log %s failed: %v\n", r.path, restoreErr)
		}
		_ = r.reopenLocked()
		return err
	}
	r.file = file
	r.size = 0
	return nil
}

func (r *rotatingFile) reopenLocked() error {
	file, err := os.OpenFile(r.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		r.file = nil
		r.size = 0
		_, _ = fmt.Fprintf(os.Stderr, "reopen log %s after rotation failure failed: %v\n", r.path, err)
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		r.file = nil
		r.size = 0
		_, _ = fmt.Fprintf(os.Stderr, "stat log %s after rotation failure failed: %v\n", r.path, err)
		return err
	}
	r.file = file
	r.size = info.Size()
	return nil
}
