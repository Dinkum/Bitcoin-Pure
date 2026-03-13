package logging

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const DefaultMaxSizeBytes int64 = 20 * 1024 * 1024

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
	return slog.Default().With("component", name)
}

func ComponentWith(base *slog.Logger, name string) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	return base.With("component", name)
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
	}
	switch format {
	case "json":
		return slog.NewJSONHandler(dst, options), nil
	default:
		return slog.NewTextHandler(dst, options), nil
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
	case "", "text":
		return "text", nil
	case "json":
		return "json", nil
	default:
		return "", errors.New("unsupported log format: " + raw)
	}
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
		return err
	}
	if err := os.Rename(r.path, backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(r.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	r.file = file
	r.size = 0
	return nil
}
