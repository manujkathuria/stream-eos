// Package logger provides process-wide, printf-style logging backed by slog.
package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"time"
)

type Format string

const (
	JSON Format = "json"
	Text Format = "text"
)

// Config is applied once at startup. The caller owns Output.
type Config struct {
	Service   string
	Format    Format
	Level     slog.Leveler
	Output    io.Writer
	AddSource bool
}

// New configures package logging. Defaults are JSON, INFO, and stdout.
// It sets slog's process-wide default, which also redirects standard log output.
func New(cfg Config) error {
	output := cfg.Output
	if output == nil {
		output = os.Stdout
	}
	options := &slog.HandlerOptions{Level: cfg.Level, AddSource: cfg.AddSource}
	var handler slog.Handler
	switch cfg.Format {
	case "", JSON:
		handler = slog.NewJSONHandler(output, options)
	case Text:
		handler = slog.NewTextHandler(output, options)
	default:
		return fmt.Errorf("unsupported log format %q: use json or text", cfg.Format)
	}
	log := slog.New(handler)
	if cfg.Service != "" {
		log = log.With("service", cfg.Service)
	}
	slog.SetDefault(log)
	return nil
}

func Info(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelInfo, format, args...)
}
func Warn(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelWarn, format, args...)
}

// Error logs without exiting the process.
func Error(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelError, format, args...)
}
func Debug(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelDebug, format, args...)
}

func write(ctx context.Context, level slog.Level, format string, args ...any) {
	if ctx == nil {
		ctx = context.Background()
	}
	log := slog.Default()
	if !log.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // Skip Callers, write, and the level function.
	record := slog.NewRecord(time.Now(), level, fmt.Sprintf(format, args...), pcs[0])
	_ = log.Handler().Handle(ctx, record) // Match slog's handling of output errors.
}
