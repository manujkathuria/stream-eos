package logger

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

// Includes filtering, printf formatting, encoding, and locking, but excludes I/O
// and construction. Disabled levels skip formatting, not argument evaluation.
func BenchmarkPrintf(b *testing.B) {
	for _, format := range []Format{JSON, Text} {
		for _, enabled := range []bool{false, true} {
			name := string(format) + "/disabled"
			threshold := slog.LevelWarn
			if enabled {
				name = string(format) + "/enabled"
				threshold = slog.LevelInfo
			}
			b.Run(name, func(b *testing.B) {
				previous := slog.Default()
				b.Cleanup(func() { slog.SetDefault(previous) })
				if err := New(Config{Service: "source", Format: format, Level: threshold, Output: io.Discard}); err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				b.ReportAllocs()
				for b.Loop() {
					Info(ctx, "batch received: records=%d bytes=%d", 4, 798)
				}
			})
		}
	}
}
