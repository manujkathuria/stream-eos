package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func envFile(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestEnvConfig(t *testing.T) {
	path := envFile(t, "# Source configuration\r\n\r\nLISTEN_ADDR = '127.0.0.1:9000'\r\nRATE=50\r\nHISTORY_UPDATES=4096\r\nINSTRUMENT_COUNT=2\r\nDISCONNECT_AFTER=4\r\nLOG_LEVEL=warn\r\nLOG_FORMAT=\"text\"\r\nSHUTDOWN_TIMEOUT=2s\r\n")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "127.0.0.1:9000" || cfg.instrumentCount != 2 || cfg.rate != 50 || cfg.historyUpdates != 4096 || cfg.disconnectAfter != 4 || cfg.logLevel != slog.LevelWarn || cfg.logFormat != "text" || cfg.shutdownTimeout != 2*time.Second {
		t.Fatalf("config=%+v", cfg)
	}
	t.Setenv("RATE", "999")
	cfg, err = loadConfig(envFile(t, "# Defaults\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.instrumentCount != 10 || cfg.rate != 1000 || cfg.historyUpdates != 128 || cfg.listen != "127.0.0.1:8081" || cfg.shutdownTimeout != 5*time.Second || cfg.logLevel != slog.LevelInfo || cfg.logFormat != "json" || cfg.disconnectAfter != 0 {
		t.Fatalf("defaults=%+v", cfg)
	}
	if os.Getenv("RATE") != "999" {
		t.Fatal("loader changed process environment")
	}
}

func TestEnvConfigRejectsInvalidInput(t *testing.T) {
	for _, text := range []string{"INSTRUMENT_COUNT=0", "INSTRUMENT_COUNT=401", "INSTRUMENT_COUNT=no", "HISTORY_UPDATES=0", "HISTORY_UPDATES=-1", "HISTORY_UPDATES=4097", "HISTORY_UPDATES=no", "RATE=0", "RATE=1000001", "RATE=no", "DISCONNECT_AFTER=-1", "SHUTDOWN_TIMEOUT=0s", "SHUTDOWN_TIMEOUT=no", "LOG_LEVEL=invalid", "LOG_FORMAT=invalid", "LISTEN_ADDR=missing-port", "LISTEN_ADDR=localhost:99999", "RATE=", "LOG_FORMAT=\"\"", "RATE=1\nRATE=2", "RTAE=10", "missing equals", "LOG_FORMAT=\"text", "RATE=1 # unsupported inline comment"} {
		t.Run(text, func(t *testing.T) {
			if _, err := loadConfig(envFile(t, text)); err == nil {
				t.Fatalf("accepted %q", text)
			}
		})
	}
	if _, err := loadConfig(filepath.Join(t.TempDir(), "missing.env")); err == nil {
		t.Fatal("missing file accepted")
	}
	if _, err := loadConfig(envFile(t, "LOG_FORMAT="+strings.Repeat("x", 70000))); err == nil {
		t.Fatal("oversized line accepted")
	}
}
