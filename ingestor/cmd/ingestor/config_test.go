package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func configFile(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func noEnvironment(string) (string, bool) { return "", false }
func TestConfigPrecedenceAndDefaults(t *testing.T) {
	path := configFile(t, "INGESTOR_ID='file'\nMAX_MESSAGES=2\nSHUTDOWN_TIMEOUT=3s\n")
	lookup := func(key string) (string, bool) {
		if key == "INGESTOR_ID" {
			return "environment", true
		}
		return "", false
	}
	cfg, err := loadConfig(path, []string{"-id", "cli"}, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.id != "cli" || cfg.maxMessages != 2 || cfg.shutdownTimeout != 3*time.Second || len(cfg.instruments) != 2 || cfg.initialBufferBytes != 512 {
		t.Fatalf("config=%+v", cfg)
	}
	cfg, err = loadConfig(path, nil, lookup)
	if err != nil || cfg.id != "environment" {
		t.Fatalf("environment precedence: %+v %v", cfg, err)
	}
	cfg, err = loadConfig(filepath.Join(t.TempDir(), "missing.env"), nil, noEnvironment)
	if err != nil || cfg.id != "A" || cfg.maxMessages != 0 {
		t.Fatalf("optional file defaults: %+v %v", cfg, err)
	}
}
func TestConfigRejectsInvalidValues(t *testing.T) {
	for _, text := range []string{
		"UNKNOWN=x", "INGESTOR_ID=A\nINGESTOR_ID=B", "LOG_LEVEL='info", "INGESTOR_ID=\"\"",
		"SOURCE_URL=http://localhost", "KAFKA_BROKERS=localhost:9092,", "INSTRUMENT_TOKENS=1,1",
		"INSTRUMENT_TOKENS=0", "MAX_MESSAGES=-1", "INITIAL_BUFFER_BYTES=0", "INITIAL_BUFFER_BYTES=77623",
		"SHUTDOWN_TIMEOUT=0s", "LOG_LEVEL=invalid", "LOG_FORMAT=invalid", "not-an-assignment",
	} {
		t.Run(text, func(t *testing.T) {
			if _, err := loadConfig(configFile(t, text), nil, noEnvironment); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	if _, err := loadConfig(filepath.Join(t.TempDir(), "missing.env"), []string{"unexpected"}, noEnvironment); err == nil {
		t.Fatal("positional argument accepted")
	}
}
