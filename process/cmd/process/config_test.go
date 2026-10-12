package main

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("PARTITION=1\nBATCH_RECORDS=20\nLOG_FORMAT=text\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"PARTITION": "2", "BATCH_RECORDS": "30", "RUN_TIMEOUT": "2s"}
	cfg, err := loadConfig(path, []string{"-partition", "0", "-batch-records", "10"}, func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.consumer.Partition != 0 || cfg.consumer.BatchRecords != 10 || cfg.timeout != 2*time.Second || cfg.logFormat != "text" {
		t.Fatalf("config=%+v", cfg)
	}
}

func TestInvalidConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), ".env")
	for _, args := range [][]string{
		{}, {"-partition", "-1"}, {"-partition", "2147483648"},
		{"-partition", "0", "-output-topic", "market.ticks.raw"},
		{"-partition", "0", "-batch-records", "0"}, {"-partition", "0", "-batch-bytes", "215"},
		{"-partition", "0", "-dedup-max-ids", "99"}, {"-partition", "0", "-dedup-max-ids", "4294967296"},
		{"-partition", "0", "-brokers", "localhost:9092,"},
		{"-partition", "0", "-timeout", "-1s"}, {"-partition", "0", "-offset", "-3"},
		{"-partition", "0", "-max-batches", "-1"}, {"-partition", "0", "-log-format", "xml"},
		{"-partition", "0", "-log-level", "nope"}, {"-partition", "0", "extra"},
	} {
		if _, err := loadConfig(missing, args, func(string) (string, bool) { return "", false }); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if _, err := loadConfig(missing, []string{"-h"}, func(string) (string, bool) { return "", false }); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
}

func TestMalformedEnv(t *testing.T) {
	for _, contents := range []string{"PARTITION=0\nPARTITION=1\n", "UNKNOWN=1\n", "PARTITION='0\n", "PARTITION\n"} {
		path := filepath.Join(t.TempDir(), ".env")
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(path, nil, func(string) (string, bool) { return "", false }); err == nil {
			t.Fatalf("accepted %q", contents)
		}
	}
}
