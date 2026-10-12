package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"stream-eos/pkg/logger"
	"stream-eos/websocket-source/feed"
)

type config struct {
	listen          string
	logLevel        slog.Level
	logFormat       logger.Format
	shutdownTimeout time.Duration
	disconnectAfter uint64
	rate            int
	historyUpdates  int
	instrumentCount int
}

// loadConfig reads only this service's .env file; it does not mutate process
// environment variables or execute shell syntax. Omitted keys use defaults.
func loadConfig(path string) (config, error) {
	cfg := config{listen: "127.0.0.1:8081", logLevel: slog.LevelInfo, logFormat: logger.JSON, shutdownTimeout: 5 * time.Second, rate: 1000, historyUpdates: feed.DefaultHistoryUpdates, instrumentCount: feed.DefaultInstrumentCount}
	file, err := os.Open(path)
	if err != nil {
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	defer file.Close()
	seen := make(map[string]bool, 6)
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		key, value, ok := strings.Cut(text, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return cfg, fmt.Errorf("%s:%d: expected KEY=VALUE", path, line)
		}
		if seen[key] {
			return cfg, fmt.Errorf("%s:%d: duplicate key %s", path, line, key)
		}
		seen[key] = true
		if value[0] == '\'' || value[0] == '"' {
			if len(value) < 2 || value[len(value)-1] != value[0] {
				return cfg, fmt.Errorf("%s:%d: unmatched quotes", path, line)
			}
			value = value[1 : len(value)-1]
		}
		var err error
		switch key {
		case "LISTEN_ADDR":
			cfg.listen = value
		case "INSTRUMENT_COUNT":
			cfg.instrumentCount, err = strconv.Atoi(value)
		case "HISTORY_UPDATES":
			cfg.historyUpdates, err = strconv.Atoi(value)
		case "RATE":
			cfg.rate, err = strconv.Atoi(value)
		case "DISCONNECT_AFTER":
			cfg.disconnectAfter, err = strconv.ParseUint(value, 10, 64)
		case "SHUTDOWN_TIMEOUT":
			cfg.shutdownTimeout, err = time.ParseDuration(value)
		case "LOG_LEVEL":
			err = cfg.logLevel.UnmarshalText([]byte(value))
		case "LOG_FORMAT":
			cfg.logFormat = logger.Format(value)
		default:
			return cfg, fmt.Errorf("%s:%d: unknown key %s", path, line, key)
		}
		if err != nil {
			return cfg, fmt.Errorf("%s:%d: invalid %s: %w", path, line, key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	_, port, err := net.SplitHostPort(cfg.listen)
	if err != nil {
		return cfg, fmt.Errorf("invalid LISTEN_ADDR: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return cfg, fmt.Errorf("LISTEN_ADDR requires a port between 0 and 65535")
	}
	if cfg.rate < 1 || cfg.rate > 1000000 {
		return cfg, fmt.Errorf("RATE must be between 1 and 1000000")
	}
	if cfg.instrumentCount < 1 || cfg.instrumentCount > 400 {
		return cfg, fmt.Errorf("INSTRUMENT_COUNT must be between 1 and 400")
	}
	if cfg.historyUpdates < 1 || cfg.historyUpdates > feed.MaxHistoryUpdates {
		return cfg, fmt.Errorf("HISTORY_UPDATES must be between 1 and %d", feed.MaxHistoryUpdates)
	}
	if cfg.shutdownTimeout <= 0 {
		return cfg, fmt.Errorf("SHUTDOWN_TIMEOUT must be positive")
	}
	if cfg.logFormat != logger.JSON && cfg.logFormat != logger.Text {
		return cfg, fmt.Errorf("LOG_FORMAT must be json or text")
	}
	return cfg, nil
}
