package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"stream-eos/pkg/logger"
	"stream-eos/pkg/protocol"
)

type config struct {
	id, sourceURL, topic string
	brokers              []string
	instruments          []uint32
	maxMessages          uint64
	initialBufferBytes   int
	shutdownTimeout      time.Duration
	logLevel             slog.Level
	logFormat            logger.Format
}

// loadConfig applies optional .env, then process environment, then CLI overrides.
// lookup is injected for deterministic tests; loading never mutates os.Environ.
func loadConfig(path string, args []string, lookup func(string) (string, bool)) (config, error) {
	values := map[string]string{
		"INGESTOR_ID": "A", "SOURCE_URL": "ws://127.0.0.1:8081/ws",
		"KAFKA_BROKERS": "localhost:9092", "KAFKA_TOPIC": "market.ticks.raw",
		"INSTRUMENT_TOKENS": "9910001,9910257", "MAX_MESSAGES": "0",
		"INITIAL_BUFFER_BYTES": "512", "SHUTDOWN_TIMEOUT": "5s", "LOG_LEVEL": "info", "LOG_FORMAT": "json",
	}
	file, err := os.Open(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return config{}, fmt.Errorf("read config: %w", err)
	}
	if err == nil {
		defer file.Close()
		seen := make(map[string]bool)
		scanner := bufio.NewScanner(file)
		for line := 1; scanner.Scan(); line++ {
			text := strings.TrimSpace(scanner.Text())
			if text == "" || strings.HasPrefix(text, "#") {
				continue
			}
			key, value, ok := strings.Cut(text, "=")
			key, value = strings.TrimSpace(key), strings.TrimSpace(value)
			if !ok || key == "" || value == "" {
				return config{}, fmt.Errorf("%s:%d: expected KEY=VALUE", path, line)
			}
			if _, known := values[key]; !known {
				return config{}, fmt.Errorf("%s:%d: unknown key %s", path, line, key)
			}
			if seen[key] {
				return config{}, fmt.Errorf("%s:%d: duplicate key %s", path, line, key)
			}
			seen[key] = true
			if value[0] == '\'' || value[0] == '"' {
				if len(value) < 2 || value[len(value)-1] != value[0] {
					return config{}, fmt.Errorf("%s:%d: unmatched quotes", path, line)
				}
				value = value[1 : len(value)-1]
			}
			values[key] = value
		}
		if err := scanner.Err(); err != nil {
			return config{}, fmt.Errorf("read config: %w", err)
		}
	}
	for key := range values {
		if value, ok := lookup(key); ok {
			values[key] = value
		}
	}
	flags := flag.NewFlagSet("ingestor", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	id := flags.String("id", values["INGESTOR_ID"], "ingestor identity")
	sourceURL := flags.String("url", values["SOURCE_URL"], "WebSocket source URL")
	brokers := flags.String("brokers", values["KAFKA_BROKERS"], "comma-separated Kafka brokers")
	topic := flags.String("topic", values["KAFKA_TOPIC"], "input topic")
	tokens := flags.String("tokens", values["INSTRUMENT_TOKENS"], "comma-separated instrument tokens")
	messages := flags.String("max-messages", values["MAX_MESSAGES"], "stop after N acknowledged market messages")
	initial := flags.String("initial-buffer-bytes", values["INITIAL_BUFFER_BYTES"], "initial reusable buffer capacity")
	shutdown := flags.String("shutdown-timeout", values["SHUTDOWN_TIMEOUT"], "Kafka cleanup deadline")
	level := flags.String("log-level", values["LOG_LEVEL"], "log level")
	format := flags.String("log-format", values["LOG_FORMAT"], "json or text")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments")
	}
	cfg := config{id: strings.TrimSpace(*id), sourceURL: *sourceURL, topic: strings.TrimSpace(*topic), logFormat: logger.Format(*format)}
	if cfg.id == "" || cfg.topic == "" {
		return cfg, fmt.Errorf("identity and topic are required")
	}
	u, err := url.Parse(cfg.sourceURL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return cfg, fmt.Errorf("SOURCE_URL must be a ws:// or wss:// URL")
	}
	for _, broker := range strings.Split(*brokers, ",") {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return cfg, fmt.Errorf("empty Kafka broker")
		}
		cfg.brokers = append(cfg.brokers, broker)
	}
	seenTokens := make(map[uint32]bool)
	for _, raw := range strings.Split(*tokens, ",") {
		n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
		if err != nil || n == 0 {
			return cfg, fmt.Errorf("invalid instrument token %q", raw)
		}
		token := uint32(n)
		if seenTokens[token] {
			return cfg, fmt.Errorf("duplicate instrument token %d", token)
		}
		seenTokens[token] = true
		cfg.instruments = append(cfg.instruments, token)
	}
	if len(cfg.instruments) > 400 {
		return cfg, fmt.Errorf("at most 400 instruments supported")
	}
	cfg.maxMessages, err = strconv.ParseUint(*messages, 10, 64)
	if err != nil {
		return cfg, fmt.Errorf("MAX_MESSAGES: %w", err)
	}
	cfg.initialBufferBytes, err = strconv.Atoi(*initial)
	if err != nil || cfg.initialBufferBytes < 1 || cfg.initialBufferBytes > protocol.MaxMessageBytes {
		return cfg, fmt.Errorf("INITIAL_BUFFER_BYTES must be 1..%d", protocol.MaxMessageBytes)
	}
	cfg.shutdownTimeout, err = time.ParseDuration(*shutdown)
	if err != nil || cfg.shutdownTimeout <= 0 {
		return cfg, fmt.Errorf("SHUTDOWN_TIMEOUT must be a positive duration")
	}
	if err := cfg.logLevel.UnmarshalText([]byte(*level)); err != nil {
		return cfg, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	if cfg.logFormat != logger.JSON && cfg.logFormat != logger.Text {
		return cfg, fmt.Errorf("LOG_FORMAT must be json or text")
	}
	return cfg, nil
}
