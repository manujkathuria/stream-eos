package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"stream-eos/pkg/logger"
	"stream-eos/process/internal/kafka"
)

type config struct {
	consumer                                   kafka.ConsumerConfig
	outputTopic, progressTopic, changelogTopic string
	maxIDs                                     int
	maxBatches                                 uint64
	bootstrapLegacy                            bool
	timeout                                    time.Duration
	logLevel                                   slog.Level
	logFormat                                  logger.Format
}

// loadConfig applies defaults, optional .env, process environment, then flags.
func loadConfig(path string, args []string, lookup func(string) (string, bool)) (config, error) {
	values := map[string]string{
		"PARTITION": "-1", "KAFKA_BROKERS": "localhost:9092", "INPUT_TOPIC": "market.ticks.raw",
		"OUTPUT_TOPIC": "market.ticks.processed", "PROGRESS_TOPIC": "market.ticks.progress", "CHANGELOG_TOPIC": "market.ticks.dedup",
		"START_OFFSET": "earliest", "BATCH_RECORDS": "100", "BATCH_BYTES": "21600", "DEDUP_MAX_IDS": "1000000",
		"MAX_BATCHES": "0", "BOOTSTRAP_LEGACY_DEDUP": "false", "RUN_TIMEOUT": "0s", "LOG_LEVEL": "info", "LOG_FORMAT": "json",
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
	flags := flag.NewFlagSet("process", flag.ContinueOnError)
	partition := flags.String("partition", values["PARTITION"], "explicit input partition (required)")
	brokers := flags.String("brokers", values["KAFKA_BROKERS"], "comma-separated brokers")
	inputTopic := flags.String("topic", values["INPUT_TOPIC"], "input topic")
	outputTopic := flags.String("output-topic", values["OUTPUT_TOPIC"], "JSON output topic")
	progressTopic := flags.String("progress-topic", values["PROGRESS_TOPIC"], "compacted progress topic")
	changelogTopic := flags.String("changelog-topic", values["CHANGELOG_TOPIC"], "compacted FIFO changelog")
	startOffset := flags.String("offset", values["START_OFFSET"], "earliest, latest, or exact offset")
	batchRecords := flags.String("batch-records", values["BATCH_RECORDS"], "maximum records per batch")
	batchBytes := flags.String("batch-bytes", values["BATCH_BYTES"], "maximum key/value bytes per batch")
	dedupMaxIDs := flags.String("dedup-max-ids", values["DEDUP_MAX_IDS"], "FIFO capacity per partition")
	maxBatches := flags.String("max-batches", values["MAX_BATCHES"], "stop after N batches; 0 unlimited")
	runTimeout := flags.String("timeout", values["RUN_TIMEOUT"], "optional total run timeout")
	logLevel := flags.String("log-level", values["LOG_LEVEL"], "log level")
	logFormat := flags.String("log-format", values["LOG_FORMAT"], "json or text")
	legacyDefault, err := strconv.ParseBool(values["BOOTSTRAP_LEGACY_DEDUP"])
	if err != nil {
		return config{}, fmt.Errorf("BOOTSTRAP_LEGACY_DEDUP: %w", err)
	}
	legacy := flags.Bool("bootstrap-legacy-dedup", legacyDefault, "rebuild legacy baseline from complete retained input")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments")
	}
	cfg := config{outputTopic: strings.TrimSpace(*outputTopic), progressTopic: strings.TrimSpace(*progressTopic), changelogTopic: strings.TrimSpace(*changelogTopic), bootstrapLegacy: *legacy, logFormat: logger.Format(*logFormat)}
	cfg.consumer.Topic = strings.TrimSpace(*inputTopic)
	for _, raw := range strings.Split(*brokers, ",") {
		broker := strings.TrimSpace(raw)
		if broker == "" {
			return cfg, fmt.Errorf("empty Kafka broker")
		}
		cfg.consumer.Brokers = append(cfg.consumer.Brokers, broker)
	}
	part, err := strconv.ParseInt(*partition, 10, 32)
	if err != nil || part < 0 {
		return cfg, fmt.Errorf("partition is required and must fit a non-negative int32")
	}
	cfg.consumer.Partition = int32(part)
	cfg.consumer.StartOffset, err = parseOffset(*startOffset)
	if err != nil {
		return cfg, err
	}
	cfg.consumer.BatchRecords, err = strconv.Atoi(*batchRecords)
	if err != nil {
		return cfg, fmt.Errorf("BATCH_RECORDS: %w", err)
	}
	cfg.consumer.BatchBytes, err = strconv.Atoi(*batchBytes)
	if err != nil {
		return cfg, fmt.Errorf("BATCH_BYTES: %w", err)
	}
	capacity, err := cfg.consumer.BatchCapacity()
	if err != nil {
		return cfg, err
	}
	cfg.maxIDs, err = strconv.Atoi(*dedupMaxIDs)
	if err != nil || cfg.maxIDs < capacity || uint64(cfg.maxIDs) > math.MaxUint32 {
		return cfg, fmt.Errorf("DEDUP_MAX_IDS must be between batch capacity %d and %d", capacity, uint64(math.MaxUint32))
	}
	topics := []string{cfg.consumer.Topic, cfg.outputTopic, cfg.progressTopic, cfg.changelogTopic}
	for i, topic := range topics {
		if topic == "" {
			return cfg, fmt.Errorf("all four topics are required")
		}
		for _, other := range topics[:i] {
			if topic == other {
				return cfg, fmt.Errorf("all four topics must differ")
			}
		}
	}
	cfg.maxBatches, err = strconv.ParseUint(*maxBatches, 10, 64)
	if err != nil {
		return cfg, fmt.Errorf("MAX_BATCHES: %w", err)
	}
	cfg.timeout, err = time.ParseDuration(*runTimeout)
	if err != nil || cfg.timeout < 0 {
		return cfg, fmt.Errorf("RUN_TIMEOUT must be a non-negative duration")
	}
	if err := cfg.logLevel.UnmarshalText([]byte(*logLevel)); err != nil {
		return cfg, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	if cfg.logFormat != logger.JSON && cfg.logFormat != logger.Text {
		return cfg, fmt.Errorf("LOG_FORMAT must be json or text")
	}
	return cfg, nil
}
func parseOffset(value string) (int64, error) {
	switch value {
	case "earliest":
		return -2, nil
	case "latest":
		return -1, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("offset must be earliest, latest, or non-negative")
	}
	return n, nil
}
