package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"stream-eos/pkg/logger"
	"stream-eos/process/internal/engine"
	"stream-eos/process/internal/kafka"
)

func main() {
	cfg, err := loadConfig(".env", os.Args[1:], os.LookupEnv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := logger.New(logger.Config{Service: "process", Level: cfg.logLevel, Format: cfg.logFormat}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}
	if err := run(ctx, cfg, slog.Default()); err != nil {
		slog.Error("process stopped", "error", err)
		os.Exit(1)
	}
}

// run owns connections; Process preserves the existing transactional boundary.
func run(ctx context.Context, settings config, log *slog.Logger) error {
	cfg := settings.consumer
	capacity, err := cfg.BatchCapacity()
	if err != nil {
		return err
	}
	if settings.maxIDs < capacity {
		return fmt.Errorf("dedup-max-ids must be at least effective batch capacity %d", capacity)
	}
	publisher, err := kafka.NewTransactionPublisher(ctx, kafka.TransactionConfig{Brokers: cfg.Brokers, InputTopic: cfg.Topic, OutputTopic: settings.outputTopic, ProgressTopic: settings.progressTopic, ChangelogTopic: settings.changelogTopic, DedupCapacity: settings.maxIDs, Partition: cfg.Partition, BatchCapacity: capacity})
	if err != nil {
		return err
	}
	defer publisher.Close()
	next, found, err := publisher.LoadProgress(ctx)
	if err != nil {
		return err
	}
	processor, err := engine.NewProcessor(cfg.Partition, settings.maxIDs, capacity, publisher)
	if err != nil {
		return err
	}
	if publisher.NeedsBootstrap() {
		if !settings.bootstrapLegacy {
			return fmt.Errorf("legacy checkpoint has no dedup state; use -bootstrap-legacy-dedup with complete retained input history")
		}
		if err := bootstrapDedup(ctx, publisher, processor, cfg, next); err != nil {
			return err
		}
	} else {
		if err := publisher.RestoreDedup(ctx, processor); err != nil {
			return err
		}
	}
	if found {
		cfg.StartOffset = next
	}
	consumer, err := kafka.NewConsumer(ctx, cfg)
	if err != nil {
		return err
	}
	defer consumer.Close()
	log.LogAttrs(ctx, slog.LevelInfo, "process started", slog.String("topic", cfg.Topic), slog.Int("partition", int(cfg.Partition)), slog.Int64("start_offset", cfg.StartOffset), slog.Bool("restored", found))
	batch := make([]kafka.Record, consumer.BatchCapacity())
	var read uint64
	for settings.maxBatches == 0 || read < settings.maxBatches {
		records, err := consumer.ReadBatch(ctx, batch)
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return nil
			}
			return err
		}
		if len(records) == 0 {
			continue
		}
		unique, duplicates, err := processor.Process(ctx, records)
		if err != nil {
			return err
		}
		first, last := records[0], records[len(records)-1]
		log.LogAttrs(ctx, slog.LevelDebug, "batch committed", slog.Int("records", len(records)), slog.Int("unique", unique), slog.Int("duplicates", duplicates), slog.Int64("first_offset", first.Offset), slog.Int64("last_offset", last.Offset))
		read++
	}
	return nil
}
