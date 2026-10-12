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

	"stream-eos/ingestor/internal/ingest"
	"stream-eos/ingestor/internal/kafka"
	"stream-eos/ingestor/internal/websocket"
	"stream-eos/pkg/logger"
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
	if err := logger.New(logger.Config{Service: "ingestor", Level: cfg.logLevel, Format: cfg.logFormat}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log := slog.Default().With(slog.String("ingestor", cfg.id))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil {
		log.LogAttrs(context.Background(), slog.LevelError, "ingestor stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

// run owns dependencies; the sequential runner owns no cleanup or connections.
func run(ctx context.Context, cfg config, log *slog.Logger) (err error) {
	publisher, err := kafka.New(ctx, kafka.Config{Brokers: cfg.brokers, Topic: cfg.topic, ID: cfg.id})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
		defer cancel()
		err = errors.Join(err, publisher.Shutdown(cleanup))
	}()
	source, err := websocket.Dial(ctx, websocket.Config{URL: cfg.sourceURL, Instruments: cfg.instruments, InitialBufferBytes: cfg.initialBufferBytes, Logger: log})
	if err != nil {
		return err
	}
	defer source.Close()
	runner, err := ingest.New(source, publisher)
	if err != nil {
		return err
	}
	log.LogAttrs(ctx, slog.LevelInfo, "ingestor started", slog.String("topic", cfg.topic), slog.Int("instruments", len(cfg.instruments)))
	stats, err := runner.Run(ctx, cfg.maxMessages)
	log.LogAttrs(context.Background(), slog.LevelInfo, "ingestor finished", slog.Uint64("acknowledged_events", stats.Events), slog.Uint64("messages", stats.Messages))
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
