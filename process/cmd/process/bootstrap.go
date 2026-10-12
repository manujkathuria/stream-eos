package main

import (
	"context"
	"fmt"
	"log/slog"
	"stream-eos/process/internal/engine"
	"stream-eos/process/internal/kafka"
	"time"
)

// Legacy migration establishes a new durable baseline without repeating output.
// It requires complete raw history through the saved checkpoint and explicit opt-in.
func bootstrapDedup(ctx context.Context, publisher *kafka.TransactionPublisher, processor *engine.Processor, cfg kafka.ConsumerConfig, next int64) error {
	migration, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	start, end, err := publisher.InputBounds(migration)
	if err != nil {
		return err
	}
	if start != 0 || end < next {
		return fmt.Errorf("legacy bootstrap requires retained input from offset 0 through %d; available %d..%d", next, start, end)
	}
	if next > 0 {
		cfg.StartOffset = 0
		reader, err := kafka.NewConsumer(migration, cfg)
		if err != nil {
			return err
		}
		defer reader.Close()
		storage := make([]kafka.Record, reader.BatchCapacity())
		position := int64(0)
		for position < next {
			records, err := reader.ReadBatch(migration, storage)
			if err != nil {
				return err
			}
			limit := 0
			for limit < len(records) && records[limit].Offset < next {
				if records[limit].Offset != position+int64(limit) {
					return fmt.Errorf("legacy bootstrap input gap at offset %d", position+int64(limit))
				}
				limit++
			}
			if limit == 0 {
				if len(records) > 0 {
					return fmt.Errorf("missing input before legacy checkpoint")
				}
				continue
			}
			if err := processor.Bootstrap(records[:limit]); err != nil {
				return err
			}
			position = records[limit-1].Offset + 1
		}
	}
	if err := publisher.BootstrapDedup(migration, processor); err != nil {
		return err
	}
	slog.InfoContext(ctx, "legacy dedup baseline committed without output", "next_offset", next)
	return nil
}
