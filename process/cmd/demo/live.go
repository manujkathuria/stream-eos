package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const liveInstrumentCount = 10

type liveOptions struct {
	enabled  bool
	duration time.Duration
	inputTPS int
}

func (o liveOptions) validate() error {
	if !o.enabled {
		return nil
	}
	if o.duration < time.Second || o.duration > time.Hour {
		return fmt.Errorf("duration must be 1s..1h")
	}
	if o.inputTPS < liveInstrumentCount || o.inputTPS > 10000000 || o.inputTPS%liveInstrumentCount != 0 {
		return fmt.Errorf("input TPS must be a multiple of 10 between 10 and 10,000,000 (10 events/message)")
	}
	return nil
}

func runLive(ctx context.Context, root, temp, brokers string, topics []string, admin *kgo.Client, ingest func(string, int) (*child, error), options liveOptions) (result error) {
	worker, err := startChild(ctx, filepath.Join(root, "process/bin/process"), temp, filepath.Join(temp, "process-live.log"),
		"-partition", "0", "-brokers", brokers, "-topic", topics[0], "-output-topic", topics[1], "-progress-topic", topics[2], "-changelog-topic", topics[3],
		"-batch-records", "100", "-batch-bytes", "21600", "-dedup-max-ids", "1000000", "-log-level", "warn")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, worker.stop()) }()
	fmt.Printf("Live offer: duration=%s, requested unique input TPS=%d, source=%d messages/sec/connection, 10 events/message, 2 ingestors, 1 processing partition, batch limit=100\n", options.duration, options.inputTPS, options.inputTPS/liveInstrumentCount)
	start := time.Now()
	a, err := ingest("A", 0)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, a.stop()) }()
	b, err := ingest("B", 0)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, b.stop()) }()
	timer := time.NewTimer(time.Until(start.Add(options.duration)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-worker.done:
		return worker.failure("processor exited during live run")
	case <-a.done:
		return a.failure("ingestor A exited during live run")
	case <-b.done:
		return b.failure("ingestor B exited during live run")
	case <-timer.C:
	}
	// Stop both intake paths together; each finishes its bounded shutdown.
	stops := make(chan error, 2)
	go func() { stops <- a.stop() }()
	go func() { stops <- b.stop() }()
	if err = errors.Join(<-stops, <-stops); err != nil {
		return err
	}
	intakeStopped := time.Now()
	target, err := logEnd(ctx, admin, topics[0])
	if err != nil {
		return err
	}
	if target == 0 {
		return fmt.Errorf("no input was stored during the live window")
	}
	if err = waitProgress(ctx, brokers, topics[2], target, worker); err != nil {
		return err
	}
	completed := time.Now()
	if err = worker.stop(); err != nil {
		return err
	}
	elapsed := completed.Sub(start)
	fmt.Printf("Measured: intake=%.3fs (includes startup/shutdown), observed drain=%.3fs, total=%.3fs\n", intakeStopped.Sub(start).Seconds(), completed.Sub(intakeStopped).Seconds(), elapsed.Seconds())
	fmt.Printf("Verifying %d Kafka input records using bounded-memory disk sorting...\n", target)
	counts, err := verifyLive(ctx, temp, brokers, topics, admin, target)
	if err != nil {
		return err
	}
	fmt.Printf("Verified counts: input=%d (A=%d B=%d), unique output=%d, duplicate copies=%d, final checkpoint=%d\n", counts.input, counts.a, counts.b, counts.unique, counts.input-counts.unique, target)
	fmt.Printf("Requested: %d unique events/sec. Achieved live + drain: %.0f Kafka input records/sec, %.0f committed unique outputs/sec\n", options.inputTPS, float64(counts.input)/elapsed.Seconds(), float64(counts.unique)/elapsed.Seconds())
	fmt.Println("Scope: Kafka-stored input to committed output, event IDs and SHA-256 payload/key fingerprints verified. Excludes post-run verification; not maximum capacity or source-loss proof.")
	return nil
}
