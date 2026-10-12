package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"stream-eos/ingestor/internal/benchtest"
	"stream-eos/ingestor/internal/kafka"
	"stream-eos/ingestor/internal/websocket"
	"stream-eos/pkg/protocol"
)

type memorySource struct {
	message []byte
	reads   int
}

func (s *memorySource) Read(context.Context) ([]byte, error) { s.reads++; return s.message, nil }

type countedPublisher struct{ calls, events, packets int }

func (p *countedPublisher) Publish(_ context.Context, events []protocol.Event) error {
	if len(events) != p.packets {
		return fmt.Errorf("incorrect decoded count")
	}
	for _, event := range events {
		if len(event.Quote) != 184 || event.ID[0] != 7 {
			return fmt.Errorf("decoded identity/quote invalid")
		}
	}
	p.calls++
	p.events += len(events)
	return nil
}

// Measures production Run and Decode with memory source/publisher boundaries.
// No WebSocket, record construction or Kafka I/O. Counts are not broker acks.
func BenchmarkIngestorLoop(b *testing.B) {
	for _, packets := range []int{2, 400} {
		b.Run(benchtest.Name(packets), func(b *testing.B) {
			source := &memorySource{message: benchtest.Message(packets)}
			publisher := &countedPublisher{packets: packets}
			runner, err := New(source, publisher)
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.SetBytes(int64(len(source.message)))
			b.ResetTimer()
			stats, err := runner.Run(ctx, uint64(b.N))
			b.StopTimer()
			if err != nil || stats.Messages != uint64(b.N) || stats.Events != uint64(b.N*packets) || source.reads != b.N || publisher.calls != b.N || publisher.events != b.N*packets {
				b.Fatalf("stats=%+v err=%v", stats, err)
			}
		})
	}
}

// Measures the connected production Run: real Source.Read, DecodeInto, buildRecords,
// franz-go ProduceSync and broker acks. Post-run storage verification is excluded.
func BenchmarkIngestorAcknowledged(b *testing.B) {
	if os.Getenv("STREAM_EOS_KAFKA_BENCH") != "1" {
		b.Skip("set STREAM_EOS_KAFKA_BENCH=1; isolated Kafka topics")
	}
	for _, packets := range []int{2, 400} {
		b.Run(benchtest.Name(packets), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			const warmup = 20
			topic := benchtest.Topic(b, ctx)
			producer, err := kafka.New(ctx, kafka.Config{Brokers: []string{"localhost:9092"}, Topic: topic, ID: "benchmark"})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := producer.Shutdown(cleanup); err != nil {
					b.Error(err)
				}
			})
			socket := benchtest.NewSocket(b, ctx, packets, warmup, b.N)
			source, err := websocket.Dial(ctx, websocket.Config{URL: socket.URL, Instruments: benchtest.Instruments(packets), Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(source.Close)
			runner, err := New(source, producer)
			if err != nil {
				b.Fatal(err)
			}
			if stats, err := runner.Run(ctx, warmup); err != nil || stats.Messages != warmup {
				b.Fatalf("warmup=%+v err=%v", stats, err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(packets * 216))
			b.ResetTimer()
			socket.Start()
			stats, err := runner.Run(ctx, uint64(b.N))
			if err != nil {
				b.Fatal(err)
			}
			if err := socket.Wait(ctx); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if stats.Messages != uint64(b.N) || stats.Events != uint64(b.N*packets) {
				b.Fatalf("stats=%+v", stats)
			}
			b.ReportMetric(float64(stats.Events)/b.Elapsed().Seconds(), "acked-events/s")
			benchtest.VerifyStored(b, ctx, topic, warmup, b.N, packets)
		})
	}
}
