package kafka

import (
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"stream-eos/ingestor/internal/benchtest"
	"stream-eos/pkg/protocol"
)

// Includes application record construction, client work and synchronous broker
// acknowledgements; excludes decoding, WebSocket, setup and stored-data checks.
func BenchmarkPublishAcknowledged(b *testing.B) {
	if os.Getenv("STREAM_EOS_KAFKA_BENCH") != "1" {
		b.Skip("set STREAM_EOS_KAFKA_BENCH=1; isolated Kafka topics")
	}
	for _, packets := range []int{2, 400} {
		b.Run(benchtest.Name(packets), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			const warmup = 20
			topic := benchtest.Topic(b, ctx)
			producer, err := New(ctx, Config{Brokers: []string{"localhost:9092"}, Topic: topic, ID: "benchmark"})
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
			events, err := protocol.Decode(benchtest.Message(packets))
			if err != nil {
				b.Fatal(err)
			}
			publish := func(batch int) {
				for i := range events {
					binary.BigEndian.PutUint64(events[i].ID[16:], uint64(batch)*400+uint64(i)+1)
				}
				if err := producer.Publish(ctx, events); err != nil {
					b.Fatal(err)
				}
			}
			for batch := range warmup {
				publish(batch)
			}
			b.ReportAllocs()
			b.SetBytes(int64(packets * 216))
			b.ResetTimer()
			for batch := warmup; batch < warmup+b.N; batch++ {
				publish(batch)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N*packets)/b.Elapsed().Seconds(), "acked-events/s")
			benchtest.VerifyStored(b, ctx, topic, warmup, b.N, packets)
		})
	}
}
