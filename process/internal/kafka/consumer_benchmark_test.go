package kafka

import (
	"context"
	"os"
	"testing"
	"time"
)

// BenchmarkReadBatch replays a fixed, read-only 100-record range from partition 1.
// Each operation starts a fresh client outside the timer, then measures reading
// the complete range. This includes cold fetch waiting and client background work;
// it is a local replay baseline, not a steady-state or end-to-end EOS benchmark.
func BenchmarkReadBatch(b *testing.B) {
	if os.Getenv("STREAM_EOS_KAFKA_BENCH") != "1" {
		b.Skip("set STREAM_EOS_KAFKA_BENCH=1 with existing data in partition 1")
	}
	const count = 100
	cfg := consumerConfig()
	cfg.Partition = 1
	cfg.StartOffset = 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	warm, err := NewConsumer(ctx, cfg)
	if err != nil {
		cancel()
		b.Fatal(err)
	}
	expected := make([]Record, 0, count)
	warmBatch := make([]Record, warm.BatchCapacity())
	for len(expected) < count {
		warm.pollLimit = count - len(expected)
		records, err := warm.ReadBatch(ctx, warmBatch)
		if err != nil {
			warm.Close()
			cancel()
			b.Fatal(err)
		}
		expected = append(expected, records...)
	}
	warm.Close()
	cancel()
	if expected[0].Offset != 0 || expected[count-1].Offset != 99 {
		b.Fatal("benchmark requires the fixed retained offset range 0..99")
	}
	for _, tc := range []struct {
		name  string
		batch int
	}{{"batch10", 10}, {"batch100", 100}} {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			cfg.BatchRecords = tc.batch
			cfg.BatchBytes = tc.batch * recordPayloadBytes
			b.ReportAllocs()
			b.SetBytes(count * recordPayloadBytes)
			observed := make([]Record, 0, count)
			batch := make([]Record, tc.batch)
			var polls int64
			for n := 0; n < b.N; n++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				consumer, err := NewConsumer(ctx, cfg)
				if err != nil {
					cancel()
					b.Fatal(err)
				}
				observed = observed[:0]
				b.StartTimer()
				for len(observed) < count {
					consumer.pollLimit = min(tc.batch, count-len(observed))
					records, err := consumer.ReadBatch(ctx, batch)
					if err != nil {
						b.StopTimer()
						consumer.Close()
						cancel()
						b.Fatal(err)
					}
					observed = append(observed, records...)
					polls++
				}
				b.StopTimer()
				consumer.Close()
				cancel()
				for i := range expected {
					if observed[i] != expected[i] {
						b.Fatalf("record mismatch at index %d", i)
					}
				}
			}
			b.ReportMetric(float64(polls)/float64(b.N), "polls/op")
			b.ReportMetric(float64(b.N*count)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
