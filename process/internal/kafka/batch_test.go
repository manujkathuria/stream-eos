package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func fixtureFetches(count int) kgo.Fetches {
	records := make([]*kgo.Record, count)
	for i := range records {
		records[i] = inputRecord()
		records[i].Offset = int64(i)
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "market.ticks.raw", Partitions: []kgo.FetchPartition{{Partition: 0, Records: records}}}}}}
}
func TestCopyBatchReuse(t *testing.T) {
	c := &Consumer{topic: "market.ticks.raw", partition: 0, pollLimit: 2}
	fetches := fixtureFetches(2)
	storage := make([]Record, 2)
	batch, err := c.copyBatch(fetches, storage)
	if err != nil || len(batch) != 2 || &batch[0] != &storage[0] {
		t.Fatalf("caller storage not used: %v", err)
	}
	retained := batch[1] // Value copy may survive caller storage reuse.
	wire := fetches[0].Topics[0].Partitions[0].Records[0]
	wire.Offset = 42
	wire.Value[4] = 9
	fetches[0].Topics[0].Partitions[0].Records = fetches[0].Topics[0].Partitions[0].Records[:1]
	batch, err = c.copyBatch(fetches, storage)
	if err != nil || len(batch) != 1 || batch[0].Offset != 42 || batch[0].Value[4] != 9 {
		t.Fatal("shorter batch or overwrite failed")
	}
	if retained.Offset != 1 || retained.Value[4] != 7 {
		t.Fatal("retained value changed")
	}
	clear(wire.Value)
	if batch[0].Value[4] != 9 {
		t.Fatal("returned record borrows library data")
	}
}
func TestBatchErrorsReturnNoPartialResults(t *testing.T) {
	c := &Consumer{topic: "market.ticks.raw", pollLimit: 2}
	storage := make([]Record, 2)
	failure := errors.New("broker failure")
	fetches := fixtureFetches(2)
	fetches[0].Topics[0].Partitions[0].Err = failure
	if batch, err := c.copyBatch(fetches, storage); batch != nil || !errors.Is(err, failure) {
		t.Fatalf("fetch error: %v", err)
	}
	fetches = fixtureFetches(2)
	fetches[0].Topics[0].Partitions[0].Records[1].Value = nil
	if batch, err := c.copyBatch(fetches, storage); batch != nil || err == nil {
		t.Fatal("partial result returned")
	}
	if batch, err := c.copyBatch(fixtureFetches(2), storage[:1:1]); batch != nil || !errors.Is(err, ErrBatchCapacity) {
		t.Fatal("capacity not enforced")
	}
	if _, err := c.copyBatch(fixtureFetches(3), make([]Record, 3)); !errors.Is(err, ErrBatchCapacity) {
		t.Fatal("poll bound ignored")
	}
	batch, err := c.copyBatch(nil, storage)
	if err != nil || len(batch) != 0 {
		t.Fatal("empty fetch failed")
	}
}
func TestReadBatchRejectsCapacityBeforePolling(t *testing.T) {
	c := &Consumer{pollLimit: 2} // Nil client proves insufficient storage never polls.
	if _, err := c.ReadBatch(context.Background(), make([]Record, 1)); !errors.Is(err, ErrBatchCapacity) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ReadBatch(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestCopyBatchZeroAllocations(t *testing.T) {
	c := &Consumer{topic: "market.ticks.raw", pollLimit: 100}
	fetches := fixtureFetches(100)
	storage := make([]Record, 100)
	allocations := testing.AllocsPerRun(100, func() {
		batch, err := c.copyBatch(fetches, storage)
		if err != nil || len(batch) != 100 {
			panic("invalid fixture")
		}
	})
	if allocations != 0 {
		t.Fatalf("copy path allocated: %g", allocations)
	}
}
func BenchmarkCopyBatch(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"batch10", 10}, {"batch100", 100}} {
		b.Run(tc.name, func(b *testing.B) {
			c := &Consumer{topic: "market.ticks.raw", pollLimit: tc.size}
			fetches := fixtureFetches(tc.size)
			storage := make([]Record, tc.size)
			b.ReportAllocs()
			b.SetBytes(int64(tc.size * recordPayloadBytes))
			for b.Loop() {
				batch, err := c.copyBatch(fetches, storage)
				if err != nil || batch[len(batch)-1].Offset != int64(tc.size-1) {
					b.Fatal("invalid result")
				}
			}
		})
	}
}

// BenchmarkCopyBatchAllocatingBaseline preserves the former copy loop solely for
// a same-fixture comparison. It excludes polling and network/client allocations.
func BenchmarkCopyBatchAllocatingBaseline(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{{"batch10", 10}, {"batch100", 100}} {
		b.Run(tc.name, func(b *testing.B) {
			fetches := fixtureFetches(tc.size)
			b.ReportAllocs()
			b.SetBytes(int64(tc.size * recordPayloadBytes))
			for b.Loop() {
				records := make([]Record, 0, fetches.NumRecords())
				iter := fetches.RecordIter()
				for !iter.Done() {
					r, err := copyInputRecord(iter.Next(), "market.ticks.raw", 0)
					if err != nil {
						b.Fatal(err)
					}
					records = append(records, r)
				}
				if len(records) != tc.size || records[len(records)-1].Offset != int64(tc.size-1) {
					b.Fatal("invalid result")
				}
			}
		})
	}
}
