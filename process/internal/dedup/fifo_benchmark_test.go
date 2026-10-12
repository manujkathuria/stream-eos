package dedup

import (
	"fmt"
	"stream-eos/process/internal/protocol"
	"testing"
)

func fullWindow(b *testing.B, capacity, batch int) *Partition {
	b.Helper()
	p, err := New(Config{Partition: 0, MaxIDs: capacity, MaxPending: batch})
	if err != nil {
		b.Fatal(err)
	}
	for start := 0; start < capacity; start += batch {
		if err := p.BeginBatch(); err != nil {
			b.Fatal(err)
		}
		for n := start; n < min(start+batch, capacity); n++ {
			if fresh, err := p.Stage(0, id(1, uint64(n))); err != nil || !fresh {
				b.Fatalf("seed: %v", err)
			}
		}
		if err := p.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return p
}

// Full-window lookup varies the target across retained IDs; setup is excluded.
func BenchmarkFIFOLookup(b *testing.B) {
	for _, capacity := range []int{10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("ids%d", capacity), func(b *testing.B) {
			p := fullWindow(b, capacity, 1000)
			p.BeginBatch()
			n := 0
			b.ReportAllocs()
			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				n = (n + 7919) % capacity
				if fresh, err := p.Stage(0, p.fifo[n]); err != nil || fresh {
					b.Fatalf("lookup: %v", err)
				}
			}
		})
	}
}

// Includes ID construction, staging, and local FIFO eviction/commit at a full
// one-million-ID window. Setup/fill, Kafka, JSON, and recovery are excluded.
// Mixed batches are 50% retained duplicates and 50% new IDs. Map churn is timed.
func BenchmarkFIFOChurn(b *testing.B) {
	for _, size := range []int{100, 1000} {
		for _, mixed := range []bool{false, true} {
			workload := "unique"
			if mixed {
				workload = "mixed"
			}
			b.Run(fmt.Sprintf("%s/batch%d", workload, size), func(b *testing.B) {
				p := fullWindow(b, 1000000, size)
				seq := uint64(0)
				b.ReportAllocs()
				b.ResetTimer()
				for batch := 0; batch < b.N; batch++ {
					if err := p.BeginBatch(); err != nil {
						b.Fatal(err)
					}
					for n := 0; n < size; n++ {
						var event protocol.EventID
						wantFresh := !mixed || n%2 == 0
						if wantFresh {
							seq++
							event = id(2, seq)
						} else {
							event = p.fifo[(p.next+n*7919)%len(p.fifo)]
						}
						if fresh, err := p.Stage(0, event); err != nil || fresh != wantFresh {
							b.Fatalf("stage fresh=%v want=%v err=%v", fresh, wantFresh, err)
						}
					}
					if err := p.Commit(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if c, n := p.Counts(); c != 1000000 || n != 0 {
					b.Fatalf("counts=%d/%d", c, n)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/record")
			})
		}
	}
}

var initializedFIFO *Partition

// Startup reservation only: does not include filling the index or network work.
func BenchmarkFIFOInitialize(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		p, err := New(Config{Partition: 0, MaxIDs: 1000000, MaxPending: 1000})
		if err != nil {
			b.Fatal(err)
		}
		initializedFIFO = p
	}
}
