package dedup

import (
	"encoding/binary"
	"fmt"
	"testing"

	"stream-eos/process/internal/kafka"
	"stream-eos/process/internal/protocol"
)

type recordFixture struct {
	records   []kafka.Record
	committed []protocol.EventID
	unique    int
}

// Fixtures use exact IDs, not payload hashes. Mixed input is 25% new IDs, 25%
// repeated pending IDs and 50% committed duplicates. Descending/interleaved
// sequences exercise valid out-of-order input. Record creation is never timed.
func makeRecordFixture(size int, workload string) recordFixture {
	f := recordFixture{records: make([]kafka.Record, size)}
	const retained = 10000
	if workload != "unique" {
		f.committed = make([]protocol.EventID, retained)
		for i := range f.committed {
			f.committed[i] = id(1, uint64(i+1))
		}
	}
	for i := range f.records {
		var event protocol.EventID
		switch workload {
		case "unique":
			event = id(2, uint64(size-i))
			f.unique++
		case "duplicates":
			event = f.committed[(i*37)%retained]
		case "mixed":
			switch i % 4 {
			case 0, 1:
				event = id(2, uint64(size-i/4))
				if i%4 == 0 {
					f.unique++
				}
			default:
				event = f.committed[(i*37)%retained]
			}
		default:
			panic("unknown workload")
		}
		r := &f.records[i]
		r.Offset = int64(i)
		copy(r.Value[:4], "SEV1")
		copy(r.Value[4:28], event[:])
		binary.BigEndian.PutUint32(r.Key[:], 9910001)
		binary.BigEndian.PutUint32(r.Value[28:32], 9910001)
	}
	return f
}

// BenchmarkRecordDedup measures extraction and staging separately from local
// commit/abort. Map/state setup and fixture construction are excluded. Each sample
// starts from the same committed cardinality; Commit includes insertion and FIFO
// bookkeeping. No Kafka, changelog, Redis, or durable transaction is involved.
// Use a fixed iteration count: timer transitions intentionally surround each batch.
func BenchmarkRecordDedup(b *testing.B) {
	for _, size := range []int{100, 1000} {
		for _, workload := range []string{"unique", "duplicates", "mixed"} {
			fixture := makeRecordFixture(size, workload)
			for _, scope := range []string{"stage", "abort", "commit"} {
				b.Run(fmt.Sprintf("%s/%s/batch%d", scope, workload, size), func(b *testing.B) {
					b.StopTimer()
					p, err := New(Config{Partition: 0, MaxIDs: len(fixture.committed) + size, MaxPending: size})
					if err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.SetBytes(int64(size * 216))
					for n := 0; n < b.N; n++ {
						// Rebuild the preallocated initial window outside timing. Local
						// insertion and FIFO bookkeeping remain inside the measured commit.
						p.committed = make(map[protocol.EventID]struct{}, p.maxIDs)
						p.next = 0
						for _, event := range fixture.committed {
							p.committed[event] = struct{}{}
							p.fifo[p.next] = event
							p.next = (p.next + 1) % len(p.fifo)
						}
						clear(p.pending)
						p.pendingOrder = p.pendingOrder[:0]
						p.active = false
						if scope == "stage" {
							if err := p.BeginBatch(); err != nil {
								b.Fatal(err)
							}
						}
						b.StartTimer()
						if scope != "stage" {
							if err := p.BeginBatch(); err != nil {
								b.Fatal(err)
							}
						}
						unique := 0
						for i := range fixture.records {
							new, err := p.Stage(0, fixture.records[i].EventID())
							if err != nil {
								b.Fatal(err)
							}
							if new {
								unique++
							}
						}
						switch scope {
						case "abort":
							if err := p.Abort(); err != nil {
								b.Fatal(err)
							}
						case "commit":
							if err := p.Commit(); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						if unique != fixture.unique {
							b.Fatalf("unique=%d want=%d", unique, fixture.unique)
						}
						committed, pending := p.Counts()
						wantCommitted, wantPending := len(fixture.committed), fixture.unique
						if scope == "commit" {
							wantCommitted += fixture.unique
							wantPending = 0
						}
						if scope == "abort" {
							wantPending = 0
						}
						if committed != wantCommitted || pending != wantPending {
							b.Fatalf("state=%d/%d want=%d/%d", committed, pending, wantCommitted, wantPending)
						}
						if scope == "stage" {
							if err := p.Abort(); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/record")
					b.ReportMetric(float64(fixture.unique), "unique/batch")
				})
			}
		}
	}
}

// Independently validate fixture classifications using a simple exact set, then
// assert local commit/abort replay semantics on those same records.
func TestRecordDedupFixtures(t *testing.T) {
	for _, size := range []int{100, 1000} {
		for _, workload := range []string{"unique", "duplicates", "mixed"} {
			t.Run(fmt.Sprintf("%s/%d", workload, size), func(t *testing.T) {
				f := makeRecordFixture(size, workload)
				seen := make(map[protocol.EventID]bool)
				for _, event := range f.committed {
					seen[event] = true
				}
				expected := make([]bool, len(f.records))
				unique := 0
				for i := range f.records {
					event := f.records[i].EventID()
					expected[i] = !seen[event]
					if expected[i] {
						unique++
					}
					seen[event] = true
				}
				if unique != f.unique {
					t.Fatalf("fixture expected %d actual %d", f.unique, unique)
				}
				p, _ := New(Config{Partition: 0, MaxIDs: len(f.committed) + size, MaxPending: size})
				for _, event := range f.committed {
					p.committed[event] = struct{}{}
					p.fifo[p.next] = event
					p.next = (p.next + 1) % len(p.fifo)
				}
				for pass := 0; pass < 2; pass++ {
					p.BeginBatch()
					for i := range f.records {
						got, err := p.Stage(0, f.records[i].EventID())
						if err != nil || got != expected[i] {
							t.Fatalf("pass=%d record=%d new=%v err=%v", pass, i, got, err)
						}
					}
					if pass == 0 {
						p.Abort()
					} else {
						p.Commit()
					}
				}
				p.BeginBatch()
				for i := range f.records {
					got, err := p.Stage(0, f.records[i].EventID())
					if err != nil || got {
						t.Fatalf("committed replay %d new=%v err=%v", i, got, err)
					}
				}
				p.Abort()
			})
		}
	}
}
