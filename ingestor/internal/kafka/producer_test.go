package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"stream-eos/ingestor/internal/benchtest"
	"stream-eos/pkg/protocol"
	"testing"
)

func TestRecordsOwnBytesAndPreserveWireFormat(t *testing.T) {
	quote := make([]byte, 184)
	binary.BigEndian.PutUint32(quote[:4], 9910001)
	var id [24]byte
	id[0] = 7
	binary.BigEndian.PutUint64(id[16:], 401)
	p := newProducer(nil, "input", "B")
	records := p.buildRecords([]protocol.Event{{ID: id, Instrument: 9910001, Quote: quote}})
	clear(quote)
	r := records[0]
	if r.Topic != "input" || len(r.Key) != 4 || binary.BigEndian.Uint32(r.Key) != 9910001 || len(r.Value) != 212 || string(r.Value[:4]) != "SEV1" || !bytes.Equal(r.Value[4:28], id[:]) || binary.BigEndian.Uint32(r.Value[28:32]) != 9910001 {
		t.Fatal("record corrupted by reuse or encoding")
	}
	if len(r.Headers) != 1 || r.Headers[0].Key != "ingestor" || string(r.Headers[0].Value) != "B" {
		t.Fatal("missing provenance")
	}
}

// Measures only production record construction, not broker publication.
func BenchmarkBuildRecords(b *testing.B) {
	for _, packets := range []int{2, 400} {
		b.Run(benchtest.Name(packets), func(b *testing.B) {
			events, err := protocol.Decode(benchtest.Message(packets))
			if err != nil {
				b.Fatal(err)
			}
			producer := newProducer(nil, "input", "A")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				records := producer.buildRecords(events)
				if len(records) != packets {
					b.Fatal("missing records")
				}
			}
		})
	}
}

func TestRecordStorageReuseAndMetadataReset(t *testing.T) {
	p := newProducer(nil, "input", "A")
	events, err := protocol.Decode(benchtest.Message(400))
	if err != nil {
		t.Fatal(err)
	}
	records := p.buildRecords(events)
	first, key, value := records[0], &records[0].Key[0], &records[0].Value[0]
	records[0].Offset = 99
	records[0].Partition = 9
	records[0].Headers[0].Key = "changed"
	for _, size := range []int{2, 400, 1} {
		records = p.buildRecords(events[:size])
		if len(records) != size || records[0] != first || &records[0].Key[0] != key || &records[0].Value[0] != value {
			t.Fatal("batch storage replaced")
		}
		if records[0].Offset != 0 || records[0].Partition != 0 || records[0].Headers[0].Key != "ingestor" {
			t.Fatal("stale client metadata")
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() { p.buildRecords(events) }); allocations != 0 {
		t.Fatalf("allocations=%v", allocations)
	}
}
func TestProducerRejectsOverlappingAndClosedUse(t *testing.T) {
	p := newProducer(nil, "input", "A")
	events, _ := protocol.Decode(benchtest.Message(2))
	p.state.Store(producerPublishing)
	if err := p.Publish(context.Background(), events); !errors.Is(err, ErrProducerBusy) {
		t.Fatal(err)
	}
	if err := p.Shutdown(context.Background()); !errors.Is(err, ErrProducerBusy) {
		t.Fatal(err)
	}
	p.state.Store(producerClosed)
	if err := p.Publish(context.Background(), events); !errors.Is(err, ErrProducerClosed) {
		t.Fatal(err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
