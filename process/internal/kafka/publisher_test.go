package kafka

import (
	"bytes"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestPrepareJSONReuse(t *testing.T) {
	p := &Publisher{topic: "output", jsonRecords: make([]kgo.Record, 2), jsonPointers: make([]*kgo.Record, 2)}
	batch := []JSONRecord{{Key: [4]byte{1}, Value: []byte(`{"id":1}`)}, {Key: [4]byte{2}, Value: []byte(`{"id":2}`)}}
	records, err := p.prepareJSON(batch)
	if err != nil || len(records) != 2 || records[0] == records[1] {
		t.Fatalf("prepare: %v", err)
	}
	if records[0].Topic != "output" || !bytes.Equal(records[1].Value, batch[1].Value) {
		t.Fatal("corrupt output")
	}
	// The client mutates metadata; the next preparation must reset it.
	records[0].Offset = 123
	records[0].Partition = 9
	first := records[0]
	records, err = p.prepareJSON(batch[:1])
	if err != nil || records[0] != first || records[0].Offset != 0 || records[0].Partition != 0 {
		t.Fatal("stale metadata")
	}
	if _, err := p.prepareJSON(make([]JSONRecord, 3)); err == nil {
		t.Fatal("accepted oversized batch")
	}
	if n := testing.AllocsPerRun(1000, func() {
		if _, err := p.prepareJSON(batch); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatalf("allocations=%v", n)
	}
}
