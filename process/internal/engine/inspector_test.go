package engine

import (
	"errors"
	"stream-eos/process/internal/dedup"
	"stream-eos/process/internal/kafka"
	"testing"
)

func record(seq byte) kafka.Record {
	var r kafka.Record
	r.Value[4] = 1
	r.Value[27] = seq
	return r
}

func TestInspectorBatchReuse(t *testing.T) {
	i, err := NewInspector(0, 10, 4)
	if err != nil {
		t.Fatal(err)
	}
	batch := []kafka.Record{record(1), record(1), record(2)}
	u, d, err := i.Inspect(batch)
	if err != nil || u != 2 || d != 1 {
		t.Fatalf("first: %d/%d %v", u, d, err)
	}
	// Overwrite the same backing storage. Committed IDs must remain independent.
	batch[0], batch[1], batch[2] = record(2), record(3), record(1)
	u, d, err = i.Inspect(batch)
	if err != nil || u != 1 || d != 2 {
		t.Fatalf("second: %d/%d %v", u, d, err)
	}
	u, d, err = i.Inspect(nil)
	if err != nil || u != 0 || d != 0 {
		t.Fatalf("empty: %d/%d %v", u, d, err)
	}
}

func TestInspectorCapacityAbortsWholeBatch(t *testing.T) {
	i, err := NewInspector(1, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = i.Inspect([]kafka.Record{record(1)}); err != nil {
		t.Fatal(err)
	}
	u, d, err := i.Inspect([]kafka.Record{record(2), record(3), record(4)})
	if !errors.Is(err, dedup.ErrCapacity) || u != 0 || d != 0 {
		t.Fatalf("overflow: %d/%d %v", u, d, err)
	}
	// E2 from the failed batch must still be accepted; E1 must remain committed.
	u, d, err = i.Inspect([]kafka.Record{record(1), record(2)})
	if err != nil || u != 1 || d != 1 {
		t.Fatalf("retry: %d/%d %v", u, d, err)
	}
}
