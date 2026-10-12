package engine

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"stream-eos/process/internal/kafka"
	"testing"
)

func TestAppendJSONMatchesReference(t *testing.T) {
	var storage [MaxJSONBytes]byte
	rng := rand.New(rand.NewPCG(1, 2))
	for sample := range 1002 {
		var r kafka.Record
		for n := range r.Value {
			switch sample {
			case 0:
				r.Value[n] = 0
			case 1:
				r.Value[n] = 255
			default:
				r.Value[n] = byte(rng.Uint32())
			}
		}
		expected, err := EncodeJSON(&r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := AppendJSON(storage[:0], &r)
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("sample %d: %v\ngot %s\nwant %s", sample, err, got, expected)
		}
		if &got[0] != &storage[0] {
			t.Fatal("reallocated output")
		}
	}
}

func TestAppendJSONRejectsCapacity(t *testing.T) {
	storage := bytes.Repeat([]byte{99}, MaxJSONBytes-1)
	if _, err := AppendJSON(storage, &kafka.Record{}); !errors.Is(err, ErrJSONCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	for _, v := range storage {
		if v != 99 {
			t.Fatal("modified rejected buffer")
		}
	}
}

func TestAppendJSONAllocations(t *testing.T) {
	var storage [MaxJSONBytes]byte
	var record kafka.Record
	allocations := testing.AllocsPerRun(1000, func() {
		data, err := AppendJSON(storage[:0], &record)
		if err != nil {
			panic(err)
		}
		jsonBenchmarkResult = data
	})
	if allocations != 0 {
		t.Fatalf("allocations=%v", allocations)
	}
}
