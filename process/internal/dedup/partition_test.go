package dedup

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"testing"

	"stream-eos/process/internal/protocol"
)

func id(run byte, seq uint64) protocol.EventID {
	var value protocol.EventID
	value[0] = run
	binary.BigEndian.PutUint64(value[16:], seq)
	return value
}
func index(t *testing.T, maxIDs, maxPending int) *Partition {
	t.Helper()
	p, err := New(Config{Partition: 1, MaxIDs: maxIDs, MaxPending: maxPending})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func stage(t *testing.T, p *Partition, event protocol.EventID, want bool) {
	t.Helper()
	got, err := p.Stage(1, event)
	if err != nil || got != want {
		t.Fatalf("new=%v want=%v err=%v", got, want, err)
	}
}
func TestPendingAndCommittedDuplicates(t *testing.T) {
	p := index(t, 10, 4)
	if err := p.BeginBatch(); err != nil {
		t.Fatal(err)
	}
	stage(t, p, id(1, 100), true)
	stage(t, p, id(1, 100), false)
	stage(t, p, id(1, 99), true)  // Out-of-order unseen event must survive.
	stage(t, p, id(2, 100), true) // Restarted source reuses sequence, not identity.
	if c, n := p.Counts(); c != 0 || n != 3 {
		t.Fatalf("counts %d %d", c, n)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	p.BeginBatch()
	stage(t, p, id(1, 100), false)
	stage(t, p, id(1, 99), false)
	stage(t, p, id(1, 101), true)
	p.Commit()
	if c, n := p.Counts(); c != 4 || n != 0 {
		t.Fatalf("counts %d %d", c, n)
	}
}
func TestAbortAllowsReplayAndKeepsCommittedIDs(t *testing.T) {
	p := index(t, 10, 4)
	p.BeginBatch()
	stage(t, p, id(1, 1), true)
	p.Commit()
	p.BeginBatch()
	stage(t, p, id(1, 2), true)
	p.Abort()
	p.BeginBatch()
	stage(t, p, id(1, 1), false)
	stage(t, p, id(1, 2), true)
	p.Abort()
	p.BeginBatch()
	stage(t, p, id(1, 2), true)
}
func TestLifecycleAndPartitionIsolation(t *testing.T) {
	p := index(t, 10, 4)
	if _, err := p.Stage(1, id(1, 1)); !errors.Is(err, ErrNoBatch) {
		t.Fatal(err)
	}
	if err := p.Commit(); !errors.Is(err, ErrNoBatch) {
		t.Fatal(err)
	}
	if err := p.Abort(); !errors.Is(err, ErrNoBatch) {
		t.Fatal(err)
	}
	p.BeginBatch()
	if err := p.BeginBatch(); !errors.Is(err, ErrBatchActive) {
		t.Fatal(err)
	}
	if _, err := p.Stage(0, id(1, 1)); !errors.Is(err, ErrPartition) {
		t.Fatal(err)
	}
	stage(t, p, id(1, 1), true)
	p.Commit()
	other := index(t, 10, 4)
	other.BeginBatch()
	stage(t, other, id(1, 1), true)
}
func TestPendingCapacityDoesNotEvict(t *testing.T) {
	p := index(t, 2, 1)
	p.BeginBatch()
	stage(t, p, id(1, 1), true)
	if _, err := p.Stage(1, id(1, 2)); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	p.Commit()
	p.BeginBatch()
	stage(t, p, id(1, 2), true)
	p.Commit()
	p.BeginBatch()
	stage(t, p, id(1, 3), true)
	stage(t, p, id(1, 1), false) // Old IDs remain protected while output is pending.
	p.Abort()
	p.BeginBatch()
	stage(t, p, id(1, 1), false)
	stage(t, p, id(1, 2), false)
	p.Abort()
}

func TestFIFOOrderWraparoundAndNoRefresh(t *testing.T) {
	p := index(t, 3, 3)
	p.BeginBatch()
	for _, seq := range []uint64{20, 10, 30} {
		stage(t, p, id(1, seq), true)
	}
	p.Commit()
	p.BeginBatch()
	stage(t, p, id(1, 20), false) // Duplicate must not refresh the oldest position.
	stage(t, p, id(1, 40), true)
	p.Commit()
	p.BeginBatch()
	stage(t, p, id(1, 10), false)
	stage(t, p, id(1, 30), false)
	stage(t, p, id(1, 40), false)
	stage(t, p, id(1, 20), true) // Evicted copy is accepted again.
	stage(t, p, id(1, 50), true)
	p.Commit()
	p.BeginBatch()
	stage(t, p, id(1, 40), false)
	stage(t, p, id(1, 20), false)
	stage(t, p, id(1, 50), false)
	stage(t, p, id(1, 10), true)
	stage(t, p, id(1, 30), true)
	p.Abort()
	if c, n := p.Counts(); c != 3 || n != 0 {
		t.Fatalf("counts=%d/%d", c, n)
	}
}

func TestFIFOOneEntry(t *testing.T) {
	p := index(t, 1, 1)
	for seq := uint64(1); seq <= 10; seq++ {
		p.BeginBatch()
		stage(t, p, id(1, seq), true)
		p.Commit()
		p.BeginBatch()
		stage(t, p, id(1, seq), false)
		p.Abort()
	}
	p.BeginBatch()
	stage(t, p, id(1, 1), true)
	p.Abort()
}

func TestInvalidConfiguration(t *testing.T) {
	for _, cfg := range []Config{{Partition: -1, MaxIDs: 1, MaxPending: 1}, {MaxIDs: 0, MaxPending: 1}, {MaxIDs: 2, MaxPending: 0}, {MaxIDs: 1, MaxPending: 2}} {
		if _, err := New(cfg); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}
func TestIdentityIsCopied(t *testing.T) {
	p := index(t, 2, 2)
	p.BeginBatch()
	event := id(1, 1)
	stage(t, p, event, true)
	event[0] = 9
	stage(t, p, id(1, 1), false)
	p.Commit()
	fresh := index(t, 2, 2)
	fresh.BeginBatch()
	stage(t, fresh, id(1, 1), true) // No recovery yet.
}
func BenchmarkDuplicateLookup(b *testing.B) {
	p, _ := New(Config{Partition: 0, MaxIDs: 10000, MaxPending: 10000})
	p.BeginBatch()
	for n := uint64(0); n < 10000; n++ {
		p.Stage(0, id(1, n))
	}
	p.Commit()
	p.BeginBatch()
	target := id(1, 5000)
	b.ReportAllocs()
	for b.Loop() {
		new, err := p.Stage(0, target)
		if err != nil || new {
			b.Fatal("duplicate not detected")
		}
	}
}

func TestFIFOAgainstOrderedModel(t *testing.T) {
	p := index(t, 7, 5)
	rng := rand.New(rand.NewPCG(4, 9))
	var retained []uint64
	contains := func(values []uint64, seq uint64) bool {
		for _, v := range values {
			if v == seq {
				return true
			}
		}
		return false
	}
	for batch := 0; batch < 1000; batch++ {
		if err := p.BeginBatch(); err != nil {
			t.Fatal(err)
		}
		var pending []uint64
		for n := 0; n < 5; n++ {
			seq := uint64(rng.IntN(20))
			want := !contains(retained, seq) && !contains(pending, seq)
			stage(t, p, id(1, seq), want)
			if want {
				pending = append(pending, seq)
			}
		}
		if batch%3 == 0 {
			if err := p.Abort(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := p.Commit(); err != nil {
				t.Fatal(err)
			}
			retained = append(retained, pending...)
			if len(retained) > 7 {
				retained = retained[len(retained)-7:]
			}
		}
		if c, n := p.Counts(); c != len(retained) || n != 0 {
			t.Fatalf("batch %d counts=%d/%d", batch, c, n)
		}
		p.BeginBatch()
		for seq := uint64(0); seq < 20; seq++ {
			fresh, err := p.Stage(1, id(1, seq))
			if contains(retained, seq) {
				if err != nil || fresh {
					t.Fatalf("retained %d: %v", seq, err)
				}
			} else {
				if !errors.Is(err, ErrCapacity) && (!fresh || err != nil) {
					t.Fatalf("absent %d: %v", seq, err)
				}
			}
		}
		p.Abort()
	}
}
