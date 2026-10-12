package market

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"stream-eos/websocket-source/internal/protocol"
)

func TestSharedUpdatesForSameSubscriptions(t *testing.T) {
	m := New(retainedUpdates, protocol.MaxPackets)
	var active [protocol.MaxPackets]bool
	for i := range active {
		active[i] = true
	}
	a := make([]byte, 22+protocol.MaxPackets*194)
	b := make([]byte, len(a))
	for position := uint64(0); position < 3; position++ {
		na, err := m.Frame(position, active, a)
		if err != nil {
			t.Fatal(err)
		}
		nb, err := m.Frame(position, active, b)
		if err != nil {
			t.Fatal(err)
		}
		if na != 77622 || na != nb || !bytes.Equal(a[:na], b[:nb]) {
			t.Fatal("feeds observed different market updates")
		}
		if binary.BigEndian.Uint16(a[20:22]) != 400 {
			t.Fatal("missing instrument packets")
		}
		for i := range protocol.MaxPackets {
			start := 22 + i*194
			if binary.BigEndian.Uint32(a[start+10:start+14]) != TokenAt(i) {
				t.Fatal("wrong instrument")
			}
		}
	}
}

func TestDifferentSubscriptionsShareInstrumentState(t *testing.T) {
	m := New(retainedUpdates, protocol.MaxPackets)
	var a, b [protocol.MaxPackets]bool
	a[0], a[1], b[1] = true, true, true
	pa, pb := make([]byte, 410), make([]byte, 216)
	if _, err := m.Frame(0, a, pa); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Frame(0, b, pb); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pa[216:410], pb[22:216]) {
		t.Fatal("shared instrument differs")
	}
}

func TestSlowClientDoesNotStopMarket(t *testing.T) {
	m := New(retainedUpdates, protocol.MaxPackets)
	var active [protocol.MaxPackets]bool
	active[0] = true
	p := make([]byte, 216)
	for i := uint64(0); i <= retainedUpdates; i++ {
		if _, err := m.Frame(i, active, p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Frame(0, active, p); !errors.Is(err, errBehindMarket) {
		t.Fatal("stale reader accepted")
	}
	if _, err := m.Frame(m.Position(), active, p); err != nil {
		t.Fatal("live reader stopped")
	}
}

// A delayed reader beyond the old 128-slot window still receives the original
// bytes, including at the oldest retained boundary after wrapping.
func TestConfiguredHistoryPreservesDelayedReader(t *testing.T) {
	const capacity = 256
	m := New(capacity, protocol.MaxPackets)
	var active [protocol.MaxPackets]bool
	active[0] = true
	original, current := make([]byte, 216), make([]byte, 216)
	if _, err := m.Frame(0, active, original); err != nil {
		t.Fatal(err)
	}
	for position := uint64(1); position < capacity; position++ {
		if _, err := m.Frame(position, active, current); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Frame(0, active, current); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, current) {
		t.Fatal("delayed reader received changed bytes")
	}
	if _, err := m.Frame(capacity, active, current); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Frame(0, active, current); !errors.Is(err, errBehindMarket) {
		t.Fatalf("overwritten history: %v", err)
	}
	if _, err := m.Frame(1, active, current); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Frame(capacity+2, active, current); !errors.Is(err, errBehindMarket) {
		t.Fatalf("future position accepted: %v", err)
	}
}

func TestConfiguredInstrumentsAndRingOwnership(t *testing.T) {
	for _, count := range []int{1, 10, 400} {
		m := New(2, count)
		if m.InstrumentCount() != count {
			t.Fatal("wrong instrument count")
		}
		var active [protocol.MaxPackets]bool
		for i := range count {
			active[i] = true
		}
		original, frame := make([]byte, 22+count*194), make([]byte, 22+count*194)
		if _, err := m.Frame(0, active, original); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Frame(1, active, frame); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Frame(0, active, frame); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(frame, original) {
			t.Fatal("writing another slot corrupted retained data")
		}
		if _, err := m.Frame(2, active, frame); err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint16(frame[20:22]) != uint16(count) {
			t.Fatal("unexpected packet count")
		}
		for i := range count {
			start := 22 + i*194
			if binary.BigEndian.Uint64(frame[start:start+8]) != uint64(801+i) {
				t.Fatal("identity stride changed")
			}
			if binary.BigEndian.Uint32(frame[start+10:start+14]) != TokenAt(i) {
				t.Fatal("wrong token")
			}
		}
		for _, slot := range m.history {
			if len(slot.Packets) != count || cap(slot.Packets) != count {
				t.Fatal("history retains unconfigured packets")
			}
		}
	}
}
