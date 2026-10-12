package dedup

import (
	"stream-eos/process/internal/protocol"
	"testing"
)

func TestFIFORecoveryPreservesEvictionOrder(t *testing.T) {
	original := index(t, 3, 3)
	original.BeginBatch()
	for _, seq := range []uint64{20, 10, 30} {
		stage(t, original, id(1, seq), true)
	}
	original.Commit()
	original.BeginBatch()
	stage(t, original, id(1, 40), true)
	original.Commit()
	state := original.StateAfterCommit()
	if state != (protocol.DedupState{Capacity: 3, Count: 3, Next: 1}) {
		t.Fatalf("metadata=%+v", state)
	}
	recovered := index(t, 3, 3)
	present := make([]bool, 3)
	// Replay deliberately out of slot order; eviction order is physical cursor.
	for _, slot := range []uint32{2, 0, 1} {
		if err := recovered.RestoreSlot(slot, original.SnapshotSlot(slot), present); err != nil {
			t.Fatal(err)
		}
	}
	if err := recovered.ActivateRestore(state, present); err != nil {
		t.Fatal(err)
	}
	recovered.BeginBatch()
	stage(t, recovered, id(1, 10), false)
	stage(t, recovered, id(1, 50), true)
	recovered.Commit()
	recovered.BeginBatch()
	stage(t, recovered, id(1, 10), true)
	stage(t, recovered, id(1, 30), false)
	stage(t, recovered, id(1, 40), false)
	stage(t, recovered, id(1, 50), false)
	recovered.Abort()
}
func TestFIFORecoveryRejectsMissingCorruptOrChangedState(t *testing.T) {
	cases := []struct {
		name    string
		state   protocol.DedupState
		slots   []protocol.EventID
		present []bool
	}{
		{"missing", protocol.DedupState{Capacity: 3, Count: 2, Next: 2}, []protocol.EventID{id(1, 1), id(1, 2), {}}, []bool{true, false, false}},
		{"duplicate", protocol.DedupState{Capacity: 3, Count: 2, Next: 2}, []protocol.EventID{id(1, 1), id(1, 1), {}}, []bool{true, true, false}},
		{"capacity", protocol.DedupState{Capacity: 4, Count: 1, Next: 1}, []protocol.EventID{id(1, 1), {}, {}}, []bool{true, false, false}},
		{"cursor", protocol.DedupState{Capacity: 3, Count: 1, Next: 2}, []protocol.EventID{id(1, 1), {}, {}}, []bool{true, false, false}},
		{"unexpected", protocol.DedupState{Capacity: 3, Count: 1, Next: 1}, []protocol.EventID{id(1, 1), {}, id(1, 3)}, []bool{true, false, true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := index(t, 3, 3)
			present := make([]bool, 3)
			for slot, exists := range tc.present {
				if exists {
					if err := p.RestoreSlot(uint32(slot), tc.slots[slot], present); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := p.ActivateRestore(tc.state, present); err == nil {
				t.Fatal("invalid state accepted")
			}
		})
	}
}
func TestPendingPlanDoesNotChangeCommittedState(t *testing.T) {
	p := index(t, 3, 2)
	p.BeginBatch()
	stage(t, p, id(1, 1), true)
	stage(t, p, id(1, 2), true)
	ids := p.PendingIDs()
	if len(ids) != 2 || ids[0] != id(1, 1) || ids[1] != id(1, 2) {
		t.Fatal("unordered pending plan")
	}
	if state := p.StateAfterCommit(); state != (protocol.DedupState{Capacity: 3, Count: 2, Next: 2}) {
		t.Fatalf("plan=%+v", state)
	}
	p.Abort()
	if state := p.StateAfterCommit(); state != (protocol.DedupState{Capacity: 3}) {
		t.Fatalf("abort changed committed plan=%+v", state)
	}
}
