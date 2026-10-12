// Package dedup tracks exact source event identities for a single input partition.
// State is in memory only. Callers must serialize access and manage durable commits.
package dedup

import (
	"errors"
	"fmt"
	"math"

	"stream-eos/process/internal/protocol"
)

var (
	ErrCapacity    = errors.New("deduplication capacity reached")
	ErrBatchActive = errors.New("deduplication batch already active")
	ErrNoBatch     = errors.New("no active deduplication batch")
	ErrPartition   = errors.New("event belongs to a different partition")
)

type Config struct {
	Partition  int32
	MaxIDs     int // Maximum retained committed IDs; FIFO eviction on local commit.
	MaxPending int // Maximum new IDs in one batch, no greater than MaxIDs.
}

// Partition encapsulates committed and tentative ID sets. It is not concurrency
// safe: one partition owner calls its methods sequentially. No borrowed buffers
// are retained; identities are fixed-size values copied into the sets.
type Partition struct {
	committed    map[protocol.EventID]struct{}
	fifo         []protocol.EventID
	pendingOrder []protocol.EventID
	pending      map[protocol.EventID]struct{}
	next         int
	maxIDs       int
	maxPending   int
	partition    int32
	active       bool
}

func New(cfg Config) (*Partition, error) {
	if cfg.Partition < 0 || cfg.MaxIDs < 1 || uint64(cfg.MaxIDs) > math.MaxUint32 || cfg.MaxPending < 1 || cfg.MaxPending > cfg.MaxIDs {
		return nil, fmt.Errorf("non-negative partition and positive capacities with max-pending <= max-IDs are required")
	}
	return &Partition{
		committed:    make(map[protocol.EventID]struct{}, cfg.MaxIDs),
		fifo:         make([]protocol.EventID, cfg.MaxIDs),
		pendingOrder: make([]protocol.EventID, 0, cfg.MaxPending),
		pending:      make(map[protocol.EventID]struct{}, cfg.MaxPending),
		maxIDs:       cfg.MaxIDs, maxPending: cfg.MaxPending, partition: cfg.Partition,
	}, nil
}

func (p *Partition) BeginBatch() error {
	if p.active {
		return ErrBatchActive
	}
	p.active = true
	return nil
}

// Stage returns true for a new ID, false for a committed or pending duplicate.
// Wrong-partition/capacity errors do not change state. On error the caller must
// abort or stop rather than skip the event and advance input progress.
func (p *Partition) Stage(partition int32, id protocol.EventID) (bool, error) {
	if partition != p.partition {
		return false, ErrPartition
	}
	if !p.active {
		return false, ErrNoBatch
	}
	if _, ok := p.committed[id]; ok {
		return false, nil
	}
	if _, ok := p.pending[id]; ok {
		return false, nil
	}
	if len(p.pending) >= p.maxPending {
		return false, ErrCapacity
	}
	p.pending[id] = struct{}{}
	p.pendingOrder = append(p.pendingOrder, id)
	return true, nil
}

// Commit promotes pending IDs in first-accepted order, evicting oldest IDs when
// the FIFO window is full. Duplicate lookups do not refresh eviction order.
// Eviction happens only here; Abort preserves the committed window.
// Commit changes only local memory. In the future engine, call
// this only after a confirmed durable Kafka transaction commit. This method does
// not write Kafka, Redis, offsets, or files. Unknown outcomes require recovery.
func (p *Partition) Commit() error {
	if !p.active {
		return ErrNoBatch
	}
	// Preserve first-accepted order, never Go's randomized map iteration order.
	for _, id := range p.pendingOrder {
		if len(p.committed) == p.maxIDs {
			delete(p.committed, p.fifo[p.next])
		}
		p.fifo[p.next] = id
		p.committed[id] = struct{}{}
		p.next++
		if p.next == len(p.fifo) {
			p.next = 0
		}
	}
	clear(p.pending)
	clear(p.pendingOrder)
	p.pendingOrder = p.pendingOrder[:0]
	p.active = false
	return nil
}

// Abort discards tentative IDs so replay can accept them again; committed IDs stay.
func (p *Partition) Abort() error {
	if !p.active {
		return ErrNoBatch
	}
	clear(p.pending)
	clear(p.pendingOrder)
	p.pendingOrder = p.pendingOrder[:0]
	p.active = false
	return nil
}

func (p *Partition) Counts() (committed, pending int) { return len(p.committed), len(p.pending) }

// PendingIDs borrows first-accepted IDs until Commit/Abort. The publisher must
// finish using this slice before returning; callers must not modify it.
func (p *Partition) PendingIDs() []protocol.EventID { return p.pendingOrder }

// StateAfterCommit describes the FIFO state if the pending batch commits.
func (p *Partition) StateAfterCommit() protocol.DedupState {
	return protocol.DedupState{Capacity: uint32(p.maxIDs), Count: uint32(min(p.maxIDs, len(p.committed)+len(p.pendingOrder))), Next: uint32((p.next + len(p.pendingOrder)) % p.maxIDs)}
}

// RestoreSlot fills caller-owned recovery storage before ActivateRestore builds
// the exact index. present tracks unique occupied slots across compacted replay.
func (p *Partition) RestoreSlot(slot uint32, id protocol.EventID, present []bool) error {
	if p.active || len(p.committed) != 0 || len(present) != len(p.fifo) || uint64(slot) >= uint64(len(p.fifo)) {
		return fmt.Errorf("invalid FIFO restore slot or lifecycle")
	}
	p.fifo[slot] = id
	present[slot] = true
	return nil
}

// ActivateRestore validates complete slot coverage and rebuilds membership.
// Recovery errors require discarding this instance, not resuming partial state.
func (p *Partition) ActivateRestore(state protocol.DedupState, present []bool) error {
	if p.active || len(p.committed) != 0 || state.Capacity != uint32(p.maxIDs) || state.Count > state.Capacity || state.Next >= state.Capacity || len(present) != len(p.fifo) {
		return fmt.Errorf("invalid FIFO recovery metadata")
	}
	if state.Count < state.Capacity && state.Next != state.Count {
		return fmt.Errorf("invalid partial FIFO cursor")
	}
	occupied := 0
	for slot, exists := range present {
		if !exists {
			continue
		}
		if state.Count < state.Capacity && uint32(slot) >= state.Count {
			return fmt.Errorf("unexpected recovered FIFO slot")
		}
		event := p.fifo[slot]
		if _, exists := p.committed[event]; exists {
			return fmt.Errorf("duplicate ID in recovered FIFO slots")
		}
		p.committed[event] = struct{}{}
		occupied++
	}
	if uint32(occupied) != state.Count {
		return fmt.Errorf("missing FIFO history: restored %d slots, require %d", occupied, state.Count)
	}
	p.next = int(state.Next)
	return nil
}

// SnapshotSlot is startup/migration-only; slots are indexed by SnapshotState.
func (p *Partition) SnapshotSlot(slot uint32) protocol.EventID { return p.fifo[slot] }
