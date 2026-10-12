package engine

import (
	"context"
	"fmt"
	"math"
	"stream-eos/process/internal/protocol"
	"stream-eos/process/internal/kafka"
)

// Output owns its JSON bytes; the key preserves instrument routing.
type Output = kafka.JSONRecord

// Publisher must finish using the batch before returning. Success commits
// output, FIFO changes, and next input offset atomically. Unknown outcomes require stopping.
type Publisher interface {
	CommitBatch(context.Context, []Output, int64, []protocol.EventID, protocol.DedupState) error
}

// Processor is sequential and partition-local. Recovery loads its FIFO from Kafka.
type Processor struct {
	inspector       *Inspector
	publisher       Publisher
	output          []Output
	buffers         [][MaxJSONBytes]byte
	recoveryPresent []bool
}

func NewProcessor(partition int32, maxIDs, batchCapacity int, publisher Publisher) (*Processor, error) {
	if publisher == nil {
		return nil, fmt.Errorf("publisher is required")
	}
	inspector, err := NewInspector(partition, maxIDs, batchCapacity)
	if err != nil {
		return nil, err
	}
	return &Processor{inspector: inspector, publisher: publisher, output: make([]Output, 0, batchCapacity), buffers: make([][MaxJSONBytes]byte, batchCapacity), recoveryPresent: make([]bool, maxIDs)}, nil
}

// Process stages IDs and transforms unique records. Output, FIFO and next offset
// commit atomically before local promotion. On any error callers stop; no automatic
// rewind is performed. Startup restores committed FIFO history before processing. Records must be ordered, validated input from one partition.
func (p *Processor) Process(ctx context.Context, records []kafka.Record) (unique, duplicates int, err error) {
	if err = ctx.Err(); err != nil {
		return 0, 0, err
	}
	if len(records) == 0 {
		return 0, 0, nil
	}
	if len(records) > cap(p.output) {
		return 0, 0, fmt.Errorf("processing batch capacity exceeded")
	}
	lastOffset := records[len(records)-1].Offset
	if lastOffset < 0 || lastOffset == math.MaxInt64 {
		return 0, 0, fmt.Errorf("invalid final input offset")
	}
	ids := p.inspector.ids
	if err = ids.BeginBatch(); err != nil {
		return 0, 0, err
	}
	defer func() {
		clear(p.output)
		p.output = p.output[:0]
		if err != nil {
			_ = ids.Abort()
		}
	}()
	for n := range records {
		if err = ctx.Err(); err != nil {
			return 0, 0, err
		}
		var fresh bool
		fresh, err = ids.Stage(p.inspector.partition, records[n].EventID())
		if err != nil {
			return 0, 0, fmt.Errorf("stage offset %d: %w", records[n].Offset, err)
		}
		if !fresh {
			duplicates++
			continue
		}
		var value []byte
		value, err = AppendJSON(p.buffers[len(p.output)][:0], &records[n])
		if err != nil {
			return 0, 0, err
		}
		p.output = append(p.output, Output{Key: records[n].Key, Value: value})
		unique++
	}
	if err = p.publisher.CommitBatch(ctx, p.output, lastOffset+1, ids.PendingIDs(), ids.StateAfterCommit()); err != nil {
		return 0, 0, err
	}
	if err = ids.Commit(); err != nil {
		return 0, 0, err
	}
	return unique, duplicates, nil
}

// RestoreSlot and RestoreFIFO are startup-only; recovery errors discard the processor.
func (p *Processor) RestoreSlot(slot uint32, event protocol.EventID) error {
	return p.inspector.ids.RestoreSlot(slot, event, p.recoveryPresent)
}
func (p *Processor) RestoreFIFO(state protocol.DedupState) error {
	if err := p.inspector.ids.ActivateRestore(state, p.recoveryPresent); err != nil {
		return err
	}
	p.recoveryPresent = nil
	return nil
}

// Bootstrap stages retained legacy input without producing output. Only use before
// processing, with complete required history and explicit migration authorization.
func (p *Processor) Bootstrap(records []kafka.Record) error {
	_, _, err := p.inspector.Inspect(records)
	return err
}
func (p *Processor) SnapshotSlot(slot uint32) protocol.EventID {
	return p.inspector.ids.SnapshotSlot(slot)
}
func (p *Processor) SnapshotState() protocol.DedupState { return p.inspector.ids.StateAfterCommit() }
