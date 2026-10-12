// Package ingest coordinates source reading, validation, and acknowledged publishing.
package ingest

import (
	"context"
	"fmt"
	"stream-eos/pkg/protocol"
)

// Source returns one market envelope. Returned bytes are borrowed until the next
// Read call. Read must unblock when its context is canceled. Calls are sequential.
type Source interface {
	Read(context.Context) ([]byte, error)
}

// Publisher acknowledges every event before returning success. It must finish
// using borrowed event storage or copy it before returning, including on error.
// Errors can follow partial publication; a batch is not guaranteed atomic.
type Publisher interface {
	Publish(context.Context, []protocol.Event) error
}

type Stats struct{ Messages, Events uint64 }
type Ingestor struct {
	source    Source
	publisher Publisher
	events    []protocol.Event
}

func New(source Source, publisher Publisher) (*Ingestor, error) {
	if source == nil || publisher == nil {
		return nil, fmt.Errorf("source and publisher are required")
	}
	return &Ingestor{source: source, publisher: publisher, events: make([]protocol.Event, 0, protocol.MaxEvents)}, nil
}

// Run is sequential; it owns reusable event storage and borrows message bytes.
// Run stops on the first read, decode, or publish failure. A zero limit runs until
// canceled; the caller owns dependency cleanup. No source redelivery is invented.
func (i *Ingestor) Run(ctx context.Context, limit uint64) (Stats, error) {
	var stats Stats
	for limit == 0 || stats.Messages < limit {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		message, err := i.source.Read(ctx)
		if err != nil {
			return stats, fmt.Errorf("read source: %w", err)
		}
		events, err := protocol.DecodeInto(message, i.events[:0])
		if err != nil {
			return stats, fmt.Errorf("decode source: %w", err)
		}
		i.events = events
		publishErr := i.publisher.Publish(ctx, events)
		count := len(events)
		// Publish has finished using the borrowed quotes, including on error.
		clear(i.events)
		i.events = i.events[:0]
		if publishErr != nil {
			return stats, fmt.Errorf("publish events: %w", publishErr)
		}
		stats.Messages++
		stats.Events += uint64(count)
	}
	return stats, nil
}
