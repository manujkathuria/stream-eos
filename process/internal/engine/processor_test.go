package engine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"stream-eos/process/internal/protocol"
	"stream-eos/process/internal/kafka"
	"testing"
)

type capturePublisher struct {
	fail       error
	calls      int
	events     []JSONEvent
	nextOffset int64
}

func (p *capturePublisher) CommitBatch(_ context.Context, batch []Output, nextOffset int64, _ []protocol.EventID, _ protocol.DedupState) error {
	p.calls++
	p.nextOffset = nextOffset
	if p.fail != nil {
		return p.fail
	}
	for _, out := range batch {
		var e JSONEvent
		if err := json.Unmarshal(out.Value, &e); err != nil {
			return err
		}
		p.events = append(p.events, e)
	}
	return nil
}

func TestJSONQuoteFields(t *testing.T) {
	r := record(7)
	for n := range 16 {
		binary.BigEndian.PutUint32(r.Value[28+n*4:32+n*4], uint32(100+n))
	}
	for n := range 10 {
		off := 28 + 64 + n*12
		binary.BigEndian.PutUint32(r.Value[off:off+4], uint32(200+n))
		binary.BigEndian.PutUint32(r.Value[off+4:off+8], uint32(300+n))
		binary.BigEndian.PutUint16(r.Value[off+8:off+10], uint16(400+n))
	}
	data, err := EncodeJSON(&r)
	if err != nil {
		t.Fatal(err)
	}
	var e JSONEvent
	if err := json.Unmarshal(data, &e); err != nil {
		t.Fatal(err)
	}
	if e.SchemaVersion != 1 || e.EventID != "010000000000000000000000000000000000000000000007" {
		t.Fatalf("identity: %+v", e)
	}
	q := e.Quote
	got := []uint32{q.InstrumentToken, q.LastPricePaise, q.LastQuantity, q.AveragePricePaise, q.Volume, q.TotalBuyQuantity, q.TotalSellQuantity, q.OpenPaise, q.HighPaise, q.LowPaise, q.ClosePaise, q.LastTradeTime, q.OpenInterest, q.OpenInterestHigh, q.OpenInterestLow, q.ExchangeTime}
	for n, v := range got {
		if v != uint32(100+n) {
			t.Fatalf("field %d: %d", n, v)
		}
	}
	for n := range 10 {
		var d DepthLevel
		if n < 5 {
			d = q.Buy[n]
		} else {
			d = q.Sell[n-5]
		}
		if d.Quantity != uint32(200+n) || d.PricePaise != uint32(300+n) || d.Orders != uint16(400+n) {
			t.Fatalf("depth %d: %+v", n, d)
		}
	}
}

func TestProcessorAcknowledgementAndReplay(t *testing.T) {
	sink := &capturePublisher{fail: errors.New("unavailable")}
	p, err := NewProcessor(0, 10, 3, sink)
	if err != nil {
		t.Fatal(err)
	}
	batch := []kafka.Record{record(1), record(1), record(2)}
	if _, _, err := p.Process(context.Background(), batch); !errors.Is(err, sink.fail) {
		t.Fatalf("failure: %v", err)
	}
	sink.fail = nil
	u, d, err := p.Process(context.Background(), batch)
	if err != nil || u != 2 || d != 1 || len(sink.events) != 2 {
		t.Fatalf("retry: %d/%d %v", u, d, err)
	}
	// Reuse input storage; previously accepted IDs remain recognized.
	batch[0], batch[1], batch[2] = record(2), record(3), record(1)
	u, d, err = p.Process(context.Background(), batch)
	if err != nil || u != 1 || d != 2 || len(sink.events) != 3 {
		t.Fatalf("next: %d/%d %v", u, d, err)
	}
	calls := sink.calls
	if _, _, err := p.Process(context.Background(), batch); err != nil || sink.calls != calls+1 {
		t.Fatalf("duplicate-only progress: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := p.Process(ctx, batch); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}

func TestProcessorFIFOOnlyEvictsAfterPublish(t *testing.T) {
	sink := &capturePublisher{}
	p, err := NewProcessor(0, 2, 2, sink)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := p.Process(ctx, []kafka.Record{record(1), record(2)}); err != nil {
		t.Fatal(err)
	}
	sink.fail = errors.New("output unavailable")
	if _, _, err := p.Process(ctx, []kafka.Record{record(3)}); err == nil {
		t.Fatal("expected publish failure")
	}
	sink.fail = nil
	u, d, err := p.Process(ctx, []kafka.Record{record(1), record(2)})
	if err != nil || u != 0 || d != 2 {
		t.Fatalf("failed output evicted history: %d/%d %v", u, d, err)
	}
	if _, _, err := p.Process(ctx, []kafka.Record{record(3)}); err != nil {
		t.Fatal(err)
	}
	u, d, err = p.Process(ctx, []kafka.Record{record(1), record(2)})
	if err != nil || u != 1 || d != 1 {
		t.Fatalf("FIFO: %d/%d %v", u, d, err)
	}
}

func TestDuplicateOnlyBatchCommitsNextOffset(t *testing.T) {
	sink := &capturePublisher{}
	p, err := NewProcessor(0, 10, 2, sink)
	if err != nil {
		t.Fatal(err)
	}
	first := record(1)
	first.Offset = 7
	if _, _, err := p.Process(context.Background(), []kafka.Record{first}); err != nil {
		t.Fatal(err)
	}
	if sink.nextOffset != 8 {
		t.Fatalf("next offset %d", sink.nextOffset)
	}
	first.Offset = 11
	u, d, err := p.Process(context.Background(), []kafka.Record{first})
	if err != nil || u != 0 || d != 1 || sink.nextOffset != 12 || sink.calls != 2 || len(sink.events) != 1 {
		t.Fatalf("duplicate batch: %d/%d offset=%d calls=%d outputs=%d err=%v", u, d, sink.nextOffset, sink.calls, len(sink.events), err)
	}
}
