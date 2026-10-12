package kafka

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"stream-eos/process/internal/protocol"
)

// Pre-encoded, fixed-schema market JSON. Numeric content stays constant so this
// measures transaction publication, not quote encoding. Identity changes per event.
func transactionJSON() ([]byte, int) {
	prefix := `{"schema_version":1,"event_id":"`
	value := prefix + strings.Repeat("0", 48) + `","quote":{`
	fields := []string{"instrument_token", "last_price_paise", "last_quantity", "average_price_paise", "volume", "total_buy_quantity", "total_sell_quantity", "open_paise", "high_paise", "low_paise", "close_paise", "last_trade_time", "open_interest", "open_interest_high", "open_interest_low", "exchange_time"}
	for i, name := range fields {
		if i > 0 {
			value += ","
		}
		n := 100000 + i
		if i == 0 {
			n = 9910001
		}
		value += fmt.Sprintf("%q:%d", name, n)
	}
	for _, side := range []string{"buy", "sell"} {
		value += `,"` + side + `":[`
		for i := 0; i < 5; i++ {
			if i > 0 {
				value += ","
			}
			value += `{"quantity":100,"price_paise":100000,"orders":10}`
		}
		value += "]"
	}
	return []byte(value + "}}"), len(prefix)
}

// Each operation includes ID stamping, FIFO metadata planning, production
// CommitBatch and confirmed EndTransaction. Setup, encoding, input consumption,
// dedup lookup, and read_committed verification are outside timing.
func BenchmarkTransactionCommit(b *testing.B) {
	if os.Getenv("STREAM_EOS_KAFKA_BENCH") != "1" {
		b.Skip("set STREAM_EOS_KAFKA_BENCH=1; isolated fixture topics")
	}
	for _, size := range []int{100, 1000} {
		b.Run(fmt.Sprintf("unique/batch%d", size), func(b *testing.B) {
			const warmup = 5
			ctx, cfg := transactionFixture(b)
			cfg.BatchCapacity, cfg.DedupCapacity = size, 1000000
			p, err := NewTransactionPublisher(ctx, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			if _, found, err := p.LoadProgress(ctx); err != nil || found {
				b.Fatalf("initial checkpoint: %v %v", found, err)
			}
			if err := p.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err != nil {
				b.Fatal(err)
			}
			template, identityAt := transactionJSON()
			batch := make([]JSONRecord, size)
			ids := make([]protocol.EventID, size)
			for i := range batch {
				batch[i].Value = bytes.Clone(template)
				binary.BigEndian.PutUint32(batch[i].Key[:], 9910001)
				ids[i][0] = 1
			}
			total := uint64(0)
			commit := func() {
				for i := range ids {
					total++
					binary.BigEndian.PutUint64(ids[i][16:], total)
					hex.Encode(batch[i].Value[identityAt:identityAt+48], ids[i][:])
				}
				state := protocol.DedupState{Capacity: uint32(cfg.DedupCapacity), Count: uint32(min(total, uint64(cfg.DedupCapacity))), Next: uint32(total % uint64(cfg.DedupCapacity))}
				if err := p.CommitBatch(ctx, batch, int64(total), ids, state); err != nil {
					b.Fatal(err)
				}
			}
			for range warmup {
				commit()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				commit()
			}
			b.StopTimer()
			b.ReportMetric(float64(len(template)), "JSON-B/event")
			b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds(), "committed-events/s")
			// Fresh transactional owner reads durable state after the measured owner closes.
			p.Close()
			recovered, err := NewTransactionPublisher(ctx, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer recovered.Close()
			next, found, err := recovered.LoadProgress(ctx)
			if err != nil || !found || next != int64(total) {
				b.Fatalf("checkpoint=%d found=%v err=%v", next, found, err)
			}
			fifo := &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}
			if err := recovered.RestoreDedup(ctx, fifo); err != nil {
				b.Fatal(err)
			}
			wantState := protocol.DedupState{Capacity: uint32(cfg.DedupCapacity), Count: uint32(min(total, uint64(cfg.DedupCapacity))), Next: uint32(total % uint64(cfg.DedupCapacity))}
			if fifo.state != wantState {
				b.Fatalf("FIFO metadata=%+v want=%+v", fifo.state, wantState)
			}
			for slot, event := range fifo.slots {
				seq := uint64(slot) + 1
				if total >= seq {
					seq += ((total - seq) / uint64(cfg.DedupCapacity)) * uint64(cfg.DedupCapacity)
				}
				var want protocol.EventID
				want[0] = 1
				binary.BigEndian.PutUint64(want[16:], seq)
				if event != want {
					b.Fatalf("FIFO slot %d mismatch", slot)
				}
			}
			observed := uint64(0)
			expected := bytes.Clone(template)
			if err := recovered.scanCommitted(ctx, cfg.OutputTopic, func(r *kgo.Record) error {
				observed++
				var want protocol.EventID
				want[0] = 1
				binary.BigEndian.PutUint64(want[16:], observed)
				hex.Encode(expected[identityAt:identityAt+48], want[:])
				if r.Partition != cfg.Partition || !bytes.Equal(r.Key, batch[0].Key[:]) || !bytes.Equal(r.Value, expected) {
					return fmt.Errorf("output mismatch at event %d", observed)
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			if observed != total {
				b.Fatalf("output count=%d want=%d", observed, total)
			}
		})
	}
}
