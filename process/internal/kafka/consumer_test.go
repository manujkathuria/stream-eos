package kafka

import (
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func consumerConfig() ConsumerConfig {
	return ConsumerConfig{Brokers: []string{"localhost:9092"}, Topic: "market.ticks.raw", Partition: 0, StartOffset: -2, BatchRecords: 100, BatchBytes: 21600}
}
func TestConsumerBounds(t *testing.T) {
	cfg := consumerConfig()
	cfg.BatchBytes = 432
	n, err := cfg.pollLimit()
	if err != nil || n != 2 {
		t.Fatalf("limit=%d err=%v", n, err)
	}
	for _, change := range []func(*ConsumerConfig){func(c *ConsumerConfig) { c.Partition = -1 }, func(c *ConsumerConfig) { c.BatchBytes = 215 }, func(c *ConsumerConfig) { c.BatchRecords = 0 }, func(c *ConsumerConfig) { c.StartOffset = -3 }, func(c *ConsumerConfig) { c.Brokers = nil }} {
		cfg := consumerConfig()
		change(&cfg)
		if _, err := cfg.pollLimit(); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
func inputRecord() *kgo.Record {
	value := make([]byte, 212)
	copy(value, "SEV1")
	value[4] = 7
	binary.BigEndian.PutUint64(value[20:28], 401)
	binary.BigEndian.PutUint32(value[28:32], 9910001)
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, 9910001)
	return &kgo.Record{Topic: "market.ticks.raw", Partition: 0, Offset: 19, Key: key, Value: value}
}
func TestConsumerRecordOwnsStorage(t *testing.T) {
	wire := inputRecord()
	record, err := copyInputRecord(wire, wire.Topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	identity := record.EventID()
	clear(wire.Key)
	clear(wire.Value)
	if identity[0] != 7 || binary.BigEndian.Uint64(identity[16:]) != 401 {
		t.Fatal("incorrect source identity")
	}
	if record.Offset != 19 || record.Value[4] != 7 || binary.BigEndian.Uint64(record.Value[20:28]) != 401 || binary.BigEndian.Uint32(record.Key[:]) != 9910001 {
		t.Fatal("record corrupted by reuse")
	}
}
func TestConsumerRejectsUnexpectedRecords(t *testing.T) {
	for _, change := range []func(*kgo.Record){func(r *kgo.Record) { r.Partition = 1 }, func(r *kgo.Record) { r.Topic = "other" }, func(r *kgo.Record) { r.Value = r.Value[:211] }, func(r *kgo.Record) { r.Key = nil }, func(r *kgo.Record) { r.Value[0] = 0 }, func(r *kgo.Record) { r.Key[0] = 0xff }} {
		r := inputRecord()
		change(r)
		if _, err := copyInputRecord(r, "market.ticks.raw", 0); err == nil {
			t.Fatal("invalid record accepted")
		}
	}
}

// Opt-in read-only integration test; does not create/delete topics or commit progress.
func TestExplicitPartitionsIntegration(t *testing.T) {
	if os.Getenv("STREAM_EOS_KAFKA_TEST") != "1" {
		t.Skip("set STREAM_EOS_KAFKA_TEST=1 with local Kafka running")
	}
	for partition := int32(0); partition < 3; partition++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cfg := consumerConfig()
		cfg.Partition = partition
		cfg.BatchRecords = 2
		cfg.BatchBytes = 432
		c, err := NewConsumer(ctx, cfg)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		// Metadata validation proves the partition exists; a canceled poll must stop.
		pollCtx, stop := context.WithCancel(ctx)
		stop()
		if _, err := c.ReadBatch(pollCtx, make([]Record, c.BatchCapacity())); err != context.Canceled {
			t.Fatalf("cancel: %v", err)
		}
		c.Close()
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := consumerConfig()
	cfg.Partition = 3
	if c, err := NewConsumer(ctx, cfg); err == nil {
		c.Close()
		t.Fatal("nonexistent partition accepted")
	}
}
func BenchmarkCopyInputRecord(b *testing.B) {
	r := inputRecord()
	b.ReportAllocs()
	for b.Loop() {
		record, err := copyInputRecord(r, r.Topic, 0)
		if err != nil || record.Offset != 19 {
			b.Fatal("invalid record")
		}
	}
}
