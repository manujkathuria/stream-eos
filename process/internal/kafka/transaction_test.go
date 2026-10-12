package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"stream-eos/process/internal/protocol"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProgressCodec(t *testing.T) {
	for _, offset := range []uint64{0, 1, math.MaxInt64} {
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], offset)
		got, err := decodeProgress(value[:])
		if err != nil || got != int64(offset) {
			t.Fatalf("%d: %d %v", offset, got, err)
		}
	}
	for _, value := range [][]byte{nil, make([]byte, 7), make([]byte, 9), {255, 255, 255, 255, 255, 255, 255, 255}} {
		if _, err := decodeProgress(value); err == nil {
			t.Fatal("accepted invalid checkpoint")
		}
	}
}

func transactionFixture(t testing.TB) (context.Context, TransactionConfig) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	base := fmt.Sprintf("stream-eos-txn-test-%d", time.Now().UnixNano())
	cfg := TransactionConfig{Brokers: []string{"localhost:9092"}, InputTopic: base + "-in", OutputTopic: base + "-out", ProgressTopic: base + "-progress", Partition: 0, BatchCapacity: 10, DedupCapacity: 10, ChangelogTopic: base + "-state"}
	admin, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 10000
	for _, topic := range []string{cfg.InputTopic, cfg.OutputTopic, cfg.ProgressTopic, cfg.ChangelogTopic} {
		entry := kmsg.CreateTopicsRequestTopic{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}
		if topic == cfg.ProgressTopic || topic == cfg.ChangelogTopic {
			policy := "compact"
			entry.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "cleanup.policy", Value: &policy}}
		}
		request.Topics = append(request.Topics, entry)
	}
	response, err := admin.Request(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range response.(*kmsg.CreateTopicsResponse).Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.TopicNames = []string{cfg.InputTopic, cfg.OutputTopic, cfg.ProgressTopic, cfg.ChangelogTopic}
		if _, err := admin.Request(cleanup, request); err != nil {
			t.Errorf("delete fixture: %v", err)
		}
	})
	return ctx, cfg
}

// Bounded read includes control markers so an aborted tail cannot cause a wait
// for a nonexistent application record. Only fixture topics are touched.
func fixtureOutput(t *testing.T, ctx context.Context, cfg TransactionConfig, committed bool) []string {
	t.Helper()
	isolation := kgo.ReadUncommitted()
	if committed {
		isolation = kgo.ReadCommitted()
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.OutputTopic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(isolation), kgo.KeepControlRecords())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	end, err := partitionOffset(ctx, reader, cfg.OutputTopic, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	if end == 0 {
		return values
	}
	for {
		fetches := reader.PollRecords(ctx, 100)
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		reached := false
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset+1 >= end {
				reached = true
			}
			if r.Offset < end && !r.Attrs.IsControl() {
				values = append(values, string(r.Value))
			}
		})
		if reached {
			return values
		}
	}
}

func TestTransactionalBatchIntegration(t *testing.T) {
	if os.Getenv("STREAM_EOS_KAFKA_TEST") != "1" {
		t.Skip("set STREAM_EOS_KAFKA_TEST=1; creates/deletes isolated fixture topics")
	}
	ctx, cfg := transactionFixture(t)
	p, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := commitFixture(ctx, p, nil, 1); err == nil {
		t.Fatal("allowed processing before progress restoration")
	}
	if _, found, err := p.LoadProgress(ctx); err != nil || found {
		t.Fatalf("initial progress: %v %v", found, err)
	}
	if err := p.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err != nil {
		t.Fatal(err)
	}
	first := []JSONRecord{{Key: [4]byte{1}, Value: []byte(`{"id":1}`)}}
	if err := commitFixture(ctx, p, first, 10); err != nil {
		t.Fatal(err)
	}
	if err := commitFixture(ctx, p, nil, 12); err != nil {
		t.Fatal(err)
	} // Duplicate-only batch.
	// Write and abort a real transaction containing both output and a checkpoint.
	if err := p.publisher.client.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	var checkpoint [40]byte
	encodeCheckpoint(&checkpoint, 99, p.state, p.generation)
	if err := p.publisher.client.ProduceSync(ctx,
		&kgo.Record{Topic: cfg.OutputTopic, Partition: 0, Key: []byte{1}, Value: []byte(`{"aborted":true}`)},
		&kgo.Record{Topic: cfg.ProgressTopic, Partition: 0, Key: p.key, Value: checkpoint[:]},
		&kgo.Record{Topic: cfg.ChangelogTopic, Partition: 0, Key: p.stateKeys[0][:], Value: make([]byte, 32)},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := p.publisher.client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if next, found, err := p.LoadProgress(ctx); err != nil || !found || next != 12 {
		t.Fatalf("after abort: %d %v %v", next, found, err)
	}
	if err := p.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err != nil {
		t.Fatal(err)
	}
	if got := fixtureOutput(t, ctx, cfg, true); len(got) != 1 || got[0] != `{"id":1}` {
		t.Fatalf("committed visibility: %v", got)
	}
	if got := fixtureOutput(t, ctx, cfg, false); len(got) != 2 {
		t.Fatalf("aborted fixture missing from raw read: %v", got)
	}
	// A replacement initializes the same stable transactional ID, fencing p.
	replacement, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if next, found, err := replacement.LoadProgress(ctx); err != nil || !found || next != 12 {
		t.Fatalf("restart: %d %v %v", next, found, err)
	}
	if err := replacement.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err != nil {
		t.Fatal(err)
	}
	if err := commitFixture(ctx, p, first, 13); err == nil {
		t.Fatal("old owner was not fenced")
	}
	if !p.poisoned {
		t.Fatal("failed owner did not stop")
	}
	if err := commitFixture(ctx, replacement, []JSONRecord{{Key: [4]byte{2}, Value: []byte(`{"id":2}`)}}, 14); err != nil {
		t.Fatal(err)
	}
	if next, found, err := replacement.LoadProgress(ctx); err != nil || !found || next != 14 {
		t.Fatalf("replacement progress: %d %v %v", next, found, err)
	}
	if got := fixtureOutput(t, ctx, cfg, true); len(got) != 2 || got[1] != `{"id":2}` {
		t.Fatalf("replacement output: %v", got)
	}
	if err := replacement.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err != nil {
		t.Fatal(err)
	}
	if err := commitFixture(ctx, replacement, nil, 13); err == nil {
		t.Fatal("allowed backwards progress")
	}
}

func TestReplacementAbortsUnfinishedTransactionIntegration(t *testing.T) {
	if os.Getenv("STREAM_EOS_KAFKA_TEST") != "1" {
		t.Skip("set STREAM_EOS_KAFKA_TEST=1; creates/deletes isolated fixture topics")
	}
	ctx, cfg := transactionFixture(t)
	old, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := old.publisher.client.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	var checkpoint [40]byte
	encodeCheckpoint(&checkpoint, 42, old.state, old.generation)
	if err := old.publisher.client.ProduceSync(ctx,
		&kgo.Record{Topic: cfg.OutputTopic, Partition: 0, Key: []byte{1}, Value: []byte(`{"unfinished":true}`)},
		&kgo.Record{Topic: cfg.ProgressTopic, Partition: 0, Key: old.key, Value: checkpoint[:]},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	replacement, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if next, found, err := replacement.LoadProgress(ctx); err != nil || found {
		t.Fatalf("unfinished progress visible: %d %v %v", next, found, err)
	}
	if err := replacement.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err != nil {
		t.Fatal(err)
	}
	if err := old.publisher.client.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Fatal("fenced owner committed unfinished transaction")
	}
	if got := fixtureOutput(t, ctx, cfg, true); len(got) != 0 {
		t.Fatalf("unfinished output visible: %v", got)
	}
	if err := commitFixture(ctx, replacement, []JSONRecord{{Key: [4]byte{1}, Value: []byte(`{"replacement":true}`)}}, 1); err != nil {
		t.Fatal(err)
	}
	if next, found, err := replacement.LoadProgress(ctx); err != nil || !found || next != 1 {
		t.Fatalf("replacement: %d %v %v", next, found, err)
	}
}

type fixtureFIFO struct {
	slots map[uint32]protocol.EventID
	state protocol.DedupState
}

func (f *fixtureFIFO) RestoreSlot(slot uint32, event protocol.EventID) error {
	f.slots[slot] = event
	return nil
}
func (f *fixtureFIFO) RestoreFIFO(state protocol.DedupState) error {
	if len(f.slots) != int(state.Count) {
		return fmt.Errorf("missing fixture slots")
	}
	f.state = state
	return nil
}
func commitFixture(ctx context.Context, p *TransactionPublisher, batch []JSONRecord, next int64) error {
	ids := make([]protocol.EventID, len(batch))
	for n := range ids {
		ids[n][0] = 1
		binary.BigEndian.PutUint64(ids[n][16:], uint64(next)+uint64(n))
	}
	state := protocol.DedupState{Capacity: p.state.Capacity, Count: uint32(min(uint64(p.state.Capacity), uint64(p.state.Count)+uint64(len(ids)))), Next: uint32((uint64(p.state.Next) + uint64(len(ids))) % uint64(p.state.Capacity))}
	return p.CommitBatch(ctx, batch, next, ids, state)
}

func TestCheckpointStateCodec(t *testing.T) {
	var value [40]byte
	generation := [16]byte{1}
	state := protocol.DedupState{Capacity: 3, Count: 3, Next: 1}
	encodeCheckpoint(&value, 12, state, generation)
	next, got, gen, legacy, err := decodeCheckpoint(value[:])
	if err != nil || legacy || next != 12 || got != state || gen != generation {
		t.Fatalf("roundtrip: %d %+v %v %v", next, got, legacy, err)
	}
	for _, bad := range []protocol.DedupState{{Capacity: 0}, {Capacity: 3, Count: 4}, {Capacity: 3, Count: 1, Next: 2}, {Capacity: 3, Count: 3, Next: 3}} {
		encodeCheckpoint(&value, 12, bad, generation)
		if _, _, _, _, err := decodeCheckpoint(value[:]); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}

func TestMissingDedupStateFailsClosedIntegration(t *testing.T) {
	if os.Getenv("STREAM_EOS_KAFKA_TEST") != "1" {
		t.Skip("set STREAM_EOS_KAFKA_TEST=1")
	}
	ctx, cfg := transactionFixture(t)
	p, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, _, err := p.LoadProgress(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err != nil {
		t.Fatal(err)
	}
	if err := commitFixture(ctx, p, []JSONRecord{{Value: []byte(`{"id":1}`)}}, 1); err != nil {
		t.Fatal(err)
	}
	// A tombstone simulates required slot history being lost/corrupted.
	if err := p.publisher.client.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := p.publisher.client.ProduceSync(ctx, &kgo.Record{Topic: cfg.ChangelogTopic, Partition: 0, Key: p.stateKeys[0][:], Value: nil}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := p.publisher.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	replacement, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if _, _, err := replacement.LoadProgress(ctx); err != nil {
		t.Fatal(err)
	}
	if err := replacement.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err == nil {
		t.Fatal("resumed with missing dedup state")
	}
	if err := commitFixture(ctx, replacement, nil, 2); err == nil {
		t.Fatal("processing allowed after failed recovery")
	}
}

type fixtureSnapshot struct {
	events []protocol.EventID
	state  protocol.DedupState
}

func (s fixtureSnapshot) SnapshotSlot(slot uint32) protocol.EventID { return s.events[slot] }
func (s fixtureSnapshot) SnapshotState() protocol.DedupState        { return s.state }

func TestLegacyBootstrapGenerationIntegration(t *testing.T) {
	if os.Getenv("STREAM_EOS_KAFKA_TEST") != "1" {
		t.Skip("set STREAM_EOS_KAFKA_TEST=1")
	}
	ctx, cfg := transactionFixture(t)
	cfg.BatchCapacity = 2
	old, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	// Preserve an old-format checkpoint. No migration output should be emitted.
	var checkpoint [8]byte
	binary.BigEndian.PutUint64(checkpoint[:], 10)
	if err := old.publisher.client.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := old.publisher.client.ProduceSync(ctx, &kgo.Record{Topic: cfg.ProgressTopic, Partition: 0, Key: old.key, Value: checkpoint[:]}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := old.publisher.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	replacement, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if next, found, err := replacement.LoadProgress(ctx); err != nil || !found || next != 10 || !replacement.NeedsBootstrap() {
		t.Fatalf("legacy: %d %v %v", next, found, err)
	}
	if err := replacement.RestoreDedup(ctx, &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}); err == nil {
		t.Fatal("legacy checkpoint silently restored empty FIFO")
	}
	// An incomplete prior snapshot is committed but has no referencing checkpoint.
	replacement.generation = [16]byte{9}
	replacement.setSlotRecord(0, 0, 7, protocol.EventID{99}, 10)
	if err := replacement.commitRecords(ctx, 1); err != nil {
		t.Fatal(err)
	}
	snapshot := fixtureSnapshot{events: []protocol.EventID{{1}, {2}, {3}}, state: protocol.DedupState{Capacity: 10, Count: 3, Next: 3}}
	if err := replacement.BootstrapDedup(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	resumed, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if next, found, err := resumed.LoadProgress(ctx); err != nil || !found || next != 10 || resumed.NeedsBootstrap() {
		t.Fatalf("migration: %d %v %v", next, found, err)
	}
	target := &fixtureFIFO{slots: make(map[uint32]protocol.EventID)}
	if err := resumed.RestoreDedup(ctx, target); err != nil {
		t.Fatal(err)
	}
	for n, event := range snapshot.events {
		if target.slots[uint32(n)] != event {
			t.Fatal("wrong snapshot generation restored")
		}
	}
	if got := fixtureOutput(t, ctx, cfg, true); len(got) != 0 {
		t.Fatalf("migration repeated output: %v", got)
	}
	cfg.DedupCapacity = 11
	incompatible, err := NewTransactionPublisher(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer incompatible.Close()
	if _, _, err := incompatible.LoadProgress(ctx); err == nil {
		t.Fatal("capacity change accepted without migration")
	}
}

// Measures only preallocated slot/checkpoint encoding and record wrappers.
// Broker I/O, transaction commits, recovery and startup allocations are excluded.
func BenchmarkFIFOStatePreparation(b *testing.B) {
	const batch = 1000
	p := &TransactionPublisher{
		publisher: &Publisher{jsonRecords: make([]kgo.Record, batch+1), jsonPointers: make([]*kgo.Record, batch+1)},
		cfg:       TransactionConfig{ChangelogTopic: "state", ProgressTopic: "progress"},
		stateKeys: make([][52]byte, batch), stateValues: make([][32]byte, batch), key: []byte("scope"),
	}
	ids := make([]protocol.EventID, batch)
	for n := range ids {
		binary.BigEndian.PutUint64(ids[n][16:], uint64(n+1))
	}
	state := protocol.DedupState{Capacity: 1000000, Count: 1000000, Next: batch}
	b.ReportAllocs()
	for b.Loop() {
		for n := range ids {
			p.setSlotRecord(n, n, uint32(n), ids[n], 1000)
		}
		p.setCheckpointRecord(batch, 1000, state)
		if len(p.publisher.jsonPointers[batch].Value) != 40 {
			b.Fatal("checkpoint")
		}
	}
}
