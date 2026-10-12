package engine

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"stream-eos/process/internal/protocol"
	"stream-eos/process/internal/kafka"
)

// Real Kafka tests the connected processor, not only the transaction adapter.
// A three-ID window makes retained duplicates and exact FIFO eviction observable.
func TestPersistentDedupRestartIntegration(t *testing.T) {
	if os.Getenv("STREAM_EOS_KAFKA_TEST") != "1" {
		t.Skip("set STREAM_EOS_KAFKA_TEST=1; creates/deletes isolated fixture topics")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	base := fmt.Sprintf("stream-eos-recovery-test-%d", time.Now().UnixNano())
	cfg := kafka.TransactionConfig{Brokers: []string{"localhost:9092"}, InputTopic: base + "-in", OutputTopic: base + "-out", ProgressTopic: base + "-progress", ChangelogTopic: base + "-state", Partition: 0, BatchCapacity: 3, DedupCapacity: 3}
	admin, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	topics := []string{cfg.InputTopic, cfg.OutputTopic, cfg.ProgressTopic, cfg.ChangelogTopic}
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 10000
	for _, topic := range topics {
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
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.TopicNames = topics
		if _, err := admin.Request(cleanup, request); err != nil {
			t.Error(err)
		}
	}()
	publish := func(sequences ...byte) {
		t.Helper()
		records := make([]*kgo.Record, len(sequences))
		for n, seq := range sequences {
			r := record(seq)
			copy(r.Value[:4], "SEV1")
			binary.BigEndian.PutUint32(r.Key[:], 9910001)
			binary.BigEndian.PutUint32(r.Value[28:32], 9910001)
			records[n] = &kgo.Record{Topic: cfg.InputTopic, Partition: 0, Key: r.Key[:], Value: r.Value[:]}
		}
		if err := admin.ProduceSync(ctx, records...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	var publisher *kafka.TransactionPublisher
	var processor *Processor
	var consumer *kafka.Consumer
	startOwner := func(wantNext int64) {
		t.Helper()
		if consumer != nil {
			consumer.Close()
			publisher.Close()
		}
		publisher, err = kafka.NewTransactionPublisher(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		next, found, err := publisher.LoadProgress(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if wantNext == 0 {
			if found {
				t.Fatal("unexpected initial checkpoint")
			}
		} else if !found || next != wantNext {
			t.Fatalf("resume=%d found=%v want=%d", next, found, wantNext)
		}
		processor, err = NewProcessor(0, 3, 3, publisher)
		if err != nil {
			t.Fatal(err)
		}
		if err := publisher.RestoreDedup(ctx, processor); err != nil {
			t.Fatal(err)
		}
		consumer, err = kafka.NewConsumer(ctx, kafka.ConsumerConfig{Brokers: cfg.Brokers, Topic: cfg.InputTopic, Partition: 0, StartOffset: next, BatchRecords: 3, BatchBytes: 3 * 216})
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		if consumer != nil {
			consumer.Close()
		}
		if publisher != nil {
			publisher.Close()
		}
	}()
	process := func(wantUnique, wantDuplicates int) {
		t.Helper()
		batch, err := consumer.ReadBatch(ctx, make([]kafka.Record, 3))
		if err != nil {
			t.Fatal(err)
		}
		unique, duplicates, err := processor.Process(ctx, batch)
		if err != nil || unique != wantUnique || duplicates != wantDuplicates {
			t.Fatalf("batch %d/%d want %d/%d err=%v", unique, duplicates, wantUnique, wantDuplicates, err)
		}
	}
	publish(20, 20, 10, 30)
	startOwner(0)
	process(2, 1)
	process(1, 0)
	startOwner(4)
	publish(20)
	process(0, 1) // Retained duplicate survives restart, progresses to 5.
	publish(40)
	// Simulate losing the successful commit response before local promotion.
	processor.publisher = lostCommitResponse{publisher}
	batch, err := consumer.ReadBatch(ctx, make([]kafka.Record, 3))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := processor.Process(ctx, batch); !errors.Is(err, errLostCommitResponse) {
		t.Fatalf("post-commit error=%v", err)
	}
	if state := processor.SnapshotState(); state.Next != 0 {
		t.Fatalf("local FIFO unexpectedly promoted: %+v", state)
	}
	// Kafka has committed ID 40 and cursor 1, despite the old local cursor 0.
	startOwner(6)
	publish(20, 10)
	process(1, 1) // ID 20 is eligible again; retained ID 10 is skipped.
	startOwner(8)
	publish(10, 30)
	process(1, 1) // Next eviction order also survives restart.
	// Inspect committed output through a captured fixture log end.
	offsetRequest := kmsg.NewPtrListOffsetsRequest()
	part := kmsg.NewListOffsetsRequestTopicPartition()
	part.Partition = 0
	part.Timestamp = -1
	offsetRequest.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: cfg.OutputTopic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{part}}}
	offsets, err := admin.Request(ctx, offsetRequest)
	if err != nil {
		t.Fatal(err)
	}
	end := offsets.(*kmsg.ListOffsetsResponse).Topics[0].Partitions[0].Offset
	reader, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.OutputTopic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var eventIDs []string
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
			if r.Attrs.IsControl() {
				return
			}
			var event JSONEvent
			if err := json.Unmarshal(r.Value, &event); err != nil {
				t.Fatal(err)
			}
			eventIDs = append(eventIDs, event.EventID)
		})
		if reached {
			break
		}
	}
	expected := []byte{20, 10, 30, 40, 20, 10}
	if len(eventIDs) != len(expected) {
		t.Fatalf("outputs=%v", eventIDs)
	}
	for n, seq := range expected {
		r := record(seq)
		if eventIDs[n] != hex.EncodeToString(r.Value[4:28]) {
			t.Fatalf("output %d=%s", n, eventIDs[n])
		}
	}
}

var errLostCommitResponse = errors.New("simulated lost commit response")

type lostCommitResponse struct{ publisher Publisher }

func (p lostCommitResponse) CommitBatch(ctx context.Context, records []Output, next int64, ids []protocol.EventID, state protocol.DedupState) error {
	if err := p.publisher.CommitBatch(ctx, records, next, ids, state); err != nil {
		return err
	}
	return errLostCommitResponse
}
