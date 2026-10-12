package benchtest

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Topic creates a unique three-partition, RF=1 fixture on the existing broker.
func Topic(b *testing.B, ctx context.Context) string {
	b.Helper()
	name := fmt.Sprintf("stream-eos-ingest-bench-%d", time.Now().UnixNano())
	admin, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		b.Fatal(err)
	}
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 10000
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: name, NumPartitions: 3, ReplicationFactor: 1}}
	response, err := admin.Request(ctx, request)
	if err != nil {
		admin.Close()
		b.Fatal(err)
	}
	for _, topic := range response.(*kmsg.CreateTopicsResponse).Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			admin.Close()
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		defer admin.Close()
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.TopicNames = []string{name}
		if _, err := admin.Request(cleanup, request); err != nil {
			b.Error(err)
		}
	})
	return name
}

// VerifyStored runs outside timing. Exact ID coverage detects losses, duplicates,
// corrupt bytes and wrong keys across all three partitions without trusting acks
// alone. The finite workload determines bounded validation storage.
func VerifyStored(b *testing.B, ctx context.Context, topic string, warmup, messages, packets int) {
	b.Helper()
	partitions := map[int32]kgo.Offset{}
	for p := int32(0); p < 3; p++ {
		partitions[p] = kgo.NewOffset().AtStart()
	}
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions}))
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	seen := make([]bool, (warmup+messages)*packets)
	expectedQuote := Message(packets)
	total := 0
	for total < len(seen) {
		fetches := client.PollRecords(ctx, 1000)
		if err := ctx.Err(); err != nil {
			b.Fatal(err)
		}
		if err := fetches.Err(); err != nil {
			b.Fatal(err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if len(record.Value) != 212 || string(record.Value[:4]) != "SEV1" || !bytes.Equal(record.Value[4:20], StreamID[:]) || len(record.Key) != 4 {
				b.Fatal("stored framing/identity mismatch")
			}
			sequence := binary.BigEndian.Uint64(record.Value[20:28])
			if sequence == 0 {
				b.Fatal("zero sequence")
			}
			batch, index := (sequence-1)/400, (sequence-1)%400
			if batch >= uint64(warmup+messages) || index >= uint64(packets) {
				b.Fatal("unexpected source event")
			}
			slot := int(batch)*packets + int(index)
			if seen[slot] {
				b.Fatal("duplicate stored event")
			}
			seen[slot] = true
			if binary.BigEndian.Uint32(record.Key) != 9910001+uint32(index)*256 || !bytes.Equal(record.Value[28:], expectedQuote[22+int(index)*194+10:22+int(index)*194+194]) {
				b.Fatal("stored quote/key mismatch")
			}
			if len(record.Headers) != 1 || record.Headers[0].Key != "ingestor" || string(record.Headers[0].Value) != "benchmark" {
				b.Fatal("stored provenance mismatch")
			}
			total++
		})
	}
	request := kmsg.NewPtrListOffsetsRequest()
	entry := kmsg.ListOffsetsRequestTopic{Topic: topic}
	for p := int32(0); p < 3; p++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = p
		part.Timestamp = -1
		entry.Partitions = append(entry.Partitions, part)
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{entry}
	response, err := client.Request(ctx, request)
	if err != nil {
		b.Fatal(err)
	}
	var end int64
	for _, topic := range response.(*kmsg.ListOffsetsResponse).Topics {
		for _, part := range topic.Partitions {
			if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
				b.Fatal(err)
			}
			end += part.Offset
		}
	}
	if end != int64(len(seen)) {
		b.Fatalf("stored offsets=%d expected=%d", end, len(seen))
	}
}
