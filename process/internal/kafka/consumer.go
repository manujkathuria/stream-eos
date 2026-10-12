package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"stream-eos/process/internal/protocol"
)

const recordPayloadBytes = 216 // Four-byte key and 212-byte SEV1 value.

// ConsumerConfig describes one explicitly assigned partition. StartOffset is a
// Kafka offset, or -2 for earliest / -1 for latest. No durable progress is loaded.
type ConsumerConfig struct {
	Brokers      []string
	Topic        string
	StartOffset  int64
	Partition    int32
	BatchRecords int
	BatchBytes   int
}

// BatchCapacity validates configuration and returns the effective poll bound.
func (c ConsumerConfig) BatchCapacity() (int, error) { return c.pollLimit() }

func (c ConsumerConfig) pollLimit() (int, error) {
	if len(c.Brokers) == 0 || c.Topic == "" {
		return 0, fmt.Errorf("brokers and topic are required")
	}
	for _, b := range c.Brokers {
		if b == "" {
			return 0, fmt.Errorf("empty broker address")
		}
	}
	if c.Partition < 0 {
		return 0, fmt.Errorf("an explicit non-negative partition is required")
	}
	if c.StartOffset < -2 {
		return 0, fmt.Errorf("invalid start offset")
	}
	if c.BatchRecords < 1 || c.BatchRecords > 10000 {
		return 0, fmt.Errorf("batch records must be between 1 and 10000")
	}
	if c.BatchBytes < recordPayloadBytes || c.BatchBytes > 16<<20 {
		return 0, fmt.Errorf("batch bytes must be between 216 and 16777216")
	}
	return min(c.BatchRecords, c.BatchBytes/recordPayloadBytes), nil
}

// Record contains fixed-size owned bytes. A copied value survives batch reuse;
// a reference into the caller's batch does not. Offset is not source identity.
type Record struct {
	Offset int64
	Value  [212]byte
	Key    [4]byte
}

// EventID returns a copy of the stable identity from a validated input record.
func (r *Record) EventID() protocol.EventID {
	var id protocol.EventID
	copy(id[:], r.Value[4:28])
	return id
}

type Consumer struct {
	client    *kgo.Client
	topic     string
	partition int32
	pollLimit int
}

func NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	limit, err := cfg.pollLimit()
	if err != nil {
		return nil, err
	}
	offset := kgo.NewOffset().At(cfg.StartOffset)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(fmt.Sprintf("consumer-%d", cfg.Partition)),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: {cfg.Partition: offset}}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxBytes(1<<20), kgo.FetchMaxPartitionBytes(1<<20),
		kgo.MaxConcurrentFetches(1),
	)
	if err != nil {
		return nil, err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := validatePartition(checkCtx, client, cfg.Topic, cfg.Partition); err != nil {
		client.Close()
		return nil, err
	}
	return &Consumer{client: client, topic: cfg.Topic, partition: cfg.Partition, pollLimit: limit}, nil
}

func validatePartition(ctx context.Context, client *kgo.Client, topic string, partition int32) error {
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
	response, err := client.Request(ctx, request)
	if err != nil {
		return fmt.Errorf("read topic metadata: %w", err)
	}
	metadata := response.(*kmsg.MetadataResponse)
	for _, t := range metadata.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			return fmt.Errorf("topic %s: %w", topic, err)
		}
		for _, p := range t.Partitions {
			if p.Partition == partition {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					return fmt.Errorf("partition %d: %w", partition, err)
				}
				return nil
			}
		}
	}
	return fmt.Errorf("topic %s does not have partition %d", topic, partition)
}

func (c *Consumer) Close() { c.client.Close() }

// ErrBatchCapacity reports insufficient caller-owned storage. The consumer never
// grows the destination. This is checked before polling so progress cannot advance.
var ErrBatchCapacity = errors.New("insufficient record batch capacity")

// BatchCapacity is the storage capacity required for one ReadBatch call.
func (c *Consumer) BatchCapacity() int { return c.pollLimit }

// ReadBatch fills caller-owned storage, without allocating a new record slice.
// Returned records alias dst: do not reuse it until every consumer has finished.
// Polling/client internals may allocate; errors may allocate contextual messages.
// Reads are sequential and cancelable, and never commit durable progress. On a
// fetch/validation error stop the reader; local position is not automatically reset.
func (c *Consumer) ReadBatch(ctx context.Context, dst []Record) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cap(dst) < c.pollLimit {
		return nil, ErrBatchCapacity
	}
	fetches := c.client.PollRecords(ctx, c.pollLimit)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.copyBatch(fetches, dst)
}

// copyBatch validates library results and copies fixed-size bytes into reusable
// application storage. Success performs no allocations after caller setup.
func (c *Consumer) copyBatch(fetches kgo.Fetches, dst []Record) ([]Record, error) {
	count := fetches.NumRecords()
	if count > c.pollLimit || count > cap(dst) {
		return nil, ErrBatchCapacity
	}
	// Inspect errors directly without building the slice returned by Fetches.Errors.
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			for _, partition := range topic.Partitions {
				if partition.Err != nil {
					return nil, fmt.Errorf("fetch %s partition %d: %w", topic.Topic, partition.Partition, partition.Err)
				}
			}
		}
	}
	records := dst[:count]
	pos := 0
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			for _, partition := range topic.Partitions {
				for _, wire := range partition.Records {
					record, err := copyInputRecord(wire, c.topic, c.partition)
					if err != nil {
						return nil, err
					}
					records[pos] = record
					pos++
				}
			}
		}
	}
	return records, nil
}

func copyInputRecord(r *kgo.Record, topic string, partition int32) (Record, error) {
	if r.Topic != topic || r.Partition != partition {
		return Record{}, fmt.Errorf("unexpected topic/partition: %s/%d", r.Topic, r.Partition)
	}
	if len(r.Key) != 4 || len(r.Value) != 212 || string(r.Value[:4]) != "SEV1" {
		return Record{}, fmt.Errorf("invalid input envelope at offset %d", r.Offset)
	}
	if binary.BigEndian.Uint32(r.Key) != binary.BigEndian.Uint32(r.Value[28:32]) {
		return Record{}, fmt.Errorf("instrument key mismatch at offset %d", r.Offset)
	}
	var record Record
	record.Offset = r.Offset
	copy(record.Key[:], r.Key)
	copy(record.Value[:], r.Value)
	return record, nil
}
