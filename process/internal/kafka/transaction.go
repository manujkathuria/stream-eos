package kafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"stream-eos/process/internal/protocol"
)

type TransactionConfig struct {
	Brokers                                                []string
	InputTopic, OutputTopic, ProgressTopic, ChangelogTopic string
	Partition                                              int32
	BatchCapacity, DedupCapacity                           int
}

// FIFORecovery owns startup storage, validates slot coverage, and rebuilds its
// membership index only after all committed slot records have been read.
type FIFORecovery interface {
	RestoreSlot(uint32, protocol.EventID) error
	RestoreFIFO(protocol.DedupState) error
}

// FIFOSnapshot is used only to migrate pre-persistence checkpoints.
type FIFOSnapshot interface {
	SnapshotSlot(uint32) protocol.EventID
	SnapshotState() protocol.DedupState
}

// TransactionPublisher commits output, changed FIFO slots and input progress.
// Methods are sequential. Transaction failures require a fresh instance/recovery.
type TransactionPublisher struct {
	publisher                                               *Publisher
	cfg                                                     TransactionConfig
	key                                                     []byte
	scope                                                   [32]byte
	generation                                              [16]byte
	progress                                                [40]byte
	stateKeys                                               [][52]byte
	stateValues                                             [][32]byte
	state                                                   protocol.DedupState
	poisoned, hasProgress, restored, legacy, progressLoaded bool
	lastNext                                                int64
}

func NewTransactionPublisher(ctx context.Context, cfg TransactionConfig) (*TransactionPublisher, error) {
	if len(cfg.Brokers) == 0 || cfg.Partition < 0 || cfg.BatchCapacity < 1 || cfg.BatchCapacity > 10000 || cfg.DedupCapacity < cfg.BatchCapacity || uint64(cfg.DedupCapacity) > math.MaxUint32 {
		return nil, fmt.Errorf("invalid brokers, partition, batch or dedup capacity")
	}
	topics := []string{cfg.InputTopic, cfg.OutputTopic, cfg.ProgressTopic, cfg.ChangelogTopic}
	for n, topic := range topics {
		if topic == "" {
			return nil, fmt.Errorf("all four topics are required")
		}
		for _, other := range topics[:n] {
			if topic == other {
				return nil, fmt.Errorf("all four topics must differ")
			}
		}
	}
	key := []byte(fmt.Sprintf("%s\x00%s\x00%d", cfg.InputTopic, cfg.OutputTopic, cfg.Partition))
	// Preserve the existing transactional identity across the persistence upgrade.
	scope := sha256.Sum256(append(append([]byte{}, key...), []byte("\x00"+cfg.ProgressTopic)...))
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID(fmt.Sprintf("engine-%d", cfg.Partition)), kgo.TransactionalID(fmt.Sprintf("stream-eos-%x", scope)), kgo.TransactionTimeout(60*time.Second), kgo.RecordDeliveryTimeout(30*time.Second), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.MaxBufferedRecords(2*cfg.BatchCapacity+1))
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*TransactionPublisher, error) { client.Close(); return nil, err }
	setup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for _, topic := range topics {
		if err := validatePartition(setup, client, topic, cfg.Partition); err != nil {
			return fail(err)
		}
	}
	if err := validateStateRetention(setup, client, cfg.ProgressTopic, cfg.ChangelogTopic); err != nil {
		return fail(err)
	}
	if err := initializeOwner(setup, client); err != nil {
		return fail(fmt.Errorf("initialize transactional owner: %w", err))
	}
	p := &TransactionPublisher{publisher: &Publisher{client: client, topic: cfg.OutputTopic, jsonRecords: make([]kgo.Record, 2*cfg.BatchCapacity+1), jsonPointers: make([]*kgo.Record, 2*cfg.BatchCapacity+1)}, cfg: cfg, key: key, scope: scope, state: protocol.DedupState{Capacity: uint32(cfg.DedupCapacity)}, stateKeys: make([][52]byte, cfg.BatchCapacity), stateValues: make([][32]byte, cfg.BatchCapacity)}
	if _, err := rand.Read(p.generation[:]); err != nil {
		return fail(err)
	}
	return p, nil
}
func (p *TransactionPublisher) Close() { p.publisher.Close() }
func (p *TransactionPublisher) NeedsBootstrap() bool {
	return p.progressLoaded && p.hasProgress && p.legacy
}

func validateFIFOState(state protocol.DedupState) error {
	if state.Capacity == 0 || state.Count > state.Capacity || state.Next >= state.Capacity || (state.Count < state.Capacity && state.Next != state.Count) {
		return fmt.Errorf("invalid persisted FIFO metadata")
	}
	return nil
}
func encodeCheckpoint(dst *[40]byte, next int64, state protocol.DedupState, generation [16]byte) {
	copy(dst[:4], "DFV1")
	binary.BigEndian.PutUint64(dst[4:12], uint64(next))
	binary.BigEndian.PutUint32(dst[12:16], state.Capacity)
	binary.BigEndian.PutUint32(dst[16:20], state.Count)
	binary.BigEndian.PutUint32(dst[20:24], state.Next)
	copy(dst[24:], generation[:])
}
func decodeProgress(value []byte) (int64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid legacy progress length")
	}
	offset := binary.BigEndian.Uint64(value)
	if offset > math.MaxInt64 {
		return 0, fmt.Errorf("invalid progress offset")
	}
	return int64(offset), nil
}
func decodeCheckpoint(value []byte) (next int64, state protocol.DedupState, generation [16]byte, legacy bool, err error) {
	if len(value) == 8 {
		next, err = decodeProgress(value)
		return next, state, generation, true, err
	}
	if len(value) != 40 || string(value[:4]) != "DFV1" {
		err = fmt.Errorf("invalid persisted checkpoint version/length")
		return
	}
	raw := binary.BigEndian.Uint64(value[4:12])
	if raw > math.MaxInt64 {
		err = fmt.Errorf("invalid progress offset")
		return
	}
	next = int64(raw)
	state = protocol.DedupState{Capacity: binary.BigEndian.Uint32(value[12:16]), Count: binary.BigEndian.Uint32(value[16:20]), Next: binary.BigEndian.Uint32(value[20:24])}
	copy(generation[:], value[24:])
	err = validateFIFOState(state)
	return
}

// LoadProgress fences first (constructor), then reads a stable committed boundary.
// A checkpoint alone never authorizes processing; RestoreDedup must also succeed.
func (p *TransactionPublisher) LoadProgress(ctx context.Context) (next int64, found bool, err error) {
	p.restored = false
	p.progressLoaded = false
	err = p.scanCommitted(ctx, p.cfg.ProgressTopic, func(r *kgo.Record) error {
		if !bytes.Equal(r.Key, p.key) {
			return nil
		}
		value, state, generation, legacy, err := decodeCheckpoint(r.Value)
		if err != nil {
			return err
		}
		if found && value < next {
			return fmt.Errorf("committed input progress moved backwards")
		}
		if !legacy && state.Capacity != uint32(p.cfg.DedupCapacity) {
			return fmt.Errorf("dedup capacity changed: checkpoint=%d configured=%d; explicit migration required", state.Capacity, p.cfg.DedupCapacity)
		}
		if found && !p.legacy && legacy {
			return fmt.Errorf("checkpoint schema moved backwards")
		}
		next, found = value, true
		p.state, p.generation, p.legacy = state, generation, legacy
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	p.lastNext, p.hasProgress, p.progressLoaded = next, found, true
	if !found {
		p.state = protocol.DedupState{Capacity: uint32(p.cfg.DedupCapacity)}
		p.legacy = false
	}
	return next, found, nil
}

// RestoreDedup rebuilds exactly the committed physical slots and cursor. Latest
// values survive compaction. Missing, corrupt, or incompatible state fails closed.
func (p *TransactionPublisher) RestoreDedup(ctx context.Context, target FIFORecovery) error {
	p.restored = false
	if p.poisoned {
		return fmt.Errorf("transaction publisher requires restart")
	}
	if !p.progressLoaded {
		return fmt.Errorf("load committed progress before FIFO recovery")
	}
	if p.legacy {
		return fmt.Errorf("legacy checkpoint lacks dedup state; explicit bootstrap required")
	}
	if target == nil {
		return fmt.Errorf("recovery target required")
	}
	if p.hasProgress {
		if err := p.scanCommitted(ctx, p.cfg.ChangelogTopic, func(r *kgo.Record) error {
			if len(r.Key) < 32 || !bytes.Equal(r.Key[:32], p.scope[:]) {
				return nil
			}
			if len(r.Key) != 52 {
				return fmt.Errorf("invalid FIFO slot key length")
			}
			if !bytes.Equal(r.Key[32:48], p.generation[:]) {
				return nil
			} // Unreferenced bootstrap generations.
			slot := binary.BigEndian.Uint32(r.Key[48:])
			if slot >= p.state.Capacity || len(r.Value) != 32 {
				return fmt.Errorf("invalid FIFO slot record")
			}
			stamp := binary.BigEndian.Uint64(r.Value[24:])
			if stamp > uint64(p.lastNext) {
				return fmt.Errorf("FIFO state is ahead of committed input progress")
			}
			var event protocol.EventID
			copy(event[:], r.Value[:24])
			return target.RestoreSlot(slot, event)
		}); err != nil {
			return err
		}
	}
	if err := target.RestoreFIFO(p.state); err != nil {
		return err
	}
	p.restored = true
	return nil
}

// CommitBatch keeps borrowed output/ID buffers until every callback finishes.
// Output, changed ring slots and checkpoint become visible in one transaction.
func (p *TransactionPublisher) CommitBatch(ctx context.Context, batch []JSONRecord, nextOffset int64, ids []protocol.EventID, state protocol.DedupState) error {
	if !p.restored {
		return fmt.Errorf("restore committed progress and FIFO before processing")
	}
	if p.poisoned {
		return fmt.Errorf("transaction publisher requires restart after previous failure")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if nextOffset < 0 || (p.hasProgress && nextOffset <= p.lastNext) || len(batch) > p.cfg.BatchCapacity || len(ids) != len(batch) {
		return fmt.Errorf("invalid transaction batch, IDs or next offset")
	}
	expected := protocol.DedupState{Capacity: p.state.Capacity, Count: uint32(min(uint64(p.state.Capacity), uint64(p.state.Count)+uint64(len(ids)))), Next: uint32((uint64(p.state.Next) + uint64(len(ids))) % uint64(p.state.Capacity))}
	if state != expected {
		return fmt.Errorf("local/persisted FIFO plan mismatch")
	}
	records, err := p.publisher.prepareJSON(batch)
	if err != nil {
		return err
	}
	for _, r := range records {
		r.Partition = p.cfg.Partition
	}
	slot := len(batch)
	for n := range ids {
		ringSlot := uint32((uint64(p.state.Next) + uint64(n)) % uint64(p.state.Capacity))
		p.setSlotRecord(slot, n, ringSlot, ids[n], nextOffset)
		slot++
	}
	p.setCheckpointRecord(slot, nextOffset, state)
	if err := p.commitRecords(ctx, slot+1); err != nil {
		return err
	}
	p.lastNext, p.hasProgress, p.state = nextOffset, true, state
	return nil
}

func (p *TransactionPublisher) setSlotRecord(recordSlot, bufferSlot int, ringSlot uint32, event protocol.EventID, next int64) {
	key := &p.stateKeys[bufferSlot]
	copy(key[:32], p.scope[:])
	copy(key[32:48], p.generation[:])
	binary.BigEndian.PutUint32(key[48:], ringSlot)
	value := &p.stateValues[bufferSlot]
	copy(value[:24], event[:])
	binary.BigEndian.PutUint64(value[24:], uint64(next))
	p.publisher.jsonRecords[recordSlot] = kgo.Record{Topic: p.cfg.ChangelogTopic, Partition: p.cfg.Partition, Key: key[:], Value: value[:]}
	p.publisher.jsonPointers[recordSlot] = &p.publisher.jsonRecords[recordSlot]
}
func (p *TransactionPublisher) setCheckpointRecord(slot int, next int64, state protocol.DedupState) {
	encodeCheckpoint(&p.progress, next, state, p.generation)
	p.publisher.jsonRecords[slot] = kgo.Record{Topic: p.cfg.ProgressTopic, Partition: p.cfg.Partition, Key: p.key, Value: p.progress[:]}
	p.publisher.jsonPointers[slot] = &p.publisher.jsonRecords[slot]
}

func (p *TransactionPublisher) commitRecords(ctx context.Context, count int) error {
	defer func() { clear(p.publisher.jsonRecords[:count]); clear(p.publisher.jsonPointers[:count]) }()
	client := p.publisher.client
	if err := client.BeginTransaction(); err != nil {
		p.poisoned = true
		return fmt.Errorf("begin batch transaction: %w", err)
	}
	if err := client.ProduceSync(ctx, p.publisher.jsonPointers[:count]...).FirstErr(); err != nil {
		p.poisoned = true
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		bufferedErr := client.AbortBufferedRecords(cleanup)
		abortErr := client.EndTransaction(cleanup, kgo.TryAbort)
		return errors.Join(fmt.Errorf("produce batch transaction: %w", err), bufferedErr, abortErr)
	}
	commit, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.EndTransaction(commit, kgo.TryCommit); err != nil {
		p.poisoned = true
		if errors.Is(err, kerr.OperationNotAttempted) || errors.Is(err, kerr.TransactionAbortable) {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			abortErr := client.EndTransaction(cleanup, kgo.TryAbort)
			return errors.Join(fmt.Errorf("batch commit requires abort: %w", err), abortErr)
		}
		return fmt.Errorf("batch commit failed or uncertain; stop and restore: %w", err)
	}
	return nil
}

// BootstrapDedup publishes a new snapshot generation without output or advancing
// legacy input progress. Only the final transaction publishes its checkpoint.
// An interrupted bootstrap leaves legacy progress intact; a retry uses a new
// generation, so partial snapshots never become the restored committed window.
func (p *TransactionPublisher) BootstrapDedup(ctx context.Context, snapshot FIFOSnapshot) error {
	if !p.NeedsBootstrap() || p.poisoned || snapshot == nil {
		return fmt.Errorf("bootstrap requires a loaded legacy checkpoint")
	}
	state := snapshot.SnapshotState()
	if err := validateFIFOState(state); err != nil {
		return err
	}
	if state.Capacity != uint32(p.cfg.DedupCapacity) {
		return fmt.Errorf("bootstrap capacity mismatch")
	}
	if _, err := rand.Read(p.generation[:]); err != nil {
		return err
	}
	count := int(state.Count)
	for start := 0; ; {
		end := min(start+p.cfg.BatchCapacity, count)
		slot := 0
		for n := start; n < end; n++ {
			p.setSlotRecord(slot, slot, uint32(n), snapshot.SnapshotSlot(uint32(n)), p.lastNext)
			slot++
		}
		final := end == count
		if final {
			p.setCheckpointRecord(slot, p.lastNext, state)
			slot++
		}
		if err := p.commitRecords(ctx, slot); err != nil {
			return err
		}
		if final {
			break
		}
		start = end
	}
	p.state, p.legacy, p.restored = state, false, true
	return nil
}

// InputBounds supports explicit legacy bootstrap only. Required input history
// must begin at zero and extend through the old checkpoint; no silent reset.
func (p *TransactionPublisher) InputBounds(ctx context.Context) (start, end int64, err error) {
	start, err = partitionOffset(ctx, p.publisher.client, p.cfg.InputTopic, p.cfg.Partition, -2)
	if err != nil {
		return
	}
	end, err = partitionOffset(ctx, p.publisher.client, p.cfg.InputTopic, p.cfg.Partition, -1)
	return
}

// scanCommitted captures a retained log boundary, includes control records to
// cross an aborted tail, and rejects nontransactional application state records.
func (p *TransactionPublisher) scanCommitted(ctx context.Context, topic string, visit func(*kgo.Record) error) error {
	restore, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start, err := partitionOffset(restore, p.publisher.client, topic, p.cfg.Partition, -2)
	if err != nil {
		return err
	}
	end, err := partitionOffset(restore, p.publisher.client, topic, p.cfg.Partition, -1)
	if err != nil {
		return err
	}
	if start >= end {
		return nil
	}
	// A bounded raw fetch exposes empty responses at compacted tail holes.
	// PollRecords alone cannot observe that it has crossed such holes.
	reader, err := kgo.NewClient(kgo.SeedBrokers(p.cfg.Brokers...), kgo.MaxVersions(kversion.V2_8_0()))
	if err != nil {
		return err
	}
	defer reader.Close()
	metadataRequest := kmsg.NewPtrMetadataRequest()
	metadataRequest.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
	metadataResponse, err := reader.Request(restore, metadataRequest)
	if err != nil {
		return err
	}
	leader := int32(-1)
	for _, t := range metadataResponse.(*kmsg.MetadataResponse).Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			return err
		}
		for _, part := range t.Partitions {
			if part.Partition == p.cfg.Partition {
				if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
					return err
				}
				leader = part.Leader
			}
		}
	}
	if leader < 0 {
		return fmt.Errorf("missing leader for %s partition %d", topic, p.cfg.Partition)
	}
	decompressor := kgo.DefaultDecompressor()
	for position := start; position < end; {
		request := kmsg.NewPtrFetchRequest()
		request.MaxWaitMillis = 500
		request.MinBytes = 1
		request.MaxBytes = 1 << 20
		request.IsolationLevel = 1
		part := kmsg.NewFetchRequestTopicPartition()
		part.Partition = p.cfg.Partition
		part.FetchOffset = position
		part.PartitionMaxBytes = 1 << 20
		request.Topics = []kmsg.FetchRequestTopic{{Topic: topic, Partitions: []kmsg.FetchRequestTopicPartition{part}}}
		response, err := reader.Broker(int(leader)).Request(restore, request)
		if err != nil {
			return fmt.Errorf("restore %s: %w", topic, err)
		}
		fetched := response.(*kmsg.FetchResponse)
		if err := kerr.ErrorForCode(fetched.ErrorCode); err != nil {
			return err
		}
		found := false
		for _, t := range fetched.Topics {
			for _, rp := range t.Partitions {
				if t.Topic != topic || rp.Partition != p.cfg.Partition {
					continue
				}
				found = true
				fp, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: position, Topic: topic, Partition: p.cfg.Partition, IsolationLevel: kgo.ReadCommitted(), KeepControlRecords: true}, &rp, decompressor, nil)
				if fp.Err != nil {
					return fp.Err
				}
				for _, r := range fp.Records {
					if r.Offset >= end || r.Attrs.IsControl() {
						continue
					}
					if !r.Attrs.IsTransactional() {
						return fmt.Errorf("nontransactional state in %s", topic)
					}
					if err := visit(r); err != nil {
						return err
					}
				}
				if len(rp.RecordBatches) == 0 && rp.LastStableOffset >= end {
					return nil
				}
				position = next
			}
		}
		if !found {
			return fmt.Errorf("missing fetch partition for %s", topic)
		}
	}
	return nil
}

func partitionOffset(ctx context.Context, client *kgo.Client, topic string, partition int32, timestamp int64) (int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	part := kmsg.NewListOffsetsRequestTopicPartition()
	part.Partition = partition
	part.Timestamp = timestamp
	request.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{part}}}
	response, err := client.Request(ctx, request)
	if err != nil {
		return 0, err
	}
	for _, t := range response.(*kmsg.ListOffsetsResponse).Topics {
		for _, p := range t.Partitions {
			if t.Topic == topic && p.Partition == partition {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					return 0, err
				}
				return p.Offset, nil
			}
		}
	}
	return 0, fmt.Errorf("missing partition offset response")
}

// The pinned client's ProducerID leaves retryable load failures reloadable.
// InitProducerID may report ConcurrentTransactions while old abort markers settle.
func initializeOwner(ctx context.Context, client *kgo.Client) error {
	for {
		_, _, err := client.ProducerID(ctx)
		if err == nil {
			return nil
		}
		if !errors.Is(err, kerr.ConcurrentTransactions) && !kerr.IsRetriable(err) {
			return err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
}

// State keys must survive indefinitely; delete retention can erase live slots.
func validateStateRetention(ctx context.Context, client *kgo.Client, topics ...string) error {
	req := kmsg.NewPtrDescribeConfigsRequest()
	for _, topic := range topics {
		req.Resources = append(req.Resources, kmsg.DescribeConfigsRequestResource{ResourceType: 2, ResourceName: topic, ConfigNames: []string{"cleanup.policy"}})
	}
	response, err := client.Request(ctx, req)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(topics))
	for _, resource := range response.(*kmsg.DescribeConfigsResponse).Resources {
		if err := kerr.ErrorForCode(resource.ErrorCode); err != nil {
			return err
		}
		for _, config := range resource.Configs {
			if config.Name == "cleanup.policy" {
				if config.Value == nil || *config.Value != "compact" {
					return fmt.Errorf("state topic %s requires cleanup.policy=compact", resource.ResourceName)
				}
				seen[resource.ResourceName] = true
			}
		}
	}
	for _, topic := range topics {
		if !seen[topic] {
			return fmt.Errorf("missing cleanup policy for state topic %s", topic)
		}
	}
	return nil
}
