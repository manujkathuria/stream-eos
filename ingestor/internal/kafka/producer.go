// Package kafka publishes source events with acknowledged, idempotent production.
package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"stream-eos/pkg/protocol"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Brokers   []string
	Topic, ID string
}

const maxBatchRecords = 400

var ErrProducerBusy = errors.New("producer already publishing; calls must be sequential")
var ErrProducerClosed = errors.New("producer is closed")

const (
	producerIdle uint32 = iota
	producerPublishing
	producerClosed
)

// recordBytes is pointer-free storage for fixed-size wire bytes.
type recordBytes struct {
	key   [4]byte
	value [212]byte
}

// Producer owns one reusable batch. Publish and Shutdown must not overlap;
// an atomic lifecycle guard rejects overlap rather than corrupting buffers.
type Producer struct {
	client   *kgo.Client
	topic    string
	idBytes  []byte
	records  []kgo.Record
	pointers []*kgo.Record
	bytes    []recordBytes
	headers  [][1]kgo.RecordHeader
	state    atomic.Uint32
}

// newProducer reserves storage once; New also establishes broker connectivity.
func newProducer(client *kgo.Client, topic, id string) *Producer {
	return &Producer{client: client, topic: topic, idBytes: []byte(id),
		records: make([]kgo.Record, maxBatchRecords), pointers: make([]*kgo.Record, maxBatchRecords),
		bytes: make([]recordBytes, maxBatchRecords), headers: make([][1]kgo.RecordHeader, maxBatchRecords)}
}

func New(ctx context.Context, cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.ID == "" {
		return nil, fmt.Errorf("brokers, topic, and identity are required")
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("empty Kafka broker")
		}
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("ingestor-"+cfg.ID), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.MaxBufferedRecords(400), kgo.RecordDeliveryTimeout(30*time.Second))
	if err != nil {
		return nil, err
	}
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(setup); err != nil {
		client.Close()
		return nil, fmt.Errorf("Kafka unavailable: %w", err)
	}
	return newProducer(client, cfg.Topic, cfg.ID), nil
}

// Shutdown flushes using a fresh caller-owned cleanup context. On timeout it
// abandons buffered work, closes the client and reports the uncertain delivery.
func (p *Producer) Shutdown(ctx context.Context) error {
	if !p.state.CompareAndSwap(producerIdle, producerClosed) {
		if p.state.Load() == producerClosed {
			return nil
		}
		return ErrProducerBusy
	}
	err := p.client.Flush(ctx)
	if err != nil {
		err = errors.Join(err, p.client.AbortBufferedRecords(ctx))
	}
	p.client.Close()
	return err
}

// Publish copies event bytes before giving them to Kafka. On success every record
// is acknowledged. A failure may follow partial success; this is not a transaction.
func (p *Producer) Publish(ctx context.Context, events []protocol.Event) error {
	if len(events) == 0 || len(events) > maxBatchRecords {
		return fmt.Errorf("invalid batch size")
	}
	for _, e := range events {
		if len(e.Quote) != 184 {
			return fmt.Errorf("invalid quote size")
		}
	}
	if !p.state.CompareAndSwap(producerIdle, producerPublishing) {
		if p.state.Load() == producerClosed {
			return ErrProducerClosed
		}
		return ErrProducerBusy
	}
	defer p.state.Store(producerIdle)
	records := p.buildRecords(events)
	// ProduceSync waits for all callbacks, including error completions. Clear
	// client-mutated metadata and release references only after that boundary.
	defer func() {
		clear(p.records[:len(events)])
		clear(p.pointers[:len(events)])
		clear(p.headers[:len(events)])
	}()
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("Kafka publish failed: %w", err)
	}
	return nil
}

// buildRecords copies input into startup-reserved wire storage. Returned records
// are borrowed until the next build; production only reuses them after callbacks.
func (p *Producer) buildRecords(events []protocol.Event) []*kgo.Record {
	for i := range events {
		e := &events[i]
		storage := &p.bytes[i]
		binary.BigEndian.PutUint32(storage.key[:], e.Instrument)
		copy(storage.value[:4], "SEV1")
		copy(storage.value[4:28], e.ID[:])
		copy(storage.value[28:], e.Quote)
		p.headers[i][0] = kgo.RecordHeader{Key: "ingestor", Value: p.idBytes}
		// Reset fields modified by the client, including offsets and partition.
		p.records[i] = kgo.Record{Topic: p.topic, Key: storage.key[:], Value: storage.value[:], Headers: p.headers[i][:]}
		p.pointers[i] = &p.records[i]
	}
	return p.pointers[:len(events)]
}
