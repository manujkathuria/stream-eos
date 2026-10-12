// Package kafka owns engine input and transactional output adapters.
package kafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	client       *kgo.Client
	topic        string
	jsonRecords  []kgo.Record
	jsonPointers []*kgo.Record
}

func (p *Publisher) Close() { p.client.Close() }

// JSONRecord owns encoded output bytes and preserves the instrument key.
type JSONRecord struct {
	Value []byte
	Key   [4]byte
}

// PublishJSON waits for acknowledgements before returning. The caller must keep
// records unchanged until return. Errors can follow partial acknowledged output.
func (p *Publisher) PublishJSON(ctx context.Context, batch []JSONRecord) error {
	if len(batch) == 0 {
		return nil
	}
	records, err := p.prepareJSON(batch)
	if err != nil {
		return err
	}
	// ProduceSync waits for every completion callback, including failures. Only
	// after it returns may wrappers and borrowed key/value storage be reused.
	defer func() { clear(p.jsonRecords[:len(batch)]); clear(p.jsonPointers[:len(batch)]) }()
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("publish JSON: %w", err)
	}
	return nil
}

// prepareJSON uses startup-allocated wrappers. Publisher calls are sequential.
func (p *Publisher) prepareJSON(batch []JSONRecord) ([]*kgo.Record, error) {
	if len(batch) > len(p.jsonRecords) {
		return nil, fmt.Errorf("JSON publishing capacity exceeded")
	}
	for n := range batch {
		p.jsonRecords[n] = kgo.Record{Topic: p.topic, Key: batch[n].Key[:], Value: batch[n].Value}
		p.jsonPointers[n] = &p.jsonRecords[n]
	}
	return p.jsonPointers[:len(batch)], nil
}
