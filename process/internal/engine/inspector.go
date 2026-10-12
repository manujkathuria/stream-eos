// Package engine coordinates partition-local input processing.
package engine

import (
	"fmt"

	"stream-eos/process/internal/dedup"
	"stream-eos/process/internal/kafka"
)

// Inspector counts unique events for one partition in memory only. It has no
// output or durable progress. One caller uses it sequentially; restart loses IDs.
type Inspector struct {
	ids       *dedup.Partition
	partition int32
}

func NewInspector(partition int32, maxIDs, batchCapacity int) (*Inspector, error) {
	ids, err := dedup.New(dedup.Config{Partition: partition, MaxIDs: maxIDs, MaxPending: batchCapacity})
	if err != nil {
		return nil, err
	}
	return &Inspector{ids: ids, partition: partition}, nil
}

// Inspect finishes before records can be reused and retains only copied IDs.
// Success promotes IDs locally for later batches. Failure discards the entire
// tentative batch; callers must stop because the reader has already advanced.
// This local promotion must not be used as a durable processing acknowledgement.
func (i *Inspector) Inspect(records []kafka.Record) (unique, duplicates int, err error) {
	if len(records) == 0 {
		return 0, 0, nil
	}
	if err = i.ids.BeginBatch(); err != nil {
		return 0, 0, err
	}
	for n := range records {
		fresh, stageErr := i.ids.Stage(i.partition, records[n].EventID())
		if stageErr != nil {
			_ = i.ids.Abort()
			return 0, 0, fmt.Errorf("deduplicate partition %d offset %d: %w", i.partition, records[n].Offset, stageErr)
		}
		if fresh {
			unique++
		} else {
			duplicates++
		}
	}
	if err = i.ids.Commit(); err != nil {
		return 0, 0, err
	}
	return unique, duplicates, nil
}
