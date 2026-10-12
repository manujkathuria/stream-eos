package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func createTopics(ctx context.Context, client *kgo.Client, topics []string, owned *[]string) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10000
	for i, topic := range topics {
		entry := kmsg.CreateTopicsRequestTopic{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}
		if i >= 2 {
			policy := "compact"
			entry.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "cleanup.policy", Value: &policy}}
		}
		req.Topics = append(req.Topics, entry)
	}
	response, err := client.Request(ctx, req)
	if err != nil {
		return err
	}
	var failure error
	for _, entry := range response.(*kmsg.CreateTopicsResponse).Topics {
		if err := kerr.ErrorForCode(entry.ErrorCode); err != nil {
			failure = fmt.Errorf("create %s: %w", entry.Topic, err)
		} else {
			*owned = append(*owned, entry.Topic)
		}
	}
	return failure
}
func deleteTopics(ctx context.Context, client *kgo.Client, topics []string) error {
	if len(topics) == 0 {
		return nil
	}
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TimeoutMillis = 5000
	req.TopicNames = topics
	response, err := client.Request(ctx, req)
	if err != nil {
		return fmt.Errorf("cleanup topics %v: %w", topics, err)
	}
	for _, entry := range response.(*kmsg.DeleteTopicsResponse).Topics {
		if err := kerr.ErrorForCode(entry.ErrorCode); err != nil {
			return fmt.Errorf("cleanup topics %v: %w", topics, err)
		}
	}
	return nil
}
func newReader(brokers, topic string, controls bool) (*kgo.Client, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxBytes(1 << 20), kgo.FetchMaxPartitionBytes(1 << 20)}
	if controls {
		opts = append(opts, kgo.KeepControlRecords())
	}
	return kgo.NewClient(opts...)
}

// Snapshot through a captured log end, including transaction control markers.
// Demo topics are quiescent while read; application records are read_committed.
func snapshot(parent context.Context, admin *kgo.Client, brokers, topic string) ([]*kgo.Record, error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	end, err := logEnd(ctx, admin, topic)
	if err != nil {
		return nil, err
	}
	if end == 0 {
		return nil, nil
	}
	reader, err := newReader(brokers, topic, true)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var result []*kgo.Record
	for {
		fetch := reader.PollRecords(ctx, 1000)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fetch.Err(); err != nil {
			return nil, err
		}
		complete := false
		fetch.EachRecord(func(r *kgo.Record) {
			if r.Offset+1 >= end {
				complete = true
			}
			if r.Offset < end && !r.Attrs.IsControl() {
				result = append(result, r)
			}
		})
		if complete {
			return result, nil
		}
	}
}

func waitProgress(parent context.Context, brokers, topic string, target int64, worker *child) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	// A child exiting early must unblock a poll immediately.
	go func() {
		select {
		case <-worker.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	reader, err := newReader(brokers, topic, false)
	if err != nil {
		return err
	}
	defer reader.Close()
	for {
		fetch := reader.PollRecords(ctx, 100)
		if err := ctx.Err(); err != nil {
			select {
			case <-worker.done:
				return worker.failure("process exited before checkpoint")
			default:
				return fmt.Errorf("wait for checkpoint %d: %w", target, err)
			}
		}
		if err := fetch.Err(); err != nil {
			return err
		}
		reached := false
		fetch.EachRecord(func(r *kgo.Record) {
			if len(r.Value) == 40 && string(r.Value[:4]) == "DFV1" && binary.BigEndian.Uint64(r.Value[4:12]) == uint64(target) {
				reached = true
			}
		})
		if reached {
			return nil
		}
	}
}

func logEnd(ctx context.Context, admin *kgo.Client, topic string) (int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition = 0
	partition.Timestamp = -1
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{partition}}}
	response, err := admin.Request(ctx, req)
	if err != nil {
		return 0, err
	}
	found := false
	var end int64
	for _, t := range response.(*kmsg.ListOffsetsResponse).Topics {
		for _, p := range t.Partitions {
			if t.Topic != topic || p.Partition != 0 {
				continue
			}
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return 0, err
			}
			end = p.Offset
			found = true
		}
	}
	if !found {
		return 0, fmt.Errorf("missing log end for %s", topic)
	}
	return end, nil
}
