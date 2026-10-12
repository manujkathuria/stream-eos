package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// BenchmarkConsumerPaths compares warmed polling with ReadBatch + EventID on
// identical retained data. One operation reads 50,000 records after 1,000 warmup
// records. Use -benchtime=1x for one uninterrupted pass per persistent client.
// Fixture production and client setup are outside the timing. This is bounded
// local backlog replay, not live-source latency or a sustained capacity claim.
func BenchmarkConsumerPaths(b *testing.B) {
	if os.Getenv("STREAM_EOS_KAFKA_BENCH") != "1" {
		b.Skip("set STREAM_EOS_KAFKA_BENCH=1; creates/deletes its own isolated fixture topic")
	}
	const warmup = 1000
	const measured = 50000
	topic := createConsumerFixture(b, warmup+measured)
	for _, size := range []int{100, 1000} {
		for _, path := range []string{"franz_poll", "readbatch_eventid"} {
			b.Run(fmt.Sprintf("%s/batch%d", path, size), func(b *testing.B) {
				b.StopTimer()
				if b.N != 1 {
					b.Fatal("use -benchtime=1x for a single warmed replay pass")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cfg := consumerConfig()
				cfg.Topic = topic
				cfg.Partition = 0
				cfg.StartOffset = 0
				cfg.BatchRecords = size
				cfg.BatchBytes = size * recordPayloadBytes
				c, err := NewConsumer(ctx, cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				storage := make([]Record, size)
				warmed := 0
				for warmed < warmup {
					c.pollLimit = min(size, warmup-warmed)
					records, err := c.ReadBatch(ctx, storage)
					if err != nil {
						b.Fatal(err)
					}
					for _, r := range records {
						if r.Offset != int64(warmed) {
							b.Fatal("warmup offset mismatch")
						}
						warmed++
					}
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				count, polls := 0, 0
				var checksum uint64
				b.ReportAllocs()
				b.SetBytes(measured * recordPayloadBytes)
				b.StartTimer()
				for count < measured {
					c.pollLimit = min(size, measured-count)
					if path == "franz_poll" {
						fetches := c.client.PollRecords(ctx, c.pollLimit)
						if ctx.Err() != nil {
							b.Fatal(ctx.Err())
						}
						if errs := fetches.Errors(); len(errs) > 0 {
							b.Fatal(errs)
						}
						for _, f := range fetches {
							for _, t := range f.Topics {
								for _, p := range t.Partitions {
									for _, r := range p.Records {
										if r.Topic != topic || r.Partition != 0 || r.Offset != int64(warmup+count) {
											b.Fatal("raw input mismatch")
										}
										checksum += uint64(r.Offset + 1)
										count++
									}
								}
							}
						}
					} else {
						records, err := c.ReadBatch(ctx, storage)
						if err != nil {
							b.Fatal(err)
						}
						for i := range records {
							r := &records[i]
							if r.Offset != int64(warmup+count) {
								b.Fatal("wrapper offset mismatch")
							}
							id := r.EventID()
							if id[0] != 1 {
								b.Fatal("stream identity mismatch")
							}
							checksum += binary.BigEndian.Uint64(id[16:])
							count++
						}
					}
					polls++
				}
				b.StopTimer()
				runtime.ReadMemStats(&after)
				want := uint64(measured) * uint64(2*warmup+measured+1) / 2
				if count != measured || checksum != want {
					b.Fatalf("records=%d checksum=%d want=%d", count, checksum, want)
				}
				b.ReportMetric(float64(measured)/b.Elapsed().Seconds(), "records/s")
				b.ReportMetric(float64(measured)/float64(polls), "records/poll")
				b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/measured, "B/record")
				b.ReportMetric(float64(after.Mallocs-before.Mallocs)/measured, "allocs/record")
			})
		}
	}
}

func createConsumerFixture(b *testing.B, count int) string {
	b.Helper()
	topic := fmt.Sprintf("stream-eos-bench-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 10000
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	response, err := client.Request(ctx, request)
	if err != nil {
		b.Fatal(err)
	}
	for _, t := range response.(*kmsg.CreateTopicsResponse).Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
		if err != nil {
			b.Error(err)
			return
		}
		defer admin.Close()
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.TimeoutMillis = 5000
		request.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
		response, err := admin.Request(ctx, request)
		if err != nil {
			b.Error(err)
			return
		}
		for _, t := range response.(*kmsg.DeleteTopicsResponse).Topics {
			if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
				b.Error(err)
			}
		}
	})
	// Synthetic fixed-width payloads, varying numeric fields; no claim of a live
	// market's compression ratio. Produce in bounded 1,000-record batches, acknowledged.
	for start := 0; start < count; start += 1000 {
		records := make([]*kgo.Record, min(1000, count-start))
		for i := range records {
			seq := uint64(start + i + 1)
			value := make([]byte, 212)
			copy(value, "SEV1")
			value[4] = 1
			binary.BigEndian.PutUint64(value[20:28], seq)
			for field := 0; field < 46; field++ {
				binary.BigEndian.PutUint32(value[28+field*4:32+field*4], uint32(seq)+uint32(field*13))
			}
			binary.BigEndian.PutUint32(value[28:32], 9910001)
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, 9910001)
			records[i] = &kgo.Record{Topic: topic, Key: key, Value: value, Headers: []kgo.RecordHeader{{Key: "ingestor", Value: []byte("benchmark")}}}
		}
		if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			b.Fatal(err)
		}
	}
	b.Logf("fixture=%s records=%d key=4B value=212B snappy acks=all replication=1", topic, count)
	return topic
}
