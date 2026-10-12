package ingest_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"stream-eos/ingestor/internal/ingest"
	"stream-eos/ingestor/internal/kafka"
	"stream-eos/ingestor/internal/websocket"
)

// Isolated broker fixture verifies the connected read/decode/publish path and
// redundant A/B identity preservation. Unit tests alone cannot prove delivery.
func TestWebSocketToKafkaIntegration(t *testing.T) {
	if os.Getenv("STREAM_EOS_KAFKA_TEST") != "1" {
		t.Skip("set STREAM_EOS_KAFKA_TEST=1; creates/deletes isolated Kafka topic")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("stream-eos-ingest-test-%d", time.Now().UnixNano())
	admin, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	response, err := admin.Request(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range response.(*kmsg.CreateTopicsResponse).Topics {
		if err := kerr.ErrorForCode(entry.ErrorCode); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.TopicNames = []string{topic}
		if _, err := admin.Request(cleanup, request); err != nil {
			t.Error(err)
		}
	}()
	message := make([]byte, 216)
	copy(message, "SMK1")
	message[4] = 7
	binary.BigEndian.PutUint16(message[20:22], 1)
	binary.BigEndian.PutUint64(message[22:30], 401)
	binary.BigEndian.PutUint16(message[30:32], 184)
	binary.BigEndian.PutUint32(message[32:36], 9910001)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&ws.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for range 2 {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
		for range 2 {
			if err := conn.WriteMessage(ws.BinaryMessage, message); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	for _, id := range []string{"A", "B"} {
		producer, err := kafka.New(ctx, kafka.Config{Brokers: []string{"localhost:9092"}, Topic: topic, ID: id})
		if err != nil {
			t.Fatal(err)
		}
		source, err := websocket.Dial(ctx, websocket.Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Instruments: []uint32{9910001}})
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			producer.Shutdown(cleanup)
			stop()
			t.Fatal(err)
		}
		runner, err := ingest.New(source, producer)
		if err != nil {
			t.Fatal(err)
		}
		stats, runErr := runner.Run(ctx, 2)
		source.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := producer.Shutdown(cleanup)
		stop()
		if runErr != nil || closeErr != nil || stats.Messages != 2 || stats.Events != 2 {
			t.Fatalf("%s stats=%+v run=%v close=%v", id, stats, runErr, closeErr)
		}
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	counts := map[string]int{}
	records := 0
	for records < 4 {
		fetches := reader.PollRecords(ctx, 4-records)
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if len(r.Value) != 212 || string(r.Value[:4]) != "SEV1" || !bytes.Equal(r.Value[4:20], message[4:20]) || !bytes.Equal(r.Value[20:28], message[22:30]) || !bytes.Equal(r.Value[28:], message[32:]) || len(r.Key) != 4 || binary.BigEndian.Uint32(r.Key) != 9910001 {
				t.Fatal("Kafka record lost source bytes or identity")
			}
			if len(r.Headers) != 1 || r.Headers[0].Key != "ingestor" {
				t.Fatal("missing provenance")
			}
			counts[string(r.Headers[0].Value)]++
			records++
		})
	}
	if counts["A"] != 2 || counts["B"] != 2 {
		t.Fatalf("provenance=%v", counts)
	}
}
