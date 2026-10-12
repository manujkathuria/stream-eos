// Command demo runs a bounded showcase against an existing Kafka broker.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"stream-eos/process/internal/engine"
	"stream-eos/process/internal/kafka"
)

func main() {
	root := flag.String("root", "..", "workspace root")
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap brokers")
	performance := flag.Bool("performance", false, "run a bounded live pipeline measurement")
	duration := flag.Duration("duration", 10*time.Second, "live source intake duration")
	inputTPS := flag.Int("input-tps", 10000, "requested unique quote events/sec before A/B duplication (multiple of 10)")
	flag.Parse()
	options := liveOptions{enabled: *performance, duration: *duration, inputTPS: *inputTPS}
	if err := options.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	timeout := 2 * time.Minute
	if options.enabled {
		timeout = options.duration + 10*time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := runDemo(ctx, *root, *brokers, options); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	if options.enabled {
		fmt.Println("PASS: live pipeline output verified. Kafka remains running.")
	} else {
		fmt.Println("PASS: duplicate suppression, restart recovery, and fresh-event delivery verified. Kafka remains running.")
	}
}

func runDemo(ctx context.Context, root, brokers string, options liveOptions) (result error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	for _, path := range []string{"websocket-source/bin/source", "ingestor/bin/ingestor", "process/bin/process"} {
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			return fmt.Errorf("build services first: %w", err)
		}
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return err
	}
	defer admin.Close()
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = admin.Ping(probe)
	cancel()
	if err != nil {
		return fmt.Errorf("Kafka unavailable at %s; start a transaction-capable broker, then retry: %w", brokers, err)
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return err
	}
	prefix := "stream-eos-demo-" + hex.EncodeToString(nonce[:])
	topics := []string{prefix + "-input", prefix + "-output", prefix + "-progress", prefix + "-dedup"}
	owned := []string{}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result = errors.Join(result, deleteTopics(cleanup, admin, owned))
	}()
	if err = createTopics(ctx, admin, topics, &owned); err != nil {
		return err
	}
	fmt.Println("Demo topics:", prefix+"-*")
	temp, err := os.MkdirTemp("", "stream-eos-demo-")
	if err != nil {
		return err
	}
	defer func() {
		if result != nil {
			fmt.Fprintln(os.Stderr, "Demo logs retained at", temp)
		} else {
			result = errors.Join(result, os.RemoveAll(temp))
		}
	}()
	sourceDir := filepath.Join(temp, "source")
	if err = os.Mkdir(sourceDir, 0700); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	address := listener.Addr().String()
	listener.Close()
	sourceRate := 20
	sourceHistory := 128
	sourceInstruments := 2
	if options.enabled {
		sourceRate = options.inputTPS / liveInstrumentCount
		sourceHistory = 4096
		sourceInstruments = liveInstrumentCount
	}
	sourceConfig := fmt.Sprintf("LISTEN_ADDR=%s\nRATE=%d\nHISTORY_UPDATES=%d\nINSTRUMENT_COUNT=%d\nDISCONNECT_AFTER=0\nLOG_LEVEL=warn\nLOG_FORMAT=json\nSHUTDOWN_TIMEOUT=3s\n", address, sourceRate, sourceHistory, sourceInstruments)
	if err = os.WriteFile(filepath.Join(sourceDir, ".env"), []byte(sourceConfig), 0600); err != nil {
		return err
	}
	source, err := startChild(ctx, filepath.Join(root, "websocket-source/bin/source"), sourceDir, filepath.Join(temp, "source.log"))
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, source.stop()) }()
	if err = waitSource(ctx, address, source); err != nil {
		return err
	}
	tokens := make([]string, sourceInstruments)
	for i := range tokens {
		tokens[i] = fmt.Sprint(9910001 + i*256)
	}
	ingest := func(id string, messages int) (*child, error) {
		return startChild(ctx, filepath.Join(root, "ingestor/bin/ingestor"), temp, filepath.Join(temp, "ingest-"+id+".log"),
			"-id", id, "-url", "ws://"+address+"/ws", "-brokers", brokers, "-topic", topics[0], "-tokens", strings.Join(tokens, ","), "-max-messages", fmt.Sprint(messages), "-log-level", "warn")
	}
	if options.enabled {
		return runLive(ctx, root, temp, brokers, topics, admin, ingest, options)
	}
	a, err := ingest("A", 20)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, a.stop()) }()
	b, err := ingest("B", 20)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, b.stop()) }()
	if err = a.wait(ctx); err != nil {
		return err
	}
	if err = b.wait(ctx); err != nil {
		return err
	}
	raw, err := snapshot(ctx, admin, brokers, topics[0])
	if err != nil {
		return err
	}
	if len(raw) != 80 {
		return fmt.Errorf("ingestion: got %d records, want 80", len(raw))
	}
	expected, err := expectedOutput(raw)
	if err != nil {
		return err
	}
	if len(expected) >= len(raw) {
		return fmt.Errorf("A/B fixture produced no overlapping IDs")
	}
	provenance := map[string]int{}
	for _, r := range raw {
		for _, h := range r.Headers {
			if h.Key == "ingestor" {
				provenance[string(h.Value)]++
			}
		}
	}
	if provenance["A"] != 40 || provenance["B"] != 40 {
		return fmt.Errorf("A/B provenance mismatch: %v", provenance)
	}
	fmt.Printf("PASS ingestion: A=40, B=40, unique IDs=%d, duplicate copies=%d\n", len(expected), len(raw)-len(expected))
	process := func(label string, target int64) error {
		worker, err := startChild(ctx, filepath.Join(root, "process/bin/process"), temp, filepath.Join(temp, label+".log"),
			"-partition", "0", "-brokers", brokers, "-topic", topics[0], "-output-topic", topics[1], "-progress-topic", topics[2], "-changelog-topic", topics[3],
			"-batch-records", "100", "-batch-bytes", "21600", "-dedup-max-ids", "1024", "-log-level", "warn")
		if err != nil {
			return err
		}
		err = waitProgress(ctx, brokers, topics[2], target, worker)
		return errors.Join(err, worker.stop())
	}
	if err = process("process-first", 80); err != nil {
		return err
	}
	if err = verifyOutput(ctx, admin, brokers, topics[1], expected); err != nil {
		return err
	}
	fmt.Printf("PASS processing: %d unique committed JSON outputs; checkpoint=80\n", len(expected))
	// Replay a retained raw Kafka record. This does not claim source replay support.
	duplicate := &kgo.Record{Topic: topics[0], Partition: 0, Key: raw[0].Key, Value: raw[0].Value, Headers: raw[0].Headers}
	if err = admin.ProduceSync(ctx, duplicate).FirstErr(); err != nil {
		return err
	}
	fresh, err := ingest("fresh", 1)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, fresh.stop()) }()
	if err = fresh.wait(ctx); err != nil {
		return err
	}
	replay, err := snapshot(ctx, admin, brokers, topics[0])
	if err != nil {
		return err
	}
	if len(replay) != 83 {
		return fmt.Errorf("restart input count=%d want 83", len(replay))
	}
	final, err := expectedOutput(replay)
	if err != nil {
		return err
	}
	if len(final) != len(expected)+2 {
		return fmt.Errorf("expected two fresh source events, got %d", len(final)-len(expected))
	}
	if err = process("process-restarted", 83); err != nil {
		return err
	}
	if err = verifyOutput(ctx, admin, brokers, topics[1], final); err != nil {
		return err
	}
	fmt.Printf("PASS restart: retained duplicate suppressed, two fresh events delivered; checkpoint=83, outputs=%d\n", len(final))
	return nil
}

func waitSource(ctx context.Context, address string, c *child) error {
	client := http.Client{Timeout: time.Second}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/", nil)
		response, err := client.Do(req)
		if err == nil {
			response.Body.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return c.failure("source exited before readiness")
		case <-tick.C:
		}
	}
}

func expectedOutput(records []*kgo.Record) (map[string][]byte, error) {
	expected := make(map[string][]byte)
	for _, r := range records {
		if len(r.Key) != 4 || len(r.Value) != 212 || string(r.Value[:4]) != "SEV1" || !bytes.Equal(r.Key, r.Value[28:32]) {
			return nil, fmt.Errorf("invalid input envelope at %d", r.Offset)
		}
		var owned kafka.Record
		copy(owned.Key[:], r.Key)
		copy(owned.Value[:], r.Value)
		encoded, err := engine.EncodeJSON(&owned)
		if err != nil {
			return nil, err
		}
		id := hex.EncodeToString(r.Value[4:28])
		if previous, found := expected[id]; found && !bytes.Equal(previous, encoded) {
			return nil, fmt.Errorf("conflicting payload for %s", id)
		}
		expected[id] = encoded
	}
	return expected, nil
}

func verifyOutput(ctx context.Context, admin *kgo.Client, brokers, topic string, expected map[string][]byte) error {
	records, err := snapshot(ctx, admin, brokers, topic)
	if err != nil {
		return err
	}
	return checkOutput(records, expected)
}

func checkOutput(records []*kgo.Record, expected map[string][]byte) error {
	if len(records) != len(expected) {
		return fmt.Errorf("output count=%d want %d", len(records), len(expected))
	}
	seen := make(map[string]bool)
	for _, r := range records {
		var event engine.JSONEvent
		if err := json.Unmarshal(r.Value, &event); err != nil {
			return err
		}
		if seen[event.EventID] || !bytes.Equal(r.Value, expected[event.EventID]) {
			return fmt.Errorf("duplicate or mismatched output ID %s", event.EventID)
		}
		if len(r.Key) != 4 || binary.BigEndian.Uint32(r.Key) != event.Quote.InstrumentToken {
			return fmt.Errorf("output key mismatch")
		}
		seen[event.EventID] = true
	}
	return nil
}
