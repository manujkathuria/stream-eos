package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"stream-eos/process/internal/engine"
	"stream-eos/process/internal/kafka"
)

type liveCounts struct{ input, a, b, unique int64 }

func inputFingerprint(r *kgo.Record) (fingerprint, error) {
	var fp fingerprint
	if len(r.Key) != 4 || len(r.Value) != 212 || string(r.Value[:4]) != "SEV1" || !bytes.Equal(r.Key, r.Value[28:32]) {
		return fp, fmt.Errorf("invalid input at offset %d", r.Offset)
	}
	var owned kafka.Record
	copy(owned.Key[:], r.Key)
	copy(owned.Value[:], r.Value)
	payload, err := engine.EncodeJSON(&owned)
	if err != nil {
		return fp, err
	}
	copy(fp[:24], r.Value[4:28])
	return payloadFingerprint(fp, r.Key, payload), nil
}
func payloadFingerprint(fp fingerprint, key, payload []byte) fingerprint {
	h := sha256.New()
	h.Write(key)
	h.Write(payload)
	h.Sum(fp[24:24])
	return fp
}
func outputFingerprint(r *kgo.Record) (fingerprint, error) {
	var fp fingerprint
	var event engine.JSONEvent
	if err := json.Unmarshal(r.Value, &event); err != nil {
		return fp, err
	}
	if len(r.Key) != 4 || binary.BigEndian.Uint32(r.Key) != event.Quote.InstrumentToken {
		return fp, fmt.Errorf("output key mismatch at %d", r.Offset)
	}
	if len(event.EventID) != 48 {
		return fp, fmt.Errorf("invalid output event ID")
	}
	if _, err := hex.Decode(fp[:24], []byte(event.EventID)); err != nil {
		return fp, err
	}
	return payloadFingerprint(fp, r.Key, r.Value), nil
}

// Read quiescent topics in bounded polls; sorted fingerprints live on disk.
// Full-run verification is outside the live/drain measurement.
func verifyLive(ctx context.Context, temp, brokers string, topics []string, admin *kgo.Client, target int64) (counts liveCounts, err error) {
	workspace, err := os.MkdirTemp(temp, "verification-")
	if err != nil {
		return counts, err
	}
	defer func() {
		if e := os.RemoveAll(workspace); err == nil {
			err = e
		}
	}()
	expected, err := newDiskSorter(workspace, "input-")
	if err != nil {
		return counts, err
	}
	err = scanTopic(ctx, brokers, topics[0], target, func(r *kgo.Record) error {
		fp, err := inputFingerprint(r)
		if err != nil {
			return err
		}
		provenance := ""
		for _, h := range r.Headers {
			if h.Key == "ingestor" {
				if provenance != "" {
					return fmt.Errorf("duplicate provenance header")
				}
				provenance = string(h.Value)
			}
		}
		switch provenance {
		case "A":
			counts.a++
		case "B":
			counts.b++
		default:
			return fmt.Errorf("invalid input provenance %q", provenance)
		}
		counts.input++
		return expected.add(ctx, fp)
	})
	if err != nil {
		return counts, err
	}
	if counts.input != target || counts.a == 0 || counts.b == 0 {
		return counts, fmt.Errorf("input/provenance counts do not match checkpoint")
	}
	expectedPath, err := expected.finish(ctx)
	if err != nil {
		return counts, err
	}
	actual, err := newDiskSorter(workspace, "output-")
	if err != nil {
		return counts, err
	}
	end, err := logEnd(ctx, admin, topics[1])
	if err != nil {
		return counts, err
	}
	err = scanTopic(ctx, brokers, topics[1], end, func(r *kgo.Record) error {
		fp, err := outputFingerprint(r)
		if err != nil {
			return err
		}
		return actual.add(ctx, fp)
	})
	if err != nil {
		return counts, err
	}
	actualPath, err := actual.finish(ctx)
	if err != nil {
		return counts, err
	}
	counts.unique, err = compareFingerprints(ctx, expectedPath, actualPath)
	if err == nil && counts.input == counts.unique {
		err = fmt.Errorf("live fixture had no A/B overlap")
	}
	return counts, err
}

func compareFingerprints(ctx context.Context, expectedPath, actualPath string) (int64, error) {
	expected, err := openFingerprints(expectedPath)
	if err != nil {
		return 0, err
	}
	defer expected.file.Close()
	actual, err := openFingerprints(actualPath)
	if err != nil {
		return 0, err
	}
	defer actual.file.Close()
	var previous fingerprint
	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		value, err := expected.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
		if count > 0 && bytes.Equal(previous[:24], value[:24]) {
			if previous != value {
				return count, fmt.Errorf("conflicting input payload for %x", value[:24])
			}
			continue
		}
		got, err := actual.next()
		if err != nil {
			return count, fmt.Errorf("missing output for %x: %w", value[:24], err)
		}
		if got != value {
			return count, fmt.Errorf("duplicate, unexpected, or mismatched output near %x", value[:24])
		}
		previous = value
		count++
	}
	if _, err := actual.next(); err != io.EOF {
		if err != nil {
			return count, err
		}
		return count, fmt.Errorf("extra or duplicate output")
	}
	return count, nil
}

func scanTopic(ctx context.Context, brokers, topic string, end int64, visit func(*kgo.Record) error) error {
	if end == 0 {
		return nil
	}
	reader, err := newReader(brokers, topic, true)
	if err != nil {
		return err
	}
	defer reader.Close()
	for {
		fetch := reader.PollRecords(ctx, 1000)
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fetch.Err(); err != nil {
			return err
		}
		iter := fetch.RecordIter()
		for !iter.Done() {
			r := iter.Next()
			if r.Offset < end && !r.Attrs.IsControl() {
				if err := visit(r); err != nil {
					return err
				}
			}
			if r.Offset+1 >= end {
				return nil
			}
		}
	}
}
