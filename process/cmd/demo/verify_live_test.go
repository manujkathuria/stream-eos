package main

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func testFingerprint(id uint64, payload byte) (fp fingerprint) {
	binary.BigEndian.PutUint64(fp[16:24], id)
	fp[24] = payload
	return
}
func sortedFixture(t *testing.T, values []fingerprint) string {
	t.Helper()
	sorter, err := newDiskSorter(t.TempDir(), "sort-")
	if err != nil {
		t.Fatal(err)
	}
	// Small chunks exercise several merge passes without a large fixture.
	sorter.chunk = make([]fingerprint, 0, 3)
	for _, value := range values {
		if err := sorter.add(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	path, err := sorter.finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return path
}
func TestDiskSortMultiplePasses(t *testing.T) {
	var values []fingerprint
	for i := uint64(35); i > 0; i-- {
		values = append(values, testFingerprint(i, 1))
	}
	path := sortedFixture(t, values)
	reader, err := openFingerprints(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.file.Close()
	for i := uint64(1); i <= 35; i++ {
		got, err := reader.next()
		if err != nil || got != testFingerprint(i, 1) {
			t.Fatalf("record %d: %x, %v", i, got, err)
		}
	}
	if _, err := reader.next(); err != io.EOF {
		t.Fatal("extra data", err)
	}
}
func TestFingerprintVerification(t *testing.T) {
	a, b, c := testFingerprint(1, 1), testFingerprint(2, 2), testFingerprint(3, 3)
	for _, tc := range []struct {
		name          string
		input, output []fingerprint
		valid         bool
	}{
		{"reordered duplicates", []fingerprint{b, a, b, a}, []fingerprint{b, a}, true},
		{"missing", []fingerprint{a, b}, []fingerprint{a}, false},
		{"duplicate output", []fingerprint{a, b}, []fingerprint{a, a, b}, false},
		{"extra", []fingerprint{a, b}, []fingerprint{a, b, c}, false},
		{"wrong payload", []fingerprint{a, b}, []fingerprint{a, testFingerprint(2, 9)}, false},
		{"conflicting input", []fingerprint{a, testFingerprint(1, 9)}, []fingerprint{a}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count, err := compareFingerprints(context.Background(), sortedFixture(t, tc.input), sortedFixture(t, tc.output))
			if (err == nil) != tc.valid {
				t.Fatalf("count=%d err=%v", count, err)
			}
			if tc.valid && count != 2 {
				t.Fatal("wrong unique count")
			}
		})
	}
}
func TestSortRejectsTruncationAndCancellation(t *testing.T) {
	path := sortedFixture(t, []fingerprint{testFingerprint(1, 1)})
	if err := os.Truncate(path, 55); err != nil {
		t.Fatal(err)
	}
	_, err := compareFingerprints(context.Background(), path, sortedFixture(t, nil))
	if err == nil {
		t.Fatal("accepted truncated fingerprint")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sorter, err := newDiskSorter(t.TempDir(), "cancel-")
	if err != nil {
		t.Fatal(err)
	}
	if err := sorter.add(ctx, fingerprint{}); err != context.Canceled {
		t.Fatal(err)
	}
}
func TestInputAndOutputFingerprintsMatch(t *testing.T) {
	raw := &kgo.Record{Key: make([]byte, 4), Value: make([]byte, 212)}
	copy(raw.Value, "SEV1")
	raw.Value[27] = 1
	binary.BigEndian.PutUint32(raw.Key, 9910001)
	copy(raw.Value[28:32], raw.Key)
	expected, err := expectedOutput([]*kgo.Record{raw})
	if err != nil {
		t.Fatal(err)
	}
	input, err := inputFingerprint(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range expected {
		output, err := outputFingerprint(&kgo.Record{Key: raw.Key, Value: payload})
		if err != nil || output != input {
			t.Fatalf("different fingerprint: %v", err)
		}
		changed := append([]byte(nil), payload...)
		changed = append(changed, ' ')
		different, err := outputFingerprint(&kgo.Record{Key: raw.Key, Value: changed})
		if err != nil || different == input {
			t.Fatal("payload bytes were not hashed")
		}
	}
	raw.Value = raw.Value[:10]
	if _, err := inputFingerprint(raw); err == nil {
		t.Fatal("accepted malformed input")
	}
}
