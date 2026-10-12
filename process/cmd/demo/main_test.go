package main

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDemoRejectsIncorrectDelivery(t *testing.T) {
	raw := func(seq byte) *kgo.Record {
		r := &kgo.Record{Key: make([]byte, 4), Value: make([]byte, 212)}
		copy(r.Value, "SEV1")
		r.Value[27] = seq
		binary.BigEndian.PutUint32(r.Key, 9910001)
		copy(r.Value[28:32], r.Key)
		return r
	}
	a, b := raw(1), raw(2)
	expected, err := expectedOutput([]*kgo.Record{a, a, b})
	if err != nil || len(expected) != 2 {
		t.Fatalf("expected=%d err=%v", len(expected), err)
	}
	output := []*kgo.Record{}
	for _, value := range expected {
		output = append(output, &kgo.Record{Key: bytes.Clone(a.Key), Value: bytes.Clone(value)})
	}
	if err := checkOutput(output, expected); err != nil {
		t.Fatal(err)
	}
	if err := checkOutput(output[:1], expected); err == nil {
		t.Fatal("accepted missing output")
	}
	if err := checkOutput([]*kgo.Record{output[0], output[0]}, expected); err == nil {
		t.Fatal("accepted duplicate output")
	}
	output[0].Key[0]++
	if err := checkOutput(output, expected); err == nil {
		t.Fatal("accepted wrong routing key")
	}
	conflict := raw(1)
	conflict.Value[40] = 99
	if _, err := expectedOutput([]*kgo.Record{a, conflict}); err == nil {
		t.Fatal("accepted conflicting source identity")
	}
	malformed := raw(3)
	malformed.Value = malformed.Value[:100]
	if _, err := expectedOutput([]*kgo.Record{malformed}); err == nil {
		t.Fatal("accepted malformed input")
	}
}

func TestLiveOptionsBounds(t *testing.T) {
	for _, tc := range []liveOptions{
		{enabled: true, duration: 0, inputTPS: 10000},
		{enabled: true, duration: time.Second, inputTPS: 0},
		{enabled: true, duration: time.Second, inputTPS: 11},
		{enabled: true, duration: time.Second, inputTPS: 10000010},
		{enabled: true, duration: 2 * time.Hour, inputTPS: 1000},
	} {
		if tc.validate() == nil {
			t.Fatalf("accepted %+v", tc)
		}
	}
	for _, tc := range []liveOptions{
		{enabled: true, duration: time.Minute, inputTPS: 100000},
		{enabled: true, duration: time.Hour, inputTPS: 1000000},
		{enabled: true, duration: time.Second, inputTPS: 10},
	} {
		if err := tc.validate(); err != nil {
			t.Fatal(err)
		}
	}
}
