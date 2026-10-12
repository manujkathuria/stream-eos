package protocol

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// Measures the production envelope decoder, including its event-slice allocation.
// Fixture construction, WebSocket reads, Kafka production and startup are excluded.
func BenchmarkDecode(b *testing.B) {
	for _, count := range []int{2, 400} {
		b.Run(fmt.Sprintf("packets=%d", count), func(b *testing.B) {
			message := make([]byte, 22+count*194)
			copy(message, "SMK1")
			message[4] = 7
			binary.BigEndian.PutUint16(message[20:22], uint16(count))
			for i := range count {
				offset := 22 + i*194
				binary.BigEndian.PutUint64(message[offset:offset+8], uint64(i+1))
				binary.BigEndian.PutUint16(message[offset+8:offset+10], 184)
				binary.BigEndian.PutUint32(message[offset+10:offset+14], 9910001+uint32(i)*256)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(message)))
			b.ResetTimer()
			for b.Loop() {
				events, err := Decode(message)
				if err != nil || len(events) != count {
					b.Fatalf("events=%d err=%v", len(events), err)
				}
			}
		})
	}
}

// Reuses startup-reserved event storage, calling the streaming decoder.
func BenchmarkDecodeInto(b *testing.B) {
	for _, count := range []int{2, 400} {
		b.Run(fmt.Sprintf("packets=%d", count), func(b *testing.B) {
			message := make([]byte, 22+count*194)
			copy(message, "SMK1")
			message[4] = 7
			binary.BigEndian.PutUint16(message[20:22], uint16(count))
			for i := range count {
				offset := 22 + i*194
				binary.BigEndian.PutUint64(message[offset:offset+8], uint64(i+1))
				binary.BigEndian.PutUint16(message[offset+8:offset+10], 184)
				binary.BigEndian.PutUint32(message[offset+10:offset+14], 9910001+uint32(i)*256)
			}
			storage := make([]Event, 0, MaxEvents)
			b.ReportAllocs()
			b.SetBytes(int64(len(message)))
			b.ResetTimer()
			for b.Loop() {
				events, err := DecodeInto(message, storage)
				if err != nil || len(events) != count {
					b.Fatalf("events=%d err=%v", len(events), err)
				}
			}
		})
	}
}
