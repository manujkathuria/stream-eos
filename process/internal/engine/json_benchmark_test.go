package engine

import (
	"encoding/binary"
	"stream-eos/process/internal/kafka"
	"testing"
)

var jsonBenchmarkResult []byte

// BenchmarkEncodeJSON includes binary decoding, event-ID hex encoding, and JSON
// marshaling. Record setup and Kafka publishing are excluded.
func BenchmarkEncodeJSON(b *testing.B) {
	var r kafka.Record
	copy(r.Value[:4], "SEV1")
	for n := range 24 {
		r.Value[4+n] = byte(n + 1)
	}
	for n := range 16 {
		binary.BigEndian.PutUint32(r.Value[28+n*4:32+n*4], uint32(100000+n))
	}
	for n := range 10 {
		off := 28 + 64 + n*12
		binary.BigEndian.PutUint32(r.Value[off:off+4], uint32(100+n))
		binary.BigEndian.PutUint32(r.Value[off+4:off+8], uint32(100000+n))
		binary.BigEndian.PutUint16(r.Value[off+8:off+10], uint16(10+n))
	}
	for _, path := range []string{"marshal", "reusable"} {
		b.Run(path, func(b *testing.B) {
			dst := make([]byte, 0, MaxJSONBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				var value []byte
				var err error
				if path == "marshal" {
					value, err = EncodeJSON(&r)
				} else {
					value, err = AppendJSON(dst, &r)
				}
				if err != nil {
					b.Fatal(err)
				}
				jsonBenchmarkResult = value
			}
		})
	}
}
