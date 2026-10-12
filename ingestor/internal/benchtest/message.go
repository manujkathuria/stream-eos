// Package benchtest contains fixtures used only by benchmark/test imports.
package benchtest

import (
	"encoding/binary"
	"fmt"
	"testing"
)

var StreamID = [16]byte{7, 8, 9}

// Message supplies fixed valid quote layouts. Only event sequences vary across
// batches; fixed payload patterns influence Kafka compression and throughput.
func Message(packets int) []byte {
	message := make([]byte, 22+packets*194)
	copy(message, "SMK1")
	copy(message[4:20], StreamID[:])
	binary.BigEndian.PutUint16(message[20:22], uint16(packets))
	for i := range packets {
		start := 22 + i*194
		binary.BigEndian.PutUint16(message[start+8:start+10], 184)
		quote := message[start+10 : start+194]
		fields := [16]uint32{9910001 + uint32(i)*256, 10000 + uint32(i), 10, 10000, 1000, 100, 200, 10000, 10100, 9900, 9990, 1700000000, 0, 0, 0, 1700000000}
		for n, value := range fields {
			binary.BigEndian.PutUint32(quote[n*4:n*4+4], value)
		}
		for n := range 10 {
			offset := 64 + n*12
			binary.BigEndian.PutUint32(quote[offset:offset+4], uint32(100+n))
			binary.BigEndian.PutUint32(quote[offset+4:offset+8], uint32(9995+n))
			binary.BigEndian.PutUint16(quote[offset+8:offset+10], uint16(n+1))
		}
	}
	SetSequence(message, 0, packets)
	return message
}
func SetSequence(message []byte, batch, packets int) {
	for i := range packets {
		start := 22 + i*194
		binary.BigEndian.PutUint64(message[start:start+8], uint64(batch)*400+uint64(i)+1)
	}
}
func Instruments(packets int) []uint32 {
	tokens := make([]uint32, packets)
	for i := range tokens {
		tokens[i] = 9910001 + uint32(i)*256
	}
	return tokens
}
func ValidateMessage(b *testing.B, message []byte, batch, packets int) {
	b.Helper()
	if len(message) != 22+packets*194 || string(message[:4]) != "SMK1" || binary.BigEndian.Uint16(message[20:22]) != uint16(packets) {
		b.Fatal("message framing changed")
	}
	for i := range packets {
		start := 22 + i*194
		if binary.BigEndian.Uint64(message[start:start+8]) != uint64(batch)*400+uint64(i)+1 || binary.BigEndian.Uint16(message[start+8:start+10]) != 184 || binary.BigEndian.Uint32(message[start+10:start+14]) != 9910001+uint32(i)*256 {
			b.Fatal("message identity/order changed")
		}
	}
}
func Name(packets int) string { return fmt.Sprintf("packets=%d", packets) }
