// Package protocol decodes the synthetic source envelope without decoding quote fields.
package protocol

import (
	"encoding/binary"
	"errors"
)

const MaxEvents = 400
const MaxMessageBytes = 22 + MaxEvents*194

// Event borrows Quote from the input message. Copy before asynchronous use.
type Event struct {
	ID         EventID // 16-byte stream ID followed by the big-endian sequence.
	Instrument uint32
	Quote      []byte
}

var (
	ErrEnvelope      = errors.New("invalid source envelope")
	ErrPacketCount   = errors.New("invalid packet count")
	ErrMessageLength = errors.New("message length does not match packet count")
	ErrPacketLength  = errors.New("invalid quote packet length")
	ErrEventCapacity = errors.New("insufficient decoded-event capacity")
)

// Decode is the allocating convenience API. Streaming callers use DecodeInto.
func Decode(p []byte) ([]Event, error) {
	count, err := envelopeCount(p)
	if err != nil {
		return nil, err
	}
	return DecodeInto(p, make([]Event, 0, count))
}

// DecodeInto overwrites caller-owned event storage without growing it. Quotes
// borrow p until that message buffer is reused; events are valid until dst is
// reused. The entire envelope is validated before changing dst. On error no
// partial batch is returned. Successful and rejected decoding allocate nothing.
func DecodeInto(p []byte, dst []Event) ([]Event, error) {
	count, err := envelopeCount(p)
	if err != nil {
		return nil, err
	}
	if cap(dst) < count {
		return nil, ErrEventCapacity
	}
	for i := range count {
		offset := 22 + i*194
		if binary.BigEndian.Uint16(p[offset+8:offset+10]) != 184 {
			return nil, ErrPacketLength
		}
	}
	events := dst[:count]
	for i := range events {
		offset := 22 + i*194
		e := &events[i]
		copy(e.ID[:16], p[4:20])
		copy(e.ID[16:], p[offset:offset+8])
		e.Quote = p[offset+10 : offset+194]
		e.Instrument = binary.BigEndian.Uint32(e.Quote[:4])
	}
	return events, nil
}
func envelopeCount(p []byte) (int, error) {
	if len(p) < 22 || string(p[:4]) != "SMK1" {
		return 0, ErrEnvelope
	}
	count := int(binary.BigEndian.Uint16(p[20:22]))
	if count < 1 || count > MaxEvents {
		return 0, ErrPacketCount
	}
	if len(p) != 22+count*194 {
		return 0, ErrMessageLength
	}
	return count, nil
}
