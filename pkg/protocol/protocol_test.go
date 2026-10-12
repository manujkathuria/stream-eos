package protocol

import (
	"encoding/binary"
	"testing"
)

func TestDecodeIdentityAndInstrument(t *testing.T) {
	p := make([]byte, 216)
	copy(p, "SMK1")
	p[4] = 7
	binary.BigEndian.PutUint16(p[20:22], 1)
	binary.BigEndian.PutUint64(p[22:30], 401)
	binary.BigEndian.PutUint16(p[30:32], 184)
	binary.BigEndian.PutUint32(p[32:36], 9910001)
	events, err := Decode(p)
	if err != nil || len(events) != 1 {
		t.Fatalf("decode: %v", err)
	}
	e := events[0]
	if e.ID[0] != 7 || binary.BigEndian.Uint64(e.ID[16:]) != 401 || e.Instrument != 9910001 || len(e.Quote) != 184 {
		t.Fatal("identity or quote corrupted")
	}
	for _, bad := range [][]byte{nil, p[:21], p[:215], append(append([]byte{}, p...), 0)} {
		if _, err := Decode(bad); err == nil {
			t.Fatal("malformed message accepted")
		}
	}
	p[30] = 0
	p[31] = 183
	if _, err := Decode(p); err == nil {
		t.Fatal("unsupported packet accepted")
	}
}

func testMessage(count int) []byte {
	p := make([]byte, 22+count*194)
	copy(p, "SMK1")
	p[4] = 7
	binary.BigEndian.PutUint16(p[20:22], uint16(count))
	for i := range count {
		offset := 22 + i*194
		binary.BigEndian.PutUint64(p[offset:offset+8], uint64(i+1))
		binary.BigEndian.PutUint16(p[offset+8:offset+10], 184)
		binary.BigEndian.PutUint32(p[offset+10:offset+14], 9910001+uint32(i)*256)
	}
	return p
}
func TestDecodeIntoCapacityValidationAndReuse(t *testing.T) {
	storage := make([]Event, 0, MaxEvents)
	first := &storage[:cap(storage)][0]
	for _, count := range []int{400, 2, 1, 400} {
		message := testMessage(count)
		events, err := DecodeInto(message, storage)
		if err != nil || len(events) != count || &events[0] != first {
			t.Fatalf("count=%d events=%d err=%v", count, len(events), err)
		}
		if events[count-1].Instrument != 9910001+uint32(count-1)*256 || binary.BigEndian.Uint64(events[count-1].ID[16:]) != uint64(count) {
			t.Fatal("decoded fields changed")
		}
		message[32] = 99
		if events[0].Quote[0] != 99 {
			t.Fatal("quote was copied instead of borrowed")
		}
		clear(events)
	}
	valid := testMessage(2)
	bad := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(bad[224:226], 183)
	storage = storage[:2]
	storage[0].ID[0] = 42
	for _, message := range [][]byte{nil, valid[:21], valid[:len(valid)-1], append(append([]byte(nil), valid...), 0), bad} {
		events, err := DecodeInto(message, storage)
		if err == nil || events != nil || storage[0].ID[0] != 42 {
			t.Fatal("malformed input exposed or changed partial output")
		}
	}
	if events, err := DecodeInto(valid, make([]Event, 0, 1)); err != ErrEventCapacity || events != nil {
		t.Fatalf("capacity error=%v", err)
	}
	for _, count := range []uint16{0, 401} {
		message := testMessage(2)
		binary.BigEndian.PutUint16(message[20:22], count)
		if _, err := DecodeInto(message, storage); err != ErrPacketCount {
			t.Fatal(err)
		}
	}
}
func TestDecodeIntoAllocations(t *testing.T) {
	storage := make([]Event, 0, MaxEvents)
	for _, count := range []int{2, 400} {
		message := testMessage(count)
		if n := testing.AllocsPerRun(1000, func() {
			if _, err := DecodeInto(message, storage); err != nil {
				panic(err)
			}
		}); n != 0 {
			t.Fatalf("count=%d allocations=%v", count, n)
		}
	}
	bad := testMessage(2)
	bad[224] = 1
	if n := testing.AllocsPerRun(1000, func() {
		if _, err := DecodeInto(bad, storage); err == nil {
			panic("invalid accepted")
		}
	}); n != 0 {
		t.Fatalf("invalid allocations=%v", n)
	}
}
