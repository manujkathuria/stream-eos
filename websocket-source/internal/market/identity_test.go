package market

import (
	"bytes"
	"encoding/binary"
	"testing"

	"stream-eos/websocket-source/internal/protocol"
)

func TestSharedEventIdentity(t *testing.T) {
	m := New(retainedUpdates, protocol.MaxPackets)
	var active [protocol.MaxPackets]bool
	active[0], active[1] = true, true
	a, b := make([]byte, 410), make([]byte, 410)
	for position := uint64(0); position < 3; position++ {
		na, err := m.Frame(position, active, a)
		if err != nil {
			t.Fatal(err)
		}
		nb, err := m.Frame(position, active, b)
		if err != nil {
			t.Fatal(err)
		}
		if na != 410 || nb != na || !bytes.Equal(a, b) {
			t.Fatal("identity differs across readers")
		}
		if string(a[:4]) != "SMK1" || !bytes.Equal(a[4:20], m.streamID[:]) || binary.BigEndian.Uint16(a[20:22]) != 2 {
			t.Fatal("invalid envelope")
		}
		for i := range 2 {
			offset := 22 + i*194
			if binary.BigEndian.Uint64(a[offset:offset+8]) != position*400+uint64(i)+1 {
				t.Fatal("incorrect event sequence")
			}
			if binary.BigEndian.Uint16(a[offset+8:offset+10]) != 184 || binary.BigEndian.Uint32(a[offset+10:offset+14]) != TokenAt(i) {
				t.Fatal("invalid wrapped packet")
			}
		}
	}
	var onlySecond [protocol.MaxPackets]bool
	onlySecond[1] = true
	filtered := make([]byte, 216)
	if _, err := m.Frame(2, onlySecond, filtered); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint64(filtered[22:30]) != 802 {
		t.Fatal("subscription filtering renumbered identity")
	}
	if New(retainedUpdates, protocol.MaxPackets).streamID == m.streamID {
		t.Fatal("source restart reused stream identity")
	}
}
