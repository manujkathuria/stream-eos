package market

import (
	"encoding/binary"
	"testing"

	"stream-eos/websocket-source/internal/protocol"
)

func TestGenerateUpdateExplicitTimestampAndQuoteState(t *testing.T) {
	m := New(retainedUpdates, protocol.MaxPackets)
	update := protocol.Update{Packets: make([][184]byte, protocol.MaxPackets)}
	generateUpdate(&update, m.quotes, 7, 12345)
	for i := range update.Packets {
		p := update.Packets[i][:]
		if binary.BigEndian.Uint32(p[:4]) != TokenAt(i) || binary.BigEndian.Uint32(p[4:8]) != 10007 || binary.BigEndian.Uint32(p[16:20]) != 17 || binary.BigEndian.Uint32(p[44:48]) != 12345 || binary.BigEndian.Uint32(p[60:64]) != 12345 {
			t.Fatalf("instrument %d: wrong quote or timestamp", i)
		}
	}
	generateUpdate(&update, m.quotes, 8, 12346)
	if binary.BigEndian.Uint32(update.Packets[0][16:20]) != 35 {
		t.Fatal("quote volume did not advance")
	}
	if m.next != 0 {
		t.Fatal("generation changed coordinator position")
	}
}
