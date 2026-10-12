package market

import (
	"testing"

	"stream-eos/websocket-source/internal/protocol"
)

// Measures copying one retained shared update into a reusable sequenced frame.
// Excludes quote generation, setup, Gorilla, and network I/O.
func BenchmarkSequencedFrameReuse(b *testing.B) {
	m := New(retainedUpdates, protocol.MaxPackets)
	var active [protocol.MaxPackets]bool
	active[0], active[1] = true, true
	var frame [410]byte
	if _, err := m.Frame(0, active, frame[:]); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(410)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.Frame(0, active, frame[:]); err != nil {
			b.Fatal(err)
		}
	}
}
