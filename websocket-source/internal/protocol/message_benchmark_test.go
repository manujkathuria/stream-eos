package protocol

import (
	"fmt"
	"testing"
)

// Construction copies one retained update into reusable connection storage.
// Excludes generation, clock, locks, rate limiting, setup and WebSocket I/O.
func BenchmarkBuildMessage(b *testing.B) {
	for _, packets := range []int{2, 10, 100, 400} {
		b.Run(fmt.Sprintf("packets=%d", packets), func(b *testing.B) {
			update := Update{Packets: make([][184]byte, packets)}
			for i := range update.Packets {
				for j := range update.Packets[i] {
					update.Packets[i][j] = byte(i + j)
				}
			}
			var active [MaxPackets]bool
			for i := range packets {
				active[i] = true
			}
			message := make([]byte, 22+packets*194)
			b.ReportAllocs()
			b.SetBytes(int64(len(message)))
			b.ResetTimer()
			for b.Loop() {
				length, err := BuildMessage(message, &update, [16]byte{1}, 0, &active)
				if err != nil || length != len(message) {
					b.Fatalf("length=%d err=%v", length, err)
				}
			}
		})
	}
}
