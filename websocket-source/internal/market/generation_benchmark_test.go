package market

import (
	"fmt"
	"testing"
)

// Advances and encodes the configured instruments into a 128-slot history.
// Setup, clock reads, locks and I/O are excluded.
func BenchmarkGenerateUpdate(b *testing.B) {
	for _, count := range []int{10, 400} {
		b.Run(fmt.Sprintf("instruments=%d", count), func(b *testing.B) {
			m := New(retainedUpdates, count)
			b.ReportAllocs()
			b.SetBytes(int64(count * 184))
			b.ResetTimer()
			for position := 0; position < b.N; position++ {
				generateUpdate(&m.history[position%retainedUpdates], m.quotes, uint64(position), 12345)
			}
			b.StopTimer()
			if m.quotes[0].Volume == 0 {
				b.Fatal("quotes did not advance")
			}
		})
	}
}
