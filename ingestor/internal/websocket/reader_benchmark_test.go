package websocket

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"stream-eos/ingestor/internal/benchtest"
)

// Measures production Source.Read over loopback, with lifetime cancellation registered outside timing,
// including Gorilla readers, buffer reuse, validation and fixture sending during timing.
func BenchmarkSourceRead(b *testing.B) {
	if os.Getenv("STREAM_EOS_WEBSOCKET_BENCH") != "1" {
		b.Skip("set STREAM_EOS_WEBSOCKET_BENCH=1 for loopback reads")
	}
	for _, packets := range []int{2, 400} {
		b.Run(benchtest.Name(packets), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			const warmup = 100
			socket := benchtest.NewSocket(b, ctx, packets, warmup, b.N)
			source, err := Dial(ctx, Config{URL: socket.URL, Instruments: benchtest.Instruments(packets), Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(source.Close)
			for batch := range warmup {
				message, err := source.Read(ctx)
				if err != nil {
					b.Fatal(err)
				}
				benchtest.ValidateMessage(b, message, batch, packets)
			}
			b.ReportAllocs()
			b.SetBytes(int64(22 + packets*194))
			b.ResetTimer()
			socket.Start()
			for batch := warmup; batch < warmup+b.N; batch++ {
				message, err := source.Read(ctx)
				if err != nil {
					b.Fatal(err)
				}
				benchtest.ValidateMessage(b, message, batch, packets)
			}
			if err := socket.Wait(ctx); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "messages/s")
		})
	}
}
