package websocket

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"stream-eos/websocket-source/internal/market"
	"stream-eos/websocket-source/internal/protocol"
)

func packetName(packets int) string { return fmt.Sprintf("packets=%d", packets) }

func messageFixture(b *testing.B, packets int) (*market.Market, [protocol.MaxPackets]bool, []byte) {
	b.Helper()
	m := market.New(128, 400)
	var active [protocol.MaxPackets]bool
	for i := range packets {
		active[i] = true
	}
	message := make([]byte, 22+packets*194)
	if _, err := m.Frame(0, active, message); err != nil {
		b.Fatal(err)
	}
	return m, active, message
}

// Delivery sends a prebuilt message over a real local TCP/WebSocket connection
// and validates every received payload using reusable client storage. Allocations
// include both goroutines and library work during timing. No compression, TLS,
// generation, rate limiting, setup or Kafka. This is not whole-source throughput.
func BenchmarkWebSocketDelivery(b *testing.B) {
	if os.Getenv("STREAM_EOS_WEBSOCKET_BENCH") != "1" {
		b.Skip("set STREAM_EOS_WEBSOCKET_BENCH=1 for loopback delivery")
	}
	for _, packets := range []int{2, 100, 400} {
		b.Run(packetName(packets), func(b *testing.B) {
			_, _, payload := messageFixture(b, packets)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			start := make(chan struct{})
			result := make(chan error, 1)
			const warmup = 100
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					result <- err
					return
				}
				defer conn.Close()
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				defer stop()
				conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				for range warmup {
					if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
						result <- err
						return
					}
				}
				select {
				case <-start:
				case <-ctx.Done():
					result <- ctx.Err()
					return
				}
				for range b.N {
					if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
						result <- err
						return
					}
				}
				result <- nil
			}))
			var client *websocket.Conn
			b.Cleanup(func() {
				cancel()
				if client != nil {
					client.Close()
				}
				server.Close()
			})
			var err error
			client, _, err = websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				b.Fatal(err)
			}
			client.SetReadDeadline(time.Now().Add(30 * time.Second))
			client.SetReadLimit(int64(len(payload)))
			storage := make([]byte, len(payload))
			var probe [1]byte
			read := func() error {
				kind, reader, err := client.NextReader()
				if err != nil {
					return err
				}
				if kind != websocket.BinaryMessage {
					return fmt.Errorf("unexpected type %d", kind)
				}
				if _, err := io.ReadFull(reader, storage); err != nil {
					return err
				}
				if n, err := reader.Read(probe[:]); n != 0 || err != io.EOF {
					return fmt.Errorf("unexpected trailing data n=%d err=%v", n, err)
				}
				if !bytes.Equal(storage, payload) {
					return fmt.Errorf("received payload mismatch")
				}
				return nil
			}
			for range warmup {
				if err := read(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			close(start)
			for range b.N {
				if err := read(); err != nil {
					b.Fatal(err)
				}
			}
			if err := <-result; err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "messages/s")
		})
	}
}
