package benchtest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type Socket struct {
	URL    string
	start  chan struct{}
	result chan error
}

// NewSocket waits for subscribe/mode, emits warmup, then waits for the start gate.
// It uses one connection and prebuilt quotes, no real source generation or pacing.
func NewSocket(b *testing.B, ctx context.Context, packets, warmup, messages int) *Socket {
	b.Helper()
	ctx, cancel := context.WithCancel(ctx)
	fixture := &Socket{start: make(chan struct{}), result: make(chan error, 1)}
	payload := Message(packets)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			fixture.result <- err
			return
		}
		defer conn.Close()
		stop := context.AfterFunc(ctx, func() { conn.Close() })
		defer stop()
		conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
		for range 2 {
			if _, _, err := conn.ReadMessage(); err != nil {
				fixture.result <- err
				return
			}
		}
		for batch := range warmup {
			SetSequence(payload, batch, packets)
			if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
				fixture.result <- err
				return
			}
		}
		select {
		case <-fixture.start:
		case <-ctx.Done():
			fixture.result <- ctx.Err()
			return
		}
		for batch := warmup; batch < warmup+messages; batch++ {
			SetSequence(payload, batch, packets)
			if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
				fixture.result <- err
				return
			}
		}
		fixture.result <- nil
	}))
	b.Cleanup(func() { cancel(); server.Close() })
	fixture.URL = "ws" + strings.TrimPrefix(server.URL, "http")
	return fixture
}
func (s *Socket) Start() { close(s.start) }
func (s *Socket) Wait(ctx context.Context) error {
	select {
	case err := <-s.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
