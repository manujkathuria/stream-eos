package websocket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	ws "github.com/gorilla/websocket"
	"io"
	"net/http"
	"net/http/httptest"
	"stream-eos/pkg/protocol"
	"stream-eos/websocket-source/feed"
	"strings"
	"testing"
	"time"
)

func TestSourceReadsSharedProtocol(t *testing.T) {
	handler, err := feed.NewHandler(feed.Config{Rate: 100, DisconnectAfter: 1})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := Dial(ctx, Config{URL: "ws" + strings.TrimPrefix(server.URL, "http") + "/ws", Instruments: []uint32{9910001, 9910257}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	raw, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	events, err := protocol.Decode(raw)
	if err != nil || len(events) != 2 || events[0].Instrument != 9910001 || events[1].Instrument != 9910257 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	if _, err = s.Read(ctx); err == nil {
		t.Fatal("disconnect ignored")
	}
}
func TestCancellationUnblocksRead(t *testing.T) {
	ready := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&ws.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for range 2 {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
		close(ready)
		c.ReadMessage() // Wait for the client to close; no arbitrary sleep.
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := Dial(ctx, Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Instruments: []uint32{9910001}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	done := make(chan error, 1)
	go func() { _, err := s.Read(context.Background()); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not unblock")
	}
}

func TestMessageBufferGrowthLimitAndReuse(t *testing.T) {
	buffer := make([]byte, 0, 4)
	for _, size := range []int{4, 9, 16, 17, 3} {
		input := bytes.Repeat([]byte{byte(size)}, size)
		var oversized bool
		var err error
		previous := buffer
		buffer, oversized, err = readMessage(bytes.NewReader(input), buffer, 16)
		if err != nil || oversized != (size > 16) || !bytes.Equal(buffer, input[:min(size, 16)]) {
			t.Fatalf("size=%d oversized=%v err=%v", size, oversized, err)
		}
		if cap(buffer) > 17 {
			t.Fatal("buffer exceeded limit")
		}
		if size < cap(previous) && &buffer[:cap(buffer)][0] != &previous[:cap(previous)][0] {
			t.Fatal("reusable storage replaced")
		}
	}
}

type brokenMessageReader struct{}

func (brokenMessageReader) Read(p []byte) (int, error) { p[0] = 1; return 1, io.ErrUnexpectedEOF }
func TestTruncatedMessageFails(t *testing.T) {
	_, _, err := readMessage(brokenMessageReader{}, make([]byte, 0, 4), 16)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncation=%v", err)
	}
}

func TestOversizedMessageSkippedOnSameConnection(t *testing.T) {
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&ws.Upgrader{WriteBufferSize: 128}).Upgrade(w, r, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer c.Close()
		for range 2 {
			if _, _, err := c.ReadMessage(); err != nil {
				serverErrors <- err
				return
			}
		}
		// Includes fragmented oversized binary and text messages, an exact-limit
		// binary message, a heartbeat, and a later small message on one connection.
		for _, msg := range []struct {
			kind int
			data []byte
		}{
			{ws.BinaryMessage, bytes.Repeat([]byte{1}, protocol.MaxMessageBytes+2000)},
			{ws.TextMessage, bytes.Repeat([]byte{'x'}, protocol.MaxMessageBytes+1)},
			{ws.BinaryMessage, bytes.Repeat([]byte{2}, protocol.MaxMessageBytes)},
			{ws.BinaryMessage, []byte{0}},
			{ws.BinaryMessage, []byte{3, 4, 5}},
		} {
			writer, err := c.NextWriter(msg.kind)
			if err != nil {
				serverErrors <- err
				return
			}
			if _, err = writer.Write(msg.data); err != nil {
				serverErrors <- err
				return
			}
			if err = writer.Close(); err != nil {
				serverErrors <- err
				return
			}
		}
		serverErrors <- nil
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := Dial(ctx, Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Instruments: []uint32{9910001}, InitialBufferBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, err := s.Read(ctx)
	if err != nil || len(first) != protocol.MaxMessageBytes || first[0] != 2 {
		t.Fatalf("first length=%d err=%v", len(first), err)
	}
	backing := &first[0]
	second, err := s.Read(ctx)
	if err != nil || !bytes.Equal(second, []byte{3, 4, 5}) {
		t.Fatalf("second=%v err=%v", second, err)
	}
	if &second[0] != backing {
		t.Fatal("storage was not reused")
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMessageBuffer(b *testing.B) {
	for _, size := range []int{410, protocol.MaxMessageBytes} {
		input := bytes.Repeat([]byte{1}, size)
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/reuse=%v", size, reuse), func(b *testing.B) {
				reader := bytes.NewReader(input)
				buffer := make([]byte, 0, size+1)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					reader.Reset(input)
					var err error
					if reuse {
						var large bool
						buffer, large, err = readMessage(reader, buffer, protocol.MaxMessageBytes)
						if large {
							b.Fatal("oversized")
						}
					} else {
						buffer, err = io.ReadAll(reader)
					}
					if err != nil || len(buffer) != size {
						b.Fatalf("length=%d err=%v", len(buffer), err)
					}
				}
			})
		}
	}
}

func TestCancellationUnblocksOversizedDrain(t *testing.T) {
	sending := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&ws.Upgrader{WriteBufferSize: 128}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for range 2 {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
		writer, err := c.NextWriter(ws.BinaryMessage)
		if err != nil {
			return
		}
		if _, err = writer.Write(bytes.Repeat([]byte{1}, protocol.MaxMessageBytes+256)); err != nil {
			return
		}
		// Keep the fragmented message open: draining must be cancelable.
		close(sending)
		c.ReadMessage()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := Dial(ctx, Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Instruments: []uint32{9910001}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	done := make(chan error, 1)
	go func() { _, err := s.Read(ctx); done <- err }()
	select {
	case <-sending:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("oversized drain did not cancel")
	}
}

func TestLifetimeCancellationClosesIdleConnection(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&ws.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for range 2 {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
		c.ReadMessage()
		close(closed)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Dial(ctx, Config{URL: "ws" + strings.TrimPrefix(server.URL, "http"), Instruments: []uint32{9910001}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cancel()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("idle connection did not close")
	}
}
