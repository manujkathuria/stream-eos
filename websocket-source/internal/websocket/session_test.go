package websocket

import (
	"encoding/binary"
	"encoding/json"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"stream-eos/websocket-source/internal/market"
)

func dialSource(t *testing.T, s *httptest.Server) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetReadDeadline(time.Now().Add(6 * time.Second))
	kind, p, err := c.ReadMessage()
	if err != nil || kind != websocket.TextMessage || !json.Valid(p) {
		t.Fatalf("initial text: %s %v", p, err)
	}
	return c
}
func TestIdleAndMultiPacketReconnect(t *testing.T) {
	s := httptest.NewServer(sourceHandler(2))
	defer s.Close()
	c := dialSource(t, s)
	kind, p, err := c.ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || len(p) != 1 {
		t.Fatalf("idle: %d %d %v", kind, len(p), err)
	}
	for _, raw := range []string{`{"a":"subscribe","v":[9910001,9910257]}`, `{"a":"mode","v":["full",[9910001,9910257]]}`} {
		if err := c.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for n := range 2 {
		kind, p, err = c.ReadMessage()
		if err != nil || kind != websocket.BinaryMessage || len(p) != 410 {
			t.Fatalf("active: %d %d %v", kind, len(p), err)
		}
		if binary.BigEndian.Uint16(p[20:22]) != 2 {
			t.Fatal("count")
		}
		for i := range 2 {
			o := 22 + i*194
			if binary.BigEndian.Uint16(p[o+8:o+10]) != 184 || binary.BigEndian.Uint32(p[o+10:o+14]) != market.TokenAt(i) || binary.BigEndian.Uint32(p[o+14:o+18]) != uint32(10000+n) {
				t.Fatal("packet content")
			}
		}
	}
	_, _, err = c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseGoingAway) {
		t.Fatalf("close: %v", err)
	}
	fresh := dialSource(t, s)
	_, p, err = fresh.ReadMessage()
	if err != nil || len(p) != 1 {
		t.Fatalf("fresh not idle %v", err)
	}
}
func TestTextError(t *testing.T) {
	s := httptest.NewServer(sourceHandler(0))
	defer s.Close()
	c := dialSource(t, s)
	c.WriteMessage(websocket.TextMessage, []byte(`{"a":"mode","v":["ltp",[9910001]]}`))
	kind, p, err := c.ReadMessage()
	var e textError
	if err != nil || kind != websocket.TextMessage || json.Unmarshal(p, &e) != nil || e.Type != "error" || e.Data == "" {
		t.Fatalf("error: %s %v", p, err)
	}
}

func TestSecondConnectionContinuesSharedMarket(t *testing.T) {
	s := httptest.NewServer(sourceHandler(2))
	defer s.Close()
	for client := range 2 {
		c := dialSource(t, s)
		for _, raw := range []string{`{"a":"subscribe","v":[9910001]}`, `{"a":"mode","v":["full",[9910001]]}`} {
			if err := c.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
				t.Fatal(err)
			}
		}
		for n := range 2 {
			_, p, err := c.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			if len(p) != 216 || binary.BigEndian.Uint32(p[36:40]) != uint32(10000+client*2+n) {
				t.Fatal("new connection restarted market instead of observing shared state")
			}
		}
		if _, _, err := c.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseGoingAway) {
			t.Fatal(err)
		}
		c.Close()
	}
}

func sourceHandler(disconnectAfter uint64) http.HandlerFunc {
	return Handler(market.New(128, 400), 0, disconnectAfter)
}
