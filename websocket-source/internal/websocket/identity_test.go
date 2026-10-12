package websocket

import (
	"encoding/binary"
	"github.com/gorilla/websocket"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSequencedWebSocketProtocol(t *testing.T) {
	s := httptest.NewServer(sourceHandler(1))
	defer s.Close()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := c.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{"a":"subscribe","v":[9910001]}`, `{"a":"mode","v":["full",[9910001]]}`} {
		if err := c.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	kind, p, err := c.ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || len(p) != 216 || string(p[:4]) != "SMK1" || binary.BigEndian.Uint64(p[22:30]) != 1 {
		t.Fatalf("invalid sequenced frame: kind=%d bytes=%d err=%v", kind, len(p), err)
	}
}
