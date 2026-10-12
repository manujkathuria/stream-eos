package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"stream-eos/websocket-source/feed"
)

func TestShutdownClosesIdleAndActiveWebSockets(t *testing.T) {
	for _, active := range []bool{false, true} {
		name := "idle"
		if active {
			name = "active"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			handler, err := feed.NewHandler(feed.Config{Rate: 10})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: handler, BaseContext: func(net.Listener) context.Context { return ctx }}
			result := make(chan error, 1)
			go func() { result <- serve(ctx, server, listener, handler, 2*time.Second) }()
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String()+"/ws", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil {
				t.Fatal(err)
			}
			if active {
				for _, cmd := range []string{`{"a":"subscribe","v":[9910001]}`, `{"a":"mode","v":["full",[9910001]]}`} {
					if err := conn.WriteMessage(websocket.TextMessage, []byte(cmd)); err != nil {
						t.Fatal(err)
					}
				}
				if _, _, err := conn.ReadMessage(); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			for {
				_, _, err = conn.ReadMessage()
				if err != nil {
					break
				}
			}
			var closeErr *websocket.CloseError
			if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseGoingAway {
				t.Fatalf("close=%v", err)
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown did not finish")
			}
			request := httptest.NewRequest(http.MethodGet, "/ws", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "shutting down") {
				t.Fatal("shutdown accepted a new session")
			}
		})
	}
}
