package websocket

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"stream-eos/websocket-source/internal/market"
)

const heartbeatInterval = 2 * time.Second

type textError struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

// Handler serves sessions sharing one market. Pacing belongs to each connection.
func Handler(m *market.Market, interval time.Duration, disconnectAfter uint64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		shutdownDone := make(chan struct{})
		stop := context.AfterFunc(r.Context(), func() {
			defer close(shutdownDone)
			// Gorilla permits WriteControl and Close concurrently with the writer.
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "source shutting down"), time.Now().Add(time.Second))
			_ = conn.Close()
		})
		defer func() {
			if !stop() {
				<-shutdownDone
			}
		}()
		defer conn.Close()
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if conn.WriteJSON(textError{Type: "message", Data: "Synthetic market source: subscribe and select full mode to receive quotes"}) != nil {
			return
		}
		state := subscriptions{instrumentCount: m.InstrumentCount()}
		done := make(chan struct{})
		errors := make(chan string, 8)
		readerDone := make(chan struct{})
		defer func() { close(done); conn.Close(); <-readerDone }()
		go func() {
			defer close(readerDone)
			defer conn.Close() // Unblock writer on peer disconnect or read failure.
			conn.SetReadLimit(4096)
			for {
				kind, raw, err := conn.ReadMessage()
				if err != nil {
					return
				}
				problem := "commands must be text messages"
				if kind == websocket.TextMessage {
					problem = state.apply(raw)
				}
				if problem != "" {
					select {
					case errors <- problem:
					case <-done:
						return
					}
				}
			}
		}()

		var rateTick <-chan time.Time
		if interval > 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			rateTick = ticker.C
		}
		message := make([]byte, 22+194*m.InstrumentCount())
		heartbeat := [1]byte{0}
		position := m.Position()
		wasActive := false
		var sent uint64
		var seenVersion uint64
		nextHeartbeat := time.Now().Add(heartbeatInterval)
		idlePoll := time.NewTicker(10 * time.Millisecond)
		defer idlePoll.Stop()
		for {
			// One writer handles data and errors; shutdown may send a concurrent
			// control frame, which Gorilla supports. No shared buffer queue.
			select {
			case problem := <-errors:
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if conn.WriteJSON(textError{Type: "error", Data: problem}) != nil {
					return
				}
			default:
			}
			state.mu.Lock()
			active := state.full
			version := state.version
			state.mu.Unlock()
			// A command starts a fresh idle interval. An in-flight frame can finish
			// after unsubscribe; subsequent snapshots use the updated state.
			if version != seenVersion {
				seenVersion = version
				nextHeartbeat = time.Now().Add(heartbeatInterval)
			}
			activeCount := 0
			now := time.Now()
			for _, enabled := range active[:m.InstrumentCount()] {
				if enabled {
					activeCount++
				}
			}
			conn.SetWriteDeadline(now.Add(5 * time.Second))
			if activeCount == 0 {
				wasActive = false
				if !now.Before(nextHeartbeat) {
					if conn.WriteMessage(websocket.BinaryMessage, heartbeat[:]) != nil {
						return
					}
					nextHeartbeat = now.Add(heartbeatInterval)
				}
				select {
				case <-readerDone:
					return
				case <-idlePoll.C:
				}
				continue
			}
			if !wasActive {
				position = m.Position() // New/resumed subscriptions start live.
				wasActive = true
			}
			if rateTick != nil {
				select {
				case <-rateTick:
				case <-readerDone:
					return
				}
			}
			length, err := m.Frame(position, active, message[:])
			if err != nil {
				conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(time.Second))
				return
			}
			if conn.WriteMessage(websocket.BinaryMessage, message[:length]) != nil {
				return
			}
			nextHeartbeat = time.Now().Add(heartbeatInterval)
			position++
			sent++
			if disconnectAfter != 0 && sent >= disconnectAfter {
				conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "mock reconnect exercise"), time.Now().Add(time.Second))
				return
			}
		}
	}
}
