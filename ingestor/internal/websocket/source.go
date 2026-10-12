// Package websocket implements the synthetic feed's source transport.
package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	ws "github.com/gorilla/websocket"

	"stream-eos/pkg/protocol"
)

type Config struct {
	URL                string
	Instruments        []uint32
	InitialBufferBytes int
	Logger             *slog.Logger
}
type Source struct {
	conn             *ws.Conn
	once             sync.Once
	buffer           []byte
	log              *slog.Logger
	lifetime         context.Context
	stopCancellation func() bool
}
type subscription struct {
	Action string   `json:"a"`
	Tokens []uint32 `json:"v"`
}
type modeCommand struct {
	Action string `json:"a"`
	Values [2]any `json:"v"`
}

// Dial binds the connection lifetime to ctx. Canceling ctx closes the socket,
// including while no Read is active. Close releases the cancellation registration.
func Dial(ctx context.Context, cfg Config) (*Source, error) {
	if cfg.URL == "" || len(cfg.Instruments) == 0 || len(cfg.Instruments) > 400 {
		return nil, fmt.Errorf("URL and 1–400 instruments are required")
	}
	initial := cfg.InitialBufferBytes
	if initial == 0 {
		initial = 512 // Room for a typical two-quote envelope (410 bytes).
	}
	if initial < 1 || initial > protocol.MaxMessageBytes {
		return nil, fmt.Errorf("initial buffer must be between 1 and %d bytes", protocol.MaxMessageBytes)
	}
	conn, _, err := ws.DefaultDialer.DialContext(ctx, cfg.URL, nil)
	if err != nil {
		return nil, err
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	s := &Source{conn: conn, buffer: make([]byte, 0, initial), log: log, lifetime: ctx}
	// Enforce the application limit while reading. Gorilla's SetReadLimit
	// permanently closes the connection, preventing skip-and-continue.
	// Capture conn directly: cancellation can run before registration returns.
	s.stopCancellation = context.AfterFunc(ctx, func() { conn.Close() })
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteJSON(subscription{Action: "subscribe", Tokens: cfg.Instruments}); err != nil {
		s.Close()
		return nil, err
	}
	if err := conn.WriteJSON(modeCommand{Action: "mode", Values: [2]any{"full", cfg.Instruments}}); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Close permanently ends the connection and unblocks an outstanding Read.
func (s *Source) Close() {
	s.once.Do(func() {
		s.stopCancellation()
		s.conn.Close()
	})
}

// Read is sequential. Returned bytes are borrowed until the next Read.
// Reads using the lifetime context reuse its cancellation registration. A separate
// read context remains supported; canceling either context closes the connection.
func (s *Source) Read(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.lifetime.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() != s.lifetime.Done() {
		stop := context.AfterFunc(ctx, func() { s.Close() })
		defer stop()
	}
	for {
		kind, reader, err := s.conn.NextReader()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if lifetimeErr := s.lifetime.Err(); lifetimeErr != nil {
				return nil, lifetimeErr
			}
			return nil, err
		}
		var oversized bool
		s.buffer, oversized, err = readMessage(reader, s.buffer, protocol.MaxMessageBytes)
		if oversized {
			s.log.LogAttrs(ctx, slog.LevelWarn, "dropping oversized WebSocket message", slog.Int("type", kind), slog.Int("limit_bytes", protocol.MaxMessageBytes))
			// Consume the remainder without retaining it; preserve message boundaries.
			_, err = io.Copy(io.Discard, reader)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if lifetimeErr := s.lifetime.Err(); lifetimeErr != nil {
				return nil, lifetimeErr
			}
			return nil, err
		}
		if oversized {
			continue
		}
		p := s.buffer
		switch kind {
		case ws.BinaryMessage:
			if len(p) == 1 {
				continue
			}
			return p, nil
		case ws.TextMessage:
			var notice struct {
				Type string `json:"type"`
				Data string `json:"data"`
			}
			if err := json.Unmarshal(p, &notice); err != nil {
				return nil, fmt.Errorf("invalid source notice: %w", err)
			}
			if notice.Type == "error" {
				return nil, fmt.Errorf("source error: %s", notice.Data)
			}
		}
	}
}

// readMessage retains at most limit payload bytes plus one byte of reusable
// headroom to distinguish an exact-limit message from an oversized message.
func readMessage(reader io.Reader, buffer []byte, limit int) ([]byte, bool, error) {
	buffer = buffer[:0]
	for {
		if len(buffer) == cap(buffer) {
			capacity := min(limit+1, max(1, cap(buffer)*2))
			grown := make([]byte, len(buffer), capacity)
			copy(grown, buffer)
			buffer = grown
		}
		end := min(cap(buffer), limit+1)
		n, err := reader.Read(buffer[len(buffer):end])
		buffer = buffer[:len(buffer)+n]
		if err != nil && err != io.EOF {
			return buffer, false, err
		}
		if len(buffer) > limit {
			return buffer[:limit], true, nil
		}
		if err == io.EOF {
			return buffer, false, nil
		}
		if n == 0 {
			return buffer, false, io.ErrNoProgress
		}
	}
}
