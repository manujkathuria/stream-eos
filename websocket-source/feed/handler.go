// Package feed exposes the synthetic source handler and session lifecycle.
package feed

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"stream-eos/websocket-source/internal/market"
	"stream-eos/websocket-source/internal/websocket"
)

const DefaultInstrumentCount = 10

const DefaultHistoryUpdates = 128
const MaxHistoryUpdates = 4096

type Config struct {
	DisconnectAfter uint64
	Rate            int
	InstrumentCount int // Zero uses DefaultInstrumentCount.
	HistoryUpdates  int // Zero uses DefaultHistoryUpdates.
}

// Handler owns the shared market and tracks sessions through shutdown.
type Handler struct {
	mux      *http.ServeMux
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	sessions sync.WaitGroup
	stopping bool
}

// NewHandler owns a shared market; all connections observe the same event IDs.
func NewHandler(cfg Config) (*Handler, error) {
	if cfg.Rate < 1 || cfg.Rate > 1000000 {
		return nil, fmt.Errorf("rate must be between 1 and 1000000")
	}
	if cfg.HistoryUpdates == 0 {
		cfg.HistoryUpdates = DefaultHistoryUpdates
	}
	if cfg.HistoryUpdates < 1 || cfg.HistoryUpdates > MaxHistoryUpdates {
		return nil, fmt.Errorf("history updates must be between 1 and %d", MaxHistoryUpdates)
	}
	if cfg.InstrumentCount == 0 {
		cfg.InstrumentCount = DefaultInstrumentCount
	}
	if cfg.InstrumentCount < 1 || cfg.InstrumentCount > 400 {
		return nil, fmt.Errorf("instrument count must be between 1 and 400")
	}
	m := market.New(cfg.HistoryUpdates, cfg.InstrumentCount)
	interval := time.Second / time.Duration(cfg.Rate)
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{mux: http.NewServeMux(), ctx: ctx, cancel: cancel}
	h.mux.HandleFunc("/ws", websocket.Handler(m, interval, cfg.DisconnectAfter))
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		http.Error(w, "source is shutting down", http.StatusServiceUnavailable)
		return
	}
	h.sessions.Add(1)
	h.mu.Unlock()
	defer h.sessions.Done()
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(h.ctx, cancel)
	defer stop()
	defer cancel()
	h.mux.ServeHTTP(w, r.WithContext(ctx))
}

// Shutdown rejects new sessions, cancels existing ones, and waits for completion.
// HTTP server shutdown must be performed separately; it does not own WebSockets.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	h.stopping = true
	h.cancel()
	h.mu.Unlock()
	done := make(chan struct{})
	go func() { h.sessions.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
