// Package websocket owns subscriptions, pacing, and socket delivery.
package websocket

import (
	"encoding/json"
	"stream-eos/websocket-source/internal/market"
	"sync"

	"stream-eos/websocket-source/internal/protocol"
)

// Each connection owns message storage and subscriptions. Market state is shared.
type subscriptions struct {
	mu              sync.Mutex
	subscribed      [protocol.MaxPackets]bool
	full            [protocol.MaxPackets]bool
	version         uint64
	instrumentCount int
}

type command struct {
	Action string          `json:"a"`
	Values json.RawMessage `json:"v"`
}

func tokenIndex(token uint32) int {
	for i := range protocol.MaxPackets {
		if market.TokenAt(i) == token {
			return i
		}
	}
	return -1
}

// Validate the entire command before changing state. Successful commands have
// no acknowledgement, as in Kite; errors are local mock text messages.
func (s *subscriptions) apply(raw []byte) string {
	var c command
	if json.Unmarshal(raw, &c) != nil {
		return "invalid command JSON"
	}
	var tokens []uint32
	switch c.Action {
	case "subscribe", "unsubscribe":
		if json.Unmarshal(c.Values, &tokens) != nil {
			return "expected token array"
		}
	case "mode":
		var values []json.RawMessage
		if json.Unmarshal(c.Values, &values) != nil || len(values) != 2 {
			return "expected mode and token array"
		}
		var mode string
		if json.Unmarshal(values[0], &mode) != nil || mode != "full" {
			return "this mock supports only full mode"
		}
		if json.Unmarshal(values[1], &tokens) != nil {
			return "expected token array"
		}
	default:
		return "unsupported action"
	}
	if len(tokens) > protocol.MaxPackets {
		return "too many requested tokens"
	}
	for _, token := range tokens {
		if i := tokenIndex(token); i < 0 || i >= s.instrumentCount {
			return "unknown synthetic token"
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.Action == "mode" {
		for _, token := range tokens {
			if !s.subscribed[tokenIndex(token)] {
				return "subscribe before setting mode"
			}
		}
	}
	for _, token := range tokens {
		i := tokenIndex(token)
		switch c.Action {
		case "subscribe":
			s.subscribed[i] = true
		case "unsubscribe":
			s.subscribed[i], s.full[i] = false, false
		case "mode":
			s.full[i] = true
		}
	}
	s.version++
	return ""
}
