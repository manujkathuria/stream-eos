package market

import (
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"stream-eos/websocket-source/internal/protocol"
)

const retainedUpdates = 128

var errBehindMarket = errors.New("client fell behind retained market updates")

// Market belongs to the server, not a connection. The fastest subscribed reader
// advances this synthetic market. Other readers observe the same retained updates.
// This bounded history is internal fan-out, not a client replay API.
type Market struct {
	mu       sync.Mutex
	next     uint64
	quotes   []Quote
	history  []protocol.Update
	streamID [16]byte
}

// New allocates fixed history and quote storage once. Callers validate positive
// history capacity and an instrument count within the protocol limit.
func New(historyUpdates, instrumentCount int) *Market {
	m := &Market{history: make([]protocol.Update, historyUpdates), quotes: make([]Quote, instrumentCount)}
	packets := make([][184]byte, historyUpdates*instrumentCount)
	for i := range m.history {
		start, end := i*instrumentCount, (i+1)*instrumentCount
		m.history[i].Packets = packets[start:end:end]
	}
	if _, err := rand.Read(m.streamID[:]); err != nil {
		panic("cannot create source stream identity: " + err.Error())
	}
	for i := range m.quotes {
		m.quotes[i] = newQuote()
		m.quotes[i].InstrumentToken = TokenAt(i)
	}
	return m
}

// InstrumentCount is immutable for the lifetime of this market.
func (m *Market) InstrumentCount() int { return len(m.quotes) }

func (m *Market) Position() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next
}

// Frame coordinates shared history and holds the lock through generation and
// construction, preventing overwrite while a reader copies a retained update.
// All clients at the same position receive identical event bytes and identities.
func (m *Market) Frame(position uint64, active [protocol.MaxPackets]bool, message []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if position > m.next || m.next-position > uint64(len(m.history)) {
		return 0, errBehindMarket
	}
	update := &m.history[position%uint64(len(m.history))]
	if position == m.next {
		generateUpdate(update, m.quotes, position, uint32(time.Now().Unix()))
		m.next++
	}
	return protocol.BuildMessage(message, update, m.streamID, position, &active)
}

// TokenAt returns the synthetic instrument token at the fixed protocol index.
func TokenAt(index int) uint32 { return 9910001 + uint32(index)*256 }
