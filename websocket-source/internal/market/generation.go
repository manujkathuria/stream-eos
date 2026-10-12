package market

import "stream-eos/websocket-source/internal/protocol"

// generateUpdate advances all instrument quotes and overwrites every packet in
// caller-owned update storage. It performs no allocation, locking, pacing or
// clock reads. The coordinator owns exclusive access to quotes and update.
func generateUpdate(update *protocol.Update, quotes []Quote, position uint64, timestamp uint32) {
	for i := range quotes {
		updateQuote(&quotes[i], uint32(position), timestamp)
		encodePacket(update.Packets[i][:], &quotes[i])
	}
}
