package market

import "encoding/binary"

// Prices are integer paise. These Go types are values, not a wire-memory layout.
type DepthLevel struct {
	Quantity   uint32
	PricePaise uint32
	Orders     uint16
}

type Quote struct {
	InstrumentToken   uint32
	LastPricePaise    uint32
	LastQuantity      uint32
	AveragePricePaise uint32
	Volume            uint32
	TotalBuyQuantity  uint32
	TotalSellQuantity uint32
	OpenPaise         uint32
	HighPaise         uint32
	LowPaise          uint32
	ClosePaise        uint32
	LastTradeTime     uint32 // Unix seconds.
	OpenInterest      uint32
	OpenInterestHigh  uint32
	OpenInterestLow   uint32
	ExchangeTime      uint32 // Unix seconds.
	Buy               [5]DepthLevel
	Sell              [5]DepthLevel
}

func newQuote() Quote {
	return Quote{InstrumentToken: 9910001, OpenPaise: 10000,
		HighPaise: 10000, LowPaise: 10000, ClosePaise: 9950}
}

// updateQuote produces a deterministic, bounded price walk around Rs 100.
// sequence starts at zero. The caller owns the quote and updates it sequentially.
func updateQuote(q *Quote, sequence, timestamp uint32) {
	position := sequence % 200
	if position > 100 {
		position = 200 - position
	}
	q.LastPricePaise = 10000 + position
	q.LastQuantity = 10 + sequence%20
	q.Volume += q.LastQuantity
	q.AveragePricePaise = (q.OpenPaise + q.LastPricePaise) / 2
	if q.LastPricePaise > q.HighPaise {
		q.HighPaise = q.LastPricePaise
	}
	if q.LastPricePaise < q.LowPaise {
		q.LowPaise = q.LastPricePaise
	}
	q.LastTradeTime, q.ExchangeTime = timestamp, timestamp
	// This is a stock quote: open interest fields remain zero.
	q.TotalBuyQuantity, q.TotalSellQuantity = 0, 0
	for i := range q.Buy {
		level := uint32(i + 1)
		q.Buy[i] = DepthLevel{Quantity: 100 + level*10 + sequence%10,
			PricePaise: q.LastPricePaise - level*5, Orders: uint16(i + 2)}
		q.Sell[i] = DepthLevel{Quantity: 120 + level*10 + sequence%10,
			PricePaise: q.LastPricePaise + level*5, Orders: uint16(i + 3)}
		q.TotalBuyQuantity += q.Buy[i].Quantity
		q.TotalSellQuantity += q.Sell[i].Quantity
	}
}

// encodeMessage overwrites every byte. No struct memory is sent over the wire.
// The caller may reuse message after the synchronous WebSocket write returns.
func encodeMessage(message *[188]byte, q *Quote) {
	binary.BigEndian.PutUint16(message[0:2], 1)
	binary.BigEndian.PutUint16(message[2:4], 184)
	encodePacket(message[4:], q)
}

func encodePacket(p []byte, q *Quote) {
	fields := [16]uint32{q.InstrumentToken, q.LastPricePaise, q.LastQuantity,
		q.AveragePricePaise, q.Volume, q.TotalBuyQuantity, q.TotalSellQuantity,
		q.OpenPaise, q.HighPaise, q.LowPaise, q.ClosePaise, q.LastTradeTime,
		q.OpenInterest, q.OpenInterestHigh, q.OpenInterestLow, q.ExchangeTime}
	for i, value := range fields {
		binary.BigEndian.PutUint32(p[i*4:i*4+4], value)
	}
	for i := range 10 {
		var level DepthLevel
		if i < 5 {
			level = q.Buy[i]
		} else {
			level = q.Sell[i-5]
		}
		offset := 64 + i*12
		binary.BigEndian.PutUint32(p[offset:offset+4], level.Quantity)
		binary.BigEndian.PutUint32(p[offset+4:offset+8], level.PricePaise)
		binary.BigEndian.PutUint16(p[offset+8:offset+10], level.Orders)
		p[offset+10], p[offset+11] = 0, 0
	}
}
