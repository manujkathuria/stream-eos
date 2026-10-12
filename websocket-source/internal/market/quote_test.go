package market

import (
	"encoding/binary"
	"testing"
)

func TestFullQuoteWireFieldsAndReuse(t *testing.T) {
	q := Quote{InstrumentToken: 1, LastPricePaise: 2, LastQuantity: 3, AveragePricePaise: 4,
		Volume: 5, TotalBuyQuantity: 6, TotalSellQuantity: 7, OpenPaise: 8, HighPaise: 9,
		LowPaise: 10, ClosePaise: 11, LastTradeTime: 12, OpenInterest: 13,
		OpenInterestHigh: 14, OpenInterestLow: 15, ExchangeTime: 16}
	for i := range q.Buy {
		q.Buy[i] = DepthLevel{Quantity: uint32(100 + i), PricePaise: uint32(200 + i), Orders: uint16(300 + i)}
		q.Sell[i] = DepthLevel{Quantity: uint32(400 + i), PricePaise: uint32(500 + i), Orders: uint16(600 + i)}
	}
	var message [188]byte
	for i := range message {
		message[i] = 255
	}
	encodeMessage(&message, &q)
	if binary.BigEndian.Uint16(message[:2]) != 1 || binary.BigEndian.Uint16(message[2:4]) != 184 {
		t.Fatal("bad framing")
	}
	p := message[4:]
	for i := 0; i < 16; i++ {
		if got := binary.BigEndian.Uint32(p[i*4 : i*4+4]); got != uint32(i+1) {
			t.Fatalf("scalar offset %d: %d", i*4, got)
		}
	}
	for i := 0; i < 10; i++ {
		offset := 64 + i*12
		quantity, price, orders := uint32(100+i), uint32(200+i), uint16(300+i)
		if i >= 5 {
			quantity, price, orders = uint32(400+i-5), uint32(500+i-5), uint16(600+i-5)
		}
		if binary.BigEndian.Uint32(p[offset:offset+4]) != quantity || binary.BigEndian.Uint32(p[offset+4:offset+8]) != price || binary.BigEndian.Uint16(p[offset+8:offset+10]) != orders {
			t.Fatalf("depth slot %d", i)
		}
		if p[offset+10] != 0 || p[offset+11] != 0 {
			t.Fatalf("padding slot %d", i)
		}
	}
	q = Quote{}
	encodeMessage(&message, &q)
	for i, b := range message[4:] {
		if b != 0 {
			t.Fatalf("stale byte %d", i)
		}
	}
}

func TestGeneratedQuote(t *testing.T) {
	q := newQuote()
	var previousVolume uint32
	for sequence := uint32(0); sequence < 1000; sequence++ {
		updateQuote(&q, sequence, 1700000000+sequence)
		if q.Volume != previousVolume+q.LastQuantity || q.LowPaise > q.LastPricePaise || q.HighPaise < q.LastPricePaise {
			t.Fatal("invalid trade")
		}
		if q.OpenInterest != 0 || q.OpenInterestHigh != 0 || q.OpenInterestLow != 0 {
			t.Fatal("stock OI")
		}
		var buy, sell uint32
		for i := range q.Buy {
			if q.Buy[i].PricePaise >= q.LastPricePaise || q.Sell[i].PricePaise <= q.LastPricePaise {
				t.Fatal("crossed depth")
			}
			buy += q.Buy[i].Quantity
			sell += q.Sell[i].Quantity
		}
		if buy != q.TotalBuyQuantity || sell != q.TotalSellQuantity {
			t.Fatal("depth totals")
		}
		previousVolume = q.Volume
	}
}

func BenchmarkUpdateAndEncode(b *testing.B) {
	q := newQuote()
	var message [188]byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		updateQuote(&q, uint32(i), 1700000000)
		encodeMessage(&message, &q)
	}
}
