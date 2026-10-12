package engine

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"stream-eos/process/internal/kafka"
)

type DepthLevel struct {
	Quantity   uint32 `json:"quantity"`
	PricePaise uint32 `json:"price_paise"`
	Orders     uint16 `json:"orders"`
}

type Quote struct {
	InstrumentToken   uint32        `json:"instrument_token"`
	LastPricePaise    uint32        `json:"last_price_paise"`
	LastQuantity      uint32        `json:"last_quantity"`
	AveragePricePaise uint32        `json:"average_price_paise"`
	Volume            uint32        `json:"volume"`
	TotalBuyQuantity  uint32        `json:"total_buy_quantity"`
	TotalSellQuantity uint32        `json:"total_sell_quantity"`
	OpenPaise         uint32        `json:"open_paise"`
	HighPaise         uint32        `json:"high_paise"`
	LowPaise          uint32        `json:"low_paise"`
	ClosePaise        uint32        `json:"close_paise"`
	LastTradeTime     uint32        `json:"last_trade_time"`
	OpenInterest      uint32        `json:"open_interest"`
	OpenInterestHigh  uint32        `json:"open_interest_high"`
	OpenInterestLow   uint32        `json:"open_interest_low"`
	ExchangeTime      uint32        `json:"exchange_time"`
	Buy               [5]DepthLevel `json:"buy"`
	Sell              [5]DepthLevel `json:"sell"`
}

type JSONEvent struct {
	SchemaVersion int    `json:"schema_version"`
	EventID       string `json:"event_id"`
	Quote         Quote  `json:"quote"`
}

// EncodeJSON accepts a validated SEV1 record. Prices remain integer paise;
// timestamps remain Unix seconds. The returned bytes are independently owned.
func EncodeJSON(r *kafka.Record) ([]byte, error) {
	var q Quote
	p := r.Value[28:]
	q.InstrumentToken = binary.BigEndian.Uint32(p[0:4])
	q.LastPricePaise = binary.BigEndian.Uint32(p[4:8])
	q.LastQuantity = binary.BigEndian.Uint32(p[8:12])
	q.AveragePricePaise = binary.BigEndian.Uint32(p[12:16])
	q.Volume = binary.BigEndian.Uint32(p[16:20])
	q.TotalBuyQuantity = binary.BigEndian.Uint32(p[20:24])
	q.TotalSellQuantity = binary.BigEndian.Uint32(p[24:28])
	q.OpenPaise = binary.BigEndian.Uint32(p[28:32])
	q.HighPaise = binary.BigEndian.Uint32(p[32:36])
	q.LowPaise = binary.BigEndian.Uint32(p[36:40])
	q.ClosePaise = binary.BigEndian.Uint32(p[40:44])
	q.LastTradeTime = binary.BigEndian.Uint32(p[44:48])
	q.OpenInterest = binary.BigEndian.Uint32(p[48:52])
	q.OpenInterestHigh = binary.BigEndian.Uint32(p[52:56])
	q.OpenInterestLow = binary.BigEndian.Uint32(p[56:60])
	q.ExchangeTime = binary.BigEndian.Uint32(p[60:64])
	for n := range 10 {
		off := 64 + n*12
		level := DepthLevel{Quantity: binary.BigEndian.Uint32(p[off : off+4]), PricePaise: binary.BigEndian.Uint32(p[off+4 : off+8]), Orders: binary.BigEndian.Uint16(p[off+8 : off+10])}
		if n < 5 {
			q.Buy[n] = level
		} else {
			q.Sell[n-5] = level
		}
	}
	return json.Marshal(JSONEvent{SchemaVersion: 1, EventID: hex.EncodeToString(r.Value[4:28]), Quote: q})
}

// MaxJSONBytes bounds this fixed schema: uint32 values use at most 10 digits,
// uint16 values at most 5, and identity always uses 48 hexadecimal characters.
// No user-provided strings require escaping. Caller storage must have this capacity.
const MaxJSONBytes = 2048

var ErrJSONCapacity = errors.New("insufficient JSON buffer capacity")

// AppendJSON overwrites dst, borrowing its storage until publishing completes.
// After setup it performs no allocations and never grows the caller buffer.
func AppendJSON(dst []byte, r *kafka.Record) ([]byte, error) {
	if cap(dst) < MaxJSONBytes {
		return nil, ErrJSONCapacity
	}
	dst = dst[:0]
	dst = append(dst, `{"schema_version":1,"event_id":"`...)
	const digits = "0123456789abcdef"
	for _, v := range r.Value[4:28] {
		dst = append(dst, digits[v>>4], digits[v&15])
	}
	dst = append(dst, `","quote":{`...)
	p := r.Value[28:]
	dst = append(dst, `"instrument_token":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[0:4])), 10)
	dst = append(dst, `,"last_price_paise":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[4:8])), 10)
	dst = append(dst, `,"last_quantity":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[8:12])), 10)
	dst = append(dst, `,"average_price_paise":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[12:16])), 10)
	dst = append(dst, `,"volume":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[16:20])), 10)
	dst = append(dst, `,"total_buy_quantity":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[20:24])), 10)
	dst = append(dst, `,"total_sell_quantity":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[24:28])), 10)
	dst = append(dst, `,"open_paise":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[28:32])), 10)
	dst = append(dst, `,"high_paise":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[32:36])), 10)
	dst = append(dst, `,"low_paise":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[36:40])), 10)
	dst = append(dst, `,"close_paise":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[40:44])), 10)
	dst = append(dst, `,"last_trade_time":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[44:48])), 10)
	dst = append(dst, `,"open_interest":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[48:52])), 10)
	dst = append(dst, `,"open_interest_high":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[52:56])), 10)
	dst = append(dst, `,"open_interest_low":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[56:60])), 10)
	dst = append(dst, `,"exchange_time":`...)
	dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[60:64])), 10)
	for side := range 2 {
		if side == 0 {
			dst = append(dst, `,"buy":[`...)
		} else {
			dst = append(dst, `,"sell":[`...)
		}
		for n := range 5 {
			if n != 0 {
				dst = append(dst, ',')
			}
			off := 64 + (side*5+n)*12
			dst = append(dst, `{"quantity":`...)
			dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[off:off+4])), 10)
			dst = append(dst, `,"price_paise":`...)
			dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint32(p[off+4:off+8])), 10)
			dst = append(dst, `,"orders":`...)
			dst = strconv.AppendUint(dst, uint64(binary.BigEndian.Uint16(p[off+8:off+10])), 10)
			dst = append(dst, '}')
		}
		dst = append(dst, ']')
	}
	return append(dst, '}', '}'), nil
}
