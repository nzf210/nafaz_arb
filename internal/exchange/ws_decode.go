package exchange

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// decodeIndodaxDepth decodes Indodax's depth payload. Levels arrive as
// [price, quantity] where both may be strings or numbers.
func decodeIndodaxDepth(symbol string, payload []byte, now time.Time) (domain.OrderBook, error) {
	var raw struct {
		Buy  [][]any `json:"buy"`
		Sell [][]any `json:"sell"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode indodax depth: %w", err)
	}
	b := domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Bids:      parseLevels(raw.Buy, 1.0),
		Asks:      parseLevels(raw.Sell, 1.0),
		Timestamp: now.UTC(),
		Source:    "websocket",
	}
	if seq := parseSequenceID(payload); seq > 0 {
		b.SequenceID = seq
	}
	b.ReceivedAt = now.UTC()
	if len(b.Bids) == 0 || len(b.Asks) == 0 {
		return domain.OrderBook{}, fmt.Errorf("indodax depth has no two-sided book")
	}
	return b, nil
}

// decodeOKXBook decodes an OKX books5/books snapshot: data[0].bids/asks as
// [price, size, ...] string triplets, with ts in milliseconds.
func decodeOKXBook(symbol string, payload []byte, contractSize float64, now time.Time) (domain.OrderBook, error) {
	var raw struct {
		Arg  struct{ InstID string } `json:"arg"`
		Data []struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
			Ts   string     `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode okx book: %w", err)
	}
	if len(raw.Data) == 0 {
		return domain.OrderBook{}, fmt.Errorf("okx book has no data")
	}
	if contractSize <= 0 {
		contractSize = 1.0
	}
	d := raw.Data[0]
	b := domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Bids:      parseStringLevels(d.Bids, contractSize),
		Asks:      parseStringLevels(d.Asks, contractSize),
		Timestamp: now.UTC(),
		Source:    "websocket",
	}
	if ms, err := strconv.ParseInt(d.Ts, 10, 64); err == nil && ms > 0 {
		b.Timestamp = time.UnixMilli(ms).UTC()
	}
	if seq := parseSequenceID(payload); seq > 0 {
		b.SequenceID = seq
	}
	b.ReceivedAt = now.UTC()
	if len(b.Bids) == 0 || len(b.Asks) == 0 {
		return domain.OrderBook{}, fmt.Errorf("okx book has no two-sided book")
	}
	return b, nil
}

// parseSequenceID attempts to extract a monotonic sequence identifier from a raw
// websocket payload. Different venues expose it under different keys:
//   - OKX: "seqId" (string) on the data object
//   - MEXC: "s" (int64) at the top level
//   - Indodax: no sequence field — returns 0 to signal "unavailable".
func parseSequenceID(payload []byte) int64 {
	var probe struct {
		Data []struct {
			SeqID string `json:"seqId"`
		} `json:"data"`
		Seq int64 `json:"s"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return 0
	}
	if len(probe.Data) > 0 && probe.Data[0].SeqID != "" {
		if v, err := strconv.ParseInt(probe.Data[0].SeqID, 10, 64); err == nil {
			return v
		}
	}
	if probe.Seq > 0 {
		return probe.Seq
	}
	return 0
}

// bookDepth sums quantity across price levels where both price and quantity are positive.
func bookDepth(levels []domain.PriceLevel) float64 {
	sum := 0.0
	for _, l := range levels {
		if l.Price > 0 && l.Quantity > 0 {
			sum += l.Quantity
		}
	}
	return sum
}

// decodeMEXCSpotDepth decodes MEXC spot depth (bids/asks as [price, qty] strings).
func decodeMEXCSpotDepth(symbol string, payload []byte, now time.Time) (domain.OrderBook, error) {
	var raw struct {
		Bids      [][]string `json:"bids"`
		Asks      [][]string `json:"asks"`
		Timestamp int64      `json:"timestamp"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode mexc spot depth: %w", err)
	}
	b := domain.OrderBook{
		Exchange:  "mexc",
		Symbol:    symbol,
		Bids:      parseStringLevels(raw.Bids, 1.0),
		Asks:      parseStringLevels(raw.Asks, 1.0),
		Timestamp: now.UTC(),
		Source:    "websocket",
	}
	if raw.Timestamp > 0 {
		b.Timestamp = time.UnixMilli(raw.Timestamp).UTC()
	}
	if seq := parseSequenceID(payload); seq > 0 {
		b.SequenceID = seq
	}
	b.ReceivedAt = now.UTC()
	if len(b.Bids) == 0 || len(b.Asks) == 0 {
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth has no two-sided book")
	}
	return b, nil
}
