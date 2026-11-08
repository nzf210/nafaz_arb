package exchange

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDecodeOKXBook(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"arg": map[string]any{"channel": "books5", "instId": "BTC-USDT-SWAP"},
		"data": []map[string]any{{
			"bids": [][]string{{"100.5", "2", "0", "3"}, {"100.0", "1", "0", "1"}},
			"asks": [][]string{{"101.0", "2", "0", "3"}},
			"ts":   "1700000000000",
		}},
	})

	book, err := decodeOKXBook("BTC-USDT-SWAP", payload, 0.01, time.Now())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if book.Exchange != "okx" || book.Symbol != "BTC-USDT-SWAP" {
		t.Fatalf("wrong identity: %+v", book)
	}
	if book.Bids[0].Quantity != 0.02 {
		t.Fatalf("contract size must scale quantity, got %v", book.Bids[0].Quantity)
	}
	if got := book.Timestamp.UnixMilli(); got != 1700000000000 {
		t.Fatalf("venue timestamp must win over local clock, got %d", got)
	}
}

func TestDecodeOKXBookRejectsEmptyData(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"event": "subscribe"})
	if _, err := decodeOKXBook("X", payload, 1, time.Now()); err == nil {
		t.Fatal("expected error for payload without data")
	}
}

func TestDecodeIndodaxDepth(t *testing.T) {
	payload := []byte(`{"buy":[["1000","2"],["999","3"]],"sell":[["1001","2"]]}`)
	book, err := decodeIndodaxDepth("btc_idr", payload, time.Now())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if book.Bids[0].Price != 1000 || book.Asks[0].Price != 1001 {
		t.Fatalf("unexpected levels: %+v", book)
	}
}

func TestDecodeIndodaxDepthRejectsOneSided(t *testing.T) {
	if _, err := decodeIndodaxDepth("btc_idr", []byte(`{"buy":[["1","1"]],"sell":[]}`), time.Now()); err == nil {
		t.Fatal("one-sided book must be rejected")
	}
}

func TestDecodeMEXCSpotDepth(t *testing.T) {
	payload := []byte(`{"bids":[["10.5","2"]],"asks":[["10.6","3"]],"timestamp":1700000000000}`)
	book, err := decodeMEXCSpotDepth("BTCUSDT", payload, time.Now())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if book.Bids[0].Quantity != 2 || book.Asks[0].Quantity != 3 {
		t.Fatalf("unexpected levels: %+v", book)
	}
	if got := book.Timestamp.UnixMilli(); got != 1700000000000 {
		t.Fatalf("unknown ts means local clock, got %d", got)
	}
}

func TestDecodeMEXCSpotDepthUnknownTimestampFallsBackToNow(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	payload := []byte(`{"bids":[["10.5","2"]],"asks":[["10.6","3"]]}`)
	book, err := decodeMEXCSpotDepth("BTCUSDT", payload, now)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !book.Timestamp.Equal(now) {
		t.Fatalf("expected local clock %v, got %v", now, book.Timestamp)
	}
}

// A cached websocket book must short-circuit the REST fetch, and an absent one
// must fall through to REST so enabling the feed never loses coverage.
