package exchange

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestWSBookCacheServesFreshBooks(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: now,
	})

	book, ok, _, _, _, _ := cache.Get("okx", "BTC-USDT-SWAP", 2*time.Second)
	if !ok || book.Bids[0].Price != 100 {
		t.Fatalf("expected cached book, ok=%v book=%+v", ok, book)
	}
	hits, misses := cache.Stats()
	if hits != 1 || misses != 0 {
		t.Fatalf("hits=%d misses=%d, want 1/0", hits, misses)
	}
}

func TestWSBookCacheRejectsStaleBooks(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: now,
	})

	now = now.Add(5 * time.Second)
	if _, ok, _, _, _, _ := cache.Get("okx", "BTC-USDT-SWAP", 2*time.Second); ok {
		t.Fatal("stale book must not be served")
	}
	if _, misses := cache.Stats(); misses != 1 {
		t.Fatalf("expected a miss, got %d", misses)
	}
}

func TestWSBookCacheUnknownSymbolMisses(t *testing.T) {
	cache := NewWSBookCache()
	if _, ok, _, _, _, _ := cache.Get("okx", "NOPE-USDT", 0); ok {
		t.Fatal("unknown symbol must miss")
	}
}

func TestWSBookCacheIgnoresOneSidedBooks(t *testing.T) {
	cache := NewWSBookCache()
	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids: []domain.PriceLevel{{Price: 100, Quantity: 1}},
	})
	if cache.entries() != 0 {
		t.Fatal("one-sided book must not be cached")
	}
}

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
func TestTrackedBookForSymbolPrefersCacheThenFallsBack(t *testing.T) {
	l := NewLiveAdapter()
	restCalls := 0
	rest := func() (domain.OrderBook, error) {
		restCalls++
		return domain.OrderBook{
			Exchange: "okx", Symbol: "BTC-USDT-SWAP",
			Bids:      []domain.PriceLevel{{Price: 1, Quantity: 1}},
			Asks:      []domain.PriceLevel{{Price: 2, Quantity: 1}},
			Timestamp: time.Now(),
		}, nil
	}

	// Feed disabled: always REST.
	if _, err := l.trackedBookForSymbol("okx", "BTC-USDT-SWAP", rest); err != nil {
		t.Fatal(err)
	}
	if restCalls != 1 {
		t.Fatalf("expected REST call, got %d", restCalls)
	}

	l.mu.Lock()
	l.wsCache = NewWSBookCache()
	l.wsMaxAge = time.Minute
	l.wsCache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 42, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 43, Quantity: 1}},
		Timestamp: time.Now(),
	})
	l.mu.Unlock()

	book, err := l.trackedBookForSymbol("okx", "BTC-USDT-SWAP", rest)
	if err != nil {
		t.Fatal(err)
	}
	if book.Bids[0].Price != 42 {
		t.Fatalf("expected ws book, got %+v", book)
	}
	if restCalls != 1 {
		t.Fatalf("ws hit must not hit REST, calls=%d", restCalls)
	}

	// A symbol the feed never saw still reaches REST.
	if _, err := l.trackedBookForSymbol("okx", "ETH-USDT-SWAP", rest); err != nil {
		t.Fatal(err)
	}
	if restCalls != 2 {
		t.Fatalf("expected REST fallback, calls=%d", restCalls)
	}
}

func TestTrackedBookForSymbolStillHonoursMaintenance(t *testing.T) {
	l := NewLiveAdapter()
	l.SetHealthPolicy(HealthPolicy{Window: time.Minute, ErrorRateThreshold: 0.5, MinRequests: 1})
	for i := 0; i < 3; i++ {
		l.recordOutcome("okx", ErrExchangeUnavailable)
	}
	l.mu.Lock()
	l.wsCache = NewWSBookCache()
	l.wsMaxAge = time.Minute
	l.wsCache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 42, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 43, Quantity: 1}},
		Timestamp: time.Now(),
	})
	l.mu.Unlock()

	_, err := l.trackedBookForSymbol("okx", "BTC-USDT-SWAP", func() (domain.OrderBook, error) {
		t.Fatal("must not call REST for a downed exchange")
		return domain.OrderBook{}, nil
	})
	if err == nil {
		t.Fatal("expected exchange-unavailable error")
	}
}

func TestWebSocketStatsWithoutFeed(t *testing.T) {
	l := NewLiveAdapter()
	hits, misses := l.WebSocketStats()
	if hits != 0 || misses != 0 {
		t.Fatalf("no feed means no stats, got %d/%d", hits, misses)
	}
}

func TestWSClientStopIsIdempotent(t *testing.T) {
	client := NewWSClient(NewWSBookCache(), nil)
	client.Stop()
	client.Stop()
	if connected, failures := client.Stats(); connected != 0 || failures != 0 {
		t.Fatalf("unexpected stats %d/%d", connected, failures)
	}
}

func TestWSBookCacheReturnsOutOfOrderFlag(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	// Store first book with seq 100
	cache.Store(domain.OrderBook{
		Exchange:   "okx",
		Symbol:     "BTC-USDT-SWAP",
		SequenceID: 100,
		Bids:       []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:       []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp:  now,
	})

	// Store second book with seq 90 (out-of-order, less than 100)
	cache.Store(domain.OrderBook{
		Exchange:   "okx",
		Symbol:     "BTC-USDT-SWAP",
		SequenceID: 90,
		Bids:       []domain.PriceLevel{{Price: 95, Quantity: 1}},
		Asks:       []domain.PriceLevel{{Price: 96, Quantity: 1}},
		Timestamp:  now,
	})

	// Get should return isOutOfOrder=true and isGap=true (gap of 9 = 100-90-1)
	_, ok, isDuplicate, isOutOfOrder, isGap, err := cache.Get("okx", "BTC-USDT-SWAP", 0)
	if !ok {
		t.Fatal("expected book to be found")
	}
	if !isOutOfOrder {
		t.Fatal("expected isOutOfOrder=true for sequence 90 after 100")
	}
	if isDuplicate {
		t.Fatal("expected isDuplicate=false when sequence changed")
	}
	if !isGap {
		t.Fatal("expected isGap=true for sequence 90 after 100 (gap=9)")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hits, _ := cache.Stats()
	if hits != 1 {
		t.Fatalf("expected 1 hit, got %d", hits)
	}
}

func TestWSBookCacheReturnsGapFlag(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	// Store first book with seq 100
	cache.Store(domain.OrderBook{
		Exchange:   "okx",
		Symbol:     "BTC-USDT-SWAP",
		SequenceID: 100,
		Bids:       []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:       []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp:  now,
	})

	// Store second book with seq 95 (gap of 4 missing sequences)
	cache.Store(domain.OrderBook{
		Exchange:   "okx",
		Symbol:     "BTC-USDT-SWAP",
		SequenceID: 95,
		Bids:       []domain.PriceLevel{{Price: 95, Quantity: 1}},
		Asks:       []domain.PriceLevel{{Price: 96, Quantity: 1}},
		Timestamp:  now,
	})

	// Get should return isGap=true (gap of 4 = 100-95-1 = 4)
	_, ok, isDuplicate, _, isGap, err := cache.Get("okx", "BTC-USDT-SWAP", 0)
	if !ok {
		t.Fatal("expected book to be found")
	}
	if !isGap {
		t.Fatal("expected isGap=true for sequence 95 after 100 (gap=4)")
	}
	if isDuplicate {
		t.Fatal("expected isDuplicate=false when sequence changed")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hits, _ := cache.Stats()
	if hits != 1 {
		t.Fatalf("expected 1 hit, got %d", hits)
	}
}

func TestWSBookCacheGhostOrderbookDetection(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	// Store a book with near-zero depth (ghost)
	cache.Store(domain.OrderBook{
		Exchange:   "okx",
		Symbol:     "BTC-USDT-SWAP",
		SequenceID: 100,
		Bids:       []domain.PriceLevel{{Price: 100, Quantity: 0}, {Price: 99, Quantity: 0}},
		Asks:       []domain.PriceLevel{{Price: 101, Quantity: 0}, {Price: 102, Quantity: 0}},
		Timestamp:  now,
	})

	// Get should succeed but the ghost detection was logged during Store
	_, ok, _, _, _, err := cache.Get("okx", "BTC-USDT-SWAP", 0)
	if !ok {
		t.Fatal("expected book to be found even if ghost")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWSBookCacheDuplicateDetection(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	// Store first book with seq 100
	cache.Store(domain.OrderBook{
		Exchange:   "okx",
		Symbol:     "BTC-USDT-SWAP",
		SequenceID: 100,
		Bids:       []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:       []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp:  now,
	})

	// Store second book with same seq 100 (duplicate)
	cache.Store(domain.OrderBook{
		Exchange:   "okx",
		Symbol:     "BTC-USDT-SWAP",
		SequenceID: 100,
		Bids:       []domain.PriceLevel{{Price: 95, Quantity: 1}},
		Asks:       []domain.PriceLevel{{Price: 96, Quantity: 1}},
		Timestamp:  now,
	})

	// Get should return isDuplicate=true
	_, ok, isDuplicate, _, _, err := cache.Get("okx", "BTC-USDT-SWAP", 0)
	if !ok {
		t.Fatal("expected book to be found")
	}
	if !isDuplicate {
		t.Fatal("expected isDuplicate=true for same sequence 100")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hits, _ := cache.Stats()
	if hits != 1 {
		t.Fatalf("expected 1 hit, got %d", hits)
	}
}

func TestWSBookCacheCleanupStaleBooks(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	// Store a fresh book
	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: now,
	})

	// Store a second book that will go idle
	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "ETH-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: now,
	})

	// Advance the clock past the cleanup maxAge so neither book has received
	// updates recently. Cleanup works on last-update time, not venue timestamp.
	now = now.Add(10 * time.Minute)

	// Refresh the first book so it stays "live" while ETH-USDT-SWAP goes idle.
	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: now,
	})

	// Cleanup with a maxAge of 5 minutes should remove the idle book
	removed := cache.CleanupStaleBooks(5 * time.Minute)
	if removed != 1 {
		t.Fatalf("expected 1 stale book to be cleaned up, got %d", removed)
	}

	// The stale book should no longer be served
	if _, ok, _, _, _, _ := cache.Get("okx", "ETH-USDT-SWAP", 0); ok {
		t.Fatal("stale book should have been cleaned up")
	}

	// The fresh book should still be served
	if _, ok, _, _, _, _ := cache.Get("okx", "BTC-USDT-SWAP", 0); !ok {
		t.Fatal("fresh book should not have been cleaned up")
	}
}

func TestValidateAfterReconnect(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now()
	cache.now = func() time.Time { return now }

	// No cached book yet - should fail validation
	if err := cache.ValidateAfterReconnect("okx", "BTC-USDT-SWAP", time.Minute); err == nil {
		t.Fatal("expected error for missing cached book after reconnect")
	}

	// Store a valid book
	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: now,
	})

	// Should pass validation for a fresh book
	if err := cache.ValidateAfterReconnect("okx", "BTC-USDT-SWAP", time.Minute); err != nil {
		t.Fatalf("expected valid book after reconnect, got: %v", err)
	}

	// Should fail validation for a stale book
	cache.Store(domain.OrderBook{
		Exchange: "okx", Symbol: "ETH-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: now.Add(-10 * time.Minute),
	})
	if err := cache.ValidateAfterReconnect("okx", "ETH-USDT-SWAP", time.Minute); err == nil {
		t.Fatal("expected stale book to fail post-reconnect validation")
	}
}

func TestWSBookCacheExchangeHealthTracking(t *testing.T) {
	cache := NewWSBookCache()

	// No entry yet: healthy by default (absence of evidence).
	if !cache.IsExchangeConnectionHealthy("okx", time.Minute) {
		t.Fatal("exchange without health entry must default to healthy")
	}

	// A stream-level error marks the exchange unhealthy.
	cache.RecordExchangeError("okx", fmt.Errorf("dial failed"))
	if cache.IsExchangeConnectionHealthy("okx", time.Minute) {
		t.Fatal("exchange with recorded error must be unhealthy")
	}

	// A reconnect restores health.
	cache.RecordReconnect("okx", "BTC-USDT-SWAP")
	if !cache.IsExchangeConnectionHealthy("okx", time.Minute) {
		t.Fatal("exchange should recover after reconnect")
	}
}

func TestWSConnectionHealthyWithoutFeed(t *testing.T) {
	l := NewLiveAdapter()
	if !l.WSConnectionHealthy("okx") {
		t.Fatal("feed disabled must report healthy so REST-only scans are not blocked")
	}
}

func TestWSConnectionHealthyAfterStreamError(t *testing.T) {
	l := NewLiveAdapter()
	l.mu.Lock()
	l.wsCache = NewWSBookCache()
	l.wsMaxAge = time.Minute
	l.mu.Unlock()

	if !l.WSConnectionHealthy("okx") {
		t.Fatal("healthy feed must report healthy")
	}

	l.wsCache.RecordExchangeError("okx", fmt.Errorf("stream died"))
	if l.WSConnectionHealthy("okx") {
		t.Fatal("feed with stream error must report unhealthy")
	}
}
