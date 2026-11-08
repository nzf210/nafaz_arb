package exchange

import (
	"fmt"
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

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
