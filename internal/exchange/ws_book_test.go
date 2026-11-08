package exchange

import (
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

