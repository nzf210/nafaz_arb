package exchange

import (
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

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

