package scanner

import (
	"context"
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"

)

func TestMockScanReturnsMatchedPairs(t *testing.T) {
	cfg := config.Config{
		Mode:                  "MOCK",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.5,
		MaxNetROIPct:          2,
		MaxSlippagePct:        1,
		SpotTakerFeePct:       0.3,
		PerpTakerFeePct:       0.05,
		CheckOpenPositions:    true,
		ExcludeOpenOrders:     true,
	}
	s := New(cfg, exchange.NewMockAdapter())
	r, _, err := s.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !r.Complete {
		t.Fatalf("expected complete scan, errors=%v", r.Errors)
	}
	if r.CompletedAt.Before(r.StartedAt) || r.DurationMS < 0 {
		t.Fatalf("invalid scan timing: started=%s completed=%s duration=%dms", r.StartedAt, r.CompletedAt, r.DurationMS)
	}
	if r.MarketDiscoveryDurationMS < 0 || r.ExposureCheckDurationMS < 0 || r.PairEvaluationDurationMS < 0 {
		t.Fatalf("phase timings must be non-negative: %+v", r)
	}
	if len(r.ExchangeMetrics) == 0 {
		t.Fatal("expected per-exchange request metrics")
	}
	if r.MatchedPairs != 2 {
		t.Fatalf("matched=%d, want 2", r.MatchedPairs)
	}
	if len(r.Opportunities) != 2 {
		t.Fatalf("opportunities=%d, want 2", len(r.Opportunities))
	}
	for _, op := range r.Opportunities {
		switch op.BaseAsset {
		case "BTC":
			verifyBTCOpportunity(t, op.TradingEligible, op.ExclusionReasons)
		case "ETH":
			verifyETHOpportunity(t, op.ExclusionReasons)
		}
	}
}

func verifyBTCOpportunity(t *testing.T, tradingEligible bool, exclusionReasons []string) {
	t.Helper()
	if tradingEligible {
		t.Fatal("BTC with open exposure must be excluded")
	}
	if !hasReason(exclusionReasons, "RECONCILIATION_REQUIRED") {
		t.Fatalf("BTC reasons=%v, want RECONCILIATION_REQUIRED", exclusionReasons)
	}
}

func verifyETHOpportunity(t *testing.T, exclusionReasons []string) {
	t.Helper()
	if hasReason(exclusionReasons, "OPEN_POSITION_EXISTS") || hasReason(exclusionReasons, "RECONCILIATION_REQUIRED") {
		t.Fatalf("ETH incorrectly excluded by BTC exposure: %v", exclusionReasons)
	}
}

func hasReason(reasons []string, target string) bool {
	for _, reason := range reasons {
		if reason == target {
			return true
		}
	}
	return false
}

func TestConcurrentScanSorting(t *testing.T) {
	cfg := config.Config{
		Mode:                  "MOCK",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          10,
		MaxSlippagePct:        5,
		SpotTakerFeePct:       0.1,
		PerpTakerFeePct:       0.05,
	}
	s := New(cfg, exchange.NewMockAdapter())
	r, _, err := s.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(r.Opportunities); i++ {
		if r.Opportunities[i-1].NetROIPct < r.Opportunities[i].NetROIPct {
			t.Errorf("opportunities not sorted by NetROIPct desc: %f < %f",
				r.Opportunities[i-1].NetROIPct, r.Opportunities[i].NetROIPct)
		}
	}
}

type emptyBookAdapter struct {
	exchange.MockAdapter
}

func (e *emptyBookAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if symbol == "BTCUSDT" {
		return domain.OrderBook{}, nil // empty order book
	}
	return e.MockAdapter.SpotOrderBook(ctx, symbol)
}

func TestEmptyBookFreezeCircuitBreaker(t *testing.T) {
	cfg := config.Config{
		Mode:                 "MOCK",
		QuoteAssets:          []string{"USDT"},
		CapitalTotal:         1000,
		AllocationPerPair:    0.5,
		PerpLeverage:         1,
		EmptyBookFreezeHours: 6,
	}
	adapter := &emptyBookAdapter{MockAdapter: *exchange.NewMockAdapter()}
	s := New(cfg, adapter)

	// Scan 1: BTCUSDT has empty order book -> consecutiveCount = 1
	r1, _, _ := s.Scan(context.Background())
	if r1.FrozenPairs != 0 {
		t.Errorf("expected 0 frozen pairs on scan 1, got %d", r1.FrozenPairs)
	}

	// Scan 2: consecutiveCount = 2
	s.Scan(context.Background())

	// Scan 3: consecutiveCount = 3 -> should freeze BTCUSDT
	s.Scan(context.Background())

	// Scan 4: BTCUSDT is now frozen!
	r4, _, _ := s.Scan(context.Background())
	if r4.FrozenPairs != 1 {
		t.Errorf("expected 1 frozen pair on scan 4, got %d", r4.FrozenPairs)
	}
	if r4.ActivePairs != 1 {
		t.Errorf("expected 1 active pair (ETH), got %d", r4.ActivePairs)
	}
}

func TestCalculateSettlements(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		holdingHours  int
		intervalHours int
		nextFunding   time.Time
		want          int
	}{
		{
			name:          "zero holding",
			holdingHours:  0,
			intervalHours: 8,
			nextFunding:   now.Add(2 * time.Hour),
			want:          0,
		},
		{
			name:          "zero interval",
			holdingHours:  8,
			intervalHours: 0,
			nextFunding:   now.Add(2 * time.Hour),
			want:          0,
		},
		{
			name:          "exits before next funding",
			holdingHours:  2,
			intervalHours: 8,
			nextFunding:   now.Add(4 * time.Hour),
			want:          0,
		},
		{
			name:          "hits next funding exactly once",
			holdingHours:  4,
			intervalHours: 8,
			nextFunding:   now.Add(2 * time.Hour),
			want:          1,
		},
		{
			name:          "multiple settlements over 24h",
			holdingHours:  24,
			intervalHours: 8,
			nextFunding:   now.Add(2 * time.Hour),
			want:          3,
		},
		{
			name:          "next funding in past rolls over",
			holdingHours:  8,
			intervalHours: 8,
			nextFunding:   now.Add(-10 * time.Minute),
			want:          1,
		},
		{
			name:          "fallback when next funding is zero time",
			holdingHours:  8,
			intervalHours: 8,
			nextFunding:   time.Time{},
			want:          1,
		},
		{
			name:          "fallback half interval when zero time",
			holdingHours:  4,
			intervalHours: 8,
			nextFunding:   time.Time{},
			want:          1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := calculateSettlements(tc.holdingHours, tc.intervalHours, tc.nextFunding, now)
			if got != tc.want {
				t.Errorf("calculateSettlements(%d, %d, %v, %v) = %d, want %d",
					tc.holdingHours, tc.intervalHours, tc.nextFunding, now, got, tc.want)
			}
		})
	}
}

