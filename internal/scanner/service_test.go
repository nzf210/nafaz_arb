package scanner

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/shopspring/decimal"
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

type hybridMockAdapter struct {
	exchange.MockAdapter
	spotBid float64
	spotAsk float64
	perpBid float64
	perpAsk float64
	frRate  float64
}

func (h *hybridMockAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: h.spotBid, Quantity: 100}},
		Asks:      []domain.PriceLevel{{Price: h.spotAsk, Quantity: 100}},
	}, nil
}

func (h *hybridMockAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: h.perpBid, Quantity: 100}},
		Asks:      []domain.PriceLevel{{Price: h.perpAsk, Quantity: 100}},
	}, nil
}

func (h *hybridMockAdapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	now := time.Now().UTC()
	return domain.FundingRate{
		Rate:            h.frRate,
		IntervalHours:   8,
		Timestamp:       now,
		NextFundingTime: now.Add(2 * time.Hour),
	}, nil
}

func (h *hybridMockAdapter) OpenExposures(ctx context.Context) ([]domain.OpenExposure, error) {
	return nil, nil
}

func TestHybridOpportunityCalculation(t *testing.T) {
	adapter := &hybridMockAdapter{
		spotBid: 100.0,
		spotAsk: 100.0,
		perpBid: 102.0,
		perpAsk: 102.0,
		frRate:  0.0005,
	}

	cfg := config.Config{
		Mode:                  "MOCK",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          5.0,
		MaxSlippagePct:        1.0,
		SpotTakerFeePct:       0.1,
		PerpTakerFeePct:       0.05,
		CheckOpenPositions:    false,
	}

	s := New(cfg, adapter)
	r, complete, err := s.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if !complete {
		t.Fatalf("expected scan to be complete")
	}

	if len(r.Opportunities) == 0 {
		t.Fatalf("expected opportunities, got 0")
	}

	op := r.Opportunities[0]
	if op.SpreadPnL <= 0 {
		t.Errorf("expected positive SpreadPnL from basis convergence, got %f", op.SpreadPnL)
	}
	if op.FundingPnL <= 0 {
		t.Errorf("expected positive FundingPnL, got %f", op.FundingPnL)
	}
	if op.NetProfit <= 0 {
		t.Errorf("expected positive NetProfit, got %f", op.NetProfit)
	}
	if !op.TradingEligible {
		t.Errorf("expected opportunity to be TradingEligible, got reasons: %v", op.ExclusionReasons)
	}
}

func TestUnfavorableRegimeNegativeBasisAndFunding(t *testing.T) {
	adapter := &hybridMockAdapter{
		spotBid: 102.0,
		spotAsk: 102.0,
		perpBid: 100.0,
		perpAsk: 100.0,
		frRate:  -0.0005,
	}

	cfg := config.Config{
		Mode:                  "MOCK",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          5.0,
		CheckOpenPositions:    false,
	}

	s := New(cfg, adapter)
	r, _, err := s.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}

	for _, op := range r.Opportunities {
		if op.TradingEligible {
			t.Errorf("expected %s to be excluded under unfavorable regime", op.BaseAsset)
		}
		if !hasReason(op.ExclusionReasons, "UNFAVORABLE_REGIME_NEGATIVE_BASIS_AND_FUNDING") {
			t.Errorf("expected UNFAVORABLE_REGIME_NEGATIVE_BASIS_AND_FUNDING exclusion reason, got: %v", op.ExclusionReasons)
		}
	}
}

func TestDecimalPrecisionVWAPAndProfit(t *testing.T) {
	// Test multi-level depth with fractional quantities that typically cause float64 drift
	levels := []domain.PriceLevel{
		{Price: 100.1, Quantity: 0.1},
		{Price: 100.2, Quantity: 0.2},
		{Price: 100.3, Quantity: 0.3},
	}
	// qty = 0.1 + 0.2 + 0.3 = 0.6
	// In float64: 0.1 + 0.2 != 0.3
	qty := decimal.RequireFromString("0.6")
	v, err := vwap(levels, qty)
	if err != nil {
		t.Fatalf("unexpected vwap error: %v", err)
	}

	// Expected: (0.1*100.1 + 0.2*100.2 + 0.3*100.3) / 0.6
	// = (10.01 + 20.04 + 30.09) / 0.6 = 60.14 / 0.6
	expected := decimal.RequireFromString("60.14").Div(decimal.RequireFromString("0.6"))
	if !v.Equal(expected) {
		t.Errorf("vwap = %s, want %s", v.String(), expected.String())
	}
}

func TestMakerTakerOpportunityCalculation(t *testing.T) {
	adapter := &hybridMockAdapter{
		spotBid: 100.0,
		spotAsk: 102.0, // Bid-Ask spread on spot is 2.0
		perpBid: 105.0,
		perpAsk: 105.0,
		frRate:  0.0005,
	}

	cfgMakerTaker := config.Config{
		Mode:                  "MOCK",
		ExecutionMode:         "MAKER_TAKER",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          20.0,
		MaxSlippagePct:        5.0,
		SpotMakerFeePct:       0.0,
		SpotTakerFeePct:       0.3,
		PerpTakerFeePct:       0.05,
		MaxOrderTTLSeconds:    45,
		CheckOpenPositions:    false,
	}

	sMaker := New(cfgMakerTaker, adapter)
	rMaker, complete, err := sMaker.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("maker-taker scan error: %v, complete=%v", err, complete)
	}
	if len(rMaker.Opportunities) == 0 {
		t.Fatalf("expected opportunities in maker-taker mode")
	}

	opMaker := rMaker.Opportunities[0]
	if opMaker.ExecutionMode != "MAKER_TAKER" {
		t.Errorf("execution_mode = %s, want MAKER_TAKER", opMaker.ExecutionMode)
	}
	// In Maker-Taker mode, SpotEntryPrice is Maker: LocalBestBid + 1 tick (100.0 + 1.0 = 101.0)
	if opMaker.SpotEntryPrice != 101.0 {
		t.Errorf("SpotEntryPrice = %f, want Maker price (bid + 1 tick) 101.0", opMaker.SpotEntryPrice)
	}
	if opMaker.SpotTargetLimitPrice != 101.0 {
		t.Errorf("SpotTargetLimitPrice = %f, want 101.0", opMaker.SpotTargetLimitPrice)
	}
	if opMaker.SpotQueueDepth != 100.0 {
		t.Errorf("SpotQueueDepth = %f, want 100.0", opMaker.SpotQueueDepth)
	}
	if opMaker.MaxOrderTTLSeconds != 45 {
		t.Errorf("MaxOrderTTLSeconds = %d, want 45", opMaker.MaxOrderTTLSeconds)
	}
	if opMaker.MinAcceptablePerpPrice <= 0 || opMaker.MinAcceptablePerpPrice > 105.0 {
		t.Errorf("MinAcceptablePerpPrice = %f, expected between 0 and 105.0", opMaker.MinAcceptablePerpPrice)
	}

	// Compare against Taker-Taker mode
	cfgTakerTaker := cfgMakerTaker
	cfgTakerTaker.ExecutionMode = "TAKER_TAKER"

	sTaker := New(cfgTakerTaker, adapter)
	rTaker, _, _ := sTaker.Scan(context.Background())
	opTaker := rTaker.Opportunities[0]

	// Maker-Taker captures the 2.0 bid-ask spread and lower fees, so its Net Profit should be higher
	if opMaker.NetProfit <= opTaker.NetProfit {
		t.Errorf("maker-taker net profit (%f) should be higher than taker-taker (%f)",
			opMaker.NetProfit, opTaker.NetProfit)
	}
	if opMaker.SpreadPnL <= opTaker.SpreadPnL {
		t.Errorf("maker-taker spread PnL (%f) should be higher than taker-taker (%f)",
			opMaker.SpreadPnL, opTaker.SpreadPnL)
	}
}

type shallowBookAdapter struct {
	exchange.MockAdapter
	availableQty float64
	spotPrice    float64
	perpPrice    float64
}

func (s *shallowBookAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: s.spotPrice, Quantity: s.availableQty}},
		Asks:      []domain.PriceLevel{{Price: s.spotPrice, Quantity: s.availableQty}},
	}, nil
}

func (s *shallowBookAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: s.perpPrice, Quantity: s.availableQty}},
		Asks:      []domain.PriceLevel{{Price: s.perpPrice, Quantity: s.availableQty}},
	}, nil
}

func (s *shallowBookAdapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	return domain.FundingRate{Rate: 0.0005, IntervalHours: 8, Timestamp: time.Now().UTC()}, nil
}

func (s *shallowBookAdapter) OpenExposures(ctx context.Context) ([]domain.OpenExposure, error) {
	return nil, nil
}

func TestAdaptivePositionSizing(t *testing.T) {
	// Budget is 1000 * 0.5 = 500 USDT.
	// At spot=100 and perp=105, targetQty is ~2.439 coins (~500 capital).
	// Shallow book only has 1.0 coin available (~205 capital).
	adapter := &shallowBookAdapter{
		availableQty: 1.0,
		spotPrice:    100.0,
		perpPrice:    105.0,
	}

	enabled := true
	cfgAdaptive := config.Config{
		Mode:                  "MOCK",
		ExecutionMode:         "MAKER_TAKER",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          20.0,
		MaxSlippagePct:        5.0,
		EnableAdaptiveSizing:  &enabled,
		MinAllocatedCapital:   10.0,
		CheckOpenPositions:    false,
	}

	sAdaptive := New(cfgAdaptive, adapter)
	rAdaptive, complete, err := sAdaptive.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("scan error: %v, complete=%v", err, complete)
	}
	if len(rAdaptive.Opportunities) == 0 {
		t.Fatalf("expected opportunity with adaptive sizing enabled")
	}

	op := rAdaptive.Opportunities[0]
	if op.Quantity != 1.0 {
		t.Errorf("Quantity = %f, want adapted 1.0", op.Quantity)
	}
	if op.AllocatedCapital <= 0 || op.AllocatedCapital > 300 {
		t.Errorf("AllocatedCapital = %f, expected ~205 USDT", op.AllocatedCapital)
	}
	if op.CapacityPct <= 0 || op.CapacityPct >= 100 {
		t.Errorf("CapacityPct = %f, expected around 41%%", op.CapacityPct)
	}
	if !op.TradingEligible {
		t.Errorf("expected TradingEligible=true, got exclusion: %v", op.ExclusionReasons)
	}

	// Without adaptive sizing, targetQty 2.439 cannot fit 1.0 depth -> fails
	disabled := false
	cfgDisabled := cfgAdaptive
	cfgDisabled.EnableAdaptiveSizing = &disabled

	sDisabled := New(cfgDisabled, adapter)
	rDisabled, _, _ := sDisabled.Scan(context.Background())
	if len(rDisabled.Opportunities) != 0 {
		t.Fatalf("expected 0 opportunities without adaptive sizing when depth is insufficient, got %d", len(rDisabled.Opportunities))
	}

	// If available depth gives capital below MinAllocatedCapital (e.g. 0.01 coin * 205 = 2.05 < 10)
	adapterDust := &shallowBookAdapter{
		availableQty: 0.01,
		spotPrice:    100.0,
		perpPrice:    105.0,
	}
	sDust := New(cfgAdaptive, adapterDust)
	rDust, _, _ := sDust.Scan(context.Background())
	if len(rDust.Opportunities) != 0 {
		t.Fatalf("expected 0 opportunities when available capital is below MinAllocatedCapital")
	}
}

type syntheticMockAdapter struct {
	exchange.MockAdapter
	idrRate   float64
	rateCalls int
}

func (s *syntheticMockAdapter) USDTIDRRate(ctx context.Context) (float64, error) {
	s.rateCalls++
	return s.idrRate, nil
}

func (s *syntheticMockAdapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "indodax", Symbol: "solidr", BaseAsset: "SOL", QuoteAsset: "IDR", MarketType: "SPOT", Active: true},
	}, nil
}

func (s *syntheticMockAdapter) PerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "okx", Symbol: "SOL-USDT-SWAP", BaseAsset: "SOL", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
	}, nil
}

func (s *syntheticMockAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	// Spot SOL is Rp 2,400,000. At 16,000 IDR/USDT, that's $150 USDT!
	return domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: 2400000.0, Quantity: 10}},
		Asks:      []domain.PriceLevel{{Price: 2400000.0, Quantity: 10}},
	}, nil
}

func (s *syntheticMockAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	// Perp SOL is $160 USDT.
	return domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: 160.0, Quantity: 10}},
		Asks:      []domain.PriceLevel{{Price: 160.0, Quantity: 10}},
	}, nil
}

func (s *syntheticMockAdapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	return domain.FundingRate{Rate: 0.0002, IntervalHours: 8, Timestamp: time.Now().UTC()}, nil
}

func (s *syntheticMockAdapter) OpenExposures(ctx context.Context) ([]domain.OpenExposure, error) {
	return nil, nil
}

func TestIDRMarketsNeverEnterConfiguredQuoteUniverse(t *testing.T) {
	adapter := &syntheticMockAdapter{idrRate: 16000.0}
	cfg := config.Config{
		Mode:                 "LIVE",
		QuoteAssets:          []string{"USDT", "USDC"},
		EnableSyntheticIDR:   true, // legacy config must not override the quote allowlist
		SyntheticUSDTIDRRate: 16000.0,
		CapitalTotal:         1000,
		AllocationPerPair:    0.5,
		PerpLeverage:         1,
	}
	s := New(cfg, adapter)
	r, complete, err := s.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("USDT/USDC-only scan must complete without IDR FX, err=%v complete=%v", err, complete)
	}
	if r.MatchedPairs != 0 || len(r.Opportunities) != 0 {
		t.Fatalf("IDR spot market must not be synthesized into the scan universe: matched=%d opportunities=%d", r.MatchedPairs, len(r.Opportunities))
	}
	if adapter.rateCalls != 0 {
		t.Fatalf("IDR FX ticker must not be requested for a USDT/USDC-only scan, calls=%d", adapter.rateCalls)
	}
}

func TestEmptyBookFreezeMinutesCircuitBreaker(t *testing.T) {
	cfg := config.Config{
		Mode:                   "MOCK",
		QuoteAssets:            []string{"USDT"},
		CapitalTotal:           1000,
		AllocationPerPair:      0.5,
		PerpLeverage:           1,
		EmptyBookFreezeMinutes: 15,
	}
	adapter := &emptyBookAdapter{MockAdapter: *exchange.NewMockAdapter()}
	s := New(cfg, adapter)

	// Scan 1
	s.Scan(context.Background())
	// Scan 2
	s.Scan(context.Background())
	// Scan 3 -> should freeze with 15 minutes
	s.Scan(context.Background())

	frozen := s.FrozenPairs()
	if len(frozen) != 1 {
		t.Fatalf("expected 1 frozen pair, got %d", len(frozen))
	}
	duration := time.Until(frozen[0].FrozenUntil)
	if duration > 16*time.Minute || duration < 13*time.Minute {
		t.Fatalf("expected freeze duration around 15m, got %v", duration)
	}
}

func TestFeeAssetSurcharge(t *testing.T) {
	adapter := &hybridMockAdapter{
		spotBid: 100.0,
		spotAsk: 102.0,
		perpBid: 105.0,
		perpAsk: 105.0,
		frRate:  0.0005,
	}

	baseCfg := config.Config{
		Mode:                  "MOCK",
		ExecutionMode:         "TAKER_TAKER",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          20.0,
		MaxSlippagePct:        5.0,
		SpotMakerFeePct:       0.0,
		SpotTakerFeePct:       0.3,
		PerpTakerFeePct:       0.05,
		MaxOrderTTLSeconds:    45,
		CheckOpenPositions:    false,
	}

	sBase := New(baseCfg, adapter)
	rBase, complete, err := sBase.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("baseline scan error: %v, complete=%v", err, complete)
	}
	if len(rBase.Opportunities) == 0 {
		t.Fatalf("expected baseline opportunity")
	}

	withSurcharge := baseCfg
	withSurcharge.FeeAssetSurchargePct = 0.1
	sSur := New(withSurcharge, adapter)
	rSur, _, err := sSur.Scan(context.Background())
	if err != nil {
		t.Fatalf("surcharge scan error: %v", err)
	}
	if len(rSur.Opportunities) == 0 {
		t.Fatalf("expected opportunity with surcharge")
	}

	opBase := rBase.Opportunities[0]
	opSur := rSur.Opportunities[0]
	if opSur.TradingFees <= opBase.TradingFees {
		t.Errorf("surcharge fees (%f) should exceed baseline fees (%f)", opSur.TradingFees, opBase.TradingFees)
	}
	if opSur.NetProfit >= opBase.NetProfit {
		t.Errorf("surcharge net profit (%f) should be below baseline (%f)", opSur.NetProfit, opBase.NetProfit)
	}

	// Surcharge must also feed MinAcceptablePerpPrice (exit-side guard), i.e.
	// the higher cost floor must be at least as strict.
	if opSur.MinAcceptablePerpPrice < opBase.MinAcceptablePerpPrice {
		t.Errorf("surcharge min acceptable perp price (%f) should be >= baseline (%f)",
			opSur.MinAcceptablePerpPrice, opBase.MinAcceptablePerpPrice)
	}
}

// failingFXAdapter returns an IDR quote market but always fails the FX ticker.
type failingFXAdapter struct {
	exchange.MockAdapter
}

func (f *failingFXAdapter) USDTIDRRate(ctx context.Context) (float64, error) {
	return 0, fmt.Errorf("ticker down")
}

func (f *failingFXAdapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "indodax", Symbol: "solbidr", BaseAsset: "SOL", QuoteAsset: "BIDR", MarketType: "SPOT", Active: true},
	}, nil
}

func (f *failingFXAdapter) PerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "okx", Symbol: "SOL-USDT-SWAP", BaseAsset: "SOL", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
	}, nil
}

func (f *failingFXAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: 2400000.0, Quantity: 10}},
		Asks:      []domain.PriceLevel{{Price: 2400000.0, Quantity: 10}},
	}, nil
}

func (f *failingFXAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Timestamp: time.Now().UTC(),
		Bids:      []domain.PriceLevel{{Price: 160.0, Quantity: 10}},
		Asks:      []domain.PriceLevel{{Price: 160.0, Quantity: 10}},
	}, nil
}

func (f *failingFXAdapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	return domain.FundingRate{Rate: 0.0002, IntervalHours: 8, Timestamp: time.Now().UTC()}, nil
}

// TestFXFailureCannotAbortUSDTUSDCScan verifies that legacy IDR markets are
// excluded by the configured quote allowlist before any FX preflight is considered.
func TestFXFailureCannotAbortUSDTUSDCScan(t *testing.T) {
	cfg := config.Config{
		Mode:                 "LIVE",
		QuoteAssets:          []string{"USDT", "USDC"},
		EnableSyntheticIDR:   true,
		SyntheticUSDTIDRRate: 16000.0,
		CapitalTotal:         1000,
		AllocationPerPair:    0.5,
		PerpLeverage:         1,
	}
	s := New(cfg, &failingFXAdapter{})
	r, complete, err := s.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("scan must complete when only an excluded IDR market is present, err=%v complete=%v", err, complete)
	}
	if r.MatchedPairs != 0 || len(r.Opportunities) != 0 {
		t.Fatalf("IDR/BIDR pair unexpectedly entered scan universe: matched=%d opportunities=%d", r.MatchedPairs, len(r.Opportunities))
	}
	if len(r.Errors) != 0 {
		t.Fatalf("excluded IDR market must not create scan errors: %v", r.Errors)
	}
}

func TestInvalidIDRFXCannotAffectUSDTUSDCScan(t *testing.T) {
	cfg := config.Config{
		Mode:                 "LIVE",
		QuoteAssets:          []string{"USDT", "USDC"},
		EnableSyntheticIDR:   true,
		SyntheticUSDTIDRRate: 16000,
		CapitalTotal:         1000,
		AllocationPerPair:    0.5,
		PerpLeverage:         1,
	}
	adapter := &syntheticMockAdapter{idrRate: math.NaN()}
	s := New(cfg, adapter)
	r, complete, err := s.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("non-finite IDR FX must not abort USDT/USDC scan, err=%v complete=%v", err, complete)
	}
	if adapter.rateCalls != 0 || r.MatchedPairs != 0 {
		t.Fatalf("IDR FX should be unused and IDR market excluded: rate_calls=%d matched=%d", adapter.rateCalls, r.MatchedPairs)
	}
}

func TestApplyEvaluationResultsCountsFailedPairs(t *testing.T) {
	result := domain.ScanResult{}
	complete := applyEvaluationResults([]evalResult{
		{err: fmt.Errorf("pair data unavailable")},
	}, &result)
	if !complete {
		t.Fatal("non-fatal pair-level errors should not mark the whole scan incomplete")
	}
	if result.FailedPairs != 1 {
		t.Fatalf("failed_pairs=%d, want 1", result.FailedPairs)
	}
}
