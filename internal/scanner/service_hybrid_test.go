package scanner

import (
	"context"
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	
	"github.com/shopspring/decimal"
)

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

