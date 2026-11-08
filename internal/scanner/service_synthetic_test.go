package scanner

import (
	"context"
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"

)

type syntheticMockAdapter struct {
	exchange.MockAdapter
	idrRate float64
}

func (s *syntheticMockAdapter) USDTIDRRate(ctx context.Context) (float64, error) {
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

func TestSyntheticIDRArbitrage(t *testing.T) {
	adapter := &syntheticMockAdapter{idrRate: 16000.0}

	cfg := config.Config{
		Mode:                  "MOCK",
		ExecutionMode:         "MAKER_TAKER",
		QuoteAssets:           []string{"USDT"},
		EnableSyntheticIDR:    true,
		SyntheticUSDTIDRRate:  16000.0,
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          20.0,
		MaxSlippagePct:        5.0,
		CheckOpenPositions:    false,
	}

	s := New(cfg, adapter)
	r, complete, err := s.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("scan error: %v, complete=%v", err, complete)
	}
	if len(r.Opportunities) != 1 {
		t.Fatalf("expected 1 synthetic opportunity, got %d", len(r.Opportunities))
	}

	op := r.Opportunities[0]
	if !op.IsSynthetic {
		t.Errorf("expected IsSynthetic=true")
	}
	if op.SyntheticFXRate != 16000.0 {
		t.Errorf("SyntheticFXRate = %f, want 16000.0", op.SyntheticFXRate)
	}
	if op.OriginalSpotQuote != "IDR" {
		t.Errorf("OriginalSpotQuote = %s, want IDR", op.OriginalSpotQuote)
	}
	if op.QuoteAsset != "USDT" {
		t.Errorf("QuoteAsset = %s, want USDT", op.QuoteAsset)
	}
	// In Maker-Taker mode, Spot target price is (2,400,000 + 1 tick) / 16,000 = ~150.0000625
	if op.SpotEntryPrice < 150.0 || op.SpotEntryPrice > 150.01 {
		t.Errorf("SpotEntryPrice = %f, want ~150.00006", op.SpotEntryPrice)
	}
	if op.TargetLimitBuyIDR != 2400001.0 {
		t.Errorf("TargetLimitBuyIDR = %f, want 2400001.0", op.TargetLimitBuyIDR)
	}
	if op.PerpEntryPrice != 160.0 {
		t.Errorf("PerpEntryPrice = %f, want 160.0", op.PerpEntryPrice)
	}
	if op.SpreadPnL <= 0 {
		t.Errorf("expected positive SpreadPnL, got %f", op.SpreadPnL)
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
