package scanner

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"

)

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

// TestFXFallbackExcludesPair verifies that a pair priced from a fallback FX rate
// is never published for trading: IDR pairs must be skipped entirely when the
// live USDT/IDR ticker is unavailable, since no hardcoded or synthetic rate is
// ever acceptable for IDR spread math.
func TestFXFallbackExcludesPair(t *testing.T) {
	cfg := config.Config{
		Mode: "MOCK", ExecutionMode: "MAKER_TAKER", QuoteAssets: []string{"USDT"},
		EnableSyntheticIDR: true, SyntheticUSDTIDRRate: 16000.0,
		CapitalTotal: 1000, AllocationPerPair: 0.5, PerpLeverage: 1,
	}
	s := New(cfg, &failingFXAdapter{})
	r, complete, err := s.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("FX failure for an IDR pair must not abort the scan, err=%v complete=%v", err, complete)
	}
	if r.FailedPairs != 1 || len(r.Errors) == 0 || !strings.Contains(r.Errors[0], "USDT/IDR live ticker unavailable") {
		t.Fatalf("expected visible pair-level FX error, failed=%d errors=%v", r.FailedPairs, r.Errors)
	}
}

func TestFXPreflightRejectsNonFiniteRate(t *testing.T) {
	cfg := config.Config{
		Mode: "MOCK", QuoteAssets: []string{"USDT"}, EnableSyntheticIDR: true,
		SyntheticUSDTIDRRate: 16000, CapitalTotal: 1000, AllocationPerPair: 0.5, PerpLeverage: 1,
	}
	adapter := &syntheticMockAdapter{idrRate: math.NaN()}
	s := New(cfg, adapter)
	r, complete, err := s.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("invalid FX must skip only the IDR pair, err=%v complete=%v", err, complete)
	}
	if r.FailedPairs != 1 || len(r.Errors) == 0 || !strings.Contains(r.Errors[0], "USDT/IDR live ticker unavailable") {
		t.Fatalf("expected visible pair-level invalid-FX error, failed=%d errors=%v", r.FailedPairs, r.Errors)
	}
}

type mixedFXAdapter struct {
	failingFXAdapter
}

func (m *mixedFXAdapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "indodax", Symbol: "solbidr", BaseAsset: "SOL", QuoteAsset: "BIDR", MarketType: "SPOT", Active: true},
		{Exchange: "tokocrypto", Symbol: "btcusdt", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
	}, nil
}

func (m *mixedFXAdapter) PerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "okx", Symbol: "SOL-USDT-SWAP", BaseAsset: "SOL", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
		{Exchange: "okx", Symbol: "BTC-USDT-SWAP", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
	}, nil
}

func (m *mixedFXAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if symbol == "btcusdt" {
		return domain.OrderBook{
			Exchange: "tokocrypto", Symbol: symbol, Timestamp: time.Now().UTC(),
			Bids: []domain.PriceLevel{{Price: 100, Quantity: 10}},
			Asks: []domain.PriceLevel{{Price: 101, Quantity: 10}},
		}, nil
	}
	return m.failingFXAdapter.SpotOrderBook(ctx, symbol)
}

func (m *mixedFXAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if symbol == "BTC-USDT-SWAP" {
		return domain.OrderBook{
			Exchange: "okx", Symbol: symbol, Timestamp: time.Now().UTC(),
			Bids: []domain.PriceLevel{{Price: 105, Quantity: 10}},
			Asks: []domain.PriceLevel{{Price: 106, Quantity: 10}},
		}, nil
	}
	return m.failingFXAdapter.PerpetualOrderBook(ctx, symbol)
}

func TestIDRFXFailureDoesNotBlockDirectUSDTMarkets(t *testing.T) {
	cfg := config.Config{
		Mode: "MOCK", ExecutionMode: "TAKER_TAKER",
		QuoteAssets: []string{"USDT", "USDC"}, EnableSyntheticIDR: true,
		SyntheticUSDTIDRRate: 16000, CapitalTotal: 1000,
		AllocationPerPair: 0.5, PerpLeverage: 1,
		EstimatedHoldingHours: 8, MinNetROIPct: 0.1, MaxNetROIPct: 20,
		CheckOpenPositions: false,
	}
	s := New(cfg, &mixedFXAdapter{})
	r, complete, err := s.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("IDR FX failure must not abort direct-quote scan, err=%v complete=%v", err, complete)
	}
	if r.MatchedPairs != 2 {
		t.Fatalf("matched pairs=%d, want 2 (one synthetic IDR and one direct USDT)", r.MatchedPairs)
	}
	if r.FailedPairs != 1 || len(r.Errors) != 1 || !strings.Contains(r.Errors[0], "USDT/IDR live ticker unavailable") {
		t.Fatalf("expected only IDR pair to fail visibly, failed=%d errors=%v", r.FailedPairs, r.Errors)
	}
	if len(r.Opportunities) != 1 || r.Opportunities[0].SpotSymbol != "btcusdt" {
		t.Fatalf("direct USDT pair should still be evaluated, opportunities=%+v", r.Opportunities)
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
