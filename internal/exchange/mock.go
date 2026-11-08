package exchange

import (
	"context"
	"fmt"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

type MockAdapter struct {
	fundingRate float64
}

const (
	BTCUSDSWP        = "BTC-USDT-SWAP"
	MockPerpExchange = "okx"
)

func NewMockAdapter() *MockAdapter { return &MockAdapter{fundingRate: 0.0003} }

func (m *MockAdapter) SetFundingRate(rate float64) {
	m.fundingRate = rate
}

// USDTIDRRate returns a deterministic conversion rate for MOCK scans only.
// LIVE mode uses LiveAdapter and never falls back to this simulated value.
func (m *MockAdapter) USDTIDRRate(context.Context) (float64, error) {
	return 16000.0, nil
}

func (m *MockAdapter) SpotMarkets(context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "indodax", Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
		{Exchange: "indodax", Symbol: "ETHUSDT", BaseAsset: "ETH", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
	}, nil
}
func (m *MockAdapter) PerpetualMarkets(context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "okx", Symbol: BTCUSDSWP, BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true, ContractSize: 1},
		{Exchange: "okx", Symbol: "ETH-USDT-SWAP", BaseAsset: "ETH", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true, ContractSize: 1},
	}, nil
}
func (m *MockAdapter) SpotOrderBook(_ context.Context, symbol string) (domain.OrderBook, error) {
	mid := 100.0
	if symbol == "BTCUSDT" {
		mid = 60000
	}
	return book("indodax", symbol, mid-0.10, mid+0.10), nil
}
func (m *MockAdapter) PerpetualOrderBook(_ context.Context, symbol string) (domain.OrderBook, error) {
	mid := 100.2
	if symbol == BTCUSDSWP {
		mid = 60050
	}
	return book("okx", symbol, mid-0.10, mid+0.10), nil
}
func (m *MockAdapter) FundingRate(_ context.Context, symbol string) (domain.FundingRate, error) {
	if symbol == "" {
		return domain.FundingRate{}, fmt.Errorf("empty symbol")
	}
	now := time.Now().UTC()
	rate := m.fundingRate
	if rate == 0 {
		rate = 0.0003
	}
	return domain.FundingRate{Rate: rate, IntervalHours: 8, Timestamp: now, NextFundingTime: now.Add(4 * time.Hour)}, nil
}
func book(exchange, symbol string, bid, ask float64) domain.OrderBook {
	now := time.Now().UTC()
	return domain.OrderBook{Exchange: exchange, Symbol: symbol, Timestamp: now,
		Bids: []domain.PriceLevel{{Price: bid, Quantity: 10}, {Price: bid - 0.1, Quantity: 20}},
		Asks: []domain.PriceLevel{{Price: ask, Quantity: 10}, {Price: ask + 0.1, Quantity: 20}}}
}

func (m *MockAdapter) OpenExposures(context.Context) ([]domain.OpenExposure, error) {
	return []domain.OpenExposure{{Exchange: "okx", Symbol: BTCUSDSWP, BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Kind: "POSITION", Quantity: 0.01, Active: true, Managed: false}}, nil
}
