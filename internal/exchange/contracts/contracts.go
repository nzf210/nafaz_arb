// Package contracts defines venue-neutral exchange capabilities.
package contracts

import (
	"context"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// ReadOnlyAdapter exposes public market data only. Trading credentials and
// order placement are intentionally outside this interface.
type MarketDataAdapter interface {
	SpotMarkets(context.Context) ([]domain.Market, error)
	PerpetualMarkets(context.Context) ([]domain.Market, error)
	SpotOrderBook(context.Context, string) (domain.OrderBook, error)
	PerpetualOrderBook(context.Context, string) (domain.OrderBook, error)
	FundingRate(context.Context, string) (domain.FundingRate, error)
	OpenExposures(context.Context) ([]domain.OpenExposure, error)
}

// ReadOnlyAdapter is retained as a compatibility alias.
type ReadOnlyAdapter = MarketDataAdapter

// RateProvider is an optional venue capability for fiat conversion.
type RateProvider interface {
	USDTIDRRate(context.Context) (float64, error)
}
