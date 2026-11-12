package exchange

import (
	"context"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// MEXCVenue encapsulates modular logic for MEXC exchange (spot and perpetual).
type MEXCVenue struct {
	adapter *LiveAdapter
}

// NewMEXCVenue creates a MEXC venue module bound to the adapter.
func NewMEXCVenue(adapter *LiveAdapter) *MEXCVenue {
	return &MEXCVenue{adapter: adapter}
}

// Venue returns the venue identifier.
func (v *MEXCVenue) Venue() string {
	return "mexc"
}

// FetchSpotMarkets discovers active MEXC spot markets.
func (v *MEXCVenue) FetchSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	return v.adapter.fetchMEXCSpotMarkets(ctx)
}

// FetchSpotOrderBook fetches top-of-book depth for a MEXC spot market.
func (v *MEXCVenue) FetchSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return v.adapter.fetchMEXCSpotOrderBook(ctx, symbol)
}

// FetchPerpMarkets discovers active MEXC contract/futures markets.
func (v *MEXCVenue) FetchPerpMarkets(ctx context.Context) ([]domain.Market, error) {
	return v.adapter.fetchMEXCPerpetualMarkets(ctx)
}

// FetchPerpOrderBook fetches top-of-book depth for a MEXC contract/futures market.
func (v *MEXCVenue) FetchPerpOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return v.adapter.fetchMEXCOrderBook(ctx, symbol)
}

// FetchFundingRate fetches live funding rate for a MEXC contract market.
func (v *MEXCVenue) FetchFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	return v.adapter.fetchMEXCFundingRate(ctx, symbol)
}
