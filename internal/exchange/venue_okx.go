package exchange

import (
	"context"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// OKXVenue encapsulates modular logic for OKX exchange.
type OKXVenue struct {
	adapter *LiveAdapter
}

// NewOKXVenue creates an OKX venue module bound to the adapter.
func NewOKXVenue(adapter *LiveAdapter) *OKXVenue {
	return &OKXVenue{adapter: adapter}
}

// Venue returns the venue identifier.
func (v *OKXVenue) Venue() string {
	return "okx"
}

// FetchPerpMarkets discovers active OKX SWAP instruments.
func (v *OKXVenue) FetchPerpMarkets(ctx context.Context) ([]domain.Market, error) {
	return v.adapter.fetchOKXPerpetualMarkets(ctx)
}

// FetchPerpOrderBook fetches top-of-book depth for an OKX swap market.
func (v *OKXVenue) FetchPerpOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return v.adapter.fetchOKXOrderBook(ctx, symbol)
}

// FetchFundingRate fetches live funding rate for an OKX swap instrument.
func (v *OKXVenue) FetchFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	return v.adapter.fetchOKXFundingRate(ctx, symbol)
}
