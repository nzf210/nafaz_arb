package exchange

import (
	"context"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// IndodaxVenue encapsulates modular logic for Indodax exchange.
type IndodaxVenue struct {
	adapter *LiveAdapter
}

// NewIndodaxVenue creates an Indodax venue module bound to the adapter.
func NewIndodaxVenue(adapter *LiveAdapter) *IndodaxVenue {
	return &IndodaxVenue{adapter: adapter}
}

// Venue returns the venue identifier.
func (v *IndodaxVenue) Venue() string {
	return "indodax"
}

// FetchSpotMarkets discovers active Indodax spot markets.
func (v *IndodaxVenue) FetchSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	return v.adapter.fetchIndodaxSpotMarkets(ctx)
}

// FetchSpotOrderBook fetches top-of-book depth for an Indodax spot market.
func (v *IndodaxVenue) FetchSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return v.adapter.fetchIndodaxSpotOrderBook(ctx, symbol)
}

// FetchUSDTIDRRate fetches the current live USDT/IDR rate from Indodax ticker.
func (v *IndodaxVenue) FetchUSDTIDRRate(ctx context.Context) (float64, error) {
	return v.adapter.fetchIndodaxUSDTIDRRate(ctx)
}
