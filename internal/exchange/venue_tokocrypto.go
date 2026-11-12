package exchange

import (
	"context"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// TokocryptoVenue encapsulates modular logic for Tokocrypto exchange.
type TokocryptoVenue struct {
	adapter *LiveAdapter
}

// NewTokocryptoVenue creates a Tokocrypto venue module bound to the adapter.
func NewTokocryptoVenue(adapter *LiveAdapter) *TokocryptoVenue {
	return &TokocryptoVenue{adapter: adapter}
}

// Venue returns the venue identifier.
func (v *TokocryptoVenue) Venue() string {
	return "tokocrypto"
}

// FetchSpotMarkets discovers active Tokocrypto spot markets.
func (v *TokocryptoVenue) FetchSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	return v.adapter.fetchTokocryptoSpotMarkets(ctx)
}

// FetchSpotOrderBook fetches top-of-book depth for a Tokocrypto spot market.
func (v *TokocryptoVenue) FetchSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return v.adapter.fetchTokocryptoSpotOrderBook(ctx, symbol)
}

// FetchUSDTIDRRate fetches live USDT/IDR FX mid-price from Tokocrypto depth.
func (v *TokocryptoVenue) FetchUSDTIDRRate(ctx context.Context) (float64, error) {
	return v.adapter.fetchTokocryptoUSDTIDRRate(ctx)
}
