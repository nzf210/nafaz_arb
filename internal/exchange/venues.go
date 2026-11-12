package exchange

import (
	"context"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// SpotVenueModule defines the modular contract for a spot exchange venue in the scanner.
type SpotVenueModule interface {
	Venue() string
	FetchSpotMarkets(ctx context.Context) ([]domain.Market, error)
	FetchSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error)
}

// PerpVenueModule defines the modular contract for a perpetual/futures exchange venue in the scanner.
type PerpVenueModule interface {
	Venue() string
	FetchPerpMarkets(ctx context.Context) ([]domain.Market, error)
	FetchPerpOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error)
	FetchFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error)
}

// FXVenueModule defines the modular contract for exchange rate feeds (e.g. USDT/IDR).
type FXVenueModule interface {
	Venue() string
	FetchUSDTIDRRate(ctx context.Context) (float64, error)
}

// VenueRegistry holds registered modular venue implementations.
type VenueRegistry struct {
	spotVenues map[string]SpotVenueModule
	perpVenues map[string]PerpVenueModule
}

// NewVenueRegistry creates an empty registry.
func NewVenueRegistry() *VenueRegistry {
	return &VenueRegistry{
		spotVenues: make(map[string]SpotVenueModule),
		perpVenues: make(map[string]PerpVenueModule),
	}
}

// RegisterSpot registers a spot venue module.
func (r *VenueRegistry) RegisterSpot(v SpotVenueModule) {
	r.spotVenues[v.Venue()] = v
}

// RegisterPerp registers a perp venue module.
func (r *VenueRegistry) RegisterPerp(v PerpVenueModule) {
	r.perpVenues[v.Venue()] = v
}

// GetSpot returns the spot venue module by name.
func (r *VenueRegistry) GetSpot(venue string) (SpotVenueModule, bool) {
	v, ok := r.spotVenues[venue]
	return v, ok
}

// GetPerp returns the perp venue module by name.
func (r *VenueRegistry) GetPerp(venue string) (PerpVenueModule, bool) {
	v, ok := r.perpVenues[venue]
	return v, ok
}
