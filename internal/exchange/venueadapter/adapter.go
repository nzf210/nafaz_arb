// Package venueadapter scopes the compatibility LiveAdapter to one CEX.
// This keeps registry consumers venue-oriented while individual client packages
// are migrated to implement the neutral contracts directly.
package venueadapter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
)

type Adapter struct {
	name string
	source contracts.MarketDataAdapter
}

func New(name string, source contracts.MarketDataAdapter) (*Adapter, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || source == nil {
		return nil, fmt.Errorf("venue name and source adapter are required")
	}
	return &Adapter{name: name, source: source}, nil
}

func (a *Adapter) Name() string { return a.name }

func (a *Adapter) filterMarkets(markets []domain.Market) []domain.Market {
	filtered := make([]domain.Market, 0, len(markets))
	for _, market := range markets {
		if strings.EqualFold(market.Exchange, a.name) {
			filtered = append(filtered, market)
		}
	}
	return filtered
}

func (a *Adapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	markets, err := a.source.SpotMarkets(ctx)
	if err != nil { return nil, err }
	return a.filterMarkets(markets), nil
}

func (a *Adapter) PerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	markets, err := a.source.PerpetualMarkets(ctx)
	if err != nil { return nil, err }
	return a.filterMarkets(markets), nil
}

func (a *Adapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if !a.ownsSpotSymbol(symbol) {
		return domain.OrderBook{}, fmt.Errorf("%s does not own spot symbol %q", a.name, symbol)
	}
	return a.source.SpotOrderBook(ctx, symbol)
}

func (a *Adapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if !a.ownsPerpetualSymbol(symbol) {
		return domain.OrderBook{}, fmt.Errorf("%s does not own perpetual symbol %q", a.name, symbol)
	}
	return a.source.PerpetualOrderBook(ctx, symbol)
}

func (a *Adapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	if !a.ownsPerpetualSymbol(symbol) {
		return domain.FundingRate{}, fmt.Errorf("%s does not own funding symbol %q", a.name, symbol)
	}
	return a.source.FundingRate(ctx, symbol)
}

func (a *Adapter) OpenExposures(ctx context.Context) ([]domain.OpenExposure, error) {
	items, err := a.source.OpenExposures(ctx)
	if err != nil { return nil, err }
	filtered := make([]domain.OpenExposure, 0, len(items))
	for _, item := range items {
		if strings.EqualFold(item.Exchange, a.name) { filtered = append(filtered, item) }
	}
	return filtered, nil
}

func (a *Adapter) USDTIDRRate(ctx context.Context) (float64, error) {
	if a.name != "indodax" { return 0, fmt.Errorf("%s does not provide USDT/IDR conversion", a.name) }
	provider, ok := a.source.(contracts.RateProvider)
	if !ok { return 0, fmt.Errorf("%s USDT/IDR conversion is unavailable", a.name) }
	return provider.USDTIDRRate(ctx)
}

func (a *Adapter) ownsSpotSymbol(symbol string) bool {
	switch a.name {
	case "tokocrypto": return strings.HasPrefix(strings.ToLower(symbol), "toko:")
	case "mexc": return strings.HasPrefix(strings.ToLower(symbol), "mexc_spot:")
	case "indodax": return !strings.Contains(symbol, ":")
	default: return false
	}
}

func (a *Adapter) ownsPerpetualSymbol(symbol string) bool {
	switch a.name {
	case "mexc": return strings.HasPrefix(strings.ToLower(symbol), "mexc:")
	case "okx": return !strings.Contains(strings.ToLower(symbol), "mexc:")
	default: return false
	}
}

func (a *Adapter) ExchangeAvailable(name string) bool {
	if reporter, ok := a.source.(interface{ ExchangeAvailable(string) bool }); ok {
		return reporter.ExchangeAvailable(a.name)
	}
	return true
}

func (a *Adapter) WSConnectionHealthy(name string) bool {
	if reporter, ok := a.source.(interface{ WSConnectionHealthy(string) bool }); ok {
		return reporter.WSConnectionHealthy(a.name)
	}
	return true
}

func (a *Adapter) LastBookUpdate(name, symbol string) time.Time {
	if reporter, ok := a.source.(interface{ LastBookUpdate(string, string) time.Time }); ok {
		return reporter.LastBookUpdate(a.name, symbol)
	}
	return time.Time{}
}

func (a *Adapter) AccountAvailable(name string) (exchange.AccountStatus, error) {
	if reporter, ok := a.source.(interface{ AccountAvailable(string) (exchange.AccountStatus, error) }); ok {
		return reporter.AccountAvailable(a.name)
	}
	return exchange.AccountStatus{}, fmt.Errorf("%s account status is unavailable", a.name)
}

var _ contracts.MarketDataAdapter = (*Adapter)(nil)
var _ contracts.RateProvider = (*Adapter)(nil)
