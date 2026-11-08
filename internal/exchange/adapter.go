package exchange

import (
	"context"
	"errors"
	"strings"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// ReadOnlyAdapter describes public market-data access only; it has no order methods.
type ReadOnlyAdapter interface {
	SpotMarkets(context.Context) ([]domain.Market, error)
	PerpetualMarkets(context.Context) ([]domain.Market, error)
	SpotOrderBook(context.Context, string) (domain.OrderBook, error)
	PerpetualOrderBook(context.Context, string) (domain.OrderBook, error)
	FundingRate(context.Context, string) (domain.FundingRate, error)
	OpenExposures(context.Context) ([]domain.OpenExposure, error)
}

// RateProvider is an optional interface adapters can implement to provide live FX conversion rates (e.g. USDT/IDR).
type RateProvider interface {
	USDTIDRRate(context.Context) (float64, error)
}

// OKXMetricsProvider is an optional interface adapters implement to expose
// their OKX rate-limiter observability. Adapters without it report nothing.
type OKXMetricsProvider interface {
	OKXMetrics() OKXMetricsSnapshot
}

func isEligibleMarket(m domain.Market, marketType string, allowedQuotes map[string]bool) bool {
	return m.Active && strings.EqualFold(m.MarketType, marketType) && allowedQuotes[strings.ToUpper(m.QuoteAsset)]
}

func marketPairKey(m domain.Market) string {
	return strings.ToUpper(m.BaseAsset + "/" + m.QuoteAsset)
}

func MatchMarkets(spot, perp []domain.Market, quotes []string) [][2]domain.Market {
	return MatchMarketsWithSynthetic(spot, perp, quotes, false)
}

func isSyntheticIDRMarket(m domain.Market) bool {
	if !m.Active || !strings.EqualFold(m.MarketType, "SPOT") {
		return false
	}
	quote := strings.ToUpper(m.QuoteAsset)
	return quote == "IDR" || quote == "BIDR"
}

func indexPerpMarkets(perp []domain.Market, allowed map[string]bool) map[string][]domain.Market {
	perpByKey := make(map[string][]domain.Market)
	for _, m := range perp {
		if isEligibleMarket(m, "PERPETUAL", allowed) {
			key := marketPairKey(m)
			perpByKey[key] = append(perpByKey[key], m)
		}
	}
	return perpByKey
}

func matchSpotMarket(s domain.Market, perpByKey map[string][]domain.Market, allowed map[string]bool, enableSyntheticIDR bool) [][2]domain.Market {
	var matches [][2]domain.Market
	if isEligibleMarket(s, "SPOT", allowed) {
		for _, p := range perpByKey[marketPairKey(s)] {
			matches = append(matches, [2]domain.Market{s, p})
		}
	}
	if enableSyntheticIDR && isSyntheticIDRMarket(s) {
		synthKey := strings.ToUpper(s.BaseAsset + "/USDT")
		for _, p := range perpByKey[synthKey] {
			matches = append(matches, [2]domain.Market{s, p})
		}
	}
	return matches
}

// MatchMarketsWithSynthetic matches spot and perpetual markets, optionally pairing IDR/BIDR spot markets with USDT perpetuals.
func MatchMarketsWithSynthetic(spot, perp []domain.Market, quotes []string, enableSyntheticIDR bool) [][2]domain.Market {
	allowed := make(map[string]bool, len(quotes))
	for _, q := range quotes {
		allowed[strings.ToUpper(q)] = true
	}
	perpByKey := indexPerpMarkets(perp, allowed)

	var out [][2]domain.Market
	for _, s := range spot {
		out = append(out, matchSpotMarket(s, perpByKey, allowed, enableSyntheticIDR)...)
	}
	if len(out) == 0 && len(spot)+len(perp) == 0 {
		return nil
	}
	return out
}

var ErrLiveAdapterNotConfigured = errors.New("live exchange adapters are not configured in this boilerplate; use MOCK mode")
