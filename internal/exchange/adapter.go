package exchange

import (
	"context"
	"errors"
	"strings"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
)

// ReadOnlyAdapter is kept as a compatibility alias for venue-neutral contracts.
type ReadOnlyAdapter = contracts.ReadOnlyAdapter

// RateProvider is the optional venue-neutral fiat conversion capability.
type RateProvider = contracts.RateProvider

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
