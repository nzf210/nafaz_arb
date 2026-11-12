package exchange

import (
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/indodax"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/mexc"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/okx"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/tokocrypto"
)

// VenueAdapters creates one adapter per CEX. Each adapter owns its venue's
// market discovery/cache and client mapping; the compatibility facade remains
// the shared book transport so existing WS managers, REST fallback and health
// accounting stay intact during migration.
func (l *LiveAdapter) VenueAdapters() map[string]contracts.MarketDataAdapter {
	l.mu.RLock()
	indodaxClient, tokocryptoClient := l.indodaxClient, l.tokocryptoClient
	mexcClient, okxClient := l.mexcClient, l.okxClient
	target := strings.ToLower(strings.TrimSpace(l.targetPerpExchange))
	l.mu.RUnlock()

	includeOKX := target == "" || target == "okx" || target == "all"
	includeMEXC := target == "" || target == "mexc" || target == "all"
	return map[string]contracts.MarketDataAdapter{
		"indodax": indodax.NewAdapter(indodaxClient, l, func(err error) { l.recordOutcome("indodax", err) }),
		"tokocrypto": tokocrypto.NewAdapter(tokocryptoClient, l, l.SubscribeTokocryptoBooks, func(err error) { l.recordOutcome("tokocrypto", err) }),
		"okx": okx.NewAdapter(okxClient, l, includeOKX, func(markets []domain.Market) {
			l.mu.Lock()
			l.cachedOKXPerp = append([]domain.Market(nil), markets...)
			l.lastOKXPerpFetch = time.Now().UTC()
			for _, market := range markets {
				if market.AmountSpec != nil && market.AmountSpec.BasePerUnit > 0 {
					l.contractSizes[market.Symbol] = market.AmountSpec.BasePerUnit
					l.contractSizes["okx:"+market.Symbol] = market.AmountSpec.BasePerUnit
				}
			}
			l.mu.Unlock()
			l.SubscribeOKXBooks(markets)
		}, func(err error) { l.recordOutcome("okx", err) }),
		"mexc": mexc.NewAdapter(mexcClient, l, includeMEXC, func(markets []domain.Market) {
			l.mu.Lock()
			l.cachedMEXCPerp = append([]domain.Market(nil), markets...)
			for _, market := range markets {
				if market.AmountSpec != nil && market.AmountSpec.BasePerUnit > 0 {
					raw := strings.TrimPrefix(market.Symbol, "mexc:")
					l.contractSizes[raw] = market.AmountSpec.BasePerUnit
					l.contractSizes[market.Symbol] = market.AmountSpec.BasePerUnit
				}
			}
			l.mu.Unlock()
			l.SubscribeMEXCPerpetualBooks(markets)
		}, func(err error) { l.recordOutcome("mexc", err) }),
	}
}
