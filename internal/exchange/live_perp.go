package exchange

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

type okxInstrumentData struct {
	InstID    string `json:"instId"`
	InstType  string `json:"instType"`
	CtVal     string `json:"ctVal"`
	CtValCcy  string `json:"ctValCcy"`
	SettleCcy string `json:"settleCcy"`
	State     string `json:"state"`
	TickSz    string `json:"tickSz"`
	LotSz     string `json:"lotSz"`
	MinSz     string `json:"minSz"`
}

func extractOKXBaseAsset(instID, ctValCcy string) string {
	if ctValCcy != "" {
		return ctValCcy
	}
	parts := strings.Split(instID, "-")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func (l *LiveAdapter) getCachedOKXPerp() ([]domain.Market, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedOKXPerp) > 0 && time.Since(l.lastOKXPerpFetch) < 1*time.Hour {
		return l.cachedOKXPerp, true
	}
	return nil, false
}

func (l *LiveAdapter) processOKXInstrument(inst okxInstrumentData) (domain.Market, bool) {
	if inst.State != "live" || inst.InstID == "" {
		return domain.Market{}, false
	}

	baseAsset := extractOKXBaseAsset(inst.InstID, inst.CtValCcy)

	ctVal, _ := strconv.ParseFloat(inst.CtVal, 64)
	tickSz, _ := strconv.ParseFloat(inst.TickSz, 64)
	lotSz, _ := strconv.ParseFloat(inst.LotSz, 64)
	minSz, _ := strconv.ParseFloat(inst.MinSz, 64)

	market := domain.Market{
		Exchange:     "okx",
		Symbol:       inst.InstID,
		BaseAsset:    strings.ToUpper(baseAsset),
		QuoteAsset:   strings.ToUpper(inst.SettleCcy),
		MarketType:   "PERPETUAL",
		Active:       true,
		ContractSize: ctVal,
		TickSize:     tickSz,
		LotSize:      lotSz,
		MinOrder:     minSz,
	}
	if ctVal > 0 && lotSz > 0 && minSz >= 0 &&
		!math.IsNaN(ctVal) && !math.IsInf(ctVal, 0) &&
		!math.IsNaN(lotSz) && !math.IsInf(lotSz, 0) &&
		!math.IsNaN(minSz) && !math.IsInf(minSz, 0) &&
		strings.EqualFold(inst.CtValCcy, baseAsset) {
		market.AmountSpec = &domain.AmountSpec{
			Unit:        "CONTRACT",
			Step:        lotSz,
			Min:         minSz,
			BasePerUnit: ctVal,
		}
		l.contractSizes[inst.InstID] = ctVal
		l.contractSizes["okx:"+inst.InstID] = ctVal
	} else {
		log.Printf("okx: instrument %s has incomplete or unsupported amount rules; execution disabled for market", inst.InstID)
	}
	return market, true
}

// PerpetualMarkets discovers active perpetual SWAP instruments on OKX and MEXC, respecting targetPerpExchange filter if set.
func (l *LiveAdapter) PerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	var allMarkets []domain.Market

	l.mu.RLock()
	targetEx := l.targetPerpExchange
	l.mu.RUnlock()

	includeOKX := targetEx == "" || targetEx == "okx" || targetEx == "all"
	includeMEXC := targetEx == "" || targetEx == "mexc" || targetEx == "all"

	var errs []string
	if includeOKX {
		okxMarkets, err := l.fetchOKXPerpetualMarkets(ctx)
		if err == nil {
			allMarkets = append(allMarkets, okxMarkets...)
			l.SubscribeOKXBooks(okxMarkets)
		} else {
			errs = append(errs, fmt.Sprintf("okx: %v", err))
		}
	}

	if includeMEXC {
		mexcMarkets, err := l.fetchMEXCPerpetualMarkets(ctx)
		if err == nil {
			allMarkets = append(allMarkets, mexcMarkets...)
		} else {
			errs = append(errs, fmt.Sprintf("mexc: %v", err))
		}
	}

	if len(allMarkets) == 0 {
		return nil, fmt.Errorf("failed to discover perpetual markets (target: %s, errors: %s)", targetEx, strings.Join(errs, "; "))
	}

	return allMarkets, nil
}

func (l *LiveAdapter) fetchOKXPerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	if cached, ok := l.getCachedOKXPerp(); ok { return cached, nil }
	l.mu.RLock(); client := l.okxClient; l.mu.RUnlock()
	if client == nil { return l.getFallbackOKXPerp(fmt.Errorf("okx client is not configured")) }
	markets, err := client.PerpetualMarkets(ctx)
	if err != nil { return l.getFallbackOKXPerp(err) }
	l.mu.Lock()
	for _, market := range markets {
		if market.AmountSpec != nil && market.AmountSpec.BasePerUnit > 0 {
			l.contractSizes[market.Symbol] = market.AmountSpec.BasePerUnit
			l.contractSizes["okx:"+market.Symbol] = market.AmountSpec.BasePerUnit
		}
	}
	if len(markets) > 0 { l.cachedOKXPerp = markets; l.lastOKXPerpFetch = time.Now().UTC() }
	l.mu.Unlock()
	return markets, nil
}

func (l *LiveAdapter) getFallbackOKXPerp(err error) ([]domain.Market, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedOKXPerp) > 0 {
		return l.cachedOKXPerp, nil
	}
	return nil, err
}

func (l *LiveAdapter) fetchMEXCPerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	l.mu.RLock()
	client := l.mexcClient
	l.mu.RUnlock()
	if client == nil { return nil, fmt.Errorf("mexc client is not configured") }
	markets, err := client.PerpetualMarkets(ctx)
	if err != nil { return nil, err }
	l.mu.Lock()
	for _, market := range markets {
		if market.AmountSpec != nil && market.AmountSpec.BasePerUnit > 0 {
			raw := strings.TrimPrefix(market.Symbol, "mexc:")
			l.contractSizes[raw] = market.AmountSpec.BasePerUnit
			l.contractSizes[market.Symbol] = market.AmountSpec.BasePerUnit
		}
	}
	l.mu.Unlock()
	return markets, nil
}
