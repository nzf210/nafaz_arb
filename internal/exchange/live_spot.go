package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// USDTIDRRate retrieves the current live USDT/IDR exchange rate from Indodax ticker with caching.
func (l *LiveAdapter) USDTIDRRate(ctx context.Context) (float64, error) {
	l.mu.RLock()
	if l.lastUSDTIDRRate > 0 && time.Since(l.lastUSDTIDRTime) < 60*time.Second {
		rate := l.lastUSDTIDRRate
		l.mu.RUnlock()
		return rate, nil
	}
	client := l.indodaxClient
	l.mu.RUnlock()
	if client == nil {
		return 0, fmt.Errorf("indodax client is not configured")
	}
	rate, err := client.USDTIDRRate(ctx)
	if err != nil { return 0, err }
	l.mu.Lock()
	l.lastUSDTIDRRate = rate
	l.lastUSDTIDRTime = time.Now().UTC()
	l.mu.Unlock()
	return rate, nil
}

// SpotMarkets discovers active spot pairs on Tokocrypto and Indodax (and MEXC Spot if target is mexc/all).
func (l *LiveAdapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	var allMarkets []domain.Market
	var errs []string

	tokoMarkets, err := l.fetchTokocryptoSpotMarkets(ctx)
	if err == nil {
		allMarkets = append(allMarkets, tokoMarkets...)
	} else {
		errs = append(errs, fmt.Sprintf("toko: %v", err))
	}

	indodaxMarkets, err := l.fetchIndodaxSpotMarkets(ctx)
	if err == nil {
		allMarkets = append(allMarkets, indodaxMarkets...)
	} else {
		errs = append(errs, fmt.Sprintf("indodax: %v", err))
	}


	if len(allMarkets) == 0 {
		return nil, fmt.Errorf("failed to discover spot markets (%s)", strings.Join(errs, "; "))
	}

	return allMarkets, nil
}

func (l *LiveAdapter) fetchTokocryptoSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	l.mu.RLock()
	if len(l.cachedTokocryptoSpot) > 0 && time.Since(l.lastTokocryptoFetch) < 1*time.Hour {
		res := l.cachedTokocryptoSpot
		l.mu.RUnlock()
		return res, nil
	}
	l.mu.RUnlock()

	endpoint := l.tokocryptoBaseURL + "/open/v1/common/symbols"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return l.getFallbackTokocryptoSpot(err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return l.getFallbackTokocryptoSpot(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return l.getFallbackTokocryptoSpot(fmt.Errorf("tokocrypto symbols status: %d", resp.StatusCode))
	}

	var raw struct {
		Code int `json:"code"`
		Data struct {
			List []struct {
				Symbol            string `json:"symbol"`
				BaseAsset         string `json:"baseAsset"`
				QuoteAsset        string `json:"quoteAsset"`
				SpotTradingEnable int    `json:"spotTradingEnable"`
				Filters           []struct {
					FilterType string `json:"filterType"`
					StepSize   string `json:"stepSize"`
					MinQty     string `json:"minQty"`
				} `json:"filters"`
			} `json:"list"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return l.getFallbackTokocryptoSpot(fmt.Errorf("decode tokocrypto symbols: %w", err))
	}

	var markets []domain.Market
	for _, s := range raw.Data.List {
		if s.SpotTradingEnable != 1 || s.Symbol == "" || s.BaseAsset == "" || s.QuoteAsset == "" {
			continue
		}
		var amountSpec *domain.AmountSpec
		for _, filter := range s.Filters {
			if strings.EqualFold(filter.FilterType, "LOT_SIZE") ||
				strings.EqualFold(filter.FilterType, "LOT_SIZE_FILTER") {
				step, stepErr := strconv.ParseFloat(filter.StepSize, 64)
				minimum, minErr := strconv.ParseFloat(filter.MinQty, 64)
				if stepErr == nil && minErr == nil && step > 0 && minimum >= 0 &&
					!math.IsNaN(step) && !math.IsInf(step, 0) &&
					!math.IsNaN(minimum) && !math.IsInf(minimum, 0) {
					amountSpec = &domain.AmountSpec{
						Unit:        "BASE",
						Step:        step,
						Min:         minimum,
						BasePerUnit: 1,
					}
				}
				break
			}
		}
		if amountSpec == nil {
			log.Printf("tokocrypto: symbol %s has no valid LOT_SIZE filter; execution disabled for market", s.Symbol)
		}
		market := domain.Market{
			Exchange:   "tokocrypto",
			Symbol:     "toko:" + s.Symbol,
			BaseAsset:  strings.ToUpper(s.BaseAsset),
			QuoteAsset: strings.ToUpper(s.QuoteAsset),
			MarketType: "SPOT",
			Active:     true,
			AmountSpec: amountSpec,
		}
		if amountSpec != nil {
			market.LotSize = amountSpec.Step
			market.MinOrder = amountSpec.Min
		}
		markets = append(markets, market)
	}

	if len(markets) > 0 {
		l.mu.Lock()
		l.cachedTokocryptoSpot = markets
		l.lastTokocryptoFetch = time.Now().UTC()
		l.mu.Unlock()
	}

	return markets, nil
}

func (l *LiveAdapter) getFallbackTokocryptoSpot(err error) ([]domain.Market, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedTokocryptoSpot) > 0 {
		return l.cachedTokocryptoSpot, nil
	}
	return nil, err
}

func (l *LiveAdapter) fetchIndodaxSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	l.mu.RLock()
	if len(l.cachedIndodaxSpot) > 0 && time.Since(l.lastIndodaxSpotFetch) < time.Hour {
		res := l.cachedIndodaxSpot
		l.mu.RUnlock()
		return res, nil
	}
	client := l.indodaxClient
	l.mu.RUnlock()
	if client == nil { return l.getFallbackIndodaxSpot(fmt.Errorf("indodax client is not configured")) }
	markets, err := client.Markets(ctx)
	if err != nil { return l.getFallbackIndodaxSpot(err) }
	if len(markets) > 0 {
		l.mu.Lock()
		l.cachedIndodaxSpot = markets
		l.lastIndodaxSpotFetch = time.Now().UTC()
		l.mu.Unlock()
	}
	return markets, nil
}

func (l *LiveAdapter) getFallbackIndodaxSpot(err error) ([]domain.Market, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedIndodaxSpot) > 0 {
		return l.cachedIndodaxSpot, nil
	}
	return nil, err
}
