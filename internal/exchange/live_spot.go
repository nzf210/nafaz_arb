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
	l.mu.RUnlock()

	endpoint := l.indodaxBaseURL + "/api/ticker/usdtidr"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("indodax usdtidr ticker req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("indodax usdtidr ticker request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("indodax usdtidr ticker status: %d", resp.StatusCode)
	}

	var raw struct {
		Ticker struct {
			Buy  string `json:"buy"`
			Sell string `json:"sell"`
			Last string `json:"last"`
		} `json:"ticker"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return 0, fmt.Errorf("decode indodax usdtidr: %w", err)
	}

	last, err := strconv.ParseFloat(raw.Ticker.Last, 64)
	if err != nil || last <= 0 {
		buy, _ := strconv.ParseFloat(raw.Ticker.Buy, 64)
		sell, _ := strconv.ParseFloat(raw.Ticker.Sell, 64)
		if buy > 0 && sell > 0 {
			last = (buy + sell) / 2
		}
	}
	if last <= 0 {
		return 0, fmt.Errorf("invalid indodax usdtidr rate: %s", raw.Ticker.Last)
	}

	l.mu.Lock()
	l.lastUSDTIDRRate = last
	l.lastUSDTIDRTime = time.Now().UTC()
	l.mu.Unlock()

	return last, nil
}

// SpotMarkets discovers only the supported local spot venues: Tokocrypto and Indodax.
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
	if len(l.cachedIndodaxSpot) > 0 && time.Since(l.lastIndodaxSpotFetch) < 1*time.Hour {
		res := l.cachedIndodaxSpot
		l.mu.RUnlock()
		return res, nil
	}
	l.mu.RUnlock()

	endpoint := l.indodaxBaseURL + "/api/pairs"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return l.getFallbackIndodaxSpot(err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return l.getFallbackIndodaxSpot(fmt.Errorf("indodax pairs request: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return l.getFallbackIndodaxSpot(fmt.Errorf("indodax pairs status: %d", resp.StatusCode))
	}

	var indodaxPairs []struct {
		ID                string  `json:"id"`
		Symbol            string  `json:"symbol"`
		BaseCurrency      string  `json:"base_currency"`   // quote currency (e.g. usdt, idr)
		TradedCurrency    string  `json:"traded_currency"` // base currency (e.g. btc, eth)
		QuantityIncrement float64 `json:"quantity_increment"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&indodaxPairs); err != nil {
		return l.getFallbackIndodaxSpot(fmt.Errorf("decode indodax pairs: %w", err))
	}

	var markets []domain.Market
	for _, p := range indodaxPairs {
		if p.ID == "" || p.TradedCurrency == "" || p.BaseCurrency == "" {
			continue
		}
		if p.QuantityIncrement <= 0 || math.IsNaN(p.QuantityIncrement) || math.IsInf(p.QuantityIncrement, 0) {
			log.Printf("indodax: skipping pair %s with missing or invalid quantity_increment", p.ID)
			continue
		}
		markets = append(markets, domain.Market{
			Exchange:   "indodax",
			Symbol:     p.ID,
			BaseAsset:  strings.ToUpper(p.TradedCurrency),
			QuoteAsset: strings.ToUpper(p.BaseCurrency),
			MarketType: "SPOT",
			Active:     true,
			LotSize:    p.QuantityIncrement,
			MinOrder:   p.QuantityIncrement,
			AmountSpec: &domain.AmountSpec{
				Unit:        "BASE",
				Step:        p.QuantityIncrement,
				Min:         p.QuantityIncrement,
				BasePerUnit: 1,
			},
		})
	}

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
