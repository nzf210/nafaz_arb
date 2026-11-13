package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// USDTIDRRate retrieves the current live USDT/IDR exchange rate from Indodax WS/ticker
// with singleflight deduping and live Tokocrypto USDT_IDR depth fallback.
func (l *LiveAdapter) USDTIDRRate(ctx context.Context) (float64, error) {
	l.mu.RLock()
	indodaxWS := l.indodaxBooks
	indodaxMaxAge := l.indodaxWSMaxAge
	if indodaxMaxAge <= 0 {
		indodaxMaxAge = 60 * time.Second
	}
	if l.lastUSDTIDRRate > 0 && time.Since(l.lastUSDTIDRTime) < 60*time.Second {
		rate := l.lastUSDTIDRRate
		l.mu.RUnlock()
		return rate, nil
	}
	l.mu.RUnlock()

	if indodaxWS != nil {
		if rate, ok := indodaxWS.USDTIDRRate(indodaxMaxAge); ok && rate > 0 {
			return rate, nil
		}
	}

	l.usdtIDRMu.Lock()
	if flight := l.usdtIDRFlight; flight != nil {
		l.usdtIDRMu.Unlock()
		select {
		case <-flight.done:
			return flight.rate, flight.err
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	flight := &usdtIDRCall{done: make(chan struct{})}
	l.usdtIDRFlight = flight
	l.usdtIDRMu.Unlock()

	rate, err := l.resolveLiveUSDTIDRRate(ctx)
	flight.rate = rate
	flight.err = err
	close(flight.done)

	l.usdtIDRMu.Lock()
	l.usdtIDRFlight = nil
	l.usdtIDRMu.Unlock()
	return rate, err
}

func (l *LiveAdapter) resolveLiveUSDTIDRRate(ctx context.Context) (float64, error) {
	started := time.Now()
	rate, indodaxErr := l.indodaxVenue().FetchUSDTIDRRate(ctx)
	l.recordScanMetric("indodax", started, indodaxErr)
	if indodaxErr == nil && rate > 0 {
		l.mu.Lock()
		l.lastUSDTIDRRate = rate
		l.lastUSDTIDRTime = time.Now().UTC()
		l.mu.Unlock()
		return rate, nil
	}

	l.recordScanError(fmt.Sprintf("indodax usdtidr unavailable (%v); trying tokocrypto USDT_IDR", indodaxErr))
	tokoStarted := time.Now()
	tokoRate, tokoErr := l.tokocryptoVenue().FetchUSDTIDRRate(ctx)
	l.recordScanMetric("tokocrypto", tokoStarted, tokoErr)
	if tokoErr == nil && tokoRate > 0 {
		l.mu.Lock()
		l.lastUSDTIDRRate = tokoRate
		l.lastUSDTIDRTime = time.Now().UTC()
		l.mu.Unlock()
		return tokoRate, nil
	}

	return 0, fmt.Errorf("usdtidr unavailable: indodax=(%v); tokocrypto=(%v)", indodaxErr, tokoErr)
}

func (l *LiveAdapter) fetchIndodaxUSDTIDRRate(ctx context.Context) (float64, error) {
	endpoint := l.indodaxBaseURL + "/api/ticker/usdtidr"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("indodax usdtidr ticker req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.indodaxDo(ctx, req)
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
	return last, nil
}

func (l *LiveAdapter) fetchTokocryptoUSDTIDRRate(ctx context.Context) (float64, error) {
	book, err := l.fetchTokocryptoSpotOrderBook(ctx, "toko:USDT_IDR")
	if err != nil { return 0, fmt.Errorf("tokocrypto usdtidr order book: %w", err) }
	if len(book.Bids) == 0 || len(book.Asks) == 0 || book.Bids[0].Price <= 0 || book.Asks[0].Price <= book.Bids[0].Price {
		return 0, fmt.Errorf("tokocrypto usdtidr book invalid or one-sided")
	}
	rate := (book.Bids[0].Price + book.Asks[0].Price) / 2
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 { return 0, fmt.Errorf("tokocrypto usdtidr midpoint is invalid") }
	return rate, nil
}

// SpotMarkets discovers active spot pairs on Tokocrypto, Indodax, and MEXC Spot.
func (l *LiveAdapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	var allMarkets []domain.Market
	var errs []string

	tokoStart := time.Now()
	tokoMarkets, err := l.tokocryptoVenue().FetchSpotMarkets(ctx)
	l.recordScanMetric("tokocrypto", tokoStart, err)
	if err == nil {
		allMarkets = append(allMarkets, tokoMarkets...)
	} else {
		msg := fmt.Sprintf("tokocrypto spot discovery: %v", err)
		errs = append(errs, msg)
		l.recordScanError(msg)
	}

	indodaxStart := time.Now()
	indodaxMarkets, indodaxErr, usedIndodaxFallback := l.fetchIndodaxSpotMarketsWithStatus(ctx)
	l.recordScanMetric("indodax", indodaxStart, indodaxErr)
	if len(indodaxMarkets) > 0 {
		allMarkets = append(allMarkets, indodaxMarkets...)
		if usedIndodaxFallback && indodaxErr != nil {
			l.recordScanError(fmt.Sprintf("indodax spot discovery (%v; using cached fallback)", indodaxErr))
		}
	} else if indodaxErr != nil {
		msg := fmt.Sprintf("indodax spot discovery: %v", indodaxErr)
		errs = append(errs, msg)
		l.recordScanError(msg)
	}

	if strings.TrimSpace(l.mexcSpotBaseURL) != "" {
		mexcStart := time.Now()
		mexcMarkets, err := l.mexcVenue().FetchSpotMarkets(ctx)
		l.recordScanMetric("mexc", mexcStart, err)
		if err == nil {
			allMarkets = append(allMarkets, mexcMarkets...)
		} else {
			msg := fmt.Sprintf("mexc spot discovery: %v", err)
			errs = append(errs, msg)
			l.recordScanError(msg)
		}
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

	resp, err := l.tokocryptoDo(ctx, req)
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
				Type              int    `json:"type"`
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

	if raw.Code != 0 { return l.getFallbackTokocryptoSpot(fmt.Errorf("tokocrypto symbols API error code: %d", raw.Code)) }
	var markets []domain.Market
	for _, s := range raw.Data.List {
		if s.SpotTradingEnable != 1 || s.Symbol == "" || s.BaseAsset == "" || s.QuoteAsset == "" { continue }
		if s.Type != 1 && s.Type != 3 { log.Printf("tokocrypto: skipping symbol %s with unsupported symbol type %d", s.Symbol, s.Type); continue }
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
			SymbolType: s.Type,
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
	markets, err, _ := l.fetchIndodaxSpotMarketsWithStatus(ctx)
	return markets, err
}

func (l *LiveAdapter) fetchIndodaxSpotMarketsWithStatus(ctx context.Context) ([]domain.Market, error, bool) {
	l.mu.RLock()
	if len(l.cachedIndodaxSpot) > 0 && time.Since(l.lastIndodaxSpotFetch) < 1*time.Hour {
		res := l.cachedIndodaxSpot
		l.mu.RUnlock()
		return res, nil, false
	}
	l.mu.RUnlock()

	endpoint := l.indodaxBaseURL + "/api/pairs"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		fb, fbErr := l.getFallbackIndodaxSpot(err)
		return fb, err, fbErr == nil && len(fb) > 0
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.indodaxDo(ctx, req)
	if err != nil {
		wrapped := fmt.Errorf("indodax pairs request: %w", err)
		fb, fbErr := l.getFallbackIndodaxSpot(wrapped)
		return fb, wrapped, fbErr == nil && len(fb) > 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		statusErr := fmt.Errorf("indodax pairs status: %d", resp.StatusCode)
		fb, fbErr := l.getFallbackIndodaxSpot(statusErr)
		return fb, statusErr, fbErr == nil && len(fb) > 0
	}

	var indodaxPairs []struct {
		ID                string  `json:"id"`
		Symbol            string  `json:"symbol"`
		BaseCurrency      string  `json:"base_currency"`   // quote currency (e.g. usdt, idr)
		TradedCurrency    string  `json:"traded_currency"` // base currency (e.g. btc, eth)
		QuantityIncrement float64 `json:"quantity_increment"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&indodaxPairs); err != nil {
		decErr := fmt.Errorf("decode indodax pairs: %w", err)
		fb, fbErr := l.getFallbackIndodaxSpot(decErr)
		return fb, decErr, fbErr == nil && len(fb) > 0
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
		cacheDir := l.marketCacheDir
		l.mu.Unlock()
		if cacheDir != "" {
			l.saveIndodaxSpotCache(cacheDir, markets)
		}
	}

	return markets, nil, false
}

func (l *LiveAdapter) getFallbackIndodaxSpot(err error) ([]domain.Market, error) {
	l.mu.RLock()
	cached := l.cachedIndodaxSpot
	cacheDir := l.marketCacheDir
	l.mu.RUnlock()

	if len(cached) > 0 {
		return cached, nil
	}

	searchDirs := []string{cacheDir, "/app/cache", "cache", "/app/data/scanner/cache", "/app/data/cache"}
	for _, dir := range searchDirs {
		if dir == "" {
			continue
		}
		if loaded := l.loadIndodaxSpotCache(dir); len(loaded) > 0 {
			l.mu.Lock()
			l.cachedIndodaxSpot = loaded
			l.mu.Unlock()
			log.Printf("indodax: loaded %d spot markets from fallback disk cache (%s)", len(loaded), dir)
			return loaded, nil
		}
	}

	return nil, err
}

func (l *LiveAdapter) saveIndodaxSpotCache(dir string, markets []domain.Market) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	data, err := json.Marshal(markets)
	if err != nil {
		return
	}
	path := filepath.Join(dir, "indodax_spot_markets.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

func (l *LiveAdapter) loadIndodaxSpotCache(dir string) []domain.Market {
	path := filepath.Join(dir, "indodax_spot_markets.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var markets []domain.Market
	if err := json.Unmarshal(data, &markets); err != nil || len(markets) == 0 {
		return nil
	}
	return markets
}

func (l *LiveAdapter) fetchMEXCSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	l.mu.RLock()
	if len(l.cachedMEXCSpot) > 0 && time.Since(l.lastMEXCSpotFetch) < 1*time.Hour {
		res := l.cachedMEXCSpot
		l.mu.RUnlock()
		return res, nil
	}
	l.mu.RUnlock()

	endpoint := l.mexcSpotBaseURL + "/api/v3/defaultSymbols"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return l.getFallbackMEXCSpot(fmt.Errorf("mexc spot symbols req: %w", err))
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.mexcSpotDo(ctx, req)
	if err != nil {
		return l.getFallbackMEXCSpot(fmt.Errorf("mexc spot symbols request: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return l.getFallbackMEXCSpot(fmt.Errorf("mexc spot symbols status: %d", resp.StatusCode))
	}

	var raw struct {
		Code int      `json:"code"`
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return l.getFallbackMEXCSpot(fmt.Errorf("decode mexc spot symbols: %w", err))
	}

	var markets []domain.Market
	for _, sym := range raw.Data {
		var base, quote string
		switch {
		case strings.HasSuffix(sym, "USDT"):
			quote = "USDT"
			base = strings.TrimSuffix(sym, "USDT")
		case strings.HasSuffix(sym, "USDC"):
			quote = "USDC"
			base = strings.TrimSuffix(sym, "USDC")
		default:
			continue
		}
		if base == "" {
			continue
		}
		markets = append(markets, domain.Market{
			Exchange:   "mexc",
			Symbol:     MEXC_SPOT + sym,
			BaseAsset:  strings.ToUpper(base),
			QuoteAsset: strings.ToUpper(quote),
			MarketType: "SPOT",
			Active:     true,
		})
	}
	if len(markets) > 0 {
		l.mu.Lock()
		l.cachedMEXCSpot = markets
		l.lastMEXCSpotFetch = time.Now().UTC()
		l.mu.Unlock()
	}
	return markets, nil
}

func (l *LiveAdapter) getFallbackMEXCSpot(err error) ([]domain.Market, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedMEXCSpot) > 0 {
		return l.cachedMEXCSpot, nil
	}
	return nil, err
}
