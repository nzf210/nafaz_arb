package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FundingInfo struct {
	Symbol               string    `json:"symbol"`
	LastFundingRate      float64   `json:"last_funding_rate"`
	NextFundingTime      time.Time `json:"next_funding_time"`
	NextFundingTimeMS    int64     `json:"next_funding_time_ms"`
	MinutesToNextFunding float64   `json:"minutes_to_next_funding"`
	LastUpdated          time.Time `json:"last_updated"`
}

type binancePremiumIndexItem struct {
	Symbol          string `json:"symbol"`
	LastFundingRate string `json:"lastFundingRate"`
	NextFundingTime int64  `json:"nextFundingTime"`
	Time            int64  `json:"time"`
}

type FundingWorker struct {
	mu           sync.RWMutex
	cache        map[string]FundingInfo
	client       *http.Client
	baseURL      string
	interval     time.Duration
	stopCh       chan struct{}
	running      bool
	fetchFn      func(ctx context.Context) ([]FundingInfo, error)
	onNegativeFn func(symbol string, info FundingInfo)
}

func NewFundingWorker(intervalSeconds int, baseURL string) *FundingWorker {
	if intervalSeconds < 10 {
		intervalSeconds = 30
	}
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	return &FundingWorker{
		cache:    make(map[string]FundingInfo),
		client:   &http.Client{Timeout: 15 * time.Second},
		baseURL:  strings.TrimRight(baseURL, "/"),
		interval: time.Duration(intervalSeconds) * time.Second,
		stopCh:   make(chan struct{}),
	}
}

// SetFetchFunc allows overriding the HTTP fetcher (useful for testing or other exchanges).
func (w *FundingWorker) SetFetchFunc(fn func(ctx context.Context) ([]FundingInfo, error)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fetchFn = fn
}

// SetOnNegativeCallback sets a callback invoked when a negative funding rate is observed.
func (w *FundingWorker) SetOnNegativeCallback(fn func(symbol string, info FundingInfo)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onNegativeFn = fn
}

// SetFundingInfo directly sets funding info in cache (useful for tests or priming).
func (w *FundingWorker) SetFundingInfo(info FundingInfo) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.setFundingInfoLocked(info)
}

func normalizeFundingSymbol(sym string) string {
	s := strings.ToUpper(strings.TrimSpace(sym))
	s = strings.TrimPrefix(s, "MEXC:")
	s = strings.TrimPrefix(s, "OKX:")
	s = strings.TrimPrefix(s, "BINANCE:")
	s = strings.TrimPrefix(s, "BYBIT:")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "SWAP", "")
	return s
}

func (w *FundingWorker) setFundingInfoLocked(info FundingInfo) {
	now := time.Now().UTC()
	if !info.NextFundingTime.IsZero() {
		info.MinutesToNextFunding = info.NextFundingTime.Sub(now).Minutes()
		info.NextFundingTimeMS = info.NextFundingTime.UnixMilli()
	}
	info.LastUpdated = now

	sym := strings.ToUpper(strings.TrimSpace(info.Symbol))
	w.cache[sym] = info

	// Also index normalized variants for cross-exchange symbol mapping (e.g., BTCUSDT, BTC-USDT-SWAP, mexc:BTC_USDT)
	norm := normalizeFundingSymbol(sym)
	if norm != "" && norm != sym {
		w.cache[norm] = info
	}
}

// GetFundingInfo returns the latest cached FundingInfo for the given symbol.
func (w *FundingWorker) GetFundingInfo(symbol string) (FundingInfo, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	sym := strings.ToUpper(strings.TrimSpace(symbol))
	info, ok := w.cache[sym]
	if !ok {
		norm := normalizeFundingSymbol(sym)
		info, ok = w.cache[norm]
	}
	if !ok {
		return FundingInfo{}, false
	}

	// Recompute fresh MinutesToNextFunding dynamically
	now := time.Now().UTC()
	if !info.NextFundingTime.IsZero() {
		next := info.NextFundingTime
		for next.Before(now) {
			next = next.Add(8 * time.Hour)
		}
		info.NextFundingTime = next
		info.NextFundingTimeMS = next.UnixMilli()
		info.MinutesToNextFunding = next.Sub(now).Minutes()
	}
	return info, true
}

// PollOnce executes a single polling cycle.
func (w *FundingWorker) PollOnce(ctx context.Context) error {
	var items []FundingInfo
	var err error

	w.mu.RLock()
	fetchFn := w.fetchFn
	w.mu.RUnlock()

	if fetchFn != nil {
		items, err = fetchFn(ctx)
	} else {
		items, err = w.fetchBinancePremiumIndex(ctx)
	}

	if err != nil {
		return err
	}

	w.mu.Lock()
	var negativeItems []FundingInfo
	for _, item := range items {
		w.setFundingInfoLocked(item)
		if item.LastFundingRate < 0 {
			negativeItems = append(negativeItems, item)
		}
	}
	onNeg := w.onNegativeFn
	w.mu.Unlock()

	if onNeg != nil {
		for _, neg := range negativeItems {
			onNeg(neg.Symbol, neg)
		}
	}
	return nil
}

func (w *FundingWorker) fetchBinancePremiumIndex(ctx context.Context) ([]FundingInfo, error) {
	endpoint := w.baseURL + "/fapi/v1/premiumIndex"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("binance premiumIndex req: %w", err)
	}
	req.Header.Set("User-Agent", "NafazScanner/1.0")

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance premiumIndex do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance premiumIndex status: %d", resp.StatusCode)
	}

	var rawItems []binancePremiumIndexItem
	if err := json.NewDecoder(resp.Body).Decode(&rawItems); err != nil {
		return nil, fmt.Errorf("binance premiumIndex decode: %w", err)
	}

	out := make([]FundingInfo, 0, len(rawItems))
	now := time.Now().UTC()
	for _, raw := range rawItems {
		rate, err := strconv.ParseFloat(raw.LastFundingRate, 64)
		if err != nil {
			continue
		}
		var nextTime time.Time
		if raw.NextFundingTime > 0 {
			nextTime = time.UnixMilli(raw.NextFundingTime).UTC()
		}
		out = append(out, FundingInfo{
			Symbol:          raw.Symbol,
			LastFundingRate: rate,
			NextFundingTime: nextTime,
			LastUpdated:     now,
		})
	}
	return out, nil
}

// Start begins the background polling loop.
func (w *FundingWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	// Initial poll immediately
	_ = w.PollOnce(ctx)

	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stopCh:
				return
			case <-ticker.C:
				_ = w.PollOnce(ctx)
			}
		}
	}()
}

// Stop terminates the background polling loop.
func (w *FundingWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.running {
		return
	}
	w.running = false
	close(w.stopCh)
}
