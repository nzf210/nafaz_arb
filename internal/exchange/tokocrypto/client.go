package tokocrypto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const defaultBaseURL = "https://www.tokocrypto.com"
const defaultMBXDepthURL = "https://www.tokocrypto.site"
const defaultNextMeDepthURL = "https://cloudme-toko.2meta.app"

type StatusError struct { StatusCode int }
func (e *StatusError) Error() string { return fmt.Sprintf("tokocrypto HTTP status: %d", e.StatusCode) }
func IsUnavailable(err error) bool {
	var statusErr *StatusError
	if !errors.As(err, &statusErr) { return false }
	return statusErr.StatusCode == http.StatusBadGateway || statusErr.StatusCode == http.StatusServiceUnavailable || statusErr.StatusCode == http.StatusGatewayTimeout
}

// Client owns Tokocrypto public spot market-data behavior.
type Client struct {
	httpClient *http.Client
	baseURL string
	mbxDepthURL string
	nextMeDepthURL string
	symbolMu sync.RWMutex
	symbolTypes map[string]int
}

func New(httpClient *http.Client, baseURL string) *Client {
	if httpClient == nil { httpClient = http.DefaultClient }
	if strings.TrimSpace(baseURL) == "" { baseURL = defaultBaseURL }
	baseURL = strings.TrimRight(baseURL, "/")
	depthMBX, depthNextMe := defaultMBXDepthURL, defaultNextMeDepthURL
	// A custom base URL is used by httptest fixtures; keep depth paths on it.
	if baseURL != defaultBaseURL {
		depthMBX, depthNextMe = strings.TrimRight(baseURL, "/"), strings.TrimRight(baseURL, "/")
	}
	return &Client{httpClient: httpClient, baseURL: strings.TrimRight(baseURL, "/"),
		mbxDepthURL: depthMBX, nextMeDepthURL: depthNextMe, symbolTypes: make(map[string]int)}
}

func (c *Client) request(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil { return nil, err }
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; NafazScanner/1.0)")
	return c.httpClient.Do(req)
}

func (c *Client) Markets(ctx context.Context) ([]domain.Market, error) {
	resp, err := c.request(ctx, "/open/v1/common/symbols")
	if err != nil { return nil, fmt.Errorf("tokocrypto symbols request: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return nil, &StatusError{StatusCode: resp.StatusCode} }
	var raw struct {
		Code int `json:"code"`
		Data struct {
			List []struct {
				Symbol string `json:"symbol"`
				SymbolType int `json:"symbolType"`
				BaseAsset string `json:"baseAsset"`
				QuoteAsset string `json:"quoteAsset"`
				SpotTradingEnable int `json:"spotTradingEnable"`
				Filters []struct {
					FilterType string `json:"filterType"`
					StepSize string `json:"stepSize"`
					MinQty string `json:"minQty"`
				} `json:"filters"`
			} `json:"list"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil { return nil, fmt.Errorf("decode tokocrypto symbols: %w", err) }
	markets := make([]domain.Market, 0, len(raw.Data.List))
	symbolTypes := make(map[string]int, len(raw.Data.List))
	for _, item := range raw.Data.List {
		if item.SpotTradingEnable != 1 || item.Symbol == "" || item.BaseAsset == "" || item.QuoteAsset == "" { continue }
		symbolType := item.SymbolType
		if symbolType == 0 { symbolType = 1 } // Older payloads are MBX.
		symbolTypes[item.Symbol] = symbolType
		if symbolType != 1 && symbolType != 3 { continue }
		var amount *domain.AmountSpec
		for _, filter := range item.Filters {
			if !strings.EqualFold(filter.FilterType, "LOT_SIZE") && !strings.EqualFold(filter.FilterType, "LOT_SIZE_FILTER") { continue }
			step, stepErr := strconv.ParseFloat(filter.StepSize, 64)
			minimum, minErr := strconv.ParseFloat(filter.MinQty, 64)
			if stepErr == nil && minErr == nil && step > 0 && minimum >= 0 &&
				!math.IsNaN(step) && !math.IsInf(step, 0) && !math.IsNaN(minimum) && !math.IsInf(minimum, 0) {
				amount = &domain.AmountSpec{Unit: "BASE", Step: step, Min: minimum, BasePerUnit: 1}
			}
			break
		}
		market := domain.Market{Exchange: "tokocrypto", Symbol: "toko:"+item.Symbol,
			BaseAsset: strings.ToUpper(item.BaseAsset), QuoteAsset: strings.ToUpper(item.QuoteAsset),
			MarketType: "SPOT", SymbolType: symbolType, Active: true, AmountSpec: amount}
		if amount != nil { market.LotSize, market.MinOrder = amount.Step, amount.Min }
		markets = append(markets, market)
	}
	c.symbolMu.Lock()
	c.symbolTypes = symbolTypes
	c.symbolMu.Unlock()
	return markets, nil
}

func (c *Client) OrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSymbol := strings.TrimPrefix(symbol, "toko:")
	c.symbolMu.RLock()
	symbolType := c.symbolTypes[rawSymbol]
	c.symbolMu.RUnlock()
	if symbolType == 0 { symbolType = 1 }
	depthSymbol := strings.ReplaceAll(rawSymbol, "_", "")
	baseURL, path := c.mbxDepthURL, "/api/v3/depth?symbol="+url.QueryEscape(depthSymbol)+"&limit=20"
	if symbolType == 3 {
		baseURL = c.nextMeDepthURL
		path = "/api/v1/depth?symbol="+url.QueryEscape(depthSymbol)+"&limit=20"
	} else if symbolType != 1 {
		return domain.OrderBook{}, fmt.Errorf("unsupported Tokocrypto symbolType %d for %s", symbolType, rawSymbol)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil { return domain.OrderBook{}, fmt.Errorf("build tokocrypto depth request: %w", err) }
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; NafazScanner/1.0)")
	resp, err := c.httpClient.Do(req)
	if err != nil { return domain.OrderBook{}, fmt.Errorf("tokocrypto depth request: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return domain.OrderBook{}, &StatusError{StatusCode: resp.StatusCode} }
	var raw struct {
		LastUpdateID int64 `json:"lastUpdateId"`
		Timestamp int64 `json:"timestamp"`
		EventTime int64 `json:"E"`
		TradeTime int64 `json:"T"`
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
		Data *struct {
			LastUpdateID int64 `json:"lastUpdateId"`
			Timestamp int64 `json:"timestamp"`
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil { return domain.OrderBook{}, fmt.Errorf("decode tokocrypto depth: %w", err) }
	if raw.Data != nil {
		raw.LastUpdateID, raw.Bids, raw.Asks = raw.Data.LastUpdateID, raw.Data.Bids, raw.Data.Asks
		if raw.Data.Timestamp > 0 { raw.Timestamp = raw.Data.Timestamp }
	}
	if raw.LastUpdateID <= 0 || len(raw.Bids) == 0 || len(raw.Asks) == 0 {
		return domain.OrderBook{}, fmt.Errorf("tokocrypto depth snapshot is incomplete for %s", rawSymbol)
	}
	timestamp := raw.Timestamp
	if timestamp <= 0 { timestamp = raw.EventTime }
	if timestamp <= 0 { timestamp = raw.TradeTime }
	at := time.Now().UTC()
	if timestamp > 0 { at = time.UnixMilli(timestamp).UTC() }
	return domain.OrderBook{Exchange:"tokocrypto", Symbol:symbol,
		Bids: parseLevels(raw.Bids), Asks: parseLevels(raw.Asks),
		SequenceID: raw.LastUpdateID, Timestamp:at, ReceivedAt:time.Now().UTC(), Source:"rest"}, nil
}

func parseLevels(rows [][]string) []domain.PriceLevel {
	levels := make([]domain.PriceLevel, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 { continue }
		price, pErr := strconv.ParseFloat(row[0], 64)
		quantity, qErr := strconv.ParseFloat(row[1], 64)
		if pErr != nil || qErr != nil || price <= 0 || quantity <= 0 ||
			math.IsNaN(price) || math.IsInf(price, 0) || math.IsNaN(quantity) || math.IsInf(quantity, 0) { continue }
		levels = append(levels, domain.PriceLevel{Price:price, Quantity:quantity})
	}
	return levels
}
