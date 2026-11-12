package indodax

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	defaultBaseURL = "https://indodax.com"
	userAgent = "Mozilla/5.0 (compatible; NafazScanner/1.0)"
)

// Client owns Indodax-specific HTTP endpoints and payload normalization.
type Client struct {
	httpClient *http.Client
	baseURL string
}

func New(httpClient *http.Client, baseURL string) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL
	}
	return &Client{httpClient: httpClient, baseURL: strings.TrimRight(baseURL, "/")}
}

func (c *Client) request(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil { return nil, err }
	req.Header.Set("User-Agent", userAgent)
	return c.httpClient.Do(req)
}

func (c *Client) Markets(ctx context.Context) ([]domain.Market, error) {
	resp, err := c.request(ctx, "/api/pairs")
	if err != nil { return nil, fmt.Errorf("indodax pairs request: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return nil, fmt.Errorf("indodax pairs status: %d", resp.StatusCode) }

	var pairs []struct {
		ID string `json:"id"`
		BaseCurrency string `json:"base_currency"`
		TradedCurrency string `json:"traded_currency"`
		QuantityIncrement float64 `json:"quantity_increment"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pairs); err != nil {
		return nil, fmt.Errorf("decode indodax pairs: %w", err)
	}
	markets := make([]domain.Market, 0, len(pairs))
	for _, pair := range pairs {
		if pair.ID == "" || pair.TradedCurrency == "" || pair.BaseCurrency == "" { continue }
		if pair.QuantityIncrement <= 0 || math.IsNaN(pair.QuantityIncrement) || math.IsInf(pair.QuantityIncrement, 0) { continue }
		markets = append(markets, domain.Market{
			Exchange: "indodax", Symbol: pair.ID,
			BaseAsset: strings.ToUpper(pair.TradedCurrency),
			QuoteAsset: strings.ToUpper(pair.BaseCurrency),
			MarketType: "SPOT", Active: true,
			LotSize: pair.QuantityIncrement, MinOrder: pair.QuantityIncrement,
			AmountSpec: &domain.AmountSpec{Unit: "BASE", Step: pair.QuantityIncrement, Min: pair.QuantityIncrement, BasePerUnit: 1},
		})
	}
	return markets, nil
}

func (c *Client) OrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	pair := strings.ToLower(strings.TrimSpace(symbol))
	resp, err := c.request(ctx, "/api/depth/"+url.PathEscape(pair))
	if err != nil { return domain.OrderBook{}, fmt.Errorf("indodax depth request: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return domain.OrderBook{}, fmt.Errorf("indodax depth status: %d", resp.StatusCode) }

	var raw struct {
		Buy [][]any `json:"buy"`
		Sell [][]any `json:"sell"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode indodax depth: %w", err)
	}
	now := time.Now().UTC()
	return domain.OrderBook{
		Exchange: "indodax", Symbol: symbol,
		Bids: parseLevels(raw.Buy), Asks: parseLevels(raw.Sell),
		Timestamp: now, ReceivedAt: now, Source: "receive_time",
	}, nil
}

func parseLevels(rows [][]any) []domain.PriceLevel {
	levels := make([]domain.PriceLevel, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 { continue }
		price, priceOK := number(row[0])
		quantity, quantityOK := number(row[1])
		if !priceOK || !quantityOK || price <= 0 || quantity <= 0 { continue }
		levels = append(levels, domain.PriceLevel{Price: price, Quantity: quantity})
	}
	return levels
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case string:
		parsed, err := strconv.ParseFloat(typed, 64)
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	default:
		return 0, false
	}
}

func (c *Client) USDTIDRRate(ctx context.Context) (float64, error) {
	resp, err := c.request(ctx, "/api/ticker/usdtidr")
	if err != nil { return 0, fmt.Errorf("indodax usdtidr ticker request: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return 0, fmt.Errorf("indodax usdtidr ticker status: %d", resp.StatusCode) }
	var raw struct {
		Ticker struct {
			Buy string `json:"buy"`
			Sell string `json:"sell"`
			Last string `json:"last"`
		} `json:"ticker"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil { return 0, fmt.Errorf("decode indodax usdtidr: %w", err) }
	last, err := strconv.ParseFloat(raw.Ticker.Last, 64)
	if err != nil || last <= 0 {
		buy, _ := strconv.ParseFloat(raw.Ticker.Buy, 64)
		sell, _ := strconv.ParseFloat(raw.Ticker.Sell, 64)
		if buy > 0 && sell > 0 { last = (buy + sell) / 2 }
	}
	if last <= 0 { return 0, fmt.Errorf("invalid indodax usdtidr rate: %s", raw.Ticker.Last) }
	return last, nil
}
