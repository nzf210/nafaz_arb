package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// Binance USD-M perpetual market discovery. Symbols are prefixed internally so
// order-book and funding requests cannot accidentally route to another venue.
func (l *LiveAdapter) fetchBinancePerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	endpoint := strings.TrimRight(l.binanceBaseURL, "/") + "/fapi/v1/exchangeInfo"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil { return nil, fmt.Errorf("binance exchangeInfo request: %w", err) }
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
	resp, err := l.client.Do(req)
	if err != nil { return nil, fmt.Errorf("binance exchangeInfo: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return nil, fmt.Errorf("binance exchangeInfo status: %d", resp.StatusCode) }

	var payload struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
			ContractType string `json:"contractType"`
			Status string `json:"status"`
			BaseAsset string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
			Filters []struct {
				FilterType string `json:"filterType"`
				TickSize string `json:"tickSize"`
				StepSize string `json:"stepSize"`
				MinQty string `json:"minQty"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil { return nil, fmt.Errorf("decode binance exchangeInfo: %w", err) }
	markets := make([]domain.Market, 0, len(payload.Symbols))
	for _, s := range payload.Symbols {
		quote := strings.ToUpper(s.QuoteAsset)
		if s.Status != "TRADING" || s.ContractType != "PERPETUAL" || s.Symbol == "" || s.BaseAsset == "" || (quote != "USDT" && quote != "USDC") { continue }
		m := domain.Market{Exchange:"binance", Symbol:"binance:"+s.Symbol, BaseAsset:strings.ToUpper(s.BaseAsset), QuoteAsset:quote, MarketType:"PERPETUAL", Active:true, ContractSize:1}
		for _, filter := range s.Filters {
			switch filter.FilterType {
			case "PRICE_FILTER": m.TickSize, _ = strconv.ParseFloat(filter.TickSize, 64)
			case "LOT_SIZE":
				m.LotSize, _ = strconv.ParseFloat(filter.StepSize, 64)
				m.MinOrder, _ = strconv.ParseFloat(filter.MinQty, 64)
			}
		}
		markets = append(markets, m)
	}
	return markets, nil
}

// Bybit V5 linear perpetual discovery. A cursor is followed until the exchange
// reports no further page, so markets after the first 500 are not silently lost.
func (l *LiveAdapter) fetchBybitPerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	markets := make([]domain.Market, 0)
	cursor := ""
	for page := 0; page < 20; page++ {
		q := url.Values{"category":{"linear"}, "limit":{"1000"}}
		if cursor != "" { q.Set("cursor", cursor) }
		endpoint := strings.TrimRight(l.bybitBaseURL, "/") + "/v5/market/instruments-info?" + q.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil { return nil, fmt.Errorf("bybit instruments request: %w", err) }
		req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
		resp, err := l.client.Do(req)
		if err != nil { return nil, fmt.Errorf("bybit instruments: %w", err) }
		var payload struct {
			RetCode int `json:"retCode"`
			RetMsg string `json:"retMsg"`
			Result struct {
				NextPageCursor string `json:"nextPageCursor"`
				List []struct {
					Symbol string `json:"symbol"`
					ContractType string `json:"contractType"`
					Status string `json:"status"`
					BaseCoin string `json:"baseCoin"`
					QuoteCoin string `json:"quoteCoin"`
					SettleCoin string `json:"settleCoin"`
					PriceFilter struct { TickSize string `json:"tickSize"` } `json:"priceFilter"`
					LotSizeFilter struct { QtyStep string `json:"qtyStep"`; MinOrderQty string `json:"minOrderQty"` } `json:"lotSizeFilter"`
				} `json:"list"`
			} `json:"result"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
		resp.Body.Close()
		if decodeErr != nil { return nil, fmt.Errorf("decode bybit instruments: %w", decodeErr) }
		if resp.StatusCode != http.StatusOK { return nil, fmt.Errorf("bybit instruments status: %d", resp.StatusCode) }
		if payload.RetCode != 0 { return nil, fmt.Errorf("bybit instruments error: %s (%d)", payload.RetMsg, payload.RetCode) }
		for _, s := range payload.Result.List {
			quote := strings.ToUpper(s.QuoteCoin)
			if s.Status != "Trading" || !strings.Contains(strings.ToLower(s.ContractType), "perpetual") || s.Symbol == "" || s.BaseCoin == "" || (quote != "USDT" && quote != "USDC") { continue }
			tick, _ := strconv.ParseFloat(s.PriceFilter.TickSize, 64)
			step, _ := strconv.ParseFloat(s.LotSizeFilter.QtyStep, 64)
			minQty, _ := strconv.ParseFloat(s.LotSizeFilter.MinOrderQty, 64)
			markets = append(markets, domain.Market{Exchange:"bybit", Symbol:"bybit:"+s.Symbol, BaseAsset:strings.ToUpper(s.BaseCoin), QuoteAsset:quote, MarketType:"PERPETUAL", Active:true, ContractSize:1, TickSize:tick, LotSize:step, MinOrder:minQty})
		}
		if payload.Result.NextPageCursor == "" || payload.Result.NextPageCursor == cursor { break }
		cursor = payload.Result.NextPageCursor
	}
	return markets, nil
}

func (l *LiveAdapter) fetchBinanceOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	raw := strings.TrimPrefix(symbol, "binance:")
	endpoint := strings.TrimRight(l.binanceBaseURL, "/") + "/fapi/v1/depth?symbol=" + url.QueryEscape(raw) + "&limit=1000"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil { return domain.OrderBook{}, err }
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
	resp, err := l.client.Do(req)
	if err != nil { return domain.OrderBook{}, err }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return domain.OrderBook{}, fmt.Errorf("binance futures depth status: %d", resp.StatusCode) }
	var payload struct { Bids [][]string `json:"bids"`; Asks [][]string `json:"asks"`; UpdateTime int64 `json:"E"` }
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil { return domain.OrderBook{}, fmt.Errorf("decode binance futures depth: %w", err) }
	ts := time.Now().UTC()
	if payload.UpdateTime > 0 { ts = time.UnixMilli(payload.UpdateTime).UTC() }
	return domain.OrderBook{Exchange:"binance", Symbol:symbol, Bids:parseStringLevels(payload.Bids,1), Asks:parseStringLevels(payload.Asks,1), Timestamp:ts}, nil
}

func (l *LiveAdapter) fetchBybitOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	raw := strings.TrimPrefix(symbol, "bybit:")
	q := url.Values{"category":{"linear"}, "symbol":{raw}, "limit":{"200"}}
	endpoint := strings.TrimRight(l.bybitBaseURL, "/") + "/v5/market/orderbook?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil { return domain.OrderBook{}, err }
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
	resp, err := l.client.Do(req)
	if err != nil { return domain.OrderBook{}, err }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return domain.OrderBook{}, fmt.Errorf("bybit futures orderbook status: %d", resp.StatusCode) }
	var payload struct {
		RetCode int `json:"retCode"`; RetMsg string `json:"retMsg"`
		Result struct { Bids [][]string `json:"b"`; Asks [][]string `json:"a"`; Ts int64 `json:"ts"` } `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil { return domain.OrderBook{}, fmt.Errorf("decode bybit orderbook: %w", err) }
	if payload.RetCode != 0 { return domain.OrderBook{}, fmt.Errorf("bybit orderbook error: %s (%d)", payload.RetMsg, payload.RetCode) }
	ts := time.Now().UTC()
	if payload.Result.Ts > 0 { ts = time.UnixMilli(payload.Result.Ts).UTC() }
	return domain.OrderBook{Exchange:"bybit", Symbol:symbol, Bids:parseStringLevels(payload.Result.Bids,1), Asks:parseStringLevels(payload.Result.Asks,1), Timestamp:ts}, nil
}

func (l *LiveAdapter) fetchBinanceFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	raw := strings.TrimPrefix(symbol, "binance:")
	endpoint := strings.TrimRight(l.binanceBaseURL, "/") + "/fapi/v1/premiumIndex?symbol=" + url.QueryEscape(raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil { return domain.FundingRate{}, err }
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
	resp, err := l.client.Do(req)
	if err != nil { return domain.FundingRate{}, err }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return domain.FundingRate{}, fmt.Errorf("binance premiumIndex status: %d", resp.StatusCode) }
	var payload struct { LastFundingRate string `json:"lastFundingRate"`; NextFundingTime int64 `json:"nextFundingTime"`; Time int64 `json:"time"` }
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil { return domain.FundingRate{}, err }
	rate, err := strconv.ParseFloat(payload.LastFundingRate,64)
	if err != nil { return domain.FundingRate{}, fmt.Errorf("invalid binance funding rate: %w",err) }
	ts := time.Now().UTC()
	if payload.Time > 0 { ts = time.UnixMilli(payload.Time).UTC() }
	var next time.Time
	if payload.NextFundingTime > 0 { next = time.UnixMilli(payload.NextFundingTime).UTC() }
	return domain.FundingRate{Rate:rate, IntervalHours:8, Timestamp:ts, NextFundingTime:next},nil
}

func (l *LiveAdapter) fetchBybitFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	raw := strings.TrimPrefix(symbol, "bybit:")
	q := url.Values{"category":{"linear"}, "symbol":{raw}}
	endpoint := strings.TrimRight(l.bybitBaseURL, "/") + "/v5/market/tickers?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil { return domain.FundingRate{}, err }
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
	resp, err := l.client.Do(req)
	if err != nil { return domain.FundingRate{}, err }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return domain.FundingRate{}, fmt.Errorf("bybit tickers status: %d", resp.StatusCode) }
	var payload struct {
		RetCode int `json:"retCode"`; RetMsg string `json:"retMsg"`
		Result struct { List []struct { FundingRate string `json:"fundingRate"`; NextFundingTime string `json:"nextFundingTime"`; FundingIntervalHour string `json:"fundingIntervalHour"` } `json:"list"` } `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil { return domain.FundingRate{}, err }
	if payload.RetCode != 0 || len(payload.Result.List)==0 { return domain.FundingRate{}, fmt.Errorf("bybit ticker error: %s (%d)",payload.RetMsg,payload.RetCode) }
	item := payload.Result.List[0]
	rate, err := strconv.ParseFloat(item.FundingRate,64)
	if err != nil { return domain.FundingRate{}, fmt.Errorf("invalid bybit funding rate: %w",err) }
	interval := 8
	if n, e := strconv.Atoi(item.FundingIntervalHour); e == nil && n > 0 { interval = n }
	ts := time.Now().UTC()
	var next time.Time
	if ms, e := strconv.ParseInt(item.NextFundingTime,10,64); e == nil && ms > 0 { next = time.UnixMilli(ms).UTC() }
	return domain.FundingRate{Rate:rate, IntervalHours:interval, Timestamp:ts, NextFundingTime:next},nil
}
