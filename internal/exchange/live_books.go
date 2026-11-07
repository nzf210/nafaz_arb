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

// trackedBook runs an order-book fetch and records its outcome for maintenance
// detection. HTTP error codes are logged so an operator can tell a venue outage
// (503/502/504) from a broken symbol.
func (l *LiveAdapter) trackedBook(exchangeName string, fetch func() (domain.OrderBook, error)) (domain.OrderBook, error) {
	return l.trackedBookForSymbol(exchangeName, "", fetch)
}

// trackedBookForSymbol is trackedBook plus a websocket cache lookup. A fresh
// realtime book short-circuits the REST fetch (and counts as a WS hit); a miss
// falls through to REST, so enabling the feed can never reduce coverage.
func (l *LiveAdapter) trackedBookForSymbol(exchangeName, symbol string, fetch func() (domain.OrderBook, error)) (domain.OrderBook, error) {
	if !l.ExchangeAvailable(exchangeName) {
		return domain.OrderBook{}, fmt.Errorf("%w: %s", ErrExchangeUnavailable, exchangeName)
	}
	if symbol != "" {
		l.mu.RLock()
		cache, maxAge := l.wsCache, l.wsMaxAge
		l.mu.RUnlock()
		if cache != nil {
			book, ok, isDuplicate, isOutOfOrder, isGap, _ := cache.Get(exchangeName, symbol, maxAge)
			if !ok {
				// Cache miss (including stale books) - fall through to REST fetch
			} else if isDuplicate {
				// Accept duplicate book (same sequence ID) - return cached book
				return book, nil
			} else if isOutOfOrder || isGap {
				// Reject out-of-order or gap book - fall through to REST fetch
			} else {
				// Valid cached book - return it
				return book, nil
			}
		}
	}
	book, err := fetch()
	l.recordOutcome(exchangeName, err)
	return book, err
}

// SpotOrderBook fetches local spot depth only. Foreign CEX spot access is deliberately rejected.
func (l *LiveAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if strings.HasPrefix(symbol, MEXC_SPOT) || strings.HasPrefix(symbol, "mexc:") || strings.HasPrefix(symbol, "binance:") || strings.HasPrefix(symbol, "bybit:") {
		return domain.OrderBook{}, fmt.Errorf("spot market is not allowed for foreign futures venue symbol %q", symbol)
	}
	if strings.HasPrefix(symbol, "toko:") {
		return l.trackedBookForSymbol("tokocrypto", symbol, func() (domain.OrderBook, error) {
			return l.fetchTokocryptoSpotOrderBook(ctx, symbol)
		})
	}

	return l.trackedBookForSymbol("indodax", symbol, func() (domain.OrderBook, error) {
		return l.fetchIndodaxSpotOrderBook(ctx, symbol)
	})
}

func (l *LiveAdapter) fetchTokocryptoSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSym := strings.TrimPrefix(symbol, "toko:")
	endpoint := fmt.Sprintf("%s/open/v1/market/depth?symbol=%s&limit=20", l.tokocryptoBaseURL, url.QueryEscape(rawSym))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("tokocrypto depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("tokocrypto depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: tokocrypto depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("tokocrypto depth status: %d", resp.StatusCode)
	}

	var raw struct {
		Code int `json:"code"`
		Data struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode tokocrypto depth: %w", err)
	}

	bids := parseStringLevels(raw.Data.Bids, 1.0)
	asks := parseStringLevels(raw.Data.Asks, 1.0)

	return domain.OrderBook{
		Exchange:  "tokocrypto",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(),
	}, nil
}

func (l *LiveAdapter) fetchIndodaxSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	pair := strings.ToLower(symbol)
	endpoint := fmt.Sprintf("%s/api/depth/%s", l.indodaxBaseURL, url.PathEscape(pair))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("indodax depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("indodax depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: indodax depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("indodax depth status: %d", resp.StatusCode)
	}

	var rawBook struct {
		Buy  [][]any `json:"buy"`  // bids
		Sell [][]any `json:"sell"` // asks
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawBook); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode indodax depth: %w", err)
	}

	bids := parseLevels(rawBook.Buy, 1.0)
	asks := parseLevels(rawBook.Sell, 1.0)

	return domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(),
	}, nil
}

func (l *LiveAdapter) fetchMEXCSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSym := strings.TrimPrefix(symbol, MEXC_SPOT)
	rawSym = strings.TrimPrefix(rawSym, "mexc:")
	endpoint := fmt.Sprintf("%s/api/v3/depth?symbol=%s&limit=20", l.mexcSpotBaseURL, url.QueryEscape(rawSym))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: mexc spot depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth status: %d", resp.StatusCode)
	}

	var rawBook struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rawBook); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode mexc spot depth: %w", err)
	}

	bids := parseStringLevels(rawBook.Bids, 1.0)
	asks := parseStringLevels(rawBook.Asks, 1.0)

	return domain.OrderBook{
		Exchange:  "mexc",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(),
	}, nil
}

// PerpetualOrderBook routes only to the foreign perpetual venue encoded in the market symbol.
func (l *LiveAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	switch {
	case isMEXCSymbol(symbol):
		return l.trackedBookForSymbol("mexc", symbol, func() (domain.OrderBook, error) { return l.fetchMEXCOrderBook(ctx, symbol) })
	case strings.HasPrefix(symbol, "binance:"):
		return l.trackedBookForSymbol("binance", symbol, func() (domain.OrderBook, error) { return l.fetchBinanceOrderBook(ctx, symbol) })
	case strings.HasPrefix(symbol, "bybit:"):
		return l.trackedBookForSymbol("bybit", symbol, func() (domain.OrderBook, error) { return l.fetchBybitOrderBook(ctx, symbol) })
	default:
		return l.trackedBookForSymbol("okx", symbol, func() (domain.OrderBook, error) { return l.fetchOKXOrderBook(ctx, symbol) })
	}
}

func isMEXCSymbol(symbol string) bool {
	return strings.HasPrefix(symbol, "mexc:") || strings.Contains(symbol, "_")
}

func cleanSymbol(symbol string) string {
	s := strings.TrimPrefix(symbol, "mexc:")
	return strings.TrimPrefix(s, "okx:")
}

func (l *LiveAdapter) fetchOKXOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v5/market/books?instId=%s&sz=20", l.okxBaseURL, url.QueryEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("okx depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("okx depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: okx depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("okx depth status: %d", resp.StatusCode)
	}

	var okxResp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
			Ts   string     `json:"ts"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&okxResp); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode okx depth: %w", err)
	}
	if okxResp.Code != "0" || len(okxResp.Data) == 0 {
		return domain.OrderBook{}, fmt.Errorf("okx depth error: %s (%s)", okxResp.Msg, okxResp.Code)
	}

	l.mu.RLock()
	ctVal, ok := l.contractSizes[symbol]
	if !ok {
		ctVal, ok = l.contractSizes[rawSymbol]
	}
	l.mu.RUnlock()
	if !ok || ctVal <= 0 {
		ctVal = 1.0
	}

	bookData := okxResp.Data[0]
	bids := parseStringLevels(bookData.Bids, ctVal)
	asks := parseStringLevels(bookData.Asks, ctVal)

	ts := time.Now().UTC()
	if ms, err := strconv.ParseInt(bookData.Ts, 10, 64); err == nil && ms > 0 {
		ts = time.UnixMilli(ms).UTC()
	}

	return domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: ts,
	}, nil
}

func (l *LiveAdapter) fetchMEXCOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v1/contract/depth/%s?limit=20", l.mexcBaseURL, url.PathEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: mexc depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("mexc depth status: %d", resp.StatusCode)
	}

	var mexcResp struct {
		Success bool `json:"success"`
		Code    int  `json:"code"`
		Data    struct {
			Bids      [][]any `json:"bids"`
			Asks      [][]any `json:"asks"`
			Timestamp int64   `json:"timestamp"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&mexcResp); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode mexc depth: %w", err)
	}
	if !mexcResp.Success {
		return domain.OrderBook{}, fmt.Errorf("mexc depth error code: %d", mexcResp.Code)
	}

	l.mu.RLock()
	ctVal, ok := l.contractSizes[symbol]
	if !ok {
		ctVal, ok = l.contractSizes[rawSymbol]
	}
	l.mu.RUnlock()
	if !ok || ctVal <= 0 {
		ctVal = 1.0
	}

	bids := parseLevels(mexcResp.Data.Bids, ctVal)
	asks := parseLevels(mexcResp.Data.Asks, ctVal)

	ts := time.Now().UTC()
	if mexcResp.Data.Timestamp > 0 {
		ts = time.UnixMilli(mexcResp.Data.Timestamp).UTC()
	}

	return domain.OrderBook{
		Exchange:  "mexc",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: ts,
	}, nil
}
