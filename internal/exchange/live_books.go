package exchange

import (
	"context"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/indodax"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/mexc"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/tokocrypto"
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
		manager, cache, maxAge, okxMaxAge := l.okxBooks, l.wsCache, l.wsMaxAge, l.okxWSMaxAge
		l.mu.RUnlock()
		if exchangeName == "okx" && manager != nil {
			if book, ok := manager.Get(symbol, okxMaxAge); ok {
				return book, nil
			}
		}
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
	var book domain.OrderBook
	var err error
	if exchangeName == "okx" {
		book, err = l.fetchOKXBookSingleflight(symbol, fetch)
		if err != nil { err = fmt.Errorf("okx REST order-book fallback failed for %s: %w", symbol, err) }
	} else {
		book, err = fetch()
	}
	l.recordOutcome(exchangeName, err)
	return book, err
}

type okxBookCall struct {
	done chan struct{}
	book domain.OrderBook
	err error
}

// fetchOKXBookSingleflight collapses concurrent cache misses for one instrument
// into one REST recovery request. Every caller observes the same result.
func (l *LiveAdapter) fetchOKXBookSingleflight(symbol string, fetch func() (domain.OrderBook, error)) (domain.OrderBook, error) {
	if symbol == "" { return fetch() }
	l.mu.Lock()
	if call := l.okxBookCalls[symbol]; call != nil {
		l.mu.Unlock()
		<-call.done
		return call.book, call.err
	}
	call := &okxBookCall{done: make(chan struct{})}
	l.okxBookCalls[symbol] = call
	l.mu.Unlock()

	call.book, call.err = fetch()
	l.mu.Lock()
	delete(l.okxBookCalls, symbol)
	close(call.done)
	l.mu.Unlock()
	return call.book, call.err
}

// SpotOrderBook fetches live order book depth from Tokocrypto, Indodax, or MEXC Spot.
func (l *LiveAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if strings.HasPrefix(symbol, "toko:") {
		return l.trackedBookForSymbol("tokocrypto", symbol, func() (domain.OrderBook, error) {
			return l.fetchTokocryptoSpotOrderBook(ctx, symbol)
		})
	}
	if strings.HasPrefix(symbol, MEXC_SPOT) || strings.HasPrefix(symbol, "mexc:") {
		return l.trackedBookForSymbol("mexc", symbol, func() (domain.OrderBook, error) {
			return l.fetchMEXCSpotOrderBook(ctx, symbol)
		})
	}
	return l.trackedBookForSymbol("indodax", symbol, func() (domain.OrderBook, error) {
		return l.fetchIndodaxSpotOrderBook(ctx, symbol)
	})
}

func (l *LiveAdapter) fetchTokocryptoSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	l.mu.RLock()
	client := l.tokocryptoClient
	l.mu.RUnlock()
	if client == nil { return domain.OrderBook{}, fmt.Errorf("tokocrypto client is not configured") }
	book, err := client.OrderBook(ctx, symbol)
	if err != nil && tokocrypto.IsUnavailable(err) {
		return domain.OrderBook{}, fmt.Errorf("%w: %v", ErrExchangeUnavailable, err)
	}
	return book, err
}

func (l *LiveAdapter) fetchIndodaxSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	l.mu.RLock()
	client := l.indodaxClient
	l.mu.RUnlock()
	if client == nil { return domain.OrderBook{}, fmt.Errorf("indodax client is not configured") }
	book, err := client.OrderBook(ctx, symbol)
	if err != nil && indodax.IsUnavailable(err) {
		return domain.OrderBook{}, fmt.Errorf("%w: %v", ErrExchangeUnavailable, err)
	}
	return book, err
}

func (l *LiveAdapter) fetchMEXCSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	l.mu.RLock()
	client := l.mexcClient
	l.mu.RUnlock()
	if client == nil { return domain.OrderBook{}, fmt.Errorf("mexc client is not configured") }
	book, err := client.SpotOrderBook(ctx, symbol)
	if err != nil && mexc.IsUnavailable(err) { return domain.OrderBook{}, fmt.Errorf("%w: %v", ErrExchangeUnavailable, err) }
	return book, err
}

// PerpetualOrderBook fetches live order book from OKX or MEXC with contract size normalization.
func (l *LiveAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if isMEXCSymbol(symbol) {
		return l.trackedBookForSymbol("mexc", symbol, func() (domain.OrderBook, error) {
			return l.fetchMEXCOrderBook(ctx, symbol)
		})
	}
	return l.trackedBookForSymbol("okx", symbol, func() (domain.OrderBook, error) {
		return l.fetchOKXOrderBook(ctx, symbol)
	})
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
	path := fmt.Sprintf("/api/v5/market/books?instId=%s&sz=20", url.QueryEscape(rawSymbol))
	resp, err := l.okxDo(ctx, path, func() (*http.Request, error) {
		return l.newOKXRequest(ctx, path)
	})
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
		Exchange:   "okx",
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		Timestamp:  ts,
		ReceivedAt: time.Now().UTC(),
		Source:     "exchange_timestamp",
	}, nil
}

func (l *LiveAdapter) fetchMEXCOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	l.mu.RLock()
	client := l.mexcClient
	contractSize, ok := l.contractSizes[symbol]
	if !ok { contractSize, ok = l.contractSizes[cleanSymbol(symbol)] }
	l.mu.RUnlock()
	if !ok || contractSize <= 0 { contractSize = 1 }
	if client == nil { return domain.OrderBook{}, fmt.Errorf("mexc client is not configured") }
	book, err := client.PerpetualOrderBook(ctx, symbol, contractSize)
	if err != nil && mexc.IsUnavailable(err) { return domain.OrderBook{}, fmt.Errorf("%w: %v", ErrExchangeUnavailable, err) }
	return book, err
}
