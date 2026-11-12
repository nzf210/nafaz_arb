package exchange

import (
	"context"
	"fmt"
	"strings"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/indodax"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/mexc"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/okx"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/tokocrypto"
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
		manager, legacyManager, cache, maxAge, okxMaxAge := l.okxVenueBooks, l.okxBooks, l.wsCache, l.wsMaxAge, l.okxWSMaxAge
		tokoBooks, mexcBooks, venueMaxAge := l.tokocryptoBooks, l.mexcPerpBooks, l.venueWSMaxAge
		l.mu.RUnlock()
		if exchangeName == "okx" && manager != nil { if book, ok := manager.Get(symbol, okxMaxAge); ok { return book, nil } }
		if exchangeName == "tokocrypto" && tokoBooks != nil { if book, ok := tokoBooks.Get(strings.TrimPrefix(symbol, "toko:"), venueMaxAge); ok { return book, nil } }
		if exchangeName == "mexc" && isMEXCSymbol(symbol) && mexcBooks != nil { if book, ok := mexcBooks.Get(cleanSymbol(symbol), venueMaxAge); ok { return book, nil } }
		if exchangeName == "okx" && legacyManager != nil { if book, ok := legacyManager.Get(symbol, okxMaxAge); ok { return book, nil } }
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
	l.mu.RLock()
	client := l.okxClient
	contractSize, ok := l.contractSizes[symbol]
	if !ok { contractSize, ok = l.contractSizes[cleanSymbol(symbol)] }
	l.mu.RUnlock()
	if !ok || contractSize <= 0 { contractSize = 1 }
	if client == nil { return domain.OrderBook{}, fmt.Errorf("okx client is not configured") }
	book, err := client.PerpetualOrderBook(ctx, symbol, contractSize)
	if err != nil && okx.IsUnavailable(err) { return domain.OrderBook{}, fmt.Errorf("%w: %v", ErrExchangeUnavailable, err) }
	if err != nil { return domain.OrderBook{}, err }
	return book, nil
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


// BootstrapTokocryptoBook seeds a snapshot after the MBX depth stream has been subscribed.
func (l *LiveAdapter) BootstrapTokocryptoBook(symbol string, snapshot tokocrypto.DepthSnapshot) error {
	l.mu.RLock(); manager := l.tokocryptoBooks; l.mu.RUnlock()
	if manager == nil { return fmt.Errorf("Tokocrypto WebSocket feed is not enabled") }
	return manager.Bootstrap(strings.TrimPrefix(symbol, "toko:"), snapshot)
}

// ApplyTokocryptoDepth forwards a decoded MBX diff-depth event to the synchronized book.
func (l *LiveAdapter) ApplyTokocryptoDepth(update tokocrypto.DepthUpdate) (bool, error) {
	l.mu.RLock(); manager := l.tokocryptoBooks; l.mu.RUnlock()
	if manager == nil { return false, fmt.Errorf("Tokocrypto WebSocket feed is not enabled") }
	return manager.Apply(update)
}

// BootstrapMEXCPerpetualBook seeds a contract snapshot with its base-quantity multiplier.
func (l *LiveAdapter) BootstrapMEXCPerpetualBook(symbol string, snapshot mexc.PerpetualDepthSnapshot, contractSize float64) error {
	l.mu.RLock(); manager := l.mexcPerpBooks; l.mu.RUnlock()
	if manager == nil { return fmt.Errorf("MEXC WebSocket feed is not enabled") }
	return manager.Bootstrap(cleanSymbol(symbol), snapshot, contractSize)
}

// ApplyMEXCPerpetualDepth forwards a decoded push.depth event.
func (l *LiveAdapter) ApplyMEXCPerpetualDepth(update mexc.PerpetualDepthUpdate) (bool, error) {
	l.mu.RLock(); manager := l.mexcPerpBooks; l.mu.RUnlock()
	if manager == nil { return false, fmt.Errorf("MEXC WebSocket feed is not enabled") }
	return manager.Apply(update)
}
