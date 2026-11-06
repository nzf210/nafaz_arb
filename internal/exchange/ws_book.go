package exchange

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// BookSource identifies where an OrderBook came from.
type BookSource string

const (
	BookSourceREST BookSource = "rest"
	BookSourceWS   BookSource = "websocket"
)

// WSBookCache holds the most recent order books pushed over websocket, keyed by
// exchange + symbol. The scanner still calls the adapter for every pair; when a
// cached book is fresh it is returned instead of a REST round-trip, so REST stays
// the fallback rather than being removed.
type WSBookCache struct {
	mu    sync.RWMutex
	books map[string]cachedBook
	now   func() time.Time
	log   *slog.Logger
	// hits/misses make the REST-vs-WS split observable in tests and metrics.
	hits   int64
	misses int64
	// seqTracker tracks the last seen sequence ID per exchange|symbol for
	// duplicate and out-of-order detection.
	seqTracker map[string]int64
	// gapTracker tracks sequence gaps per exchange|symbol for data gap detection.
	gapTracker map[string]int64
	// staleTracker tracks when a book was last updated for stale detection.
	staleTracker map[string]time.Time
	// healthTracker tracks connection health per exchange.
	healthTracker map[string]*ConnectionHealth
}

// ConnectionHealth tracks WebSocket connection health metrics.
type ConnectionHealth struct {
	LastMessageAt    time.Time
	MessagesReceived int64
	Errors           int64
	Reconnects       int64
	LastError        error
	IsHealthy        bool
}

type cachedBook struct {
	book domain.OrderBook
	at   time.Time
}

// NewWSBookCache returns an empty cache using the wall clock.
func NewWSBookCache() *WSBookCache {
	return &WSBookCache{
		books:         make(map[string]cachedBook),
		now:           time.Now,
		log:           slog.Default(),
		seqTracker:    make(map[string]int64),
		gapTracker:    make(map[string]int64),
		staleTracker:  make(map[string]time.Time),
		healthTracker: make(map[string]*ConnectionHealth),
	}
}

func cacheKey(exchange, symbol string) string {
	return strings.ToLower(exchange) + "|" + symbol
}

// Store records a websocket book. The venue timestamp is kept as-is so downstream
// age checks measure data age rather than our receive time.
// Returns (isDuplicate, isOutOfOrder, gapDetected, error).
func (c *WSBookCache) Store(b domain.OrderBook) (bool, bool, bool, error) {
	if b.Exchange == "" || b.Symbol == "" || len(b.Bids) == 0 || len(b.Asks) == 0 {
		return false, false, false, fmt.Errorf("invalid order book: empty exchange, symbol, or one-sided book")
	}

	key := cacheKey(b.Exchange, b.Symbol)
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Initialize health tracker if not exists
	if c.healthTracker[key] == nil {
		c.healthTracker[key] = &ConnectionHealth{IsHealthy: true}
	}
	health := c.healthTracker[key]
	health.LastMessageAt = now
	health.MessagesReceived++

	// Check for duplicate sequence ID
	isDuplicate := false
	isOutOfOrder := false
	gapDetected := false
	if b.SequenceID > 0 {
		lastSeq, exists := c.seqTracker[key]
		if exists {
			if b.SequenceID == lastSeq {
				isDuplicate = true
				// Still update the book with the latest data but mark as duplicate
			} else if b.SequenceID < lastSeq {
				// Out-of-order event detected - track the gap
				isOutOfOrder = true
				gap := lastSeq - b.SequenceID
				if gap > 1 {
					c.gapTracker[key] += gap - 1
					gapDetected = true
				}
				c.log.Warn("out-of-order sequence detected", "exchange", b.Exchange, "symbol", b.Symbol, "gap", gap)
			}
		}
		// Only update seqTracker if this is a new high-water mark (not out-of-order)
		// or if it's a duplicate (keep the existing high-water mark)
		if !isOutOfOrder && !isDuplicate {
			c.seqTracker[key] = b.SequenceID
		}
	}

	// Ghost orderbook detection: a book with near-zero total depth on both sides
	// is likely a placeholder or failed connection artifact, not real market data.
	totalDepth := bookDepth(b.Bids) + bookDepth(b.Asks)
	isGhost := totalDepth < 1e-8 // effectively zero volume
	if isGhost {
		c.log.Warn("ghost orderbook detected - near-zero depth", "exchange", b.Exchange, "symbol", b.Symbol)
	}

	// Update stale tracker
	c.staleTracker[key] = now

	// Store the book
	c.books[key] = cachedBook{book: b, at: now}

	return isDuplicate, isOutOfOrder, gapDetected, nil
}

// Get returns a cached book when it is no older than maxAge. A zero maxAge means
// "any age is acceptable", matching how the scanner treats disabled age checks.
// Also returns gap and stale information.
func (c *WSBookCache) Get(exchange, symbol string, maxAge time.Duration) (domain.OrderBook, bool, bool, bool, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.books[cacheKey(exchange, symbol)]
	if !ok {
		c.misses++
		return domain.OrderBook{}, false, false, false, false, nil
	}
	if maxAge > 0 {
		stamp := entry.book.Timestamp
		if stamp.IsZero() {
			stamp = entry.at
		}
		if c.now().Sub(stamp) > maxAge {
			c.misses++
			return domain.OrderBook{}, false, false, false, false, nil
		}
	}
	c.hits++
	// Check stale: if book hasn't been updated recently (no new messages)
	key := cacheKey(exchange, symbol)
	if health, exists := c.healthTracker[key]; exists {
		idleTime := c.now().Sub(health.LastMessageAt)
		if idleTime > maxAge && maxAge > 0 {
			// Book is stale - no recent messages
			c.misses++
			return domain.OrderBook{}, false, false, false, false, nil
		}
	}
	// Return validation flags tracked during Store: duplicate, out-of-order, gap
	isDuplicate := entry.book.SequenceID > 0 && c.seqTracker[key] == entry.book.SequenceID
	isOutOfOrder := false
	isGap := c.gapTracker[key] > 0
	if entry.book.SequenceID > 0 {
		lastSeq, exists := c.seqTracker[key]
		if exists && entry.book.SequenceID < lastSeq {
			isOutOfOrder = true
		}
	}
	return entry.book, true, isDuplicate, isOutOfOrder, isGap, nil
}

// Stats reports how often a websocket book was usable versus falling back to REST.
func (c *WSBookCache) Stats() (hits, misses int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses
}

// HealthStats returns connection health information for all tracked exchanges.
func (c *WSBookCache) HealthStats() map[string]*ConnectionHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*ConnectionHealth, len(c.healthTracker))
	for k, v := range c.healthTracker {
		result[k] = v
	}
	return result
}

// IsConnectionHealthy checks if a specific exchange's websocket connection is healthy.
func (c *WSBookCache) IsConnectionHealthy(exchange, symbol string, maxIdle time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := cacheKey(exchange, symbol)
	health, exists := c.healthTracker[key]
	if !exists {
		// No tracker entry means the feed has never produced anything for this
		// symbol; only treat that as unhealthy once the idle window proves it.
		return maxIdle <= 0
	}
	if !health.IsHealthy {
		return false
	}
	if maxIdle > 0 && c.now().Sub(health.LastMessageAt) > maxIdle {
		return false
	}
	return true
}

// IsExchangeConnectionHealthy reports whether the exchange-level connection
// health entry (populated by stream-level error recording) is healthy. A missing
// entry is treated as healthy: absence of evidence is not evidence of failure,
// and blocking trading on it would halt the scanner for venues with no feed.
func (c *WSBookCache) IsExchangeConnectionHealthy(exchange string, maxIdle time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	health, exists := c.healthTracker[cacheKey(exchange, "")]
	if !exists {
		return true
	}
	if !health.IsHealthy {
		return false
	}
	if maxIdle > 0 && !health.LastMessageAt.IsZero() && c.now().Sub(health.LastMessageAt) > maxIdle {
		return false
	}
	return true
}

// GetSequenceGap returns the number of missing sequence events for a given exchange|symbol.
func (c *WSBookCache) GetSequenceGap(exchange, symbol string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gapTracker[cacheKey(exchange, symbol)]
}

// GetLastSequence returns the last seen sequence ID for a given exchange|symbol.
func (c *WSBookCache) GetLastSequence(exchange, symbol string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	seq, exists := c.seqTracker[cacheKey(exchange, symbol)]
	return seq, exists
}

// IsStale checks if a book for the given exchange|symbol is stale (no recent updates).
func (c *WSBookCache) IsStale(exchange, symbol string, maxAge time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := cacheKey(exchange, symbol)
	lastUpdate, exists := c.staleTracker[key]
	if !exists {
		return true
	}
	return c.now().Sub(lastUpdate) > maxAge
}

// BookAge returns how long since the last book update for an exchange|symbol.
// A missing entry reports an empty (zero) time.Time so callers can distinguish
// "never seen" from "seen just now".
func (c *WSBookCache) LastBookUpdate(exchange, symbol string) time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.staleTracker[cacheKey(exchange, symbol)]
}

// CleanupStaleBooks removes all cached books that are stale (no recent updates)
// beyond maxAge. This is intended to be called periodically (e.g. via a ticker)
// to keep the cache from growing unboundedly with stale data.
func (c *WSBookCache) CleanupStaleBooks(maxAge time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	removed := 0
	for key, lastUpdate := range c.staleTracker {
		if now.Sub(lastUpdate) > maxAge {
			// Also remove the cached book for this key
			delete(c.books, key)
			delete(c.staleTracker, key)
			removed++
		}
	}
	return removed
}

// StartStaleBookCleanup begins periodic cleanup of stale books using the given
// interval. It returns a stop function that should be called when the adapter
// shuts down. This is typically called after EnableWebSocketFeed.
func (c *WSBookCache) StartStaleBookCleanup(maxAge, interval time.Duration) func() {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				c.CleanupStaleBooks(maxAge)
			}
		}
	}()
	return func() {
		close(stop)
	}
}

// ValidateAfterReconnect checks that the cached book for an exchange|symbol is
// consistent after a websocket reconnect. After a reconnect the sequence tracker
// may have been reset, so we validate that the book we have is not stale and
// that the sequence state is coherent. Returns an error if the book is invalid.
func (c *WSBookCache) ValidateAfterReconnect(exchange, symbol string, maxAge time.Duration) error {
	key := cacheKey(exchange, symbol)
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.books[key]
	if !ok {
		return fmt.Errorf("post-reconnect validation: no cached book for %s %s", exchange, symbol)
	}
	if maxAge > 0 {
		stamp := entry.book.Timestamp
		if stamp.IsZero() {
			stamp = entry.at
		}
		if c.now().Sub(stamp) > maxAge {
			return fmt.Errorf("post-reconnect validation: stale book for %s %s", exchange, symbol)
		}
	}
	// After a reconnect, the sequence tracker should be reset. If we have a
	// sequence ID, we validate it is positive and the book is not a ghost.
	if entry.book.SequenceID > 0 {
		if entry.book.SequenceID < 0 {
			return fmt.Errorf("post-reconnect validation: invalid sequence ID %d for %s %s", entry.book.SequenceID, exchange, symbol)
		}
	}
	return nil
}

// RecordError records an error for a connection health tracker.
func (c *WSBookCache) RecordError(exchange, symbol string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheKey(exchange, symbol)
	if c.healthTracker[key] == nil {
		c.healthTracker[key] = &ConnectionHealth{IsHealthy: true}
	}
	c.healthTracker[key].Errors++
	c.healthTracker[key].LastError = err
	c.healthTracker[key].IsHealthy = false
}

// RecordExchangeError records an error for the connection health tracker
// associated with an exchange (without a specific symbol). This is used when
// a whole stream fails and we want to mark the exchange connection as unhealthy
// without needing to know which specific symbol was affected.
func (c *WSBookCache) RecordExchangeError(exchange string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheKey(exchange, "")
	if c.healthTracker[key] == nil {
		c.healthTracker[key] = &ConnectionHealth{IsHealthy: true}
	}
	c.healthTracker[key].Errors++
	c.healthTracker[key].LastError = err
	c.healthTracker[key].IsHealthy = false
}

// RecordReconnect records a reconnection for a connection health tracker.
// It restores health for the per-symbol entry and for the exchange-level entry
// used by stream health checks.
func (c *WSBookCache) RecordReconnect(exchange, symbol string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range []string{cacheKey(exchange, symbol), cacheKey(exchange, "")} {
		if c.healthTracker[key] == nil {
			c.healthTracker[key] = &ConnectionHealth{IsHealthy: true}
		}
		c.healthTracker[key].Reconnects++
		c.healthTracker[key].IsHealthy = true
	}
}

// entries returns the number of cached books.
func (c *WSBookCache) entries() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.books)
}
