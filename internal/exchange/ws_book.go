package exchange

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
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

// decodeIndodaxDepth decodes Indodax's depth payload. Levels arrive as
// [price, quantity] where both may be strings or numbers.
func decodeIndodaxDepth(symbol string, payload []byte, now time.Time) (domain.OrderBook, error) {
	var raw struct {
		Buy  [][]any `json:"buy"`
		Sell [][]any `json:"sell"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode indodax depth: %w", err)
	}
	b := domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Bids:      parseLevels(raw.Buy, 1.0),
		Asks:      parseLevels(raw.Sell, 1.0),
		Timestamp: now.UTC(),
		Source:    "websocket",
	}
	if seq := parseSequenceID(payload); seq > 0 {
		b.SequenceID = seq
	}
	b.ReceivedAt = now.UTC()
	if len(b.Bids) == 0 || len(b.Asks) == 0 {
		return domain.OrderBook{}, fmt.Errorf("indodax depth has no two-sided book")
	}
	return b, nil
}

// decodeOKXBook decodes an OKX books5/books snapshot: data[0].bids/asks as
// [price, size, ...] string triplets, with ts in milliseconds.
func decodeOKXBook(symbol string, payload []byte, contractSize float64, now time.Time) (domain.OrderBook, error) {
	var raw struct {
		Arg  struct{ InstID string } `json:"arg"`
		Data []struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
			Ts   string     `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode okx book: %w", err)
	}
	if len(raw.Data) == 0 {
		return domain.OrderBook{}, fmt.Errorf("okx book has no data")
	}
	if contractSize <= 0 {
		contractSize = 1.0
	}
	d := raw.Data[0]
	b := domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Bids:      parseStringLevels(d.Bids, contractSize),
		Asks:      parseStringLevels(d.Asks, contractSize),
		Timestamp: now.UTC(),
		Source:    "websocket",
	}
	if ms, err := strconv.ParseInt(d.Ts, 10, 64); err == nil && ms > 0 {
		b.Timestamp = time.UnixMilli(ms).UTC()
	}
	if seq := parseSequenceID(payload); seq > 0 {
		b.SequenceID = seq
	}
	b.ReceivedAt = now.UTC()
	if len(b.Bids) == 0 || len(b.Asks) == 0 {
		return domain.OrderBook{}, fmt.Errorf("okx book has no two-sided book")
	}
	return b, nil
}

// parseSequenceID attempts to extract a monotonic sequence identifier from a raw
// websocket payload. Different venues expose it under different keys:
//   - OKX: "seqId" (string) on the data object
//   - MEXC: "s" (int64) at the top level
//   - Indodax: no sequence field — returns 0 to signal "unavailable".
func parseSequenceID(payload []byte) int64 {
	var probe struct {
		Data []struct {
			SeqID string `json:"seqId"`
		} `json:"data"`
		Seq int64 `json:"s"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return 0
	}
	if len(probe.Data) > 0 && probe.Data[0].SeqID != "" {
		if v, err := strconv.ParseInt(probe.Data[0].SeqID, 10, 64); err == nil {
			return v
		}
	}
	if probe.Seq > 0 {
		return probe.Seq
	}
	return 0
}

// bookDepth sums quantity across price levels where both price and quantity are positive.
func bookDepth(levels []domain.PriceLevel) float64 {
	sum := 0.0
	for _, l := range levels {
		if l.Price > 0 && l.Quantity > 0 {
			sum += l.Quantity
		}
	}
	return sum
}

// decodeMEXCSpotDepth decodes MEXC spot depth (bids/asks as [price, qty] strings).
func decodeMEXCSpotDepth(symbol string, payload []byte, now time.Time) (domain.OrderBook, error) {
	var raw struct {
		Bids      [][]string `json:"bids"`
		Asks      [][]string `json:"asks"`
		Timestamp int64      `json:"timestamp"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode mexc spot depth: %w", err)
	}
	b := domain.OrderBook{
		Exchange:  "mexc",
		Symbol:    symbol,
		Bids:      parseStringLevels(raw.Bids, 1.0),
		Asks:      parseStringLevels(raw.Asks, 1.0),
		Timestamp: now.UTC(),
		Source:    "websocket",
	}
	if raw.Timestamp > 0 {
		b.Timestamp = time.UnixMilli(raw.Timestamp).UTC()
	}
	if seq := parseSequenceID(payload); seq > 0 {
		b.SequenceID = seq
	}
	b.ReceivedAt = now.UTC()
	if len(b.Bids) == 0 || len(b.Asks) == 0 {
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth has no two-sided book")
	}
	return b, nil
}
