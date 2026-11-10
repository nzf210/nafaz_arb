package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	tokocryptoMBXWSURL    = "wss://stream-cloud.tokocrypto.site/stream"
	tokocryptoNextMeWSURL = "wss://stream-toko.2meta.app"
	tokocryptoBookLevels  = 20
	tokocryptoMaxBuffered = 2000
)

type tokocryptoBookManager struct {
	adapter      *LiveAdapter
	cache        *WSBookCache
	markets      map[int][]string
	states       map[string]*tokocryptoBookState
	symbolNames  map[string]string
	symbolTypes  map[string]int
	contexts     map[int]context.Context
	snapshotSem  chan struct{}
	snapshotMu   sync.Mutex
	lastSnapshot time.Time
	mu           sync.Mutex
}

type tokocryptoBookState struct {
	mu          sync.Mutex
	symbol      string
	bids        map[string]float64
	asks        map[string]float64
	lastSeq     int64
	timestamp   time.Time
	initialized bool
	refreshing  bool
	buffer      []tokocryptoDepthEvent
}

type tokocryptoDepthEvent struct {
	Symbol string     `json:"s"`
	Event  int64      `json:"E"`
	First  int64      `json:"U"`
	Last   int64      `json:"u"`
	Prev   int64      `json:"pu"`
	Bids   [][]string `json:"b"`
	Asks   [][]string `json:"a"`
}

func newTokocryptoBookManager(adapter *LiveAdapter, cache *WSBookCache, markets []domain.Market) *tokocryptoBookManager {
	manager := &tokocryptoBookManager{
		adapter:     adapter,
		cache:       cache,
		markets:     map[int][]string{1: {}, 3: {}},
		states:      make(map[string]*tokocryptoBookState),
		symbolNames: make(map[string]string),
		symbolTypes: make(map[string]int),
		contexts:    make(map[int]context.Context),
		snapshotSem: make(chan struct{}, 2),
	}
	for _, market := range markets {
		if !strings.EqualFold(market.Exchange, "tokocrypto") || !market.Active {
			continue
		}
		symbolType := market.ExchangeSymbolType
		if symbolType == 0 {
			symbolType = 1
		}
		if symbolType != 1 && symbolType != 3 {
			continue
		}
		raw := strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(market.Symbol, "toko:"), "_", ""))
		if raw == "" {
			continue
		}
		canonical := "toko:" + strings.ToUpper(strings.TrimPrefix(market.Symbol, "toko:"))
		manager.markets[symbolType] = append(manager.markets[symbolType], raw)
		manager.symbolNames[raw] = canonical
		manager.symbolTypes[canonical] = symbolType
		manager.states[canonical] = &tokocryptoBookState{symbol: canonical}
	}
	return manager
}

func (m *tokocryptoBookManager) streams() []WSStream {
	var streams []WSStream
	if len(m.markets[1]) > 0 {
		streams = append(streams, WSStream{
			Name:      "tokocrypto-mbx-depth",
			URL:       tokocryptoMBXWSURL,
			Subscribe: func() ([][]byte, error) { return tokocryptoSubscribeFrames(m.markets[1], "depth@100ms") },
			Handle:    m.handleDepthFrame,
			OnConnected: func(ctx context.Context) {
				m.bootstrap(ctx, 1)
			},
		})
	}
	if len(m.markets[3]) > 0 {
		streams = append(streams, WSStream{
			Name:      "tokocrypto-nextme-depth",
			URL:       tokocryptoNextMeWSURL,
			Subscribe: func() ([][]byte, error) { return tokocryptoSubscribeFrames(m.markets[3], "depth@100ms") },
			Handle:    m.handleDepthFrame,
			OnConnected: func(ctx context.Context) {
				m.bootstrap(ctx, 3)
			},
		})
	}
	return streams
}

func tokocryptoSubscribeFrames(symbols []string, channel string) ([][]byte, error) {
	params := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		params = append(params, symbol+"@"+channel)
	}
	frame, err := json.Marshal(map[string]any{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     1,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal Tokocrypto subscription: %w", err)
	}
	return [][]byte{frame}, nil
}

func (m *tokocryptoBookManager) handleMBXFrame(message []byte) (domain.OrderBook, bool, error) {
	return m.handleDepthFrame(message)
}

func (m *tokocryptoBookManager) handleNextMeFrame(message []byte) (domain.OrderBook, bool, error) {
	return m.handleDepthFrame(message)
}

func (m *tokocryptoBookManager) handleDepthFrame(message []byte) (domain.OrderBook, bool, error) {
	var envelope struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(message, &envelope); err != nil {
		return domain.OrderBook{}, true, err
	}
	if len(envelope.Data) == 0 || !strings.Contains(envelope.Stream, "@depth") {
		return domain.OrderBook{}, true, nil
	}
	var event tokocryptoDepthEvent
	if err := json.Unmarshal(envelope.Data, &event); err != nil {
		return domain.OrderBook{}, true, err
	}
	if event.Symbol == "" {
		event.Symbol = strings.SplitN(envelope.Stream, "@", 2)[0]
	}
	if event.Last <= 0 || event.First <= 0 {
		return domain.OrderBook{}, true, fmt.Errorf("Tokocrypto depth event is missing sequence IDs")
	}
	key := m.symbolName(event.Symbol)
	state := m.state(key)
	state.mu.Lock()
	if !state.initialized {
		m.appendBuffered(state, event)
		state.mu.Unlock()
		m.scheduleRefresh(m.currentContext(key), key)
		return domain.OrderBook{}, true, nil
	}
	if event.Last <= state.lastSeq {
		state.mu.Unlock()
		return domain.OrderBook{}, true, nil
	}
	if (event.Prev > 0 && event.Prev != state.lastSeq) ||
		(event.Prev == 0 && event.First > state.lastSeq+1) {
		state.initialized = false
		state.bids = nil
		state.asks = nil
		m.appendBuffered(state, event)
		state.mu.Unlock()
		m.cache.Invalidate("tokocrypto", key)
		m.scheduleRefresh(m.currentContext(key), key)
		return domain.OrderBook{}, true, nil
	}
	applyTokocryptoLevels(state.bids, event.Bids)
	applyTokocryptoLevels(state.asks, event.Asks)
	state.lastSeq = event.Last
	state.timestamp = time.Now().UTC()
	if event.Event > 0 {
		state.timestamp = time.UnixMilli(event.Event).UTC()
	}
	book := tokocryptoStateBook(state)
	state.mu.Unlock()
	if len(book.Bids) == 0 || len(book.Asks) == 0 {
		state.mu.Lock()
		state.initialized = false
		m.appendBuffered(state, event)
		state.mu.Unlock()
		m.cache.Invalidate("tokocrypto", key)
		m.scheduleRefresh(m.currentContext(key), key)
		return domain.OrderBook{}, true, nil
	}
	_, _, _, _ = m.cache.Store(book)
	return domain.OrderBook{}, true, nil
}

func (m *tokocryptoBookManager) currentContext(symbol string) context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.contexts[m.symbolTypes[symbol]]
}

func (m *tokocryptoBookManager) symbolName(raw string) string {
	key := strings.ToLower(strings.ReplaceAll(raw, "_", ""))
	if name := m.symbolNames[key]; name != "" {
		return name
	}
	return "toko:" + strings.ToUpper(raw)
}

func (m *tokocryptoBookManager) state(symbol string) *tokocryptoBookState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[symbol]
	if state == nil {
		state = &tokocryptoBookState{symbol: symbol}
		m.states[symbol] = state
	}
	return state
}

func (m *tokocryptoBookManager) appendBuffered(state *tokocryptoBookState, event tokocryptoDepthEvent) {
	if len(state.buffer) >= tokocryptoMaxBuffered {
		state.buffer = append([]tokocryptoDepthEvent(nil), state.buffer[len(state.buffer)/2:]...)
	}
	state.buffer = append(state.buffer, event)
}

func (m *tokocryptoBookManager) bootstrap(ctx context.Context, symbolType int) {
	m.mu.Lock()
	m.contexts[symbolType] = ctx
	symbols := append([]string(nil), m.markets[symbolType]...)
	m.mu.Unlock()
	for _, raw := range symbols {
		symbol := m.symbolName(raw)
		state := m.state(symbol)
		state.mu.Lock()
		state.initialized = false
		state.bids = nil
		state.asks = nil
		state.lastSeq = 0
		state.timestamp = time.Time{}
		state.buffer = nil
		state.mu.Unlock()
		m.cache.Invalidate("tokocrypto", symbol)
	}
	for _, raw := range symbols {
		m.scheduleRefresh(ctx, m.symbolName(raw))
	}
}

func (m *tokocryptoBookManager) scheduleRefresh(ctx context.Context, symbol string) {
	state := m.state(symbol)
	state.mu.Lock()
	if ctx == nil || state.refreshing || ctx.Err() != nil {
		state.mu.Unlock()
		return
	}
	state.refreshing = true
	state.mu.Unlock()
	go m.refreshLoop(ctx, state)
}

func (m *tokocryptoBookManager) refreshLoop(ctx context.Context, state *tokocryptoBookState) {
	defer func() {
		state.mu.Lock()
		state.refreshing = false
		state.mu.Unlock()
	}()
	delay := time.Second
	for ctx.Err() == nil {
		select {
		case m.snapshotSem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if err := m.waitSnapshotSlot(ctx); err != nil {
			<-m.snapshotSem
			return
		}
		book, err := m.adapter.fetchTokocryptoSpotOrderBook(ctx, state.symbol)
		<-m.snapshotSem
		if err == nil && book.SequenceID > 0 {
			if m.installSnapshot(state, book) {
				return
			}
			err = fmt.Errorf("Tokocrypto depth sequence could not be synchronized")
		} else if err == nil {
			err = fmt.Errorf("Tokocrypto public depth snapshot has no lastUpdateId")
		}
		slog.Warn("Tokocrypto depth snapshot/re-sync failed", "symbol", state.symbol, "err", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}

func (m *tokocryptoBookManager) waitSnapshotSlot(ctx context.Context) error {
	for {
		m.snapshotMu.Lock()
		wait := 250*time.Millisecond - time.Since(m.lastSnapshot)
		if wait <= 0 {
			m.lastSnapshot = time.Now()
			m.snapshotMu.Unlock()
			return nil
		}
		m.snapshotMu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *tokocryptoBookManager) installSnapshot(state *tokocryptoBookState, snapshot domain.OrderBook) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.bids = make(map[string]float64, len(snapshot.Bids))
	state.asks = make(map[string]float64, len(snapshot.Asks))
	for _, level := range snapshot.Bids {
		state.bids[priceKey(level.Price)] = level.Quantity
	}
	for _, level := range snapshot.Asks {
		state.asks[priceKey(level.Price)] = level.Quantity
	}
	state.lastSeq = snapshot.SequenceID
	state.timestamp = snapshot.Timestamp
	pending := append([]tokocryptoDepthEvent(nil), state.buffer...)
	state.buffer = nil
	started := false
	for _, event := range pending {
		if event.Last <= state.lastSeq {
			continue
		}
		if !started {
			if event.First > state.lastSeq+1 || event.Last < state.lastSeq+1 {
				state.initialized = false
				state.buffer = pending
				m.cache.Invalidate("tokocrypto", state.symbol)
				return false
			}
			started = true
		} else if (event.Prev > 0 && event.Prev != state.lastSeq) ||
			(event.Prev == 0 && event.First > state.lastSeq+1) {
			state.initialized = false
			state.buffer = pending
			m.cache.Invalidate("tokocrypto", state.symbol)
			return false
		}
		applyTokocryptoLevels(state.bids, event.Bids)
		applyTokocryptoLevels(state.asks, event.Asks)
		state.lastSeq = event.Last
		if event.Event > 0 {
			state.timestamp = time.UnixMilli(event.Event).UTC()
		}
	}
	state.initialized = true
	book := tokocryptoStateBook(state)
	if len(book.Bids) == 0 || len(book.Asks) == 0 {
		state.initialized = false
		m.cache.Invalidate("tokocrypto", state.symbol)
		return false
	}
	_, _, _, _ = m.cache.Store(book)
	return true
}

func applyTokocryptoLevels(book map[string]float64, levels [][]string) {
	for _, level := range levels {
		if len(level) < 2 {
			continue
		}
		price, priceErr := strconv.ParseFloat(level[0], 64)
		quantity, quantityErr := strconv.ParseFloat(level[1], 64)
		if priceErr != nil || quantityErr != nil || price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
			continue
		}
		key := priceKey(price)
		if quantity <= 0 || math.IsNaN(quantity) || math.IsInf(quantity, 0) {
			delete(book, key)
			continue
		}
		book[key] = quantity
	}
}

func priceKey(price float64) string {
	return strconv.FormatFloat(price, 'f', -1, 64)
}

func tokocryptoStateBook(state *tokocryptoBookState) domain.OrderBook {
	bids := make([]domain.PriceLevel, 0, len(state.bids))
	asks := make([]domain.PriceLevel, 0, len(state.asks))
	for price, quantity := range state.bids {
		value, err := strconv.ParseFloat(price, 64)
		if err == nil && value > 0 && quantity > 0 {
			bids = append(bids, domain.PriceLevel{Price: value, Quantity: quantity})
		}
	}
	for price, quantity := range state.asks {
		value, err := strconv.ParseFloat(price, 64)
		if err == nil && value > 0 && quantity > 0 {
			asks = append(asks, domain.PriceLevel{Price: value, Quantity: quantity})
		}
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	if len(bids) > tokocryptoBookLevels {
		bids = bids[:tokocryptoBookLevels]
	}
	if len(asks) > tokocryptoBookLevels {
		asks = asks[:tokocryptoBookLevels]
	}
	now := time.Now().UTC()
	stamp := state.timestamp
	if stamp.IsZero() {
		stamp = now
	}
	return domain.OrderBook{
		Exchange:   "tokocrypto",
		Symbol:     state.symbol,
		Bids:       bids,
		Asks:       asks,
		Timestamp:  stamp,
		ReceivedAt: now,
		Source:     "websocket",
	}
}
