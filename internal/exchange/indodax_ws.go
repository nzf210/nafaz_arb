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
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	indodaxWSDefaultURL       = "wss://ws3.indodax.com/ws/"
	indodaxWSDefaultReconnect = time.Second
	indodaxWSDefaultMaxBackoff = 30 * time.Second
	indodaxWSHeartbeat        = 25 * time.Second
	indodaxWSBookDepth        = 20
)

type indodaxWSBookState struct {
	bids     []domain.PriceLevel
	asks     []domain.PriceLevel
	received time.Time
	valid    bool
}

// IndodaxBookManager maintains a Centrifuge WebSocket connection to
// wss://ws3.indodax.com/ws/ for real-time Indodax order books and live
// USDT/IDR ticker updates, reducing REST polling against Cloudflare WAF.
type IndodaxBookManager struct {
	url               string
	token             string
	log               *slog.Logger
	reconnect         time.Duration
	maxBackoff        time.Duration
	heartbeatInterval time.Duration

	mu          sync.RWMutex
	pairs       map[string]struct{}
	books       map[string]*indodaxWSBookState
	usdtIDRRate float64
	usdtIDRAt   time.Time
	connected   bool
	cancel      context.CancelFunc
	done        chan struct{}
	updates     chan struct{}
	nextID      atomic.Int64
}

func NewIndodaxBookManager(wsURL, token string, log *slog.Logger, reconnect, maxBackoff time.Duration) *IndodaxBookManager {
	if strings.TrimSpace(wsURL) == "" {
		wsURL = indodaxWSDefaultURL
	}
	if log == nil {
		log = slog.Default()
	}
	if reconnect <= 0 {
		reconnect = indodaxWSDefaultReconnect
	}
	if maxBackoff <= 0 {
		maxBackoff = indodaxWSDefaultMaxBackoff
	}
	if maxBackoff < reconnect {
		maxBackoff = reconnect
	}
	return &IndodaxBookManager{
		url:               wsURL,
		token:             strings.TrimSpace(token),
		log:               log,
		reconnect:         reconnect,
		maxBackoff:        maxBackoff,
		heartbeatInterval: indodaxWSHeartbeat,
		pairs:             make(map[string]struct{}),
		books:             make(map[string]*indodaxWSBookState),
		updates:           make(chan struct{}, 1),
	}
}

func (m *IndodaxBookManager) Start(ctx context.Context) {
	if m.token == "" {
		return
	}
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel, m.done = cancel, make(chan struct{})
	done := m.done
	m.mu.Unlock()
	go func() {
		defer close(done)
		m.run(runCtx)
	}()
}

func (m *IndodaxBookManager) Stop() {
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel, m.done = nil, nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (m *IndodaxBookManager) SetPairs(symbols []string) {
	next := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		s := strings.ToLower(strings.TrimSpace(symbol))
		if s != "" {
			next[s] = struct{}{}
		}
	}
	m.mu.Lock()
	m.pairs = next
	m.mu.Unlock()
	select {
	case m.updates <- struct{}{}:
	default:
	}
}

func (m *IndodaxBookManager) pairList() []string {
	m.mu.RLock()
	out := make([]string, 0, len(m.pairs))
	for s := range m.pairs {
		out = append(out, s)
	}
	m.mu.RUnlock()
	sort.Strings(out)
	return out
}

func (m *IndodaxBookManager) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connected
}

func (m *IndodaxBookManager) Get(symbol string, maxAge time.Duration) (domain.OrderBook, bool) {
	key := strings.ToLower(strings.TrimSpace(symbol))
	m.mu.RLock()
	state := m.books[key]
	if state == nil || !state.valid || state.received.IsZero() || (maxAge > 0 && time.Since(state.received) > maxAge) {
		m.mu.RUnlock()
		return domain.OrderBook{}, false
	}
	bids := append([]domain.PriceLevel(nil), state.bids...)
	asks := append([]domain.PriceLevel(nil), state.asks...)
	ts := state.received
	m.mu.RUnlock()
	return domain.OrderBook{
		Exchange:   "indodax",
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		Timestamp:  ts,
		ReceivedAt: ts,
		Source:     "websocket",
	}, true
}

func (m *IndodaxBookManager) USDTIDRRate(maxAge time.Duration) (float64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.usdtIDRRate <= 0 || m.usdtIDRAt.IsZero() {
		return 0, false
	}
	if maxAge > 0 && time.Since(m.usdtIDRAt) > maxAge {
		return 0, false
	}
	return m.usdtIDRRate, true
}

func (m *IndodaxBookManager) run(ctx context.Context) {
	delay := m.reconnect
	for ctx.Err() == nil {
		err := m.consume(ctx)
		if ctx.Err() != nil {
			return
		}
		m.mu.Lock()
		m.connected = false
		m.mu.Unlock()
		if err != nil {
			m.log.Warn("indodax websocket disconnected", "err", err, "retry_in", delay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < m.maxBackoff {
			delay *= 2
			if delay > m.maxBackoff {
				delay = m.maxBackoff
			}
		}
	}
}

func (m *IndodaxBookManager) consume(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, m.url, &websocket.DialOptions{
		HTTPClient: wsHTTPClient(),
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	authFrame, _ := json.Marshal(map[string]any{
		"params": map[string]string{"token": m.token},
		"id":     m.nextID.Add(1),
	})
	if err := conn.Write(ctx, websocket.MessageText, authFrame); err != nil {
		return fmt.Errorf("write auth: %w", err)
	}

	_, authResp, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read auth: %w", err)
	}
	var authCheck struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(authResp, &authCheck); err == nil && authCheck.Error != nil {
		return fmt.Errorf("indodax ws auth error %d: %s", authCheck.Error.Code, authCheck.Error.Message)
	}

	m.mu.Lock()
	m.connected = true
	m.mu.Unlock()

	if err := m.sendSubscribe(ctx, conn, "market:summary-24h"); err != nil {
		return err
	}
	subscribed := make(map[string]struct{})
	if err := m.syncSubscriptions(ctx, conn, subscribed); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()

	go func() {
		hb := time.NewTicker(m.heartbeatInterval)
		defer hb.Stop()
		for {
			select {
			case <-subCtx.Done():
				return
			case <-m.updates:
				if err := m.syncSubscriptions(subCtx, conn, subscribed); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			case <-hb.C:
				pingFrame, _ := json.Marshal(map[string]any{
					"method": 7,
					"id":     m.nextID.Add(1),
				})
				if err := conn.Write(subCtx, websocket.MessageText, pingFrame); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}
	}()

	for {
		select {
		case err := <-errCh:
			return err
		default:
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if err := m.ApplyFrame(data, time.Now().UTC()); err != nil {
			m.log.Debug("indodax ws frame ignored", "err", err)
		}
	}
}

func (m *IndodaxBookManager) syncSubscriptions(ctx context.Context, conn *websocket.Conn, subscribed map[string]struct{}) error {
	desired := m.pairList()
	desiredSet := make(map[string]struct{}, len(desired))
	for _, pair := range desired {
		desiredSet[pair] = struct{}{}
		if _, ok := subscribed[pair]; !ok {
			if err := m.sendSubscribe(ctx, conn, "market:order-book-"+pair); err != nil {
				return err
			}
			subscribed[pair] = struct{}{}
		}
	}
	for pair := range subscribed {
		if _, ok := desiredSet[pair]; !ok {
			_ = m.sendUnsubscribe(ctx, conn, "market:order-book-"+pair)
			delete(subscribed, pair)
		}
	}
	return nil
}

func (m *IndodaxBookManager) sendSubscribe(ctx context.Context, conn *websocket.Conn, channel string) error {
	frame, _ := json.Marshal(map[string]any{
		"method": 1,
		"params": map[string]string{"channel": channel},
		"id":     m.nextID.Add(1),
	})
	return conn.Write(ctx, websocket.MessageText, frame)
}

func (m *IndodaxBookManager) sendUnsubscribe(ctx context.Context, conn *websocket.Conn, channel string) error {
	frame, _ := json.Marshal(map[string]any{
		"method": 2,
		"params": map[string]string{"channel": channel},
		"id":     m.nextID.Add(1),
	})
	return conn.Write(ctx, websocket.MessageText, frame)
}

// ApplyFrame decodes one Indodax Centrifuge push message into either the
// per-pair order book cache or the live USDT/IDR ticker rate.
func (m *IndodaxBookManager) ApplyFrame(payload []byte, received time.Time) error {
	var envelope struct {
		Result struct {
			Channel string `json:"channel"`
			Data    struct {
				Data json.RawMessage `json:"data"`
			} `json:"data"`
		} `json:"result"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return err
	}
	ch := envelope.Result.Channel
	if ch == "" || len(envelope.Result.Data.Data) == 0 {
		return nil
	}
	if received.IsZero() {
		received = time.Now().UTC()
	}

	if ch == "market:summary-24h" {
		var rows [][]any
		if err := json.Unmarshal(envelope.Result.Data.Data, &rows); err != nil {
			return err
		}
		for _, row := range rows {
			if len(row) < 3 {
				continue
			}
			pair, _ := row[0].(string)
			if strings.EqualFold(pair, "usdtidr") {
				if price, ok := parseAnyFloat(row[2]); ok && price > 0 {
					m.mu.Lock()
					m.usdtIDRRate = price
					m.usdtIDRAt = received.UTC()
					m.mu.Unlock()
				}
				break
			}
		}
		return nil
	}

	if strings.HasPrefix(ch, "market:order-book-") {
		var bookMsg struct {
			Pair string           `json:"pair"`
			Bid  []map[string]any `json:"bid"`
			Ask  []map[string]any `json:"ask"`
		}
		if err := json.Unmarshal(envelope.Result.Data.Data, &bookMsg); err != nil {
			return err
		}
		pair := strings.ToLower(strings.TrimPrefix(ch, "market:order-book-"))
		if bookMsg.Pair != "" {
			pair = strings.ToLower(bookMsg.Pair)
		}
		bids := parseIndodaxWSLevels(pair, bookMsg.Bid, true)
		asks := parseIndodaxWSLevels(pair, bookMsg.Ask, false)
		if len(bids) == 0 || len(asks) == 0 {
			return fmt.Errorf("indodax ws %s book not two-sided", pair)
		}
		m.mu.Lock()
		m.books[pair] = &indodaxWSBookState{
			bids:     bids,
			asks:     asks,
			received: received.UTC(),
			valid:    true,
		}
		m.mu.Unlock()
	}
	return nil
}

func parseIndodaxWSLevels(pair string, raw []map[string]any, descending bool) []domain.PriceLevel {
	quoteSuffix := ""
	basePrefix := ""
	switch {
	case strings.HasSuffix(pair, "idr"):
		quoteSuffix = "idr_volume"
		basePrefix = strings.TrimSuffix(pair, "idr")
	case strings.HasSuffix(pair, "usdt"):
		quoteSuffix = "usdt_volume"
		basePrefix = strings.TrimSuffix(pair, "usdt")
	}
	baseVolumeKey := ""
	if basePrefix != "" {
		baseVolumeKey = basePrefix + "_volume"
	}

	levels := make([]domain.PriceLevel, 0, len(raw))
	for _, entry := range raw {
		price, ok := parseAnyFloat(entry["price"])
		if !ok || price <= 0 {
			continue
		}
		qty := 0.0
		if baseVolumeKey != "" {
			if v, found := parseAnyFloat(entry[baseVolumeKey]); found && v > 0 {
				qty = v
			}
		}
		if qty <= 0 {
			for _, key := range []string{"volume", "base_volume"} {
				if v, found := parseAnyFloat(entry[key]); found && v > 0 {
					qty = v
					break
				}
			}
		}
		if qty <= 0 {
			for k, val := range entry {
				if strings.HasSuffix(k, "_volume") && k != quoteSuffix {
					if v, found := parseAnyFloat(val); found && v > 0 {
						qty = v
						break
					}
				}
			}
		}
		if qty <= 0 && quoteSuffix != "" {
			if qv, found := parseAnyFloat(entry[quoteSuffix]); found && qv > 0 {
				qty = qv / price
			}
		}
		if qty > 0 && !math.IsNaN(qty) && !math.IsInf(qty, 0) {
			levels = append(levels, domain.PriceLevel{Price: price, Quantity: qty})
		}
	}
	sort.Slice(levels, func(i, j int) bool {
		if descending {
			return levels[i].Price > levels[j].Price
		}
		return levels[i].Price < levels[j].Price
	})
	if len(levels) > indodaxWSBookDepth {
		levels = levels[:indodaxWSBookDepth]
	}
	return levels
}

func parseAnyFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, false
		}
		return val, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	case json.Number:
		f, err := val.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}
