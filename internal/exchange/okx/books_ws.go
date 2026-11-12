package okx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	booksChannel = "books"
	defaultReconnect = time.Second
	defaultMaxBackoff = 30 * time.Second
	defaultHeartbeat = 3 * time.Second
	defaultPongTimeout = 6 * time.Second
	defaultSnapshotTimeout = 15 * time.Second
	bookDepth = 400
)

var (
	ErrBookSequenceGap = errors.New("okx order book sequence gap")
	ErrBookNoSnapshot = errors.New("okx order book snapshot not ready")
)

type bookState struct {
	bids map[string]float64
	asks map[string]float64
	seq int64
	ts time.Time
	received time.Time
	lastInstrumentAt time.Time
	valid bool
}

// BookManager owns one persistent public connection and validated local books.
// A socket being connected is deliberately not sufficient: Get only serves a
// symbol after its initial snapshot and every subsequent sequence check succeeded.
type BookManager struct {
	url string
	httpClient *http.Client
	log *slog.Logger
	contractSize func(string) float64
	reconnect time.Duration
	maxBackoff time.Duration
	heartbeatInterval time.Duration
	heartbeatTimeout time.Duration
	snapshotTimeout time.Duration

	mu sync.RWMutex
	instruments map[string]struct{}
	books map[string]*bookState
	connected bool
	cancel context.CancelFunc
	done chan struct{}
	updates chan struct{}
}

func NewBookManager(url string, log *slog.Logger, contractSize func(string) float64, reconnect, maxBackoff time.Duration) *BookManager {
	if log == nil { log = slog.Default() }
	if reconnect <= 0 { reconnect = defaultReconnect }
	if maxBackoff <= 0 { maxBackoff = defaultMaxBackoff }; if maxBackoff < reconnect { maxBackoff = reconnect }
	return &BookManager{
		url: url, log: log, contractSize: contractSize,
		reconnect: reconnect, maxBackoff: maxBackoff,
		heartbeatInterval: defaultHeartbeat, heartbeatTimeout: defaultPongTimeout, snapshotTimeout: defaultSnapshotTimeout,
		instruments: make(map[string]struct{}), books: make(map[string]*bookState),
		updates: make(chan struct{}, 1),
	}
}

// SetHTTPClient installs a long-lived websocket handshake client. Call before Start.
func (m *BookManager) SetHTTPClient(client *http.Client) { m.mu.Lock(); m.httpClient = client; m.mu.Unlock() }

func (m *BookManager) Start(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil { m.mu.Unlock(); return }
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel, m.done = cancel, make(chan struct{})
	done := m.done
	m.mu.Unlock()
	go func() { defer close(done); m.run(runCtx) }()
}

func (m *BookManager) Stop() {
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel, m.done = nil, nil
	m.mu.Unlock()
	if cancel != nil { cancel(); <-done }
}

func (m *BookManager) SetInstruments(symbols []string) {
	next := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		s := strings.TrimSpace(strings.TrimPrefix(symbol, "okx:"))
		if s != "" { next[s] = struct{}{} }
	}
	m.mu.Lock()
	m.instruments = next
	m.mu.Unlock()
	select { case m.updates <- struct{}{}: default: }
}

func (m *BookManager) instrumentList() []string {
	m.mu.RLock()
	out := make([]string, 0, len(m.instruments))
	for s := range m.instruments { out = append(out, s) }
	m.mu.RUnlock()
	sort.Strings(out)
	return out
}

func (m *BookManager) refreshReceiveTimes(received time.Time) {
	m.mu.Lock()
	for _, state := range m.books { if state.valid { state.received = received.UTC() } }
	m.mu.Unlock()
}

func (m *BookManager) IsConnected() bool {
	m.mu.RLock(); defer m.mu.RUnlock()
	return m.connected
}

func (m *BookManager) Get(symbol string, maxAge time.Duration) (domain.OrderBook, bool) {
	symbol = strings.TrimPrefix(symbol, "okx:")
	m.mu.RLock()
	state := m.books[symbol]
	if state == nil || !state.valid || state.received.IsZero() || (maxAge > 0 && time.Since(state.lastInstrumentAt) > maxAge) {
		m.mu.RUnlock()
		return domain.OrderBook{}, false
	}
	book := makeOKXBook(symbol, state, 1)
	m.mu.RUnlock()
	return book, true
}

// ApplyFrame applies one OKX books-channel message. Snapshot replaces the local
// book; updates apply price-level deltas. A broken chain invalidates the book.
func (m *BookManager) ApplyFrame(payload []byte, received time.Time) (string, bool, error) {
	var frame struct {
		Event string `json:"event"`
		Code string `json:"code"`
		Msg string `json:"msg"`
		Action string `json:"action"`
		Arg struct { InstID string `json:"instId"` } `json:"arg"`
		Data []struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
			SeqID json.RawMessage `json:"seqId"`
			PrevSeqID json.RawMessage `json:"prevSeqId"`
			Ts string `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &frame); err != nil { return "", false, fmt.Errorf("decode OKX WS frame: %w", err) }
	if frame.Event == "error" || (frame.Code != "" && frame.Code != "0") {
		return frame.Arg.InstID, false, fmt.Errorf("OKX WS subscription/event error code=%s msg=%s", frame.Code, frame.Msg)
	}
	if frame.Event != "" || frame.Action == "" || len(frame.Data) == 0 || frame.Arg.InstID == "" {
		return "", true, nil
	}
	symbol := frame.Arg.InstID
	data := frame.Data[0]
	seqID, seqOK := parseOKXSequence(data.SeqID)
	prevSeqID, prevOK := parseOKXSequence(data.PrevSeqID)
	if !seqOK || seqID < 0 {
		m.invalidateBook(symbol)
		return symbol, false, fmt.Errorf("OKX WS frame has missing or invalid seqId for %s", symbol)
	}
	if !prevOK {
		m.invalidateBook(symbol)
		return symbol, false, fmt.Errorf("OKX WS frame has missing or invalid prevSeqId for %s", symbol)
	}
	if received.IsZero() { received = time.Now().UTC() }
	contract := 1.0
	if m.contractSize != nil {
		if v := m.contractSize(symbol); v > 0 { contract = v }
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.books[symbol]
	if frame.Action == "snapshot" {
		if prevSeqID != -1 {
			m.books[symbol] = &bookState{valid:false}
			return symbol, false, fmt.Errorf("OKX snapshot prevSeqId must be -1 for %s, got %d", symbol, prevSeqID)
		}
		bids, err := parseOKXBookLevels(data.Bids, contract)
		if err != nil {
			m.books[symbol] = &bookState{valid:false}
			return symbol, false, err
		}
		asks, err := parseOKXBookLevels(data.Asks, contract)
		if err != nil {
			m.books[symbol] = &bookState{valid:false}
			return symbol, false, err
		}
		if len(bids) == 0 || len(asks) == 0 {
			m.books[symbol] = &bookState{valid:false}
			return symbol, false, fmt.Errorf("OKX snapshot is not two-sided for %s", symbol)
		}
		ts := parseOKXTimestamp(data.Ts, received)
		m.books[symbol] = &bookState{bids:bids, asks:asks, seq:seqID, ts:ts, received:received.UTC(), lastInstrumentAt:received.UTC(), valid:true}
		return symbol, false, nil
	}
	if frame.Action != "update" { return symbol, true, nil }
	if state == nil || !state.valid { return symbol, false, ErrBookNoSnapshot }
	if len(data.Bids) == 0 && len(data.Asks) == 0 {
		// OKX sends an empty heartbeat with seqId == prevSeqId == last seqId.
		// The levels and exchange timestamp stay unchanged, but the receive
		// timestamp advances because this frame confirms the stream is alive.
		if seqID == state.seq && prevSeqID == state.seq {
			state.received = received.UTC()
			state.lastInstrumentAt = received.UTC()
			return symbol, true, nil
		}
		state.valid = false
		state.bids, state.asks = nil, nil
		return symbol, false, fmt.Errorf("%w: invalid empty update for %s prevSeqId=%d cachedSeqId=%d seqId=%d", ErrBookSequenceGap, symbol, prevSeqID, state.seq, seqID)
	}
	if seqID == state.seq { return symbol, true, nil }
	if seqID < state.seq || prevSeqID != state.seq {
		state.valid = false
		state.bids, state.asks = nil, nil
		return symbol, false, fmt.Errorf("%w: %s prevSeqId=%d cachedSeqId=%d seqId=%d", ErrBookSequenceGap, symbol, prevSeqID, state.seq, seqID)
	}
	if err := applyOKXBookDelta(state.bids, data.Bids, contract); err != nil { state.valid=false; return symbol,false,err }
	if err := applyOKXBookDelta(state.asks, data.Asks, contract); err != nil { state.valid=false; return symbol,false,err }
	if len(state.bids) == 0 || len(state.asks) == 0 {
		state.valid=false
		return symbol,false,fmt.Errorf("OKX incremental update emptied one side for %s", symbol)
	}
	state.seq, state.ts, state.received, state.lastInstrumentAt = seqID, parseOKXTimestamp(data.Ts, received), received.UTC(), received.UTC()
	return symbol, false, nil
}

func (m *BookManager) invalidateBook(symbol string) {
	if symbol == "" { return }
	m.mu.Lock()
	if state := m.books[symbol]; state != nil {
		state.valid = false
		state.bids, state.asks = nil, nil
	}
	m.mu.Unlock()
}

func parseOKXSequence(raw json.RawMessage) (int64, bool) {
	value := strings.TrimSpace(string(raw))
	if value == "" || value == "null" { return 0, false }
	if len(value) >= 2 && value[0] == '"' {
		decoded, err := strconv.Unquote(value)
		if err != nil { return 0, false }
		value = decoded
	}
	seq, err := strconv.ParseInt(value, 10, 64)
	return seq, err == nil
}

func parseOKXBookLevels(levels [][]string, contract float64) (map[string]float64, error) {
	out := make(map[string]float64, len(levels))
	for _, level := range levels {
		if len(level) < 2 { return nil, fmt.Errorf("malformed OKX order book level") }
		price, err := strconv.ParseFloat(level[0], 64); if err != nil || price <= 0 { return nil, fmt.Errorf("invalid OKX book price %q", level[0]) }
		size, err := strconv.ParseFloat(level[1], 64); if err != nil || size < 0 { return nil, fmt.Errorf("invalid OKX book size %q", level[1]) }
		if size == 0 { delete(out, level[0]) } else { out[level[0]] = size * contract }
	}
	return out, nil
}

func applyOKXBookDelta(book map[string]float64, levels [][]string, contract float64) error {
	for _, level := range levels {
		if len(level) < 2 { return fmt.Errorf("malformed OKX incremental level") }
		price, err := strconv.ParseFloat(level[0],64); if err != nil || price <= 0 { return fmt.Errorf("invalid OKX delta price %q",level[0]) }
		size, err := strconv.ParseFloat(level[1],64); if err != nil || size < 0 { return fmt.Errorf("invalid OKX delta size %q",level[1]) }
		if size == 0 { delete(book,level[0]) } else { book[level[0]]=size*contract }
	}
	return nil
}

func parseOKXTimestamp(raw string, fallback time.Time) time.Time {
	if ms,err:=strconv.ParseInt(raw,10,64); err==nil && ms>0 { return time.UnixMilli(ms).UTC() }
	return fallback.UTC()
}

func makeOKXBook(symbol string, state *bookState, _ float64) domain.OrderBook {
	bids:=sortedOKXLevels(state.bids,true)
	asks:=sortedOKXLevels(state.asks,false)
	return domain.OrderBook{Exchange:"okx",Symbol:symbol,Bids:bids,Asks:asks,Timestamp:state.ts,ReceivedAt:state.received,SequenceID:state.seq,Source:"websocket"}
}

func sortedOKXLevels(levels map[string]float64, descending bool) []domain.PriceLevel {
	type item struct { price, qty float64 }
	items:=make([]item,0,len(levels))
	for raw,qty:=range levels { p,err:=strconv.ParseFloat(raw,64); if err==nil && p>0 && qty>0 { items=append(items,item{p,qty}) } }
	sort.Slice(items,func(i,j int)bool { if descending { return items[i].price>items[j].price }; return items[i].price<items[j].price })
	if len(items)>bookDepth { items=items[:bookDepth] }
	out:=make([]domain.PriceLevel,0,len(items))
	for _,v:=range items { out=append(out,domain.PriceLevel{Price:v.price,Quantity:v.qty}) }
	return out
}

type okxWSRead struct { typ websocket.MessageType; data []byte; err error }

func (m *BookManager) run(ctx context.Context) {
	delay:=m.reconnect
	for ctx.Err()==nil {
		err:=m.consume(ctx)
		if ctx.Err()!=nil { return }
		m.mu.Lock(); m.connected=false
		// A disconnected stream cannot be used as a live cache source.
		for _,s:=range m.books { s.valid=false }
		m.mu.Unlock()
		if err!=nil { m.log.Warn("OKX books websocket disconnected","err",err,"retry_in",delay) }
		timer:=time.NewTimer(delay)
		select { case <-ctx.Done(): timer.Stop(); return; case <-timer.C: }
		delay*=2; if delay>m.maxBackoff { delay=m.maxBackoff }
	}
}

func (m *BookManager) consume(ctx context.Context) error {
	conn,_,err:=websocket.Dial(ctx,m.url,&websocket.DialOptions{HTTPClient:nil})
	if err!=nil { return fmt.Errorf("dial OKX public WS: %w",err) }
	defer conn.CloseNow()
	conn.SetReadLimit(4<<20)
	m.mu.Lock(); m.connected=true; m.mu.Unlock()

	if err:=m.sendSubscriptions(ctx,conn,"subscribe",m.instrumentList()); err!=nil { return err }
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	reads:=make(chan okxWSRead,128)
	go func(){
		for { typ,data,err:=conn.Read(readCtx); select { case reads<-okxWSRead{typ,data,err}: case <-readCtx.Done(): return }; if err!=nil{return} }
	}()
	tickInterval:=m.heartbeatInterval/4
	if m.snapshotTimeout > 0 && (tickInterval <= 0 || m.snapshotTimeout/4 < tickInterval) { tickInterval = m.snapshotTimeout/4 }
	if tickInterval<=0 { tickInterval=time.Second }
	ticker:=time.NewTicker(tickInterval); defer ticker.Stop()
	var pingSent time.Time
	current:=make(map[string]struct{})
	pendingSnapshots:=make(map[string]time.Time)
	for _,s:=range m.instrumentList(){current[s]=struct{}{}; pendingSnapshots[s]=time.Now()}
	for {
		select {
		case <-ctx.Done(): return nil
		case <-ticker.C:
			if m.snapshotTimeout > 0 {
				for symbol, since := range pendingSnapshots {
					if time.Since(since) > m.snapshotTimeout { return fmt.Errorf("OKX WS initial snapshot timeout for %s", symbol) }
				}
			}
			if !pingSent.IsZero() && time.Since(pingSent)>m.heartbeatTimeout { return fmt.Errorf("OKX WS pong timeout") }
			if pingSent.IsZero() {
				if err:=conn.Write(ctx,websocket.MessageText,[]byte("ping")); err!=nil{return fmt.Errorf("OKX WS heartbeat write: %w",err)}
				pingSent=time.Now()
			}
		case <-m.updates:
			next:=m.instrumentList()
			added,removed:=diffOKXSubscriptions(current,next)
			if err:=m.sendSubscriptions(ctx,conn,"unsubscribe",removed); err!=nil{return err}
			for _,s:=range removed { delete(pendingSnapshots,s) }
			if err:=m.sendSubscriptions(ctx,conn,"subscribe",added); err!=nil{return err}
			for _,s:=range added { pendingSnapshots[s]=time.Now() }
			current=make(map[string]struct{},len(next)); for _,s:=range next{current[s]=struct{}{}}
		case frame:=<-reads:
			if frame.err!=nil{return fmt.Errorf("OKX WS read: %w",frame.err)}
			if frame.typ!=websocket.MessageText { continue }
			text:=strings.TrimSpace(string(frame.data))
			if text=="pong" { pingSent=time.Time{}; m.refreshReceiveTimes(time.Now().UTC()); continue }
			if text=="ping" { if err:=conn.Write(ctx,websocket.MessageText,[]byte("pong"));err!=nil{return err}; m.refreshReceiveTimes(time.Now().UTC()); continue }
			symbol,skip,applyErr:=m.ApplyFrame(frame.data,time.Now().UTC())
			if applyErr!=nil {
				// Any invalid book frame forces a fresh connection and a complete
				// resubscription. This avoids a burst of per-symbol unsubscribe/subscribe
				// operations when many instruments encounter a sequence gap together.
				m.log.Warn("invalid OKX book frame; reconnecting for fresh snapshots","symbol",symbol,"err",applyErr)
				return applyErr
			}
			if symbol!="" && !skip { delete(pendingSnapshots,symbol) }
		}
	}
}

func diffOKXSubscriptions(current map[string]struct{}, next []string) (added,removed []string) {
	wanted:=make(map[string]struct{},len(next))
	for _,s:=range next { wanted[s]=struct{}{}; if _,ok:=current[s];!ok { added=append(added,s) } }
	for s:=range current { if _,ok:=wanted[s];!ok { removed=append(removed,s) } }
	sort.Strings(added); sort.Strings(removed)
	return
}

func (m *BookManager) sendSubscriptions(ctx context.Context, conn *websocket.Conn, op string, symbols []string) error {
	const batchSize=50
	for start:=0;start<len(symbols);start+=batchSize {
		end:=start+batchSize; if end>len(symbols){end=len(symbols)}
		args:=make([]map[string]string,0,end-start)
		for _,s:=range symbols[start:end] { args=append(args,map[string]string{"channel":booksChannel,"instId":s}) }
		payload,err:=json.Marshal(map[string]any{"op":op,"args":args}); if err!=nil{return err}
		if err:=conn.Write(ctx,websocket.MessageText,payload);err!=nil{return fmt.Errorf("OKX %s subscription: %w",op,err)}
	}
	return nil
}
