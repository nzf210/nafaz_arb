package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/mexc"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/tokocrypto"
)

const (
	venueWSBufferSize = 512
	venueWSMaxBackoff = 30 * time.Second
)

// venueBookSupervisor owns one independently recoverable connection per symbol.
// This deliberately isolates a bad/stale symbol from the rest of the venue.
type venueBookSupervisor struct {
	name string
	mu sync.Mutex
	ctx context.Context
	cancel context.CancelFunc
	workers map[string]context.CancelFunc
	connected map[string]bool
	lastMessage map[string]time.Time
	wg sync.WaitGroup
	wsURL func(string) string
	subscribe func(string) []byte
	snapshot func(context.Context, string) error
	apply func(string, []byte) error
	invalidate func(string)
	heartbeat []byte
}

func newVenueBookSupervisor(name string, wsURL func(string) string, subscribe func(string) []byte,
	snapshot func(context.Context, string) error, apply func(string, []byte) error,
	invalidate func(string), heartbeat []byte) *venueBookSupervisor {
	return &venueBookSupervisor{name:name, wsURL:wsURL, subscribe:subscribe, snapshot:snapshot,
		apply:apply, invalidate:invalidate, heartbeat:heartbeat, workers:make(map[string]context.CancelFunc), connected:make(map[string]bool), lastMessage:make(map[string]time.Time)}
}

func (s *venueBookSupervisor) Start(parent context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil { return }
	s.ctx, s.cancel = context.WithCancel(parent)
}

func (s *venueBookSupervisor) SetSymbols(symbols []string) {
	unique := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.TrimSpace(symbol)
		if symbol != "" { unique[symbol] = struct{}{} }
	}
	s.mu.Lock()
	if s.ctx == nil || s.ctx.Err() != nil { s.mu.Unlock(); return }
	for symbol, cancel := range s.workers {
		if _, keep := unique[symbol]; !keep { cancel(); delete(s.workers, symbol) }
	}
	for symbol := range unique {
		if _, exists := s.workers[symbol]; exists { continue }
		ctx, cancel := context.WithCancel(s.ctx)
		s.workers[symbol] = cancel
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.runSymbol(ctx, symbol) }()
	}
	s.mu.Unlock()
}

func (s *venueBookSupervisor) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	for symbol, stop := range s.workers { stop(); delete(s.workers, symbol) }
	s.ctx = nil
	s.mu.Unlock()
	if cancel != nil { cancel() }
	s.wg.Wait()
}

func (s *venueBookSupervisor) runSymbol(ctx context.Context, symbol string) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := s.consumeSymbol(ctx, symbol)
		s.setConnected(symbol,false)
		if ctx.Err() != nil { return }
		if err != nil { slog.Warn("venue WebSocket reconnect scheduled","venue",s.name,"symbol",symbol,"error",err,"backoff",backoff) }
		if s.invalidate != nil { s.invalidate(symbol) }
		timer := time.NewTimer(backoff)
		select { case <-ctx.Done(): timer.Stop(); return; case <-timer.C: }
		if backoff < venueWSMaxBackoff { backoff *= 2; if backoff > venueWSMaxBackoff { backoff = venueWSMaxBackoff } }
		_ = err
	}
}

func (s *venueBookSupervisor) consumeSymbol(ctx context.Context, symbol string) error {
	conn, _, err := websocket.Dial(ctx, s.wsURL(symbol), &websocket.DialOptions{HTTPClient:wsHTTPClient()})
	if err != nil { return fmt.Errorf("%s %s dial: %w", s.name, symbol, err) }
	defer conn.CloseNow()
	s.setConnected(symbol,true)
	defer s.setConnected(symbol,false)
	conn.SetReadLimit(2 << 20)
	if frame := s.subscribe(symbol); len(frame) > 0 {
		if err := conn.Write(ctx, websocket.MessageText, frame); err != nil { return fmt.Errorf("%s %s subscribe: %w", s.name, symbol, err) }
	}

	// Read immediately after subscribe; events are bounded while REST snapshot is fetched.
	frames := make(chan []byte, venueWSBufferSize)
	readErr := make(chan error, 1)
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	go func() {
		for {
			_, payload, readErrValue := conn.Read(readCtx)
			if readErrValue != nil { select { case readErr <- readErrValue: default: }; return }
			select {
			case frames <- payload:
			default:
				select { case readErr <- fmt.Errorf("%s %s bootstrap buffer overflow", s.name, symbol): default: }
				return
			}
		}
	}()

	if err := s.snapshot(ctx, symbol); err != nil { return fmt.Errorf("%s %s snapshot: %w", s.name, symbol, err) }
	for {
		select {
		case err := <-readErr: return err
		case payload := <-frames:
			if err := s.apply(symbol, payload); err != nil { return fmt.Errorf("%s %s sequence/decode: %w", s.name, symbol, err) }; s.markMessage(symbol)
		default:
			goto synchronized
		}
	}

synchronized:
	if len(s.heartbeat) > 0 {
		ticker := time.NewTicker(15*time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done(): return ctx.Err()
			case err := <-readErr: return err
			case payload := <-frames:
				if err := s.apply(symbol, payload); err != nil { return fmt.Errorf("%s %s sequence/decode: %w", s.name, symbol, err) }; s.markMessage(symbol)
			case <-ticker.C:
				if err := conn.Write(ctx, websocket.MessageText, s.heartbeat); err != nil { return fmt.Errorf("%s %s heartbeat: %w", s.name, symbol, err) }
			}
		}
	}
	for {
		select {
		case <-ctx.Done(): return ctx.Err()
		case err := <-readErr: return err
		case payload := <-frames:
			if err := s.apply(symbol, payload); err != nil { return fmt.Errorf("%s %s sequence/decode: %w", s.name, symbol, err) }; s.markMessage(symbol)
		}
	}
}

func (l *LiveAdapter) newTokocryptoSupervisor() *venueBookSupervisor {
	return newVenueBookSupervisor("tokocrypto",
		func(symbol string) string { return "wss://stream-cloud.tokocrypto.site/ws/"+strings.ToLower(symbol)+"@depth" },
		func(string) []byte { return nil },
		func(ctx context.Context, symbol string) error {
			req, err := l.client.Get("https://www.tokocrypto.site/api/v3/depth?symbol="+url.QueryEscape(symbol)+"&limit=1000")
			if err != nil { return err }; defer req.Body.Close()
			if req.StatusCode != 200 { return fmt.Errorf("snapshot HTTP %d", req.StatusCode) }
			var payload json.RawMessage
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil { return err }
			snapshot, err := tokocrypto.DecodeDepthSnapshot(payload); if err != nil { return err }
			l.mu.RLock(); manager := l.tokocryptoBooks; l.mu.RUnlock()
			if manager == nil { return fmt.Errorf("Tokocrypto book manager is disabled") }
			return manager.Bootstrap(symbol, snapshot)
		},
		func(symbol string, payload []byte) error {
			update, err := tokocrypto.DecodeDepthUpdate(payload); if err != nil { return err }
			if update.Symbol != "" && !strings.EqualFold(update.Symbol, symbol) { return nil }
			update.Symbol = symbol
			l.mu.RLock(); manager := l.tokocryptoBooks; l.mu.RUnlock()
			if manager == nil { return fmt.Errorf("Tokocrypto book manager is disabled") }
			_, err = manager.Apply(update); return err
		},
		func(symbol string) { l.mu.RLock(); m:=l.tokocryptoBooks; l.mu.RUnlock(); if m!=nil { m.Invalidate(symbol) } }, nil)
}

func (l *LiveAdapter) newMEXCPerpetualSupervisor() *venueBookSupervisor {
	return newVenueBookSupervisor("mexc-perpetual",
		func(string) string { return "wss://contract.mexc.com/edge" },
		func(symbol string) []byte { frame, _ := json.Marshal(map[string]any{"method":"sub.depth","param":map[string]any{"symbol":symbol,"compress":false}}); return frame },
		func(ctx context.Context, symbol string) error {
			l.mu.RLock(); base:=l.mexcBaseURL; client:=l.client; size:=l.contractSizes[symbol]; manager:=l.mexcPerpBooks; l.mu.RUnlock()
			if size<=0 { return fmt.Errorf("missing contract size for %s",symbol) }
			req, err := client.Get(strings.TrimRight(base,"/")+"/api/v1/contract/depth/"+url.PathEscape(symbol)+"?limit=1000")
			if err != nil { return err }; defer req.Body.Close()
			if req.StatusCode!=200 { return fmt.Errorf("snapshot HTTP %d",req.StatusCode) }
			var raw struct { Success bool `json:"success"`; Code int `json:"code"`; Data struct { Version int64 `json:"version"`; Bids [][]json.RawMessage `json:"bids"`; Asks [][]json.RawMessage `json:"asks"`; Timestamp int64 `json:"timestamp"` } `json:"data"` }
			if err:=json.NewDecoder(req.Body).Decode(&raw);err!=nil{return err}
			if !raw.Success||raw.Data.Version<=0{return fmt.Errorf("invalid MEXC depth snapshot code=%d version=%d",raw.Code,raw.Data.Version)}
			bids,err:=decodeMEXCContractRows(raw.Data.Bids);if err!=nil{return err}; asks,err:=decodeMEXCContractRows(raw.Data.Asks);if err!=nil{return err}
			at:=time.Now().UTC();if raw.Data.Timestamp>0{at=time.UnixMilli(raw.Data.Timestamp).UTC()}
			return manager.Bootstrap(symbol,mexc.PerpetualDepthSnapshot{Version:raw.Data.Version,Bids:bids,Asks:asks,Timestamp:at},size)
		},
		func(symbol string,payload []byte) error {
			var envelope struct { Channel string `json:"channel"` }; _ = json.Unmarshal(payload,&envelope)
			if envelope.Channel != "" && envelope.Channel != "push.depth" { return nil }
			l.mu.RLock(); size:=l.contractSizes[symbol]; manager:=l.mexcPerpBooks; l.mu.RUnlock()
			update,err:=mexc.DecodePerpetualDepthUpdate(payload,size);if err!=nil{return err}
			if !strings.EqualFold(update.Symbol,symbol){return nil}; update.Symbol=symbol; _,err=manager.Apply(update);return err
		},
		func(symbol string) { l.mu.RLock(); m:=l.mexcPerpBooks; l.mu.RUnlock(); if m!=nil { m.Invalidate(symbol) } },
		[]byte(`{"method":"ping"}`))
}

func (s *venueBookSupervisor) setConnected(symbol string, connected bool) {
	s.mu.Lock(); s.connected[symbol]=connected; if !connected { delete(s.lastMessage,symbol) }; s.mu.Unlock()
}
func (s *venueBookSupervisor) markMessage(symbol string) {
	s.mu.Lock(); s.lastMessage[symbol]=time.Now().UTC(); s.mu.Unlock()
}
func (s *venueBookSupervisor) Healthy(maxAge time.Duration) bool {
	s.mu.Lock(); defer s.mu.Unlock()
	if len(s.workers)==0 { return true }
	now:=time.Now()
	for symbol:=range s.workers { if !s.connected[symbol] { return false }; last:=s.lastMessage[symbol]; if last.IsZero() || (maxAge>0 && now.Sub(last)>maxAge) { return false } }
	return true
}

func decodeMEXCContractRows(rows [][]json.RawMessage) ([][]float64,error) {
	out:=make([][]float64,0,len(rows))
	for _,row:=range rows { if len(row)<2{return nil,fmt.Errorf("depth row has fewer than two values")}; var p,q float64
		if err:=json.Unmarshal(row[0],&p);err!=nil{return nil,err}; if err:=json.Unmarshal(row[1],&q);err!=nil{return nil,err}
		out=append(out,[]float64{p,q})
	}
	return out,nil
}

func sortedUniqueSymbols(markets []string) []string {
	set:=map[string]struct{}{};for _,s:=range markets{if strings.TrimSpace(s)!=""{set[s]=struct{}{}}}
	out:=make([]string,0,len(set));for s:=range set{out=append(out,s)};sort.Strings(out);return out
}
