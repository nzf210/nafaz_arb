package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func okxFrame(action, symbol string, seq, prev int64, bids, asks string) []byte {
	return []byte(fmt.Sprintf(`{"action":%q,"arg":{"channel":"books","instId":%q},"data":[{"seqId":%d,"prevSeqId":%d,"ts":"1700000000000","bids":%s,"asks":%s}]}`, action, symbol, seq, prev, bids, asks))
}

func newTestOKXManager() *OKXBookManager {
	return NewOKXBookManager("ws://127.0.0.1:1/ws/v5/public", nil, nil, time.Millisecond, 10*time.Millisecond)
}

func TestOKXBookSnapshotAndIncrementalUpdate(t *testing.T) {
	m := newTestOKXManager()
	symbol := "BTC-USDT-SWAP"
	if _, skip, err := m.ApplyFrame(okxFrame("snapshot", symbol, 100, -1, `[["100","2","0","1"],["99","1","0","1"]]`, `[["101","3","0","1"]]`), time.Now()); err != nil || skip {
		t.Fatalf("snapshot: skip=%v err=%v", skip, err)
	}
	book, ok := m.Get(symbol, time.Second)
	if !ok || len(book.Bids) != 2 || book.Bids[0].Price != 100 || book.SequenceID != 100 {
		t.Fatalf("invalid initial snapshot: ok=%v book=%+v", ok, book)
	}
	if _, skip, err := m.ApplyFrame(okxFrame("update", symbol, 101, 100, `[["100","0","0","0"],["98","4","0","1"]]`, `[["101","5","0","1"]]`), time.Now()); err != nil || skip {
		t.Fatalf("incremental update: skip=%v err=%v", skip, err)
	}
	book, ok = m.Get(symbol, time.Second)
	if !ok || book.SequenceID != 101 || book.Bids[0].Price != 99 || book.Bids[1].Price != 98 || book.Asks[0].Quantity != 5 {
		t.Fatalf("delta was not applied to snapshot: ok=%v book=%+v", ok, book)
	}
}

func TestOKXBookRejectsUpdateBeforeSnapshot(t *testing.T) {
	m := newTestOKXManager()
	_, _, err := m.ApplyFrame(okxFrame("update", "BTC-USDT-SWAP", 11, 10, `[["100","1","0","1"]]`, `[]`), time.Now())
	if err == nil || err != ErrOKXBookNoSnapshot {
		t.Fatalf("got %v, want no-snapshot error", err)
	}
	if _, ok := m.Get("BTC-USDT-SWAP", time.Second); ok {
		t.Fatal("book without snapshot must never be served")
	}
}

func TestOKXBookSequenceMismatchInvalidatesCache(t *testing.T) {
	m := newTestOKXManager()
	symbol := "ETH-USDT-SWAP"
	_, _, err := m.ApplyFrame(okxFrame("snapshot", symbol, 20, -1, `[["10","1","0","1"]]`, `[["11","1","0","1"]]`), time.Now())
	if err != nil { t.Fatal(err) }
	_, _, err = m.ApplyFrame(okxFrame("update", symbol, 22, 19, `[["10","2","0","1"]]`, `[]`), time.Now())
	if err == nil || !strings.Contains(err.Error(), ErrOKXBookSequenceGap.Error()) {
		t.Fatalf("got %v, want sequence gap", err)
	}
	if _, ok := m.Get(symbol, time.Second); ok {
		t.Fatal("sequence gap must invalidate the cached book")
	}
}

func TestOKXBookDuplicateAndEmptyUpdateDoNotCorruptBook(t *testing.T) {
	m := newTestOKXManager()
	symbol := "SOL-USDT-SWAP"
	received := time.Now()
	_, _, err := m.ApplyFrame(okxFrame("snapshot", symbol, 5, -1, `[["20","1","0","1"]]`, `[["21","1","0","1"]]`), received)
	if err != nil { t.Fatal(err) }
	_, skip, err := m.ApplyFrame(okxFrame("update", symbol, 5, 4, `[["20","9","0","1"]]`, `[]`), time.Now())
	if err != nil || !skip { t.Fatalf("duplicate must be ignored: skip=%v err=%v", skip, err) }
	_, skip, err = m.ApplyFrame(okxFrame("update", symbol, 5, 5, `[]`, `[]`), received.Add(time.Second))
	if err != nil || !skip { t.Fatalf("empty update must be ignored: skip=%v err=%v", skip, err) }
	book, ok := m.Get(symbol, 3*time.Second)
	if !ok || book.SequenceID != 5 || book.Bids[0].Quantity != 1 || !book.ReceivedAt.Equal(received.Add(time.Second)) {
		t.Fatalf("duplicate update changed book data or heartbeat failed to refresh receive time: ok=%v book=%+v", ok, book)
	}
}

func TestOKXBookRejectsMissingSequenceFields(t *testing.T) {
	m := newTestOKXManager()
	payload := []byte(`{"action":"snapshot","arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"data":[{"prevSeqId":-1,"ts":"1700000000000","bids":[["100","1","0","1"]],"asks":[["101","1","0","1"]]}]}`)
	if _, _, err := m.ApplyFrame(payload, time.Now()); err == nil {
		t.Fatal("snapshot without seqId must be rejected")
	}
	if _, ok := m.Get("BTC-USDT-SWAP", time.Second); ok {
		t.Fatal("snapshot without sequence must never be served")
	}
}

func TestOKXBookAcceptsSequenceZeroSnapshot(t *testing.T) {
	m := newTestOKXManager()
	symbol := "DOGE-USDT-SWAP"
	_, _, err := m.ApplyFrame(okxFrame("snapshot", symbol, 0, -1, `[["0.1","10","0","1"]]`, `[["0.11","10","0","1"]]`), time.Now())
	if err != nil { t.Fatalf("seqId zero is valid for snapshot: %v", err) }
	_, _, err = m.ApplyFrame(okxFrame("update", symbol, 1, 0, `[["0.1","11","0","1"]]`, `[]`), time.Now())
	if err != nil { t.Fatal(err) }
	if book, ok := m.Get(symbol, time.Second); !ok || book.SequenceID != 1 || book.Bids[0].Quantity != 11 {
		t.Fatalf("sequence zero snapshot/update failed: ok=%v book=%+v", ok, book)
	}
}

func TestOKXBookStalesByInstrumentFrameEvenWhenConnectionHeartbeatIsRecent(t *testing.T) {
	m := newTestOKXManager()
	symbol := "LINK-USDT-SWAP"
	oldFrame := time.Now().Add(-70*time.Second)
	if _, _, err := m.ApplyFrame(okxFrame("snapshot", symbol, 1, -1, `[["10","1","0","1"]]`, `[["11","1","0","1"]]`), oldFrame); err != nil { t.Fatal(err) }
	m.refreshReceiveTimes(time.Now())
	if _, ok := m.Get(symbol, 65*time.Second); ok {
		t.Fatal("connection heartbeat must not hide a stale instrument book")
	}
}

func TestOKXBookRejectsStaleCache(t *testing.T) {
	m := newTestOKXManager()
	_, _, err := m.ApplyFrame(okxFrame("snapshot", "XRP-USDT-SWAP", 1, -1, `[["1","1","0","1"]]`, `[["2","1","0","1"]]`), time.Now().Add(-2*time.Second))
	if err != nil { t.Fatal(err) }
	if _, ok := m.Get("XRP-USDT-SWAP", time.Second); ok {
		t.Fatal("stale websocket book must not be served")
	}
}

func TestOKXBookConcurrentReadersAndUpdates(t *testing.T) {
	m := newTestOKXManager()
	symbol := "ADA-USDT-SWAP"
	if _, _, err := m.ApplyFrame(okxFrame("snapshot", symbol, 1, -1, `[["1","1","0","1"]]`, `[["2","1","0","1"]]`), time.Now()); err != nil { t.Fatal(err) }
	var wg sync.WaitGroup
	for worker:=0; worker<16; worker++ {
		wg.Add(1)
		go func(){ defer wg.Done(); for i:=0;i<200;i++ { m.Get(symbol,time.Minute) } }()
	}
	wg.Add(1)
	go func(){ defer wg.Done(); for seq:=int64(2);seq<102;seq++ { _,_,_ = m.ApplyFrame(okxFrame("update",symbol,seq,seq-1,`[["1","1","0","1"]]`,`[]`),time.Now()) } }()
	wg.Wait()
	if book,ok:=m.Get(symbol,time.Minute); !ok || book.SequenceID!=101 { t.Fatalf("unexpected final book: ok=%v book=%+v",ok,book) }
}

func TestOKXWebSocketHitAvoidsREST(t *testing.T) {
	l := NewLiveAdapter()
	manager := newTestOKXManager()
	symbol := "BTC-USDT-SWAP"
	if _, _, err := manager.ApplyFrame(okxFrame("snapshot", symbol, 7, -1, `[["100","1","0","1"]]`, `[["101","1","0","1"]]`), time.Now()); err != nil { t.Fatal(err) }
	l.mu.Lock()
	l.okxBooks = manager
	l.okxWSMaxAge = time.Second
	l.mu.Unlock()
	restCalls := 0
	book, err := l.trackedBookForSymbol("okx", symbol, func() (domain.OrderBook,error) {
		restCalls++
		return domain.OrderBook{}, fmt.Errorf("REST should not be called")
	})
	if err != nil { t.Fatal(err) }
	if restCalls != 0 || book.Source != "websocket" {
		t.Fatalf("WS cache should short-circuit REST: calls=%d source=%s", restCalls, book.Source)
	}
}

func TestOKXBookRESTFallbackSingleFlight(t *testing.T) {
	l:=NewLiveAdapter()
	var calls atomic.Int32
	fetch:=func()(domain.OrderBook,error){
		calls.Add(1); time.Sleep(25*time.Millisecond)
		return domain.OrderBook{Exchange:"okx",Symbol:"BTC-USDT-SWAP",Bids:[]domain.PriceLevel{{Price:100,Quantity:1}},Asks:[]domain.PriceLevel{{Price:101,Quantity:1}}},nil
	}
	var wg sync.WaitGroup
	for i:=0;i<20;i++ { wg.Add(1); go func(){ defer wg.Done(); if _,err:=l.trackedBookForSymbol("okx","BTC-USDT-SWAP",fetch);err!=nil{t.Error(err)} }() }
	wg.Wait()
	if got:=calls.Load(); got!=1 { t.Fatalf("fallback calls=%d, want 1",got) }
}

func TestOKXBookSubscriptionFailureReconnects(t *testing.T) {
	var connections atomic.Int32
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		conn,err:=websocket.Accept(w,r,nil); if err!=nil{return}; defer conn.CloseNow()
		n:=connections.Add(1)
		_,_,err=conn.Read(r.Context()); if err!=nil{return}
		if n==1 {
			_ = conn.Write(r.Context(),websocket.MessageText,[]byte(`{"event":"error","code":"60012","msg":"subscription rejected"}`))
			return
		}
		_ = conn.Write(r.Context(),websocket.MessageText,[]byte(`{"event":"subscribe","code":"0"}`))
		_ = conn.Write(r.Context(),websocket.MessageText,okxFrame("snapshot","BTC-USDT-SWAP",2,-1,`[["100","1","0","1"]]`,`[["101","1","0","1"]]`))
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx,cancel:=context.WithCancel(context.Background()); defer cancel()
	m:=NewOKXBookManager(websocketURL(server.URL,""),nil,nil,10*time.Millisecond,20*time.Millisecond)
	m.SetInstruments([]string{"BTC-USDT-SWAP"}); m.Start(ctx); defer m.Stop()
	deadline:=time.Now().Add(2*time.Second)
	for time.Now().Before(deadline) {
		if connections.Load()>=2 { if _,ok:=m.Get("BTC-USDT-SWAP",time.Second);ok{return} }
		time.Sleep(10*time.Millisecond)
	}
	t.Fatalf("subscription error did not recover: connections=%d",connections.Load())
}

func TestOKXBookMissingSnapshotForcesReconnect(t *testing.T) {
	var connections atomic.Int32
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		conn,err:=websocket.Accept(w,r,nil); if err!=nil{return}; defer conn.CloseNow()
		n:=connections.Add(1)
		_,_,err=conn.Read(r.Context()); if err!=nil{return}
		_ = conn.Write(r.Context(),websocket.MessageText,[]byte(`{"event":"subscribe","code":"0"}`))
		if n==1 { <-r.Context().Done(); return }
		_ = conn.Write(r.Context(),websocket.MessageText,okxFrame("snapshot","BTC-USDT-SWAP",2,-1,`[["100","1","0","1"]]`,`[["101","1","0","1"]]`))
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx,cancel:=context.WithCancel(context.Background()); defer cancel()
	m:=NewOKXBookManager(websocketURL(server.URL,""),nil,nil,10*time.Millisecond,20*time.Millisecond)
	m.snapshotTimeout=50*time.Millisecond
	m.SetInstruments([]string{"BTC-USDT-SWAP"}); m.Start(ctx); defer m.Stop()
	deadline:=time.Now().Add(2*time.Second)
	for time.Now().Before(deadline) {
		if connections.Load()>=2 { if _,ok:=m.Get("BTC-USDT-SWAP",time.Second);ok{return} }
		time.Sleep(10*time.Millisecond)
	}
	t.Fatalf("missing initial snapshot did not trigger recovery: connections=%d",connections.Load())
}

func TestOKXBookHeartbeatPingPong(t *testing.T) {
	pingSeen:=make(chan struct{},1)
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		conn,err:=websocket.Accept(w,r,nil); if err!=nil{return}; defer conn.CloseNow()
		_,_,err=conn.Read(r.Context()); if err!=nil{return}
		_ = conn.Write(r.Context(),websocket.MessageText,[]byte(`{"event":"subscribe","code":"0"}`))
		_ = conn.Write(r.Context(),websocket.MessageText,okxFrame("snapshot","BTC-USDT-SWAP",1,-1,`[["100","1","0","1"]]`,`[["101","1","0","1"]]`))
		_,msg,err:=conn.Read(r.Context()); if err!=nil{return}
		if string(msg)=="ping" {
			pingSeen<-struct{}{}
			_ = conn.Write(r.Context(),websocket.MessageText,[]byte("pong"))
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx,cancel:=context.WithCancel(context.Background()); defer cancel()
	m:=NewOKXBookManager(websocketURL(server.URL,""),nil,nil,10*time.Millisecond,20*time.Millisecond)
	m.heartbeatInterval=40*time.Millisecond
	m.heartbeatTimeout=100*time.Millisecond
	m.SetInstruments([]string{"BTC-USDT-SWAP"}); m.Start(ctx); defer m.Stop()
	select { case <-pingSeen: case <-time.After(time.Second): t.Fatal("client did not send heartbeat ping") }
	deadline:=time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if book,ok:=m.Get("BTC-USDT-SWAP",time.Second); ok && time.Since(book.ReceivedAt)<30*time.Millisecond {
			if !m.IsConnected() { t.Fatal("connection should remain alive after pong") }
			return
		}
		time.Sleep(5*time.Millisecond)
	}
	t.Fatal("pong did not refresh local receive age for unchanged book")
}

func TestOKXBookSubscriptionsAreChunkedForManyInstruments(t *testing.T) {
	var messages atomic.Int32
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		conn,err:=websocket.Accept(w,r,nil); if err!=nil{return}; defer conn.CloseNow()
		for i:=0;i<3;i++ {
			_,msg,err:=conn.Read(r.Context()); if err!=nil{return}
			if !strings.Contains(string(msg),`"channel":"books"`) { return }
			messages.Add(1)
		}
	}))
	defer server.Close()
	conn,_,err:=websocket.Dial(context.Background(),websocketURL(server.URL,""),nil); if err!=nil{t.Fatal(err)}; defer conn.CloseNow()
	symbols:=make([]string,125); for i:=range symbols { symbols[i]=fmt.Sprintf("COIN%d-USDT-SWAP",i) }
	m:=newTestOKXManager()
	if err:=m.sendSubscriptions(context.Background(),conn,"subscribe",symbols);err!=nil{t.Fatal(err)}
	deadline:=time.Now().Add(time.Second)
	for messages.Load()<3 && time.Now().Before(deadline){time.Sleep(time.Millisecond)}
	if got:=messages.Load();got!=3{t.Fatalf("subscription batches=%d, want 3",got)}
}

func TestOKXBookReconnectAndResubscribe(t *testing.T) {
	var connections atomic.Int32
	var subscribed atomic.Int32
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		conn,err:=websocket.Accept(w,r,nil); if err!=nil{return}; defer conn.CloseNow()
		n:=connections.Add(1)
		_,msg,err:=conn.Read(r.Context()); if err!=nil{return}
		if !strings.Contains(string(msg),`"channel":"books"`) || !strings.Contains(string(msg),`"instId":"BTC-USDT-SWAP"`) { return }
		subscribed.Add(1)
		ack:=[]byte(`{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"code":"0"}`)
		_ = conn.Write(r.Context(),websocket.MessageText,ack)
		snapshot:=okxFrame("snapshot","BTC-USDT-SWAP",int64(n),-1,`[["100","1","0","1"]]`,`[["101","1","0","1"]]`)
		_ = conn.Write(r.Context(),websocket.MessageText,snapshot)
		if n==1 { _=conn.Close(websocket.StatusNormalClosure,"force reconnect"); return }
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx,cancel:=context.WithCancel(context.Background()); defer cancel()
	m:=NewOKXBookManager(websocketURL(server.URL,""),nil,nil,10*time.Millisecond,20*time.Millisecond)
	m.SetInstruments([]string{"BTC-USDT-SWAP"})
	m.Start(ctx); defer m.Stop()
	deadline:=time.Now().Add(2*time.Second)
	for time.Now().Before(deadline) {
		if connections.Load()>=2 && subscribed.Load()>=2 {
			if book,ok:=m.Get("BTC-USDT-SWAP",time.Second); ok && book.SequenceID==2 { return }
		}
		time.Sleep(10*time.Millisecond)
	}
	t.Fatalf("reconnect/resubscribe failed: connections=%d subscriptions=%d",connections.Load(),subscribed.Load())
}
