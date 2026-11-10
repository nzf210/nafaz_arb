package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestTokocryptoPublicDepthRoutesBySymbolType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v3/depth":
			if r.URL.Query().Get("symbol") != "BTCUSDT" {
				t.Errorf("unexpected MBX symbol: %s", r.URL.Query().Get("symbol"))
			}
			_, _ = w.Write([]byte(`{"lastUpdateId":77,"bids":[["100","2"]],"asks":[["101","3"]],"timestamp":1700000000000}`))
		case "/api/v1/depth":
			if r.URL.Query().Get("symbol") != "BTCIDR" {
				t.Errorf("unexpected NextMe symbol: %s", r.URL.Query().Get("symbol"))
			}
			_, _ = w.Write([]byte(`{"lastUpdateId":88,"E":1700000000001,"bids":[["1000","4"]],"asks":[["1001","5"]]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs("", "", "", "", server.URL, server.URL, server.URL)
	adapter.tokocryptoSymbolTypes["toko:BTC_USDT"] = 1
	adapter.tokocryptoSymbolTypes["toko:BTC_IDR"] = 3

	mbx, err := adapter.fetchTokocryptoSpotOrderBook(context.Background(), "toko:BTC_USDT")
	if err != nil {
		t.Fatalf("MBX public depth: %v", err)
	}
	if mbx.SequenceID != 77 || mbx.Bids[0].Quantity != 2 {
		t.Fatalf("unexpected MBX snapshot: %+v", mbx)
	}
	nextMe, err := adapter.fetchTokocryptoSpotOrderBook(context.Background(), "toko:BTC_IDR")
	if err != nil {
		t.Fatalf("NextMe public depth: %v", err)
	}
	if nextMe.SequenceID != 88 || nextMe.Bids[0].Quantity != 4 {
		t.Fatalf("unexpected NextMe snapshot: %+v", nextMe)
	}
}

func TestTokocryptoMBXDepthFrameUsesCanonicalMarketSymbol(t *testing.T) {
	cache := NewWSBookCache()
	manager := newTokocryptoBookManager(NewLiveAdapter(), cache, []domain.Market{
		{Exchange: "tokocrypto", Symbol: "toko:BTC_USDT", ExchangeSymbolType: 1, Active: true},
	})
	state := manager.state("toko:BTC_USDT")
	state.initialized = true
	state.lastSeq = 11
	state.bids = map[string]float64{"100": 1}
	state.asks = map[string]float64{"101": 3}
	message := []byte(`{"stream":"btcusdt@depth@100ms","data":{"E":0,"s":"BTCUSDT","U":12,"u":13,"b":[["100","2"],["99","1"]],"a":[["101","0"],["102","3"]]}}`)
	_, skip, err := manager.handleMBXFrame(message)
	if err != nil || !skip {
		t.Fatalf("handle MBX diff frame: skip=%v err=%v", skip, err)
	}
	book, ok, _, _, _, _ := cache.Get("tokocrypto", "toko:BTC_USDT", time.Second)
	if !ok {
		t.Fatal("updated MBX book was not stored")
	}
	if book.Symbol != "toko:BTC_USDT" || book.Exchange != "tokocrypto" {
		t.Fatalf("unexpected book identity: %+v", book)
	}
	if len(book.Bids) != 2 || book.Bids[0].Price != 100 || book.Bids[0].Quantity != 2 {
		t.Fatalf("MBX diff was not applied to local book: %+v", book.Bids)
	}
	if len(book.Asks) != 1 || book.Asks[0].Price != 102 {
		t.Fatalf("MBX ask deletion/update was not applied: %+v", book.Asks)
	}
}

func TestTokocryptoNextMeSnapshotSynchronizesBufferedDiffs(t *testing.T) {
	cache := NewWSBookCache()
	manager := newTokocryptoBookManager(NewLiveAdapter(), cache, []domain.Market{
		{Exchange: "tokocrypto", Symbol: "toko:BTC_IDR", ExchangeSymbolType: 3, Active: true},
	})
	state := manager.state("toko:BTC_IDR")
	state.buffer = []tokocryptoDepthEvent{{
		Symbol: "BTCIDR", First: 101, Last: 102, Prev: 100,
		Bids: [][]string{{"100", "0"}, {"99", "2"}},
		Asks: [][]string{{"102", "1"}},
	}}
	snapshot := domain.OrderBook{
		Exchange: "tokocrypto", Symbol: "toko:BTC_IDR", SequenceID: 100,
		Bids: []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks: []domain.PriceLevel{{Price: 103, Quantity: 1}},
		Timestamp: time.Now().UTC(),
	}
	if !manager.installSnapshot(state, snapshot) {
		t.Fatal("expected snapshot and buffered diff to synchronize")
	}
	book, ok, _, _, _, _ := cache.Get("tokocrypto", "toko:BTC_IDR", time.Second)
	if !ok {
		t.Fatal("synchronized NextMe book not stored")
	}
	if len(book.Bids) != 1 || book.Bids[0].Price != 99 || book.Bids[0].Quantity != 2 {
		t.Fatalf("buffered absolute-quantity diff was not applied: %+v", book.Bids)
	}
}

func TestTokocryptoNextMeRejectsSequenceGap(t *testing.T) {
	cache := NewWSBookCache()
	manager := newTokocryptoBookManager(NewLiveAdapter(), cache, []domain.Market{
		{Exchange: "tokocrypto", Symbol: "toko:BTC_IDR", ExchangeSymbolType: 3, Active: true},
	})
	state := manager.state("toko:BTC_IDR")
	state.initialized = true
	state.lastSeq = 100
	state.bids = map[string]float64{"100": 1}
	state.asks = map[string]float64{"101": 1}
	event := []byte(`{"stream":"btcidr@depth","data":{"s":"BTCIDR","U":102,"u":103,"pu":99,"b":[["100","0"]],"a":[]}}`)
	_, skip, err := manager.handleNextMeFrame(event)
	if err != nil || !skip {
		t.Fatalf("gap frame should be consumed but not served: skip=%v err=%v", skip, err)
	}
	if state.initialized {
		t.Fatal("sequence gap must invalidate the local book")
	}
	if _, ok, _, _, _, _ := cache.Get("tokocrypto", "toko:BTC_IDR", time.Second); ok {
		t.Fatal("invalidated book must not be served")
	}
}

func TestTokocryptoWebSocketModeDoesNotFallbackToREST(t *testing.T) {
	adapter := NewLiveAdapter()
	adapter.tokocryptoWSClient = NewWSClient(NewWSBookCache(), nil)
	called := false
	_, err := adapter.trackedBookForSymbol("tokocrypto", "toko:BTC_USDT", func() (domain.OrderBook, error) {
		called = true
		return domain.OrderBook{}, nil
	})
	if err == nil {
		t.Fatal("expected missing websocket book to fail closed")
	}
	if called {
		t.Fatal("REST fallback must not run while Tokocrypto websocket mode is active")
	}
}

func TestTokocryptoHeartbeatTouchesOnlyConnectedEngineSymbols(t *testing.T) {
	cache := NewWSBookCache()
	now := time.Now().UTC()
	for _, symbol := range []string{"toko:BTC_USDT", "toko:BTC_IDR"} {
		_, _, _, err := cache.Store(domain.OrderBook{
			Exchange: "tokocrypto", Symbol: symbol,
			Bids: []domain.PriceLevel{{Price: 100, Quantity: 1}},
			Asks: []domain.PriceLevel{{Price: 101, Quantity: 1}},
			Timestamp: now.Add(-time.Minute), ReceivedAt: now.Add(-10 * time.Second),
		})
		if err != nil { t.Fatal(err) }
	}
	cache.TouchSymbols("tokocrypto", []string{"toko:BTC_USDT"})
	if _, ok, _, _, _, _ := cache.Get("tokocrypto", "toko:BTC_USDT", 3*time.Second); !ok {
		t.Fatal("heartbeat should refresh the connected MBX symbol")
	}
	if _, ok, _, _, _, _ := cache.Get("tokocrypto", "toko:BTC_IDR", 3*time.Second); ok {
		t.Fatal("MBX heartbeat must not refresh a disconnected NextMe symbol")
	}
}
