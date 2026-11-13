package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestIndodaxLimiter403CooldownBlocksSubsequentCalls(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs(server.URL, "", "", "", "")

	if _, err := adapter.SpotOrderBook(context.Background(), "btcidr"); err == nil {
		t.Fatal("expected 403 error")
	}
	if _, err := adapter.SpotOrderBook(context.Background(), "ethidr"); err == nil || !strings.Contains(err.Error(), "cooldown active") {
		t.Fatalf("expected cooldown active error on second call, got: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected only 1 HTTP request before cooldown blocked follow-ups, got %d", got)
	}
}

func TestUSDTIDRRateSingleflightAndTokocryptoFallback(t *testing.T) {
	var indodaxHits atomic.Int32
	var tokoHits atomic.Int32

	indodaxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		indodaxHits.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer indodaxServer.Close()

	tokoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open/v1/market/depth" && r.URL.Query().Get("symbol") == "USDT_IDR" {
			tokoHits.Add(1)
			time.Sleep(30 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"data":{"bids":[["18250","100"]],"asks":[["18260","100"]]}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer tokoServer.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs(indodaxServer.URL, "", "", "", tokoServer.URL)

	var wg sync.WaitGroup
	rates := make([]float64, 10)
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rates[idx], errs[idx] = adapter.USDTIDRRate(context.Background())
		}(i)
	}
	wg.Wait()

	for i := range rates {
		if errs[i] != nil {
			t.Fatalf("worker %d unexpected err: %v", i, errs[i])
		}
		if rates[i] != 18255 {
			t.Fatalf("worker %d rate=%v, want 18255", i, rates[i])
		}
	}
	if got := indodaxHits.Load(); got != 1 {
		t.Fatalf("expected singleflight to send 1 indodax call, got %d", got)
	}
	if got := tokoHits.Load(); got != 1 {
		t.Fatalf("expected singleflight to send 1 tokocrypto fallback call, got %d", got)
	}
}

func TestIndodaxDiskMarketCacheSurvivesRestartOn403(t *testing.T) {
	cacheDir := t.TempDir()
	indodaxOK := true
	indodaxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !indodaxOK {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"btcidr","symbol":"BTCIDR","base_currency":"idr","traded_currency":"btc","quantity_increment":0.00001}]`))
	}))
	defer indodaxServer.Close()

	tokoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"list":[]}}`))
	}))
	defer tokoServer.Close()

	mexcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":200,"data":[]}`))
	}))
	defer mexcServer.Close()

	first := NewLiveAdapter()
	first.SetMarketCacheDir(cacheDir)
	first.SetBaseURLs(indodaxServer.URL, "", "", mexcServer.URL, tokoServer.URL)
	if mkts, err := first.SpotMarkets(context.Background()); err != nil || len(mkts) != 1 {
		t.Fatalf("initial SpotMarkets failed: len=%d err=%v", len(mkts), err)
	}

	// Simulate restart with Indodax returning 403
	indodaxOK = false
	second := NewLiveAdapter()
	second.SetMarketCacheDir(cacheDir)
	second.SetBaseURLs(indodaxServer.URL, "", "", mexcServer.URL, tokoServer.URL)
	second.ResetScanTelemetry()

	mkts, err := second.SpotMarkets(context.Background())
	if err != nil || len(mkts) != 1 || mkts[0].Exchange != "indodax" {
		t.Fatalf("expected disk fallback indodax market, len=%d err=%v mkts=%+v", len(mkts), err, mkts)
	}
	metrics, errs := second.ConsumeScanTelemetry()
	foundIndodaxMetric := false
	for _, m := range metrics {
		if m.Exchange == "indodax" && m.Errors == 1 {
			foundIndodaxMetric = true
		}
	}
	if !foundIndodaxMetric {
		t.Fatalf("expected indodax discovery error metric even on fallback, got %+v", metrics)
	}
	if len(errs) == 0 || !strings.Contains(errs[0], "indodax spot discovery") {
		t.Fatalf("expected indodax discovery error surfaced, got %v", errs)
	}
}

func TestIndodaxBookManagerReceivesBooksAndUSDTIDR(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		// Auth frame
		_, authBytes, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var authReq map[string]any
		_ = json.Unmarshal(authBytes, &authReq)
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"id":1,"result":{"client":"c1"}}`))

		for {
			_, msg, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var req struct {
				Method int `json:"method"`
				Params struct {
					Channel string `json:"channel"`
				} `json:"params"`
				ID int `json:"id"`
			}
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}
			if req.Method == 1 && req.Params.Channel == "market:summary-24h" {
				_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"result":{"channel":"market:summary-24h","data":{"data":[["usdtidr",1794491000,18240,18100,18300,18200,"1000","100"]]}}}`))
			}
			if req.Method == 1 && req.Params.Channel == "market:order-book-btcidr" {
				_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"result":{"channel":"market:order-book-btcidr","data":{"data":{"pair":"btcidr","bid":[{"btc_volume":"0.5","idr_volume":"700000000","price":"1400000000"}],"ask":[{"btc_volume":"0.25","idr_volume":"350250000","price":"1401000000"}]}}}}`))
			}
		}
	}))
	defer server.Close()

	mgr := NewIndodaxBookManager(websocketURL(server.URL, "/"), "test-token", nil, 10*time.Millisecond, 50*time.Millisecond)
	mgr.SetPairs([]string{"btcidr"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		book, okBook := mgr.Get("btcidr", 5*time.Second)
		rate, okRate := mgr.USDTIDRRate(5 * time.Second)
		if okBook && okRate {
			if book.Source != "websocket" || len(book.Bids) != 1 || book.Bids[0].Quantity != 0.5 || book.Asks[0].Price != 1401000000 {
				t.Fatalf("unexpected WS book: %+v", book)
			}
			if rate != 18240 {
				t.Fatalf("rate=%v, want 18240", rate)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Indodax WS book and USDTIDR rate")
}

func TestTokocryptoLimiter429Cooldown(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs("", "", "", "", server.URL)

	if _, err := adapter.SpotOrderBook(context.Background(), "toko:BTC_USDT"); err == nil {
		t.Fatal("expected 429 error")
	}
	if _, err := adapter.SpotOrderBook(context.Background(), "toko:ETH_USDT"); err == nil || !strings.Contains(err.Error(), "cooldown active") {
		t.Fatalf("expected cooldown active error on second call, got: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected only 1 HTTP request before cooldown blocked follow-ups, got %d", got)
	}
}

