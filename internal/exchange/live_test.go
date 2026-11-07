package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestLiveAdapterSpotMarkets(t *testing.T) {
	indodaxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pairs" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[
				{"id":"btcusdt","symbol":"BTCUSDT","base_currency":"usdt","traded_currency":"btc"},
				{"id":"ethusdt","symbol":"ETHUSDT","base_currency":"usdt","traded_currency":"eth"}
			]`))
			return
		}
		http.NotFound(w, r)
	}))
	defer indodaxServer.Close()

	tokoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open/v1/common/symbols" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{
				"code": 0,
				"data": {
					"list": [
						{"symbol":"BTC_USDT","baseAsset":"BTC","quoteAsset":"USDT","spotTradingEnable":1},
						{"symbol":"ETH_USDT","baseAsset":"ETH","quoteAsset":"USDT","spotTradingEnable":1}
					]
				}
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer tokoServer.Close()

	mexcSpotServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/defaultSymbols" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{
				"code": 200,
				"data": ["BTCUSDT", "SOLUSDT"]
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer mexcSpotServer.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs(indodaxServer.URL, "", "", mexcSpotServer.URL, tokoServer.URL)

	markets, err := adapter.SpotMarkets(context.Background())
	if err != nil {
		t.Fatalf("SpotMarkets: %v", err)
	}
	if len(markets) != 4 {
		t.Fatalf("expected 4 local spot markets (2 Tokocrypto + 2 Indodax), got %d", len(markets))
	}
	for _, market := range markets {
		if market.Exchange != "indodax" && market.Exchange != "tokocrypto" {
			t.Fatalf("foreign exchange %q leaked into local spot discovery", market.Exchange)
		}
	}
}

func newMockOKXPerpServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v5/public/instruments":
			w.Write([]byte(`{
				"code":"0",
				"msg":"",
				"data":[
					{
						"instId":"BTC-USDT-SWAP",
						"instType":"SWAP",
						"ctVal":"0.01",
						"ctValCcy":"BTC",
						"settleCcy":"USDT",
						"state":"live",
						"tickSz":"0.1",
						"lotSz":"0.01",
						"minSz":"0.01"
					}
				]
			}`))
		case "/api/v5/market/books":
			w.Write([]byte(`{
				"code":"0",
				"msg":"",
				"data":[
					{
						"bids":[["60000","100","0","1"]],
						"asks":[["60050","200","0","1"]],
						"ts":"1690000000000"
					}
				]
			}`))
		case "/api/v5/public/funding-rate":
			w.Write([]byte(`{
				"code":"0",
				"msg":"",
				"data":[
					{
						"fundingRate":"0.0001",
						"fundingTime":"1690028800000",
						"prevFundingTime":"1690000000000",
						"ts":"1690000000000"
					}
				]
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func newMockMEXCPerpServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/contract/detail":
			w.Write([]byte(`{
				"success": true,
				"code": 0,
				"data": [
					{
						"symbol": "BTC_USDT",
						"baseCoin": "BTC",
						"quoteCoin": "USDT",
						"contractSize": 0.0001,
						"priceUnit": 0.1,
						"volUnit": 1,
						"minVol": 1,
						"state": 0
					}
				]
			}`))
		case "/api/v1/contract/depth/BTC_USDT":
			w.Write([]byte(`{
				"success": true,
				"code": 0,
				"data": {
					"bids": [[60010, 10000, 1]],
					"asks": [[60040, 20000, 1]],
					"timestamp": 1690000000000
				}
			}`))
		case "/api/v1/contract/funding_rate/BTC_USDT":
			w.Write([]byte(`{
				"success": true,
				"code": 0,
				"data": {
					"symbol": "BTC_USDT",
					"fundingRate": 0.00015,
					"collectCycle": 8,
					"timestamp": 1690000000000
				}
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestLiveAdapterPerpetualMarketsAndDepth(t *testing.T) {
	okxServer := newMockOKXPerpServer()
	defer okxServer.Close()

	mexcServer := newMockMEXCPerpServer()
	defer mexcServer.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs("", okxServer.URL, mexcServer.URL)

	// Fetch instruments from the selected OKX and MEXC perpetual venues.
	adapter.SetTargetPerpExchange("okx,mexc")
	perps, err := adapter.PerpetualMarkets(context.Background())
	if err != nil {
		t.Fatalf("PerpetualMarkets: %v", err)
	}
	if len(perps) != 2 {
		t.Fatalf("expected 2 perps (OKX + MEXC), got %d", len(perps))
	}

	// OKX Order book (100 * 0.01 = 1.0 BTC)
	okxBook, err := adapter.PerpetualOrderBook(context.Background(), "BTC-USDT-SWAP")
	if err != nil {
		t.Fatalf("OKX PerpetualOrderBook: %v", err)
	}
	if len(okxBook.Bids) != 1 || okxBook.Bids[0].Quantity != 1.0 {
		t.Errorf("expected normalized OKX bid qty 1.0, got %f", okxBook.Bids[0].Quantity)
	}

	// MEXC Order book (10000 * 0.0001 = 1.0 BTC)
	mexcBook, err := adapter.PerpetualOrderBook(context.Background(), "mexc:BTC_USDT")
	if err != nil {
		t.Fatalf("MEXC PerpetualOrderBook: %v", err)
	}
	if len(mexcBook.Bids) != 1 || mexcBook.Bids[0].Quantity != 1.0 {
		t.Errorf("expected normalized MEXC bid qty 1.0, got %f", mexcBook.Bids[0].Quantity)
	}

	// OKX Funding rate
	okxFR, err := adapter.FundingRate(context.Background(), "BTC-USDT-SWAP")
	if err != nil {
		t.Fatalf("OKX FundingRate: %v", err)
	}
	if okxFR.Rate != 0.0001 {
		t.Errorf("expected OKX rate 0.0001, got %f", okxFR.Rate)
	}

	// MEXC Funding rate
	mexcFR, err := adapter.FundingRate(context.Background(), "mexc:BTC_USDT")
	if err != nil {
		t.Fatalf("MEXC FundingRate: %v", err)
	}
	if mexcFR.Rate != 0.00015 {
		t.Errorf("expected MEXC rate 0.00015, got %f", mexcFR.Rate)
	}
}

func TestLiveAdapterSpotDepth(t *testing.T) {
	indodaxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/depth/btcusdt" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{
				"buy":[["60000","0.5"],["59900","1.2"]],
				"sell":[["60100","0.8"],["60200","2.0"]]
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer indodaxServer.Close()

	tokoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open/v1/market/depth" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{
				"code": 0,
				"data": {
					"bids":[["60020","2.0"]],
					"asks":[["60030","3.0"]]
				}
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer tokoServer.Close()

	mexcSpotServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/depth" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{
				"bids":[["60050","1.5"]],
				"asks":[["60060","2.5"]]
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer mexcSpotServer.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs(indodaxServer.URL, "", "", mexcSpotServer.URL, tokoServer.URL)

	// Tokocrypto spot depth
	tbook, err := adapter.SpotOrderBook(context.Background(), "toko:BTC_USDT")
	if err != nil {
		t.Fatalf("SpotOrderBook tokocrypto: %v", err)
	}
	if len(tbook.Bids) != 1 || tbook.Bids[0].Price != 60020 || tbook.Bids[0].Quantity != 2.0 {
		t.Errorf("unexpected tokocrypto bids: %+v", tbook.Bids)
	}

	// Indodax spot depth
	ibook, err := adapter.SpotOrderBook(context.Background(), "btcusdt")
	if err != nil {
		t.Fatalf("SpotOrderBook indodax: %v", err)
	}
	if len(ibook.Bids) != 2 || ibook.Bids[0].Price != 60000 {
		t.Errorf("unexpected indodax bids: %+v", ibook.Bids)
	}

	// Foreign CEX must never be routed through the spot order-book interface.
	if _, err := adapter.SpotOrderBook(context.Background(), "mexc_spot:BTCUSDT"); err == nil {
		t.Fatal("expected MEXC spot order book to be rejected")
	}
}

func TestMatchMarketsMultiplePerpExchanges(t *testing.T) {
	spot := []domain.Market{
		{Exchange: "tokocrypto", Symbol: "toko:BTC_USDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
		{Exchange: "indodax", Symbol: "btcusdt", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
	}
	perp := []domain.Market{
		{Exchange: "okx", Symbol: "BTC-USDT-SWAP", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
		{Exchange: "mexc", Symbol: "mexc:BTC_USDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
	}

	pairs := MatchMarkets(spot, perp, []string{"USDT"})
	if len(pairs) != 4 {
		t.Fatalf("expected 4 matched pairs (2 spot * 2 perp), got %d", len(pairs))
	}
}
