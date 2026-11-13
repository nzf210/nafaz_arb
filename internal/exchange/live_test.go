package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestLiveAdapterSpotMarkets(t *testing.T) {
	indodaxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pairs" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[
				{"id":"btcusdt","symbol":"BTCUSDT","base_currency":"usdt","traded_currency":"btc","quantity_increment":0.00000001},
				{"id":"ethusdt","symbol":"ETHUSDT","base_currency":"usdt","traded_currency":"eth","quantity_increment":0.001},
				{"id":"missingincrement","symbol":"MISSINGINCREMENT","base_currency":"usdt","traded_currency":"missing"}
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
						{"type":1,"symbol":"BTC_USDT","baseAsset":"BTC","quoteAsset":"USDT","spotTradingEnable":1,"filters":[{"filterType":"LOT_SIZE","stepSize":"0.00001","minQty":"0.0001"}]},
						{"type":1,"symbol":"ETH_USDT","baseAsset":"ETH","quoteAsset":"USDT","spotTradingEnable":1,"filters":[{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.01"}]}
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
	adapter.SetBaseURLs(indodaxServer.URL, "", "", mexcSpotServer.URL, tokoServer.URL, tokoServer.URL, tokoServer.URL)

	markets, err := adapter.SpotMarkets(context.Background())
	if err != nil {
		t.Fatalf("SpotMarkets: %v", err)
	}
	if len(markets) != 6 {
		t.Fatalf("expected 6 spot markets (2 toko + 2 indodax + 2 mexc), got %d", len(markets))
	}
	foundMEXCSpot := 0
	for _, market := range markets {
		if market.Exchange == "indodax" {
			want := 0.00000001
			if market.Symbol == "ethusdt" {
				want = 0.001
			}
			if market.LotSize != want {
				t.Errorf("Indodax %s quantity increment = %g, want %g", market.Symbol, market.LotSize, want)
			}
			if market.AmountSpec == nil || market.AmountSpec.Unit != "BASE" ||
				market.AmountSpec.Step != want || market.AmountSpec.BasePerUnit != 1 {
				t.Errorf("Indodax %s amount spec = %+v, want BASE step %g", market.Symbol, market.AmountSpec, want)
			}
		}
		if market.Exchange == "tokocrypto" {
			wantStep, wantMin := 0.00001, 0.0001
			if strings.Contains(market.Symbol, "ETH") {
				wantStep, wantMin = 0.001, 0.01
			}
			if market.AmountSpec == nil || market.AmountSpec.Unit != "BASE" ||
				market.AmountSpec.Step != wantStep || market.AmountSpec.Min != wantMin {
				t.Errorf("Tokocrypto %s amount spec = %+v, want step=%g min=%g", market.Symbol, market.AmountSpec, wantStep, wantMin)
			}
		}
		if market.Exchange == "mexc" && strings.HasPrefix(market.Symbol, MEXC_SPOT) {
			foundMEXCSpot++
		}
	}
	if foundMEXCSpot != 2 {
		t.Fatalf("expected 2 MEXC spot markets, got %d", foundMEXCSpot)
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
						"nextFundingTime":"1690028800000",
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
					"nextSettleTime": 1690028800000,
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

	// Fetch instruments from both OKX and MEXC
	perps, err := adapter.PerpetualMarkets(context.Background())
	if err != nil {
		t.Fatalf("PerpetualMarkets: %v", err)
	}
	if len(perps) != 2 {
		t.Fatalf("expected 2 perps (OKX + MEXC), got %d", len(perps))
	}
	if perps[0].AmountSpec == nil || perps[0].AmountSpec.Unit != "CONTRACT" ||
		perps[0].AmountSpec.Step != 0.01 || perps[0].AmountSpec.Min != 0.01 ||
		perps[0].AmountSpec.BasePerUnit != 0.01 {
		t.Errorf("OKX amount spec = %+v, want contract step/min 0.01 and base multiplier 0.01", perps[0].AmountSpec)
	}
	if perps[1].AmountSpec == nil || perps[1].AmountSpec.Unit != "CONTRACT" ||
		perps[1].AmountSpec.Step != 1 || perps[1].AmountSpec.Min != 1 ||
		perps[1].AmountSpec.BasePerUnit != 0.0001 {
		t.Errorf("MEXC amount spec = %+v, want contract step/min 1 and base multiplier 0.0001", perps[1].AmountSpec)
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
		if r.URL.Path == "/open/v1/common/symbols" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"code":0,"data":{"list":[{"type":1,"symbol":"BTC_USDT","baseAsset":"BTC","quoteAsset":"USDT","spotTradingEnable":1,"filters":[{"filterType":"LOT_SIZE","stepSize":"0.00001","minQty":"0.0001"}]}]}}`))
			return
		}
		if r.URL.Path == "/api/v3/depth" && r.URL.Query().Get("symbol") == "BTCUSDT" {
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
	adapter.SetBaseURLs(indodaxServer.URL, "", "", mexcSpotServer.URL, tokoServer.URL, tokoServer.URL, tokoServer.URL)

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

	// MEXC spot depth
	mbook, err := adapter.SpotOrderBook(context.Background(), "mexc_spot:BTCUSDT")
	if err != nil {
		t.Fatalf("SpotOrderBook mexc: %v", err)
	}
	if len(mbook.Bids) != 1 || mbook.Bids[0].Price != 60050 || mbook.Bids[0].Quantity != 1.5 {
		t.Errorf("unexpected mexc spot bids: %+v", mbook.Bids)
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

// Tokocrypto symbol type is authoritative: MAIN and NextMe use different
// market-data hosts and paths. Unknown/legacy types fail closed.
func TestTokocryptoSymbolTypeThreeUsesNextMeDepth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open/v1/common/symbols":
			w.Write([]byte(`{"code":0,"data":{"list":[{"type":3,"symbol":"SOL_IDR","baseAsset":"SOL","quoteAsset":"IDR","spotTradingEnable":1,"filters":[{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001"}]}]}}`))
		case "/api/v1/depth":
			if r.URL.Query().Get("symbol") != "SOLIDR" { t.Errorf("unexpected NextMe symbol: %s", r.URL.Query().Get("symbol")) }
			w.Write([]byte(`{"lastUpdateId":12,"E":1762758825812,"T":1762758825807,"symbol":"SOLIDR","bids":[["100","2"]],"asks":[["101","3"]]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs("", "", "", "", server.URL, server.URL, server.URL)
	markets, err := adapter.SpotMarkets(context.Background())
	if err != nil { t.Fatalf("SpotMarkets: %v", err) }
	if len(markets) != 1 || markets[0].SymbolType != 3 { t.Fatalf("expected symbol type 3 metadata, got %+v", markets) }
	book, err := adapter.SpotOrderBook(context.Background(), "toko:SOL_IDR")
	if err != nil { t.Fatalf("NextMe order book: %v", err) }
	if book.Source != "exchange_timestamp" || book.Timestamp.UnixMilli() != 1762758825812 {
		t.Fatalf("expected exchange event timestamp, got %+v", book)
	}
}

func TestTokocryptoUnknownSymbolTypeFailsClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"code":0,"data":{"list":[{"type":2,"symbol":"ABC_USDT","baseAsset":"ABC","quoteAsset":"USDT","spotTradingEnable":1,"filters":[{"filterType":"LOT_SIZE","stepSize":"1","minQty":"1"}]}]}}`))
	}))
	defer server.Close()
	adapter := NewLiveAdapter()
	adapter.SetBaseURLs("", "", "", "", server.URL, server.URL, server.URL)
	if _, err := adapter.SpotOrderBook(context.Background(), "toko:ABC_USDT"); err == nil {
		t.Fatal("expected unknown Tokocrypto symbol type to fail closed")
	}
}
