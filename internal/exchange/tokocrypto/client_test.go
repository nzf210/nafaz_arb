package tokocrypto

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMarketsNormalizeLotSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":{"list":[{"symbol":"BTC_USDT","baseAsset":"BTC","quoteAsset":"USDT","spotTradingEnable":1,"symbolType":1,"filters":[{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.002"}]},{"symbol":"OFF","baseAsset":"OFF","quoteAsset":"USDT","spotTradingEnable":0}]}}`))
	}))
	defer server.Close()
	markets, err := New(server.Client(), server.URL).Markets(context.Background())
	if err != nil { t.Fatal(err) }
	if len(markets) != 1 || markets[0].Symbol != "toko:BTC_USDT" { t.Fatalf("unexpected markets: %+v", markets) }
	if markets[0].AmountSpec == nil || markets[0].AmountSpec.Step != .001 { t.Fatalf("unexpected amount spec: %+v", markets[0].AmountSpec) }
}

func TestOrderBookNormalizesDepth(t *testing.T) {
	requested := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = r.URL.Path + "?" + r.URL.RawQuery
		_, _ = w.Write([]byte(`{"lastUpdateId":22,"bids":[["100","2"]],"asks":[["101","3"]]}`))
	}))
	defer server.Close()
	book, err := New(server.Client(), server.URL).OrderBook(context.Background(), "toko:BTC_USDT")
	if err != nil { t.Fatal(err) }
	if requested != "/api/v3/depth?symbol=BTCUSDT&limit=20" { t.Fatalf("MBX symbol was not normalized: %s", requested) }
	if book.Exchange != "tokocrypto" || len(book.Bids) != 1 || book.Bids[0].Quantity != 2 { t.Fatalf("unexpected book: %+v", book) }
}

func TestNextMeOrderBookUsesSymbolTypeThreeEndpoint(t *testing.T) {
	requested := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/open/v1/common/symbols" {
			_, _ = w.Write([]byte(`{"code":0,"data":{"list":[{"symbol":"BTC_IDR","type":3,"baseAsset":"BTC","quoteAsset":"IDR"}]}}`))
			return
		}
		requested = r.URL.Path + "?" + r.URL.RawQuery
		_, _ = w.Write([]byte(`{"lastUpdateId":33,"E":1700000000000,"symbol":"BTCIDR","bids":[["100","2"]],"asks":[["101","3"]]}`))
	}))
	defer server.Close()

	client := New(server.Client(), server.URL)
	markets, err := client.Markets(context.Background())
	if err != nil { t.Fatal(err) }
	if len(markets) != 1 || markets[0].SymbolType != 3 || markets[0].Symbol != "toko:BTC_IDR" { t.Fatalf("symbol type not preserved: %+v", markets) }
	book, err := client.OrderBook(context.Background(), "toko:BTC_IDR")
	if err != nil { t.Fatal(err) }
	if requested != "/api/v1/depth?symbol=BTCIDR&limit=20" { t.Fatalf("unexpected NextMe request: %s", requested) }
	if book.SequenceID != 33 || book.Timestamp.IsZero() { t.Fatalf("unexpected NextMe book: %+v", book) }
}

func TestUnsupportedTokocryptoSymbolTypeDoesNotUseMBXEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":{"list":[{"symbol":"BTCIDR","symbolType":2,"baseAsset":"BTC","quoteAsset":"IDR","spotTradingEnable":1}]}}`))
	}))
	defer server.Close()
	client := New(server.Client(), server.URL)
	markets, err := client.Markets(context.Background())
	if err != nil { t.Fatal(err) }
	if len(markets) != 0 { t.Fatalf("unsupported symbol type leaked into scanner markets: %+v", markets) }
	if _, err := client.OrderBook(context.Background(), "toko:BTCIDR"); err == nil { t.Fatal("expected unsupported symbol type error") }
}

