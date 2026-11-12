package tokocrypto

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMarketsNormalizeLotSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":{"list":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","spotTradingEnable":1,"filters":[{"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.002"}]},{"symbol":"OFF","baseAsset":"OFF","quoteAsset":"USDT","spotTradingEnable":0}]}}`))
	}))
	defer server.Close()
	markets, err := New(server.Client(), server.URL).Markets(context.Background())
	if err != nil { t.Fatal(err) }
	if len(markets) != 1 || markets[0].Symbol != "toko:BTCUSDT" { t.Fatalf("unexpected markets: %+v", markets) }
	if markets[0].AmountSpec == nil || markets[0].AmountSpec.Step != .001 { t.Fatalf("unexpected amount spec: %+v", markets[0].AmountSpec) }
}

func TestOrderBookNormalizesDepth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":{"bids":[["100","2"]],"asks":[["101","3"]]}}`))
	}))
	defer server.Close()
	book, err := New(server.Client(), server.URL).OrderBook(context.Background(), "toko:BTCUSDT")
	if err != nil { t.Fatal(err) }
	if book.Exchange != "tokocrypto" || len(book.Bids) != 1 || book.Bids[0].Quantity != 2 { t.Fatalf("unexpected book: %+v", book) }
}
