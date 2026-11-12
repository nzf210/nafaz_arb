package indodax

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMarketsNormalizePairMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pairs" { t.Fatalf("path=%s", r.URL.Path) }
		_, _ = w.Write([]byte(`[{"id":"btcidr","base_currency":"idr","traded_currency":"btc","quantity_increment":0.0001},{"id":"bad","base_currency":"idr","traded_currency":"bad","quantity_increment":0}]`))
	}))
	defer server.Close()

	markets, err := New(server.Client(), server.URL).Markets(context.Background())
	if err != nil { t.Fatal(err) }
	if len(markets) != 1 { t.Fatalf("markets=%d, want 1", len(markets)) }
	got := markets[0]
	if got.Exchange != "indodax" || got.Symbol != "btcidr" || got.BaseAsset != "BTC" || got.QuoteAsset != "IDR" {
		t.Fatalf("unexpected normalized market: %+v", got)
	}
	if got.AmountSpec == nil || got.AmountSpec.Step != .0001 { t.Fatalf("missing amount spec: %+v", got.AmountSpec) }
}

func TestOrderBookNormalizesLevels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/depth/btcidr" { t.Fatalf("path=%s", r.URL.Path) }
		_, _ = w.Write([]byte(`{"buy":[["100","2"],["0","4"],["bad","3"]],"sell":[[101,1]]}`))
	}))
	defer server.Close()

	book, err := New(server.Client(), server.URL).OrderBook(context.Background(), "BTCIDR")
	if err != nil { t.Fatal(err) }
	if book.Exchange != "indodax" || len(book.Bids) != 1 || len(book.Asks) != 1 {
		t.Fatalf("unexpected book: %+v", book)
	}
	if book.Bids[0].Price != 100 || book.Bids[0].Quantity != 2 { t.Fatalf("unexpected bid: %+v", book.Bids[0]) }
}
