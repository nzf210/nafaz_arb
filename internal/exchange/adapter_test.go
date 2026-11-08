package exchange

import (
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestMatchMarketsWithSynthetic(t *testing.T) {
	spot := []domain.Market{
		{Exchange: "indodax", Symbol: "btcidr", BaseAsset: "BTC", QuoteAsset: "IDR", MarketType: "SPOT", Active: true},
		{Exchange: "indodax", Symbol: "ethidr", BaseAsset: "ETH", QuoteAsset: "IDR", MarketType: "SPOT", Active: true},
		{Exchange: "tokocrypto", Symbol: "toko:SOLBIDR", BaseAsset: "SOL", QuoteAsset: "BIDR", MarketType: "SPOT", Active: true},
		{Exchange: "tokocrypto", Symbol: "toko:BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
	}
	perp := []domain.Market{
		{Exchange: "okx", Symbol: "BTC-USDT-SWAP", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
		{Exchange: "okx", Symbol: "ETH-USDT-SWAP", BaseAsset: "ETH", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
		{Exchange: "mexc", Symbol: "SOL_USDT", BaseAsset: "SOL", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
	}
	quotes := []string{"USDT", "USDC"}

	// When synthetic IDR is false, only direct USDT match (toko:BTCUSDT vs BTC-USDT-SWAP) should match
	directPairs := MatchMarketsWithSynthetic(spot, perp, quotes, false)
	if len(directPairs) != 1 {
		t.Fatalf("expected 1 direct pair, got %d", len(directPairs))
	}
	if directPairs[0][0].Symbol != "toko:BTCUSDT" || directPairs[0][1].Symbol != "BTC-USDT-SWAP" {
		t.Fatalf("unexpected direct pair: %+v", directPairs[0])
	}

	// When synthetic IDR is true, btcidr, ethidr, and toko:SOLBIDR should ALSO pair with their USDT perps!
	synthPairs := MatchMarketsWithSynthetic(spot, perp, quotes, true)
	// Expect: toko:BTCUSDT vs BTC-USDT-SWAP (direct), btcidr vs BTC-USDT-SWAP (synth),
	// ethidr vs ETH-USDT-SWAP (synth), toko:SOLBIDR vs SOL_USDT (synth) = total 4 pairs!
	if len(synthPairs) != 4 {
		t.Fatalf("expected 4 matched pairs with synthetic IDR enabled, got %d", len(synthPairs))
	}

	foundIDR := false
	foundBIDR := false
	for _, p := range synthPairs {
		if p[0].Symbol == "btcidr" && p[1].Symbol == "BTC-USDT-SWAP" {
			foundIDR = true
		}
		if p[0].Symbol == "toko:SOLBIDR" && p[1].Symbol == "SOL_USDT" {
			foundBIDR = true
		}
	}
	if !foundIDR {
		t.Fatal("expected btcidr to pair with BTC-USDT-SWAP")
	}
	if !foundBIDR {
		t.Fatal("expected toko:SOLBIDR to pair with SOL_USDT")
	}
}


func TestMatchMarketsForExchangesBuildsIndependentRoutes(t *testing.T) {
	spot := []domain.Market{
		{Exchange: "indodax", Symbol: "btcidr", BaseAsset: "BTC", QuoteAsset: "IDR", MarketType: "SPOT", Active: true},
		{Exchange: "tokocrypto", Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
		{Exchange: "mexc", Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "SPOT", Active: true},
	}
	perp := []domain.Market{
		{Exchange: "okx", Symbol: "BTC-USDT-SWAP", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
		{Exchange: "mexc", Symbol: "BTC_USDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
		{Exchange: "binance", Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
	}

	routes := MatchMarketsForExchanges(
		spot, perp, []string{"USDT", "USDC"},
		[]string{"indodax", "tokocrypto"},
		[]string{"okx", "mexc", "binance"},
		true,
	)
	if len(routes) != 6 {
		t.Fatalf("expected 2 approved spot venues x 3 perp venues = 6 routes, got %d", len(routes))
	}
	for _, route := range routes {
		if route[0].Exchange != "indodax" && route[0].Exchange != "tokocrypto" {
			t.Errorf("unapproved spot venue entered route: %s", route[0].Exchange)
		}
		if route[1].Exchange == "mexc" && route[0].Exchange == "mexc" {
			t.Error("foreign spot venue must not enter the configured spot universe")
		}
	}
}

func TestMatchMarketsForExchangesEmptyAllowlistMeansAllDiscovered(t *testing.T) {
	spot := []domain.Market{{Exchange: "indodax", Symbol: "btcidr", BaseAsset: "BTC", QuoteAsset: "IDR", MarketType: "SPOT", Active: true}}
	perp := []domain.Market{
		{Exchange: "okx", Symbol: "BTC-USDT-SWAP", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
		{Exchange: "mexc", Symbol: "BTC_USDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true},
	}
	routes := MatchMarketsForExchanges(spot, perp, []string{"USDT"}, nil, nil, true)
	if len(routes) != 2 {
		t.Fatalf("empty allowlists should retain all discovered perp routes, got %d", len(routes))
	}
}
