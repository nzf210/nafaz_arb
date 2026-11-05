package exchange

import (
	"context"
	"testing"
	"time"
)

func TestFundingWorker_PollAndRetrieve(t *testing.T) {
	worker := NewFundingWorker(30, "http://mock")

	now := time.Now().UTC()
	futureFunding := now.Add(25 * time.Minute)

	worker.SetFetchFunc(func(ctx context.Context) ([]FundingInfo, error) {
		return []FundingInfo{
			{
				Symbol:          "BTCUSDT",
				LastFundingRate: 0.0003,
				NextFundingTime: futureFunding,
			},
			{
				Symbol:          "ETHUSDT",
				LastFundingRate: -0.0001,
				NextFundingTime: futureFunding,
			},
		}, nil
	})

	var negativeObserved []string
	worker.SetOnNegativeCallback(func(symbol string, info FundingInfo) {
		negativeObserved = append(negativeObserved, symbol)
	})

	if err := worker.PollOnce(context.Background()); err != nil {
		t.Fatalf("poll failed: %v", err)
	}

	info, ok := worker.GetFundingInfo("BTCUSDT")
	if !ok {
		t.Fatal("expected BTCUSDT in cache")
	}
	if info.LastFundingRate != 0.0003 {
		t.Errorf("got rate %f, want 0.0003", info.LastFundingRate)
	}
	if info.MinutesToNextFunding <= 0 || info.MinutesToNextFunding > 26 {
		t.Errorf("got minutes %f, want ~25", info.MinutesToNextFunding)
	}

	// Cross-symbol query: BTC-USDT-SWAP should find BTCUSDT
	infoSwap, ok := worker.GetFundingInfo("BTC-USDT-SWAP")
	if !ok {
		t.Fatal("expected BTC-USDT-SWAP to resolve to BTCUSDT")
	}
	if infoSwap.LastFundingRate != 0.0003 {
		t.Errorf("got swap rate %f, want 0.0003", infoSwap.LastFundingRate)
	}

	// Cross-symbol query: mexc:BTC_USDT should find BTCUSDT
	infoMexc, ok := worker.GetFundingInfo("mexc:BTC_USDT")
	if !ok {
		t.Fatal("expected mexc:BTC_USDT to resolve to BTCUSDT")
	}
	if infoMexc.LastFundingRate != 0.0003 {
		t.Errorf("got mexc rate %f, want 0.0003", infoMexc.LastFundingRate)
	}

	if len(negativeObserved) != 1 || negativeObserved[0] != "ETHUSDT" {
		t.Errorf("got negativeObserved=%v, want [ETHUSDT]", negativeObserved)
	}
}
