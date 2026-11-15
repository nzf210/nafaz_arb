package scanner

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	redis "github.com/redis/go-redis/v9"
)

func TestPublishMarketSnapshot_SavesWithTTL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rc := redishandler.NewWithClient(rdb)

	cfg := config.Config{
		Mode: "MOCK",
	}
	svc := New(cfg, exchange.NewMockAdapter())
	svc.SetRedisClient(rc)

	ctx := context.Background()
	snap := MarketSnapshot{
		PairTag:        "tokocrypto:btc_usdt__binance:btcusdt",
		SpotVenue:      "tokocrypto",
		FuturesVenue:   "binance",
		SpotBestBid:    65000.0,
		SpotBestAsk:    65010.0,
		SpotBidQty:     1.5,
		SpotAskQty:     2.0,
		FuturesBestBid: 65100.0,
		FuturesBestAsk: 65105.0,
		UsdIdrRate:     16000.0,
		UpdatedAtMs:    time.Now().UnixMilli(),
	}

	svc.publishMarketSnapshot(ctx, snap)

	key := "arb:market_snapshot:tokocrypto:btc_usdt__binance:btcusdt"
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("expected key %s in redis: %v", key, err)
	}

	var got MarketSnapshot
	if err := json.Unmarshal([]byte(val), &got); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if got.PairTag != snap.PairTag || got.SpotBestBid != 65000.0 || got.FuturesBestAsk != 65105.0 {
		t.Fatalf("unexpected snapshot content: %+v", got)
	}

	ttl := mr.TTL(key)
	if ttl <= 0 || ttl > 15*time.Second {
		t.Fatalf("expected TTL around 15s, got %v", ttl)
	}
}

func TestScan_PublishesMarketSnapshots(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rc := redishandler.NewWithClient(rdb)

	cfg := config.Config{
		Mode:                  "MOCK",
		QuoteAssets:           []string{"USDT"},
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		PerpLeverage:          1,
		EstimatedHoldingHours: 8,
		MinNetROIPct:          0.5,
		MaxNetROIPct:          2,
		MaxSlippagePct:        1,
		SpotTakerFeePct:       0.3,
		PerpTakerFeePct:       0.05,
		CheckOpenPositions:    false,
		ExcludeOpenOrders:     false,
	}
	svc := New(cfg, exchange.NewMockAdapter())
	svc.SetRedisClient(rc)

	_, complete, err := svc.Scan(context.Background())
	if err != nil || !complete {
		t.Fatalf("scan failed: complete=%v err=%v", complete, err)
	}

	keys := mr.Keys()
	found := false
	for _, k := range keys {
		if len(k) > len("arb:market_snapshot:") && k[:len("arb:market_snapshot:")] == "arb:market_snapshot:" {
			found = true
			ttl := mr.TTL(k)
			if ttl <= 0 || ttl > 15*time.Second {
				t.Fatalf("expected 15s TTL on key %s, got %v", k, ttl)
			}
		}
	}
	if !found {
		t.Fatalf("expected at least one arb:market_snapshot:* key in redis, got keys: %v", keys)
	}
}

func TestScannerConfig_HotReloadViaPubSub(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rc := redishandler.NewWithClient(rdb)

	cfg := config.Config{
		Mode:                "MOCK",
		MinNetROIPct:        0.3,
		MaxNetROIPct:        10.0,
		ScanIntervalSeconds: 30,
	}
	svc := New(cfg, exchange.NewMockAdapter())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc.SetRedisClient(rc)
	svc.StartConfigSubscriber(ctx)
	time.Sleep(20 * time.Millisecond)

	// Publish single param update
	err = rc.PublishConfigUpdate(ctx, "minroi", 0.75)
	if err != nil {
		t.Fatalf("publish config update: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if got := svc.MinNetROIPct(); got != 0.75 {
		t.Fatalf("expected MinNetROIPct=0.75, got %v", got)
	}

	// Publish scan interval update
	err = rc.PublishConfigUpdate(ctx, "scan", 15)
	if err != nil {
		t.Fatalf("publish config update: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if got := svc.ScanIntervalSeconds(); got != 15 {
		t.Fatalf("expected ScanIntervalSeconds=15, got %v", got)
	}

	// Publish maxroi update
	err = rc.PublishConfigUpdate(ctx, "maxroi", 25.0)
	if err != nil {
		t.Fatalf("publish config update: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if got := svc.MaxNetROIPct(); got != 25.0 {
		t.Fatalf("expected MaxNetROIPct=25.0, got %v", got)
	}
}
