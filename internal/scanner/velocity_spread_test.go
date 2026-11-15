package scanner

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	redis "github.com/redis/go-redis/v9"
)

func TestSpreadTracker_StructuralSpreadTrap(t *testing.T) {
	st := &SpreadTracker{}

	// Feed 30 samples with constant spread = 1.8% (EMA will be 1.8, stdDev will be 0.0)
	var isStructural bool
	var ema, stdDev float64
	for i := 0; i < 30; i++ {
		ema, stdDev, _, isStructural = st.Update(1.8)
	}

	if !isStructural {
		t.Fatalf("expected structural spread trap for constant 1.8%% spread, got false (ema=%.4f, stdDev=%.4f)", ema, stdDev)
	}
	if ema <= 1.5 {
		t.Errorf("expected ema > 1.5, got %.4f", ema)
	}
	if stdDev >= 0.08 {
		t.Errorf("expected stdDev < 0.08, got %.4f", stdDev)
	}

	// First 29 samples must NOT trigger structural trap
	st2 := &SpreadTracker{}
	for i := 0; i < 29; i++ {
		_, _, _, isStructural = st2.Update(1.8)
		if isStructural {
			t.Fatalf("sample %d triggered structural trap prematurely", i+1)
		}
	}
}

func TestSpreadTracker_DynamicSpread(t *testing.T) {
	st := &SpreadTracker{}

	// Feed 30 samples with mean 1.8% but fluctuating (e.g. 1.6% to 2.0%)
	var isStructural bool
	var ema, stdDev float64
	for i := 0; i < 30; i++ {
		val := 1.8
		if i%2 == 0 {
			val = 1.6
		} else {
			val = 2.0
		}
		ema, stdDev, _, isStructural = st.Update(val)
	}

	if isStructural {
		t.Fatalf("dynamic spread should NOT trigger structural trap (ema=%.4f, stdDev=%.4f)", ema, stdDev)
	}
	if stdDev < 0.08 {
		t.Errorf("expected stdDev >= 0.08 for fluctuating spread, got %.4f", stdDev)
	}
}

func TestSpreadTracker_LowSpreadQuiet(t *testing.T) {
	st := &SpreadTracker{}

	// Constant 0.5% spread (below 1.5% threshold)
	var isStructural bool
	for i := 0; i < 35; i++ {
		_, _, _, isStructural = st.Update(0.5)
	}

	if isStructural {
		t.Fatalf("spread below 1.5%% should not be structural spread trap")
	}
}

func TestTradeVelocityScore_Formula(t *testing.T) {
	// TargetOrderUSD = 100, denom = 300
	targetUSD := 100.0

	// Case 1: 0 turnover -> velocity = 0
	if v := calculateTradeVelocityScore(0, targetUSD); v != 0 {
		t.Errorf("expected 0, got %f", v)
	}

	// Case 2: 150 turnover -> 150/300 = 0.5
	if v := calculateTradeVelocityScore(150, targetUSD); math.Abs(v-0.5) > 1e-6 {
		t.Errorf("expected 0.5, got %f", v)
	}

	// Case 3: 450 turnover -> 450/300 = 1.5
	if v := calculateTradeVelocityScore(450, targetUSD); math.Abs(v-1.5) > 1e-6 {
		t.Errorf("expected 1.5, got %f", v)
	}

	// Case 4: 900 turnover -> capped at 1.5
	if v := calculateTradeVelocityScore(900, targetUSD); math.Abs(v-1.5) > 1e-6 {
		t.Errorf("expected 1.5 capped, got %f", v)
	}
}

func TestPriorityScore_ActiveBeatsFrozen(t *testing.T) {
	targetOrderUSD := 100.0

	// Coin A: NetROIPct = +0.9%, active transactions every minute (SpotTurnover5mUSD = 450)
	scoreA, velA := calculatePriorityScore(0.9, 450.0, targetOrderUSD, 1.0)
	if velA != 1.5 {
		t.Errorf("expected velocity 1.5 for Coin A, got %f", velA)
	}
	expectedScoreA := 0.9 * 1.5 * 1.0 // 1.35
	if math.Abs(scoreA-expectedScoreA) > 1e-6 {
		t.Errorf("expected Coin A score %.4f, got %.4f", expectedScoreA, scoreA)
	}

	// Coin B: NetROIPct = +2.2%, frozen orderbook for 30 minutes (SpotTurnover5mUSD = 0)
	scoreB, velB := calculatePriorityScore(2.2, 0.0, targetOrderUSD, 1.0)
	if velB != 0.0 {
		t.Errorf("expected velocity 0 for Coin B, got %f", velB)
	}
	if scoreB != 0.0 {
		t.Errorf("expected Coin B score 0, got %f", scoreB)
	}

	// Active coin A MUST beat frozen coin B
	if scoreA <= scoreB {
		t.Fatalf("active coin A (score %.4f) should have higher priority score than frozen coin B (score %.4f)", scoreA, scoreB)
	}
}

func TestEvaluatePair_StructuralSpreadExcluded(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rc := redishandler.NewWithClient(rdb)
	defer rc.Close()

	now := time.Now().UTC()

	// Spot bid 1,000,000,000 IDR (approx 62,500 USDT)
	// Perp bid 65,000 USDT -> entry spread ~ 3.8%
	adapter := &customMockAdapter{
		rate: 16000.0,
		spotBook: domain.OrderBook{
			Exchange: "indodax", Symbol: "BTC_IDR", Timestamp: now,
			Bids: []domain.PriceLevel{{Price: 1000000000, Quantity: 1}},
			Asks: []domain.PriceLevel{{Price: 1002000000, Quantity: 1}},
		},
		perpBook: domain.OrderBook{
			Exchange: "binance", Symbol: "BTCUSDT", Timestamp: now,
			Bids: []domain.PriceLevel{{Price: 65000, Quantity: 1}},
			Asks: []domain.PriceLevel{{Price: 65010, Quantity: 1}},
		},
		fundingRate: domain.FundingRate{
			Rate:            0.0005,
			IntervalHours:   8,
			Timestamp:       now,
			NextFundingTime: now.Add(2 * time.Hour),
		},
	}

	cfg := config.Config{
		Mode:                  "MOCK",
		ExecutionMode:         "MAKER_TAKER",
		QuoteAssets:           []string{"USDT"},
		EnableSyntheticIDR:    true,
		SyntheticUSDTIDRRate:  16000.0,
		CapitalTotal:          1000,
		AllocationPerPair:     0.5,
		EnableExecutorSignals: true,
	}

	svc := New(cfg, adapter)
	svc.SetRedisClient(rc)

	pairKey := "indodax:btc_idr__binance:btcusdt"

	// Pre-seed tracker with 30 identical spread samples (> 1.5% and stdDev == 0)
	spreadSamples := make([]float64, 30)
	for i := range spreadSamples {
		spreadSamples[i] = 3.8
	}
	svc.SetSpreadSamples(pairKey, spreadSamples)

	pair := [2]domain.Market{
		{Exchange: "indodax", Symbol: "BTC_IDR", BaseAsset: "BTC", QuoteAsset: "IDR", MarketType: "SPOT", Active: true, TickSize: 1000, AmountSpec: &domain.AmountSpec{Unit: "BASE", Step: 0.00000001, Min: 0.00000001, BasePerUnit: 1}},
		{Exchange: "binance", Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true, ContractSize: 1, AmountSpec: &domain.AmountSpec{Unit: "CONTRACT", Step: 1, Min: 1, BasePerUnit: 1}},
	}

	res := svc.evaluatePair(context.Background(), pair, nil, true, now)
	if res.err != nil {
		t.Fatalf("evaluatePair error: %v", res.err)
	}

	if !res.op.IsStructuralSpread {
		t.Errorf("expected IsStructuralSpread = true")
	}
	if res.op.TradingEligible {
		t.Errorf("expected TradingEligible = false for structural spread")
	}

	hasTrapReason := false
	for _, reason := range res.op.ExclusionReasons {
		if reason == "STRUCTURAL_SPREAD_TRAP" {
			hasTrapReason = true
			break
		}
	}
	if !hasTrapReason {
		t.Errorf("expected STRUCTURAL_SPREAD_TRAP in ExclusionReasons, got %v", res.op.ExclusionReasons)
	}

	// Verify signal was NOT published to Redis Priority ZSET
	zCount, err := rdb.ZCard(context.Background(), redishandler.KeySignalPriority).Result()
	if err != nil {
		t.Fatal(err)
	}
	if zCount != 0 {
		t.Errorf("structural spread should NOT be published to Priority ZSET, found %d items", zCount)
	}
}

func TestPriorityScore_RedisZSetRanksHigherVelocityFirst(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rc := redishandler.NewWithClient(rdb)
	defer rc.Close()

	ctx := context.Background()

	// Coin A: NetROIPct = 0.9%, PriorityScore = 1.35
	payloadA := domain.ExecutorPayload{
		PairInfo: domain.PairInfo{
			LocalExchange:  "indodax",
			LocalSymbol:    "coin_a_idr",
			GlobalExchange: "okx",
			GlobalSymbol:   "coin_a_usdt",
		},
		ExecutionDirectives: domain.ExecutionDirectives{
			EstimatedNetProfitUSDT: 5.0,
			PriorityScore:          1.35,
		},
	}

	// Coin B: NetROIPct = 2.2%, PriorityScore = 0.44 (low velocity)
	payloadB := domain.ExecutorPayload{
		PairInfo: domain.PairInfo{
			LocalExchange:  "indodax",
			LocalSymbol:    "coin_b_idr",
			GlobalExchange: "okx",
			GlobalSymbol:   "coin_b_usdt",
		},
		ExecutionDirectives: domain.ExecutionDirectives{
			EstimatedNetProfitUSDT: 12.0,
			PriorityScore:          0.44,
		},
	}

	if err := rc.PublishSignal(ctx, payloadA); err != nil {
		t.Fatal(err)
	}
	if err := rc.PublishSignal(ctx, payloadB); err != nil {
		t.Fatal(err)
	}

	// Check top item in arb:signal_priority
	top, err := rdb.ZRevRangeWithScores(ctx, redishandler.KeySignalPriority, 0, 0).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(top) == 0 {
		t.Fatalf("expected item in priority zset")
	}

	expectedTopTag := "indodax:coin_a_idr__okx:coin_a_usdt"
	if top[0].Member != expectedTopTag {
		t.Errorf("got top member %s, want %s (Coin A should rank top despite lower profit due to higher velocity)", top[0].Member, expectedTopTag)
	}
	if math.Abs(top[0].Score-1.35) > 1e-6 {
		t.Errorf("got score %f, want 1.35", top[0].Score)
	}
}
