package scanner

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	redis "github.com/redis/go-redis/v9"
)

type customMockAdapter struct {
	spotBook    domain.OrderBook
	perpBook    domain.OrderBook
	fundingRate domain.FundingRate
	rate        float64
}

func (c *customMockAdapter) SpotMarkets(context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "indodax", Symbol: "BTC_IDR", BaseAsset: "BTC", QuoteAsset: "IDR", MarketType: "SPOT", Active: true, TickSize: 1000, AmountSpec: &domain.AmountSpec{Unit: "BASE", Step: 0.00000001, Min: 0.00000001, BasePerUnit: 1}},
	}, nil
}
func (c *customMockAdapter) PerpetualMarkets(context.Context) ([]domain.Market, error) {
	return []domain.Market{
		{Exchange: "binance", Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT", MarketType: "PERPETUAL", Active: true, ContractSize: 1, AmountSpec: &domain.AmountSpec{Unit: "CONTRACT", Step: 1, Min: 1, BasePerUnit: 1}},
	}, nil
}
func (c *customMockAdapter) SpotOrderBook(_ context.Context, _ string) (domain.OrderBook, error) {
	return c.spotBook, nil
}
func (c *customMockAdapter) PerpetualOrderBook(_ context.Context, _ string) (domain.OrderBook, error) {
	return c.perpBook, nil
}
func (c *customMockAdapter) FundingRate(_ context.Context, _ string) (domain.FundingRate, error) {
	return c.fundingRate, nil
}
func (c *customMockAdapter) OpenExposures(context.Context) ([]domain.OpenExposure, error) {
	return nil, nil
}
func (c *customMockAdapter) USDTIDRRate(context.Context) (float64, error) {
	return c.rate, nil
}

func TestLapis1_FundingFilter_AndEmergencyCancel(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rc := redishandler.NewWithClient(rdb)
	defer rc.Close()

	now := time.Now().UTC()

	// Case A: Negative Funding Rate -> Excluded + Publishes to arb:emergency_cancel
	ctx := context.Background()
	sub := rdb.Subscribe(ctx, redishandler.ChannelEmergencyCancel)
	defer sub.Close()
	time.Sleep(10 * time.Millisecond)

	adapterNeg := &customMockAdapter{
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
			Rate:            -0.0001, // Negative!
			IntervalHours:   8,
			Timestamp:       now,
			NextFundingTime: now.Add(2 * time.Hour),
		},
	}

	cfg := config.Config{
		Mode:                 "MOCK",
		ExecutionMode:        "MAKER_TAKER",
		QuoteAssets:          []string{"USDT"},
		EnableSyntheticIDR:   true,
		SyntheticUSDTIDRRate: 16000.0,
		CapitalTotal:         1000,
		AllocationPerPair:    0.5,
		MinFundingRate:       0.0002,
		SpotMakerFeePct:      0.0,
		PerpTakerFeePct:      0.05,
	}

	svc := New(cfg, adapterNeg)
	svc.SetRedisClient(rc)

	res, _, err := svc.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Opportunities) != 1 {
		t.Fatalf("expected 1 opportunity, got %d", len(res.Opportunities))
	}
	op := res.Opportunities[0]
	if op.TradingEligible {
		t.Errorf("expected TradingEligible=false for negative funding rate")
	}
	if !hasReason(op.ExclusionReasons, "NEGATIVE_FUNDING_RATE") {
		t.Errorf("expected exclusion NEGATIVE_FUNDING_RATE, got %v", op.ExclusionReasons)
	}

	// Case B: Funding Rate below +0.02% (e.g. 0.0001)
	adapterBelow := &customMockAdapter{
		rate:     16000.0,
		spotBook: adapterNeg.spotBook,
		perpBook: adapterNeg.perpBook,
		fundingRate: domain.FundingRate{
			Rate:            0.0001, // < 0.0002
			IntervalHours:   8,
			Timestamp:       now,
			NextFundingTime: now.Add(2 * time.Hour),
		},
	}
	svcBelow := New(cfg, adapterBelow)
	resBelow, _, _ := svcBelow.Scan(ctx)
	opBelow := resBelow.Opportunities[0]
	if !hasReason(opBelow.ExclusionReasons, "FUNDING_RATE_BELOW_MINIMUM") {
		t.Errorf("expected exclusion FUNDING_RATE_BELOW_MINIMUM, got %v", opBelow.ExclusionReasons)
	}

	// Case C: MinFundingRate=0 -> Positive funding rate passes without FUNDING_RATE_BELOW_MINIMUM
	cfgZero := cfg
	cfgZero.MinFundingRate = 0
	svcZero := New(cfgZero, adapterBelow)
	resZero, _, _ := svcZero.Scan(ctx)
	opZero := resZero.Opportunities[0]
	if hasReason(opZero.ExclusionReasons, "FUNDING_RATE_BELOW_MINIMUM") {
		t.Errorf("expected NO exclusion for MinFundingRate=0, got %v", opZero.ExclusionReasons)
	}
}

func TestExcludedSymbols(t *testing.T) {
	now := time.Now().UTC()
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
		Mode:                 "MOCK",
		ExecutionMode:        "MAKER_TAKER",
		QuoteAssets:          []string{"USDT"},
		EnableSyntheticIDR:   true,
		SyntheticUSDTIDRRate: 16000.0,
		CapitalTotal:         1000,
		AllocationPerPair:    0.5,
		ExcludedSymbols:      []string{"BTC"},
	}

	svc := New(cfg, adapter)
	res, _, err := svc.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Opportunities) != 0 {
		t.Fatalf("expected 0 opportunities after excluding BTC, got %d", len(res.Opportunities))
	}
}

func TestLapis2_MakerTakerEntrySpread_AndSnipingWindow(t *testing.T) {
	now := time.Now().UTC()

	// Global Bid: 65,000 USDT * 16,000 = 1,040,000,000 IDR
	// Local Best Bid: 1,000,000,000 IDR, TickSize: 1000 IDR
	// TargetLimitBuyIDR: 1,000,001,000 IDR
	// EntrySpreadPct: ((1,040,000,000 - 1,000,001,000) / 1,040,000,000) * 100 = ~3.846%
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
			Rate:            0.0005, // 0.05%
			IntervalHours:   8,
			Timestamp:       now,
			NextFundingTime: now.Add(20 * time.Minute), // <= 30m -> Sniping Window!
		},
	}

	cfg := config.Config{
		Mode:                    "MOCK",
		ExecutionMode:           "MAKER_TAKER",
		QuoteAssets:             []string{"USDT"},
		EnableSyntheticIDR:      true,
		SyntheticUSDTIDRRate:    16000.0,
		CapitalTotal:            1000,
		AllocationPerPair:       0.5,
		MinFundingRate:          0.0002,
		SpotMakerFeePct:         0.0,
		PerpTakerFeePct:         0.05,
		SnipingWindowMinutes:    30,
		FundingSubsidyFloorPct:  0.08,
		BaseTargetExitSpreadPct: 0.3,
	}

	svc := New(cfg, adapter)
	res, _, err := svc.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	op := res.Opportunities[0]
	if !op.IsSnipingWindow {
		t.Errorf("expected IsSnipingWindow=true for 20m to funding")
	}
	if op.MinutesToNextFunding <= 0 || op.MinutesToNextFunding > 21 {
		t.Errorf("got MinutesToNextFunding=%f, want ~20", op.MinutesToNextFunding)
	}
	if op.TargetLimitBuyIDR != 1000001000.0 {
		t.Errorf("got TargetLimitBuyIDR=%f, want 1000001000.0", op.TargetLimitBuyIDR)
	}
	// In sniping window, FundingSubsidy = 0.05% * 1.0 = 0.05%
	// Raw MinimumEntrySpreadPct = (0.0 + 0.05) - 0.05 = 0.00%, clamped to floor 0.08%
	if op.MinEntrySpreadPct < 0.079 || op.MinEntrySpreadPct > 0.081 {
		t.Errorf("got MinEntrySpreadPct=%f, expected 0.08 floor", op.MinEntrySpreadPct)
	}
	if op.EntrySpreadPct < 3.8 || op.EntrySpreadPct > 3.9 {
		t.Errorf("got EntrySpreadPct=%f, want ~3.846%%", op.EntrySpreadPct)
	}
	if op.CancelIfGlobalBidDropsBelowUSDT <= 0 {
		t.Errorf("got invalid CancelIfGlobalBidDropsBelowUSDT=%f", op.CancelIfGlobalBidDropsBelowUSDT)
	}
}

func TestRedisActivePositionAndSignalPublishing(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rc := redishandler.NewWithClient(rdb)
	defer rc.Close()

	now := time.Now().UTC()
	ctx := context.Background()

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
			NextFundingTime: now.Add(20 * time.Minute),
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
		MinFundingRate:        0.0002,
		MinNetROIPct:          0.1,
		MaxNetROIPct:          20.0,
		SpotMakerFeePct:       0.0,
		PerpTakerFeePct:       0.05,
		EnableExecutorSignals: true,
	}

	svc := New(cfg, adapter)
	svc.SetRedisClient(rc)

	// Subscribe to signal channel
	sub := rdb.Subscribe(ctx, redishandler.ChannelSignalNotify)
	defer sub.Close()
	time.Sleep(10 * time.Millisecond)

	// 1. Initial Scan: Should publish signal and live price
	res, _, err := svc.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Opportunities[0].TradingEligible {
		t.Fatalf("expected opportunity to be eligible, got reasons: %v", res.Opportunities[0].ExclusionReasons)
	}

	// Verify live price in Redis
	liveKey := redishandler.KeyPrefixLivePrice + "indodax:btc_idr__binance:btcusdt"
	liveVal, err := rdb.Get(ctx, liveKey).Result()
	if err != nil {
		t.Fatalf("failed to get live price: %v", err)
	}
	var snap domain.PricingSnapshot
	_ = json.Unmarshal([]byte(liveVal), &snap)
	if snap.GlobalBestBidUSDT != 65000 {
		t.Errorf("got global best bid %f, want 65000", snap.GlobalBestBidUSDT)
	}

	// Verify signal in Redis
	sigKey := redishandler.KeyPrefixSignal + "indodax:btc_idr__binance:btcusdt"
	sigVal, err := rdb.Get(ctx, sigKey).Result()
	if err != nil {
		t.Fatalf("failed to get signal: %v", err)
	}
	var payload domain.ExecutorPayload
	_ = json.Unmarshal([]byte(sigVal), &payload)
	if payload.ExecutionDirectives.Action != "PLACE_MAKER_BUY" {
		t.Errorf("got action %s, want PLACE_MAKER_BUY", payload.ExecutionDirectives.Action)
	}

	// 2. Set active position in Redis (written by Python)
	activeKey := redishandler.KeyPrefixActivePos + "indodax:btc_idr__binance:btcusdt"
	_ = rdb.Set(ctx, activeKey, `{"symbol":"btc_idr","qty":0.01}`, 0).Err()

	// Scan again: Scanner should still update live price, but NOT emit new PLACE_MAKER_BUY
	_ = rdb.Del(ctx, sigKey).Err() // delete previous signal to verify it isn't recreated
	res2, _, _ := svc.Scan(ctx)
	op2 := res2.Opportunities[0]
	if op2.TradingEligible {
		t.Errorf("expected TradingEligible=false when active position exists")
	}
	if !hasReason(op2.ExclusionReasons, "ACTIVE_POSITION_EXISTS_IN_EXECUTOR") {
		t.Errorf("expected ACTIVE_POSITION_EXISTS_IN_EXECUTOR, got %v", op2.ExclusionReasons)
	}

	// Signal key should NOT exist now
	_, err = rdb.Get(ctx, sigKey).Result()
	if err != redis.Nil {
		t.Errorf("expected signal key to not be set when active position exists")
	}

	// Test that when EnableExecutorSignals is false (default), no signal is published even if eligible
	cfgDisabled := cfg
	cfgDisabled.EnableExecutorSignals = false
	svcDisabled := New(cfgDisabled, adapter)
	svcDisabled.SetRedisClient(rc)
	_ = rdb.Del(ctx, sigKey).Err()
	_ = rdb.Del(ctx, activeKey).Err()
	resDisabled, _, errDisabled := svcDisabled.Scan(ctx)
	if errDisabled != nil {
		t.Fatal(errDisabled)
	}
	if !resDisabled.Opportunities[0].TradingEligible {
		t.Fatalf("expected opportunity to be eligible, got reasons: %v", resDisabled.Opportunities[0].ExclusionReasons)
	}
	_, err = rdb.Get(ctx, sigKey).Result()
	if err != redis.Nil {
		t.Errorf("expected no signal published when EnableExecutorSignals=false, got err=%v", err)
	}
}

func TestValidateOrderBookAge(t *testing.T) {
	now := time.Now().UTC()
	fresh := domain.OrderBook{Exchange: "indodax", Symbol: "btc_idr", Timestamp: now.Add(-1 * time.Second)}
	freshPerp := domain.OrderBook{Exchange: "okx", Symbol: "BTC-USDT", Timestamp: now.Add(-2 * time.Second)}
	stale := domain.OrderBook{Exchange: "okx", Symbol: "BTC-USDT", Timestamp: now.Add(-10 * time.Minute)}
	unstamped := domain.OrderBook{Exchange: "okx", Symbol: "BTC-USDT"}

	tests := []struct {
		name       string
		sb, pb     domain.OrderBook
		maxAgeSecs int
		wantErr    bool
	}{
		{"disabled when maxAge<=0", fresh, stale, 0, false},
		{"fresh books pass", fresh, freshPerp, 5, false},
		{"stale spot rejected", stale, freshPerp, 5, true},
		{"stale perp rejected", fresh, stale, 5, true},
		{"unstamped book rejected", fresh, unstamped, 5, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOrderBookAge(tc.sb, tc.pb, now, tc.maxAgeSecs)
			if tc.wantErr && err == nil {
				t.Fatalf("expected stale error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateOrderBookIntegrity(t *testing.T) {
	sb := domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: time.Now(),
	}
	pb := domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Bids:      []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:      []domain.PriceLevel{{Price: 101, Quantity: 1}},
		Timestamp: time.Now(),
	}

	// No checksum set - should pass (skip check); venues without checksums
	// must not be rejected.
	if err := validateOrderBookIntegrity(sb, pb); err != nil {
		t.Fatalf("expected no error when checksum is empty, got: %v", err)
	}

	// A matching checksum passes.
	sb.Checksum = orderBookChecksum(sb)
	if err := validateOrderBookIntegrity(sb, pb); err != nil {
		t.Fatalf("expected valid checksum to pass, got: %v", err)
	}

	// A mismatched checksum fails.
	sb.Checksum = "deadbeef"
	if err := validateOrderBookIntegrity(sb, pb); err == nil {
		t.Fatal("expected checksum mismatch to fail validation")
	}
}

func TestValidateCrossExchangeTimestampAlignment(t *testing.T) {
	base := time.Now()
	sb := domain.OrderBook{Exchange: "indodax", Symbol: "btcusdt", Timestamp: base}
	pb := domain.OrderBook{Exchange: "okx", Symbol: "BTC-USDT-SWAP", Timestamp: base}

	// Disabled when maxSkew is zero.
	pb.Timestamp = base.Add(-time.Hour)
	if err := validateCrossExchangeTimestampAlignment(sb, pb, 0); err != nil {
		t.Fatalf("expected zero maxSkew to disable check, got: %v", err)
	}

	// Fresh books within skew pass.
	pb.Timestamp = base.Add(-2 * time.Second)
	if err := validateCrossExchangeTimestampAlignment(sb, pb, 5*time.Second); err != nil {
		t.Fatalf("expected books within skew to pass, got: %v", err)
	}

	// Skew beyond the limit fails, in either direction.
	pb.Timestamp = base.Add(-30 * time.Second)
	if err := validateCrossExchangeTimestampAlignment(sb, pb, 5*time.Second); err == nil {
		t.Fatal("expected excessive skew to fail validation")
	}
	pb.Timestamp = base.Add(30 * time.Second)
	if err := validateCrossExchangeTimestampAlignment(sb, pb, 5*time.Second); err == nil {
		t.Fatal("expected excessive forward skew to fail validation")
	}

	// Missing timestamps fail.
	sb.Timestamp = time.Time{}
	if err := validateCrossExchangeTimestampAlignment(sb, pb, 5*time.Second); err == nil {
		t.Fatal("expected missing timestamp to fail validation")
	}
}
