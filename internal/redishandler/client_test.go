package redishandler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *Client) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to run miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	return mr, NewWithClient(rdb)
}

func TestRedisHandler_PublishSignalAndLivePrice(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()

	// Subscribe to arb:signal_notify to check publication
	sub := client.rdb.Subscribe(ctx, ChannelSignalNotify)
	defer sub.Close()
	// Wait a moment for subscription to be active
	time.Sleep(10 * time.Millisecond)

	payload := domain.ExecutorPayload{
		PairInfo: domain.PairInfo{
			LocalExchange:  "indodax",
			GlobalExchange: "binance",
			LocalSymbol:    "btc_idr",
			GlobalSymbol:   "BTCUSDT",
			BaseAsset:      "BTC",
			QuoteAsset:     "IDR",
			LocalPriceTick: 1000,
		},
		PricingSnapshot: domain.PricingSnapshot{
			LocalBestBid:      1000000000,
			LocalBestAsk:      1001000000,
			GlobalBestBidUSDT: 65000,
			GlobalBestAskUSDT: 65010,
			USDTIDRRate:       16000,
			TimestampMS:       time.Now().UnixMilli(),
		},
		FundingMetrics: domain.FundingMetrics{
			CurrentFundingRate:   0.0003,
			NextFundingTimeMS:    time.Now().Add(1 * time.Hour).UnixMilli(),
			MinutesToNextFunding: 60,
			IsSnipingWindow:      false,
		},
		ExecutionDirectives: domain.ExecutionDirectives{
			Action:                          "PLACE_MAKER_BUY",
			RecommendedLimitBuyIDR:          1000001000,
			EntrySpreadPct:                  0.25,
			MinEntrySpreadPct:               0.10,
			BaseTargetExitSpreadPct:         0.30,
			MaxPositionUSDT:                 500,
			CancelIfGlobalBidDropsBelowUSDT: 64800,
		},
	}

	if err := client.PublishSignal(ctx, payload); err != nil {
		t.Fatalf("PublishSignal failed: %v", err)
	}

	// Verify key was set in Redis
	sigKey := KeyPrefixSignal + "indodax:btc_idr__binance:btcusdt"
	val, err := client.rdb.Get(ctx, sigKey).Result()
	if err != nil {
		t.Fatalf("failed to get signal key: %v", err)
	}
	var parsed domain.ExecutorPayload
	if err := json.Unmarshal([]byte(val), &parsed); err != nil {
		t.Fatalf("failed to parse signal JSON: %v", err)
	}
	if parsed.PairInfo.LocalSymbol != "btc_idr" {
		t.Errorf("got symbol %s, want btc_idr", parsed.PairInfo.LocalSymbol)
	}

	// Verify live price key
	liveKey := KeyPrefixLivePrice + "indodax:btc_idr__binance:btcusdt"
	liveVal, err := client.rdb.Get(ctx, liveKey).Result()
	if err != nil {
		t.Fatalf("failed to get live price key: %v", err)
	}
	var parsedLive domain.PricingSnapshot
	if err := json.Unmarshal([]byte(liveVal), &parsedLive); err != nil {
		t.Fatalf("failed to parse live price JSON: %v", err)
	}
	if parsedLive.GlobalBestBidUSDT != 65000 {
		t.Errorf("got global bid %f, want 65000", parsedLive.GlobalBestBidUSDT)
	}

	// Signals must expire fast (SignalTTL); live prices keep the longer DefaultTTL.
	sigTTL := client.rdb.TTL(ctx, sigKey).Val()
	if sigTTL <= 0 || sigTTL > SignalTTL {
		t.Errorf("signal TTL = %v, want >0 and <= %v", sigTTL, SignalTTL)
	}
	liveTTL := client.rdb.TTL(ctx, liveKey).Val()
	if liveTTL <= SignalTTL || liveTTL > DefaultTTL {
		t.Errorf("live price TTL = %v, want >%v and <=%v", liveTTL, SignalTTL, DefaultTTL)
	}
}

func TestRedisHandler_ActivePositionAndEmergencyCancel(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()

	hasPos, err := client.HasActivePosition(ctx, "indodax", "btc_idr")
	if err != nil || hasPos {
		t.Fatalf("expected no active position initially, got %v, err=%v", hasPos, err)
	}

	// Simulate Python setting active position
	posKey := KeyPrefixActivePos + "indodax:btc_idr"
	posData := `{"local_exchange":"indodax","symbol":"btc_idr","entry_price_idr":1000000000,"short_entry_usdt":65000,"qty":0.01,"accumulated_funding_usdt":5.0}`
	_ = client.rdb.Set(ctx, posKey, posData, 0).Err()

	hasPos, err = client.HasActivePosition(ctx, "indodax", "btc_idr")
	if err != nil || !hasPos {
		t.Fatalf("expected active position true, got %v, err=%v", hasPos, err)
	}

	pos, err := client.GetActivePosition(ctx, "indodax", "btc_idr")
	if err != nil || pos == nil {
		t.Fatalf("failed to get active pos: %v", err)
	}
	if pos.Qty != 0.01 || pos.AccumulatedFundingUSDT != 5.0 {
		t.Errorf("unexpected pos data: %+v", pos)
	}

	// Test emergency cancel publish
	sub := client.rdb.Subscribe(ctx, ChannelEmergencyCancel)
	defer sub.Close()
	time.Sleep(10 * time.Millisecond)

	if err := client.PublishEmergencyCancel(ctx, "indodax", "btc_idr", "CANCEL_ALL_NEGATIVE_FUNDING", -0.0001); err != nil {
		t.Fatalf("emergency cancel failed: %v", err)
	}
}


func TestCleanPairKeyIncludesBothExchangeLegs(t *testing.T) {
	first := CleanPairKey("tokocrypto", "BTC/IDR", "okx", "BTC/USDT")
	second := CleanPairKey("tokocrypto", "BTC/IDR", "mexc", "BTC/USDT")
	if first == second {
		t.Fatalf("different futures venues collided: %q", first)
	}
	if first != "tokocrypto:btc_idr__okx:btc_usdt" {
		t.Fatalf("unexpected full pair key: %q", first)
	}
}

func TestRedisHandler_ActivePositionsAreIsolatedByFuturesVenue(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()
	ctx := context.Background()

	okxKey := KeyPrefixActivePos + CleanPairKey("tokocrypto", "BTC/IDR", "okx", "BTC/USDT")
	if err := client.rdb.Set(ctx, okxKey, `{"qty":0.01}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	okxActive, err := client.HasActivePosition(ctx, "tokocrypto", "BTC/IDR", "okx", "BTC/USDT")
	if err != nil || !okxActive {
		t.Fatalf("expected OKX position active, got %v err=%v", okxActive, err)
	}
	mexcActive, err := client.HasActivePosition(ctx, "tokocrypto", "BTC/IDR", "mexc", "BTC/USDT")
	if err != nil || mexcActive {
		t.Fatalf("MEXC pair must remain independent, got %v err=%v", mexcActive, err)
	}
}


func TestPublishSignalRanksByEstimatedNetProfitUSDT(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()
	ctx := context.Background()

	publish := func(symbol string, profit float64) {
		t.Helper()
		payload := domain.ExecutorPayload{
			PairInfo: domain.PairInfo{LocalExchange: "indodax", GlobalExchange: "okx", LocalSymbol: symbol, GlobalSymbol: symbol + "USDT"},
			ExecutionDirectives: domain.ExecutionDirectives{EstimatedNetProfitUSDT: profit},
		}
		if err := client.PublishSignal(ctx, payload); err != nil { t.Fatal(err) }
	}
	publish("btc_idr", 1.25)
	publish("eth_idr", 3.75)
	publish("sol_idr", 2.50)

	items, err := client.rdb.ZRevRangeWithScores(ctx, KeySignalPriority, 0, -1).Result()
	if err != nil { t.Fatal(err) }
	if len(items) != 3 { t.Fatalf("got %d ranked signals, want 3", len(items)) }
	if items[0].Member != "indodax:eth_idr__okx:eth_idrusdt" || items[0].Score != 3.75 {
		t.Fatalf("highest priority = %+v, want ETH at 3.75 USDT", items[0])
	}
}

func TestGetDynamicFeeProfileRequiresFreshVerifiedExchangeSnapshot(t *testing.T) {
	mr, rc := setupTestRedis(t)
	defer mr.Close()
	defer rc.Close()
	ctx := context.Background()
	profile := DynamicFeeProfile{
		MakerFeePct: 0.02,
		TakerFeePct: 0.05,
		Source:      "EXCHANGE_API",
		Verified:    true,
		FetchedAtMS: time.Now().UnixMilli(),
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	key := "arb:fee_profile:okx:perp:btc_usdt_usdt"
	if err := rc.rdb.Set(ctx, key, raw, 5*time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := rc.GetDynamicFeeProfile(ctx, "OKX", "PERP", "BTC/USDT:USDT")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.TakerFeePct != 0.05 || !got.Verified {
		t.Fatalf("unexpected fee profile: %+v", got)
	}

	profile.Verified = false
	raw, _ = json.Marshal(profile)
	_ = rc.rdb.Set(ctx, key, raw, 5*time.Minute).Err()
	got, err = rc.GetDynamicFeeProfile(ctx, "okx", "perp", "BTC/USDT:USDT")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("unverified profile must be ignored: %+v", got)
	}

	profile.Verified = true
	profile.FetchedAtMS = time.Now().Add(-6 * time.Minute).UnixMilli()
	raw, _ = json.Marshal(profile)
	_ = rc.rdb.Set(ctx, key, raw, 5*time.Minute).Err()
	got, err = rc.GetDynamicFeeProfile(ctx, "okx", "perp", "BTC/USDT:USDT")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("stale profile must be ignored: %+v", got)
	}
}


func TestRequestRecoveryRetryWaitsForExecutorResult(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sub := client.rdb.Subscribe(ctx, ChannelRecoveryRequest)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe recovery requests: %v", err)
	}
	go func() {
		message := <-sub.Channel()
		if message == nil {
			return
		}
		var request map[string]string
		if json.Unmarshal([]byte(message.Payload), &request) != nil {
			return
		}
		reply, _ := json.Marshal(RecoveryRetryReply{
			RequestID: request["request_id"],
			Action: request["action"],
			Complete: true,
			PendingOrders: map[string]string{},
			ScopeHalts: map[string]string{},
			TimestampMS: time.Now().UnixMilli(),
		})
		_ = client.rdb.Set(ctx, KeyPrefixRecoveryReply+request["request_id"], reply, 30*time.Second).Err()
	}()

	result, err := client.RequestRecoveryRetry(ctx)
	if err != nil {
		t.Fatalf("RequestRecoveryRetry: %v", err)
	}
	if !result.Complete || result.RequestID == "" {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
}


func TestRequestOrphanResolutionCarriesExactOperatorTarget(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sub := client.rdb.Subscribe(ctx, ChannelRecoveryRequest)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe recovery requests: %v", err)
	}
	go func() {
		message := <-sub.Channel()
		if message == nil { return }
		var request map[string]string
		if json.Unmarshal([]byte(message.Payload), &request) != nil { return }
		if request["action"] != "resolve_orphan" || request["orphan_id"] != "orphan-123" || request["pair_tag"] != "indodax:btc_idr" {
			return
		}
		reply, _ := json.Marshal(RecoveryRetryReply{
			RequestID: request["request_id"], Action: request["action"], Resolved: true,
			Complete: true, PendingOrders: map[string]string{}, ScopeHalts: map[string]string{},
			TimestampMS: time.Now().UnixMilli(),
		})
		_ = client.rdb.Set(ctx, KeyPrefixRecoveryReply+request["request_id"], reply, 30*time.Second).Err()
	}()
	reply, err := client.RequestOrphanResolution(ctx, "orphan-123", "indodax:btc_idr")
	if err != nil { t.Fatalf("RequestOrphanResolution: %v", err) }
	if !reply.Resolved || reply.Action != "resolve_orphan" {
		t.Fatalf("unexpected orphan resolution result: %+v", reply)
	}
}


func TestSpotOrderLimitPersistsAndBroadcasts(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sub := client.rdb.Subscribe(ctx, ChannelSpotOrderLimitUpdate)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil { t.Fatalf("subscribe spot order limit: %v", err) }
	if err := client.SetSpotOrderLimit(ctx, "TOKOCRYPTO", 4); err != nil { t.Fatalf("SetSpotOrderLimit: %v", err) }
	limits, err := client.GetSpotOrderLimits(ctx)
	if err != nil { t.Fatalf("GetSpotOrderLimits: %v", err) }
	if limits["tokocrypto"] != 4 { t.Fatalf("persisted limits = %#v, want tokocrypto=4", limits) }
	select {
	case message := <-sub.Channel():
		var update map[string]interface{}
		if err := json.Unmarshal([]byte(message.Payload), &update); err != nil { t.Fatal(err) }
		if update["exchange"] != "tokocrypto" || update["max_open_spot_orders"] != float64(4) {
			t.Fatalf("unexpected spot order limit event: %#v", update)
		}
	case <-ctx.Done():
		t.Fatal("spot order limit update was not broadcast")
	}
	if err := client.SetSpotOrderLimit(ctx, "indodax", 0); err == nil { t.Fatal("expected invalid limit to be rejected") }
}


func TestListActivePositionsReturnsAuthoritativeRecords(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()
	ctx := context.Background()
	position := domain.ActivePosition{
		LocalExchange: "indodax", GlobalExchange: "okx",
		Symbol: "btc_idr", Qty: 0.25,
	}
	payload, err := json.Marshal(position)
	if err != nil { t.Fatal(err) }
	if err := client.rdb.Set(ctx, KeyPrefixActivePos+"indodax:btc_idr", payload, 0).Err(); err != nil { t.Fatal(err) }
	got, err := client.ListActivePositions(ctx)
	if err != nil { t.Fatal(err) }
	if len(got) != 1 || got[0].Qty != 0.25 || got[0].GlobalExchange != "okx" {
		t.Fatalf("unexpected active positions: %+v", got)
	}
}

func TestListActivePositionsFailsClosedOnMalformedRecord(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()
	ctx := context.Background()
	if err := client.rdb.Set(ctx, KeyPrefixActivePos+"broken", "{", 0).Err(); err != nil { t.Fatal(err) }
	if _, err := client.ListActivePositions(ctx); err == nil {
		t.Fatal("expected malformed active position to fail closed")
	}
}

func TestListActivePositionsRequiresRedis(t *testing.T) {
	if _, err := (&Client{}).ListActivePositions(context.Background()); err == nil {
		t.Fatal("expected disconnected Redis to fail closed")
	}
}
