package scanner

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/shopspring/decimal"
)

func bookWithTouch(price, qty float64) domain.OrderBook {
	return domain.OrderBook{
		Bids: []domain.PriceLevel{{Price: price, Quantity: qty}},
		Asks: []domain.PriceLevel{{Price: price, Quantity: qty}},
	}
}

// Guards the assumed decimal ordering: every liquidity assertion below depends on
// LessThan meaning "looks smaller", so a broken numeric backend must fail loudly
// here rather than silently passing the filter tests.
func TestDecimalOrderingSanity(t *testing.T) {
	if !decimal.NewFromFloat(6).LessThan(decimal.NewFromFloat(50)) {
		t.Fatal("decimal ordering is broken: 6 must be less than 50")
	}
	fifty := decimal.NewFromFloat(100).Mul(decimal.NewFromFloat(0.5))
	if !fifty.Equal(decimal.NewFromFloat(50)) {
		t.Fatalf("100*0.5 must equal 50, got %s", fifty)
	}
}

func TestLiquidityFilterDisabledByDefault(t *testing.T) {
	s := New(config.Config{CapitalTotal: 1000, AllocationPerPair: 0.5}, nil)
	thin := bookWithTouch(100, 0.0001)
	if err := s.validateLiquidityVelocity(thin, thin, decimal.NewFromFloat(5)); err != nil {
		t.Fatalf("filter must be off when min_top_of_book_quote_usdt is 0, got %v", err)
	}
}

func TestLiquidityFilterRejectsThinSpotBook(t *testing.T) {
	s := New(config.Config{CapitalTotal: 1000, AllocationPerPair: 0.5, MinTopOfBookQuoteUSDT: 50}, nil)
	// $100/unit with 0.25 units resting = $25 at the touch; position needs $600.
	thinSpot := bookWithTouch(100, 0.25)
	deepPerp := bookWithTouch(100, 100)
	err := s.validateLiquidityVelocity(thinSpot, deepPerp, decimal.NewFromFloat(600))
	if err == nil {
		t.Fatal("expected rejection for thin spot book")
	}
	if !strings.Contains(err.Error(), "spot top-of-book") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLiquidityFilterRejectsThinPerpBook(t *testing.T) {
	s := New(config.Config{CapitalTotal: 1000, AllocationPerPair: 0.5, MinTopOfBookQuoteUSDT: 50}, nil)
	err := s.validateLiquidityVelocity(bookWithTouch(100, 100), bookWithTouch(100, 0.25), decimal.NewFromFloat(600))
	if err == nil || !strings.Contains(err.Error(), "perp top-of-book") {
		t.Fatalf("expected perp rejection, got %v", err)
	}
}

func TestLiquidityFilterPassesDeepBook(t *testing.T) {
	s := New(config.Config{CapitalTotal: 1000, AllocationPerPair: 0.5, MinTopOfBookQuoteUSDT: 50}, nil)
	deep := bookWithTouch(100, 100)
	if err := s.validateLiquidityVelocity(deep, deep, decimal.NewFromFloat(600)); err != nil {
		t.Fatalf("deep book must pass, got %v", err)
	}
}

// The threshold must never exceed the size we would actually trade, otherwise a
// small position would be rejected for liquidity it does not need.
func TestLiquidityThresholdCappedAtRequiredNotional(t *testing.T) {
	s := New(config.Config{CapitalTotal: 1000, AllocationPerPair: 0.5, MinTopOfBookQuoteUSDT: 5000}, nil)
	// $20 notional at the touch, but the position only needs $5.
	book := bookWithTouch(100, 0.2)
	if err := s.validateLiquidityVelocity(book, book, decimal.NewFromFloat(5)); err != nil {
		t.Fatalf("threshold should shrink to required notional, got %v", err)
	}
}

func TestLiquidityFilterHandlesEmptyBook(t *testing.T) {
	s := New(config.Config{CapitalTotal: 1000, AllocationPerPair: 0.5, MinTopOfBookQuoteUSDT: 50}, nil)
	empty := domain.OrderBook{}
	if err := s.validateLiquidityVelocity(empty, empty, decimal.NewFromFloat(10)); err == nil {
		t.Fatal("empty book has zero liquidity and must be rejected")
	}
}

func TestRequiredNotionalIsAllocationBudget(t *testing.T) {
	s := New(config.Config{CapitalTotal: 1000, AllocationPerPair: 0.5, MinAllocatedCapital: 10}, nil)
	if got := s.requiredNotionalUsdt(); !got.Equal(decimal.NewFromFloat(500)) {
		t.Fatalf("expected 500, got %s", got)
	}
}

// A minimum capital above the budget must win, otherwise every pair would be
// judged against a position size smaller than the smallest one we can open.
func TestRequiredNotionalNeverBelowMinCapital(t *testing.T) {
	s := New(config.Config{CapitalTotal: 100, AllocationPerPair: 0.05, MinAllocatedCapital: 10}, nil)
	if got := s.requiredNotionalUsdt(); !got.Equal(decimal.NewFromFloat(10)) {
		t.Fatalf("expected floor of 10, got %s", got)
	}
}

func TestOrderBookImbalanceSymmetricIsZero(t *testing.T) {
	// One mirrored level per side: 100 * 1 vs 100.4 * (100/100.4).
	book := domain.OrderBook{
		Bids: []domain.PriceLevel{{Price: 100.0, Quantity: 1}},
		Asks: []domain.PriceLevel{{Price: 100.4, Quantity: 100.0 / 100.4}},
	}
	if got := orderBookImbalance(book); math.Abs(got) > 1e-9 {
		t.Fatalf("symmetric book must be 0, got %v", got)
	}
}

func TestOrderBookImbalanceBidHeavyIsPositive(t *testing.T) {
	book := domain.OrderBook{
		Bids: []domain.PriceLevel{{Price: 100, Quantity: 5}},
		Asks: []domain.PriceLevel{{Price: 100.2, Quantity: 1}},
	}
	got := orderBookImbalance(book)
	if got <= 0 || got >= 1 {
		t.Fatalf("bid-heavy book must be in (0,1), got %v", got)
	}
}

func TestOrderBookImbalanceAskHeavyIsNegative(t *testing.T) {
	book := domain.OrderBook{
		Bids: []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks: []domain.PriceLevel{{Price: 100.2, Quantity: 9}},
	}
	if got := orderBookImbalance(book); got >= 0 || got <= -1 {
		t.Fatalf("ask-heavy book must be in (-1,0), got %v", got)
	}
}

func TestOrderBookImbalanceIgnoresDistantLevels(t *testing.T) {
	// The huge bid 10% away is outside the 1% band and must not dominate.
	book := domain.OrderBook{
		Bids: []domain.PriceLevel{{Price: 100, Quantity: 1}, {Price: 90, Quantity: 1000}},
		Asks: []domain.PriceLevel{{Price: 100.2, Quantity: 1}},
	}
	got := orderBookImbalance(book)
	if got > 0.2 {
		t.Fatalf("levels beyond 1%% must be ignored, got %v", got)
	}
}

func TestOrderBookImbalanceOneSidedAndEmpty(t *testing.T) {
	if got := orderBookImbalance(domain.OrderBook{Asks: []domain.PriceLevel{{Price: 1, Quantity: 1}}}); got != -1 {
		t.Fatalf("bids empty -> -1 (sell pressure), got %v", got)
	}
	if got := orderBookImbalance(domain.OrderBook{Bids: []domain.PriceLevel{{Price: 1, Quantity: 1}}}); got != 1 {
		t.Fatalf("asks empty -> 1 (buy pressure), got %v", got)
	}
	if got := orderBookImbalance(domain.OrderBook{}); got != 0 {
		t.Fatalf("empty book -> 0, got %v", got)
	}
}

func TestOrderBookImbalanceGuardDisabledByDefault(t *testing.T) {
	s := New(config.Config{}, nil)
	sb := bookWithTouch(100, 10)
	// One-sided books produce extreme OBI; with the guard disabled they pass.
	oneSided := domain.OrderBook{Exchange: "okx", Symbol: "X", Bids: []domain.PriceLevel{{Price: 100, Quantity: 1}}}
	if err := s.validateOrderBookImbalance(sb, oneSided); err != nil {
		t.Fatalf("expected guard disabled when max is 0, got: %v", err)
	}
}

func TestOrderBookImbalanceGuardRejectsOneSidedBook(t *testing.T) {
	s := New(config.Config{MaxOrderBookImbalance: 0.8}, nil)
	// A one-sided book yields OBI = +/-1, far above the threshold.
	pb := domain.OrderBook{Exchange: "okx", Symbol: "X", Asks: []domain.PriceLevel{{Price: 101, Quantity: 1}}}
	if err := s.validateOrderBookImbalance(bookWithTouch(100, 10), pb); err == nil {
		t.Fatal("expected one-sided book to be rejected by the imbalance guard")
	}
}

func TestOrderBookImbalanceGuardPassesBalancedBook(t *testing.T) {
	s := New(config.Config{MaxOrderBookImbalance: 0.8}, nil)
	sb := domain.OrderBook{
		Exchange: "indodax", Symbol: "btcusdt",
		Bids: []domain.PriceLevel{{Price: 100, Quantity: 5}, {Price: 99.5, Quantity: 5}},
		Asks: []domain.PriceLevel{{Price: 100.5, Quantity: 5}, {Price: 101, Quantity: 5}},
	}
	pb := domain.OrderBook{
		Exchange: "okx", Symbol: "X-SWAP",
		Bids: []domain.PriceLevel{{Price: 100, Quantity: 6}, {Price: 99.5, Quantity: 4}},
		Asks: []domain.PriceLevel{{Price: 100.5, Quantity: 5}, {Price: 101, Quantity: 5}},
	}
	if err := s.validateOrderBookImbalance(sb, pb); err != nil {
		t.Fatalf("expected balanced books to pass, got: %v", err)
	}
}

// staticAdapter wraps an existing ReadOnlyAdapter with a canned LastBookUpdate
// map, so the guard can be exercised without wiring a full mock adapter.
type staticAdapter struct {
	exchange.ReadOnlyAdapter
	updates map[string]time.Time
}

func (a staticAdapter) LastBookUpdate(exchange, symbol string) time.Time {
	return a.updates[exchange+"/"+symbol]
}

func TestLastTradeAgeDisabledByDefault(t *testing.T) {
	s := New(config.Config{}, nil)
	s.adapter = staticAdapter{updates: map[string]time.Time{
		"indodax/btc_idr": time.Now().Add(-time.Hour),
	}}
	if err := s.validateLastTradeAge(time.Now(),
		domain.Market{}, domain.Market{},
		domain.OrderBook{Exchange: "indodax", Symbol: "btc_idr"},
		domain.OrderBook{Exchange: "okx", Symbol: "X"}); err != nil {
		t.Fatalf("guard must be disabled when max is 0, got: %v", err)
	}
}

func TestLastTradeAgeRejectsQuietBook(t *testing.T) {
	now := time.Now()
	s := New(config.Config{MaxLastTradeAgeSeconds: 60}, nil)
	s.adapter = staticAdapter{updates: map[string]time.Time{
		"indodax/btc_idr": now.Add(-10 * time.Minute),
		"okx/BTC-USDT":    now,
	}}
	err := s.validateLastTradeAge(now,
		domain.Market{}, domain.Market{},
		domain.OrderBook{Exchange: "indodax", Symbol: "btc_idr"},
		domain.OrderBook{Exchange: "okx", Symbol: "BTC-USDT"})
	if err == nil {
		t.Fatal("expected quiet spot book to be rejected")
	}

	// No activity data at all: cannot prove inactivity, must pass.
	s.adapter = staticAdapter{updates: map[string]time.Time{}}
	if err := s.validateLastTradeAge(now, domain.Market{}, domain.Market{},
		domain.OrderBook{Exchange: "indodax", Symbol: "btc_idr"},
		domain.OrderBook{Exchange: "okx", Symbol: "BTC-USDT"}); err != nil {
		t.Fatalf("missing activity data must pass, got: %v", err)
	}
}

func TestLastTradeAgeAdapterWithoutDataNeverBlocks(t *testing.T) {
	s := New(config.Config{MaxLastTradeAgeSeconds: 60}, nil)
	// Plain mock adapter does not implement tradeActivityProvider.
	if err := s.validateLastTradeAge(time.Now(), domain.Market{}, domain.Market{},
		domain.OrderBook{Exchange: "x", Symbol: "y"},
		domain.OrderBook{Exchange: "a", Symbol: "b"}); err != nil {
		t.Fatalf("adapter without activity data must never block, got: %v", err)
	}
}

func TestSpikePersistenceFirstScanRejectedThenAccepted(t *testing.T) {
	s := New(config.Config{SpikeSpreadPct: 1.0, MinSpikeScans: 2}, nil)
	key := "indodax/btc_idr:okx/BTC-USDT"

	// First anomalous scan: rejected as noise.
	if err := s.validateSpikePersistence(key, 2.0); err == nil {
		t.Fatal("first spike must be rejected")
	}
	// Second consecutive anomalous scan: accepted as persistent.
	if err := s.validateSpikePersistence(key, 2.0); err != nil {
		t.Fatalf("persistent spike must be accepted, got: %v", err)
	}
	// A normal spread resets the counter.
	if err := s.validateSpikePersistence(key, 0.2); err != nil {
		t.Fatalf("normal spread must pass, got: %v", err)
	}
	// Spike after reset is fresh again: rejected.
	if err := s.validateSpikePersistence(key, 3.0); err == nil {
		t.Fatal("spike after reset must be rejected again")
	}
}

func TestSpikePersistenceDisabledByDefault(t *testing.T) {
	s := New(config.Config{}, nil)
	if err := s.validateSpikePersistence("k", 99.0); err != nil {
		t.Fatalf("guard must be disabled when spike_spread_pct is 0, got: %v", err)
	}
}

func TestValidateFXRateSanity(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		if err := validateFXRateSanity(25000, 16000, 0); err != nil {
			t.Fatalf("guard must be disabled when fx_rate_sanity_pct is 0, got: %v", err)
		}
	})
	t.Run("rejects deviating rate", func(t *testing.T) {
		err := validateFXRateSanity(25000, 16000, 10)
		if err == nil {
			t.Fatal("rate 50%% above baseline must be rejected at 10%% tolerance")
		}
	})
	t.Run("accepts rate within tolerance", func(t *testing.T) {
		if err := validateFXRateSanity(16400, 16000, 5); err != nil {
			t.Fatalf("rate 2.5%% above baseline must pass at 5%% tolerance: %v", err)
		}
	})
	t.Run("rejects non-positive rate", func(t *testing.T) {
		if err := validateFXRateSanity(0, 16000, 10); err == nil {
			t.Fatal("non-positive rate must be rejected when guard enabled")
		}
	})
	t.Run("no baseline passes", func(t *testing.T) {
		if err := validateFXRateSanity(25000, 0, 10); err != nil {
			t.Fatalf("without a baseline the guard cannot judge, must pass: %v", err)
		}
	})
}
