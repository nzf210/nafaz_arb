package scanner

import (
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestValidateOrderBookAgeUsesReceiveTimeForLocalVenues(t *testing.T) {
	now := time.Now().UTC()
	book := domain.OrderBook{
		Exchange: "indodax",
		Symbol: "btc_idr",
		Timestamp: now.Add(-time.Hour),
		ReceivedAt: now.Add(-2 * time.Second),
		Source: "receive_time",
	}
	freshPerp := domain.OrderBook{
		Exchange: "okx",
		Symbol: "BTC-USDT-SWAP",
		Timestamp: now.Add(-time.Second),
		ReceivedAt: now,
		Source: "exchange_timestamp",
	}
	if err := validateOrderBookAge(book, freshPerp, now, 5); err != nil {
		t.Fatalf("fresh received local book should pass: %v", err)
	}

	book.ReceivedAt = now.Add(-6 * time.Second)
	if err := validateOrderBookAge(book, freshPerp, now, 5); err == nil {
		t.Fatal("stale receive timestamp must be rejected")
	}
}

func TestValidateOrderBookAgeRejectsMissingAndFutureTimestamp(t *testing.T) {
	now := time.Now().UTC()
	missing := domain.OrderBook{Exchange: "indodax", Symbol: "btc_idr"}
	valid := domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Timestamp: now, ReceivedAt: now, Source: "exchange_timestamp",
	}
	if err := validateOrderBookAge(missing, valid, now, 5); err == nil {
		t.Fatal("missing timestamp must be rejected")
	}
	future := valid
	future.Timestamp = now.Add(2 * time.Second)
	if err := validateOrderBookAge(future, valid, now, 5); err == nil {
		t.Fatal("future timestamp beyond clock-skew tolerance must be rejected")
	}
}

func TestValidateCrossExchangeTimestampAlignmentUsesReceiveTimes(t *testing.T) {
	now := time.Now().UTC()
	spot := domain.OrderBook{
		Exchange: "indodax", Symbol: "btc_idr",
		Timestamp: now.Add(-time.Hour), ReceivedAt: now, Source: "receive_time",
	}
	perp := domain.OrderBook{
		Exchange: "okx", Symbol: "BTC-USDT-SWAP",
		Timestamp: now.Add(-30 * time.Minute), ReceivedAt: now.Add(2 * time.Second),
		Source: "exchange_timestamp",
	}
	if err := validateCrossExchangeTimestampAlignment(spot, perp, 3*time.Second); err != nil {
		t.Fatalf("receive times within skew should pass: %v", err)
	}
	perp.ReceivedAt = now.Add(4 * time.Second)
	if err := validateCrossExchangeTimestampAlignment(spot, perp, 3*time.Second); err == nil {
		t.Fatal("receive-time skew above limit must be rejected")
	}
}

func TestNormalizeOrderBookCurrencyPreservesReceiveMetadata(t *testing.T) {
	now := time.Now().UTC()
	book := domain.OrderBook{
		Exchange: "indodax",
		Symbol: "btcusdt",
		Timestamp: now.Add(-time.Hour),
		ReceivedAt: now,
		Source: "receive_time",
		Bids: []domain.PriceLevel{{Price: 64_000.0, Quantity: 1.0}},
		Asks: []domain.PriceLevel{{Price: 64_010.0, Quantity: 2.0}},
	}

	normalized := normalizeOrderBookCurrency(book, 16_000.0)
	if !normalized.ReceivedAt.Equal(now) {
		t.Fatalf("receive time lost during FX normalization: got %s want %s", normalized.ReceivedAt, now)
	}
	if normalized.Source != "receive_time" {
		t.Fatalf("source lost during FX normalization: got %q", normalized.Source)
	}
	if normalized.Timestamp != book.Timestamp {
		t.Fatalf("source timestamp changed during FX normalization: got %s want %s", normalized.Timestamp, book.Timestamp)
	}
}
