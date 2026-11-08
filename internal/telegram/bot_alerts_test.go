package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"


)

func TestAlertOnScan(t *testing.T) {
	var mu sync.Mutex
	var sentAlerts []SendMessageReq

	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var req SendMessageReq
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				mu.Lock()
				sentAlerts = append(sentAlerts, req)
				mu.Unlock()
			}
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}

	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	res := domain.ScanResult{
		ScanID: "test-scan",
		Opportunities: []domain.Opportunity{
			{
				OpportunityKey:      "ETH/USDT",
				BaseAsset:           "ETH",
				QuoteAsset:          "USDT",
				TradingEligible:     true,
				ProfitabilityStatus: "PROFITABLE_IN_RANGE",
				NetROIPct:           1.25,
				NetProfit:           12.5,
				LastScannedAt:       time.Now().UTC(),
			},
		},
	}

	bot.onScanCompleted(context.Background(), res)

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	alertsCount := len(sentAlerts)
	mu.Unlock()

	if alertsCount != 1 {
		t.Fatalf("expected 1 alert sent, got %d", alertsCount)
	}
}

func TestAlertThresholdsAndCooldown(t *testing.T) {
	alerts := make(chan SendMessageReq, 4)
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var req SendMessageReq
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				alerts <- req
			}
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}

	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.cfg.Telegram.AlertCooldownMinutes = 15
	bot.cfg.Telegram.AlertMinNetROIPct = 1.5
	bot.cfg.Telegram.AlertMinNetProfitQuote = 10
	result := domain.ScanResult{Opportunities: []domain.Opportunity{{
		OpportunityKey:  "ETH/USDT",
		BaseAsset:       "ETH",
		QuoteAsset:      "USDT",
		TradingEligible: true,
		NetROIPct:       1.25,
		NetProfit:       12.5,
	}}}

	bot.onScanCompleted(context.Background(), result)
	select {
	case <-alerts:
		t.Fatal("alert below configured ROI threshold was sent")
	case <-time.After(50 * time.Millisecond):
	}

	bot.cfg.Telegram.AlertMinNetROIPct = 1
	bot.onScanCompleted(context.Background(), result)
	bot.onScanCompleted(context.Background(), result)
	select {
	case <-alerts:
	case <-time.After(time.Second):
		t.Fatal("expected alert to be sent")
	}
	select {
	case <-alerts:
		t.Fatal("cooldown should suppress duplicate alert")
	case <-time.After(50 * time.Millisecond):
	}

	bot.mu.Lock()
	bot.alertedKeys["ETH/USDT"] = time.Now().Add(-16 * time.Minute)
	bot.mu.Unlock()
	bot.onScanCompleted(context.Background(), result)
	select {
	case <-alerts:
	case <-time.After(time.Second):
		t.Fatal("expected alert again after cooldown elapsed")
	}
}

func TestMuteSuppressesAlerts(t *testing.T) {
	alerts := make(chan SendMessageReq, 2)
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var req SendMessageReq
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				alerts <- req
			}
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}

	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	result := domain.ScanResult{Opportunities: []domain.Opportunity{{
		OpportunityKey:      "ETH/USDT",
		BaseAsset:           "ETH",
		QuoteAsset:          "USDT",
		TradingEligible:     true,
		ProfitabilityStatus: "PROFITABLE_IN_RANGE",
		NetROIPct:           1.25,
		NetProfit:           12.5,
		LastScannedAt:       time.Now().UTC(),
	}}}

	// Set mute
	bot.mutedUntil = time.Now().UTC().Add(10 * time.Minute)
	bot.onScanCompleted(context.Background(), result)

	select {
	case <-alerts:
		t.Fatal("alert should be suppressed when muted")
	case <-time.After(50 * time.Millisecond):
	}

	// Unmute
	bot.mutedUntil = time.Time{}
	bot.onScanCompleted(context.Background(), result)

	select {
	case <-alerts:
	case <-time.After(time.Second):
		t.Fatal("expected alert to be sent after unmuting")
	}
}

