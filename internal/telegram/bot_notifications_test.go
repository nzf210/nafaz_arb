package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"


)

func TestFormatStatusTruncation(t *testing.T) {
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
	}
	bot, server, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	// Inject a simulated scan with very long errors
	longErrors := make([]string, 50)
	for i := range longErrors {
		longErrors[i] = "exchange tokocrypto orderbook fetch failed with connection reset by peer timeout error details 1234567890"
	}
	server.FinishScanForTest(domain.ScanResult{
		ScanID:       "test-scan-long",
		Complete:     false,
		Errors:       longErrors,
		TotalMarkets: 10,
	}, false, nil)

	statusStr := bot.formatStatus()
	if !strings.Contains(statusStr, "... (dipotong)") {
		t.Errorf("expected long error to be truncated with '... (dipotong)', got: %s", statusStr)
	}
	if len(statusStr) > 4096 {
		t.Errorf("expected formatted status length <= 4096, got %d", len(statusStr))
	}
}

func TestSendReplyWithMarkupHTMLEntityFallback(t *testing.T) {
	var mu sync.Mutex
	var receivedRequests []SendMessageReq

	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req SendMessageReq
		_ = json.NewDecoder(r.Body).Decode(&req)

		mu.Lock()
		receivedRequests = append(receivedRequests, req)
		reqIndex := len(receivedRequests)
		mu.Unlock()

		if reqIndex == 1 {
			// First request fails with HTML parse entity error
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(APIResponse[any]{
				Ok:          false,
				Description: "Bad Request: can't parse entities in message text",
			})
			return
		}
		// Fallback plain-text request succeeds
		_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
	}

	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	bot.sendReplyWithMarkup(context.Background(), -100123456789, 0, "Hello <b>broken <tag", nil)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedRequests) != 2 {
		t.Fatalf("expected 2 requests (original + plain-text fallback), got %d", len(receivedRequests))
	}
	if receivedRequests[0].ParseMode != "HTML" {
		t.Errorf("expected first request to be HTML, got %s", receivedRequests[0].ParseMode)
	}
	if receivedRequests[1].ParseMode != "" {
		t.Errorf("expected fallback request to have empty ParseMode, got %s", receivedRequests[1].ParseMode)
	}
}




func TestBroadcastExecutionFailure(t *testing.T) {
	var received []SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var req SendMessageReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		received = append(received, req)
		_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	bot.broadcastTradeEvent(context.Background(), redishandler.TradeEvent{
		Event:         "EXECUTION_FAILURE",
		PairTag:       "indodax:btc_idr",
		Reason:        "INSUFFICIENT_OR_UNBALANCED_BALANCE",
		Message:       "Saldo Spot/Futures tidak mencukupi.",
		LocalExchange: "indodax",
		GlobalExchange: "okx",
		State:         "FAILED",
	})

	if len(received) != 1 {
		t.Fatalf("expected one Telegram failure notification, got %d", len(received))
	}
	for _, want := range []string{"ENTRY / EXECUTION GAGAL", "INSUFFICIENT_OR_UNBALANCED_BALANCE", "Saldo Spot/Futures"} {
		if !strings.Contains(received[0].Text, want) {
			t.Errorf("Telegram failure notification missing %q: %s", want, received[0].Text)
		}
	}
}

func TestBroadcastExecutionFailureCooldown(t *testing.T) {
	var received int
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		received++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	ev := redishandler.TradeEvent{
		Event: "EXECUTION_FAILURE",
		PairTag: "indodax:btc_idr",
		Reason: "BALANCE_FEED_UNAVAILABLE",
		Message: "Saldo live tidak tersedia.",
	}
	bot.broadcastTradeEvent(context.Background(), ev)
	bot.broadcastTradeEvent(context.Background(), ev)
	if received != 1 {
		t.Fatalf("expected cooldown to suppress duplicate alert, got %d messages", received)
	}
}
