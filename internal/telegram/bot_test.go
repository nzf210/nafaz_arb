package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/api"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/history"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/nafaz/spot-perp-scanner/internal/scanner"
	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func setupTestBot(t *testing.T, handler http.HandlerFunc) (*Bot, *api.Server, *httptest.Server) {
	ts := httptest.NewServer(handler)
	cfg := config.Config{
		Mode:                     "MOCK",
		ScanIntervalSeconds:      30,
		CapitalTotal:             1000,
		AllocationPerPair:        0.5,
		PerpLeverage:             1,
		MinNetROIPct:             0.5,
		MaxNetROIPct:             2.0,
		HistoryPath:              t.TempDir() + "/test-history.json",
		MaxMissedSuccessfulScans: 2,
		QuoteAssets:              []string{"USDT"},
		Telegram: config.TelegramConfig{
			Enabled:        true,
			BotToken:       "test-token",
			ChatID:         -100123456789,
			AllowedChatIDs: []int64{-100123456789},
			AlertOnNew:     true,
		},
	}
	adapter := exchange.NewMockAdapter()
	svc := scanner.New(cfg, adapter)
	store, err := history.New(cfg.HistoryPath)
	if err != nil {
		t.Fatalf("history new: %v", err)
	}
	server := api.New(svc, store, cfg)
	bot := New(cfg, server)
	bot.SetBaseURL(ts.URL)
	bot.botUsername = "nafaz_bot"
	return bot, server, ts
}

func TestChatAllowed(t *testing.T) {
	cfg := config.Config{
		Telegram: config.TelegramConfig{
			ChatID:         -100,
			AllowedChatIDs: []int64{-100, -200},
		},
	}
	bot := New(cfg, nil)

	if !bot.isChatAllowed(-100) {
		t.Errorf("expected -100 to be allowed")
	}
	if !bot.isChatAllowed(-200) {
		t.Errorf("expected -200 to be allowed")
	}
	if bot.isChatAllowed(-300) {
		t.Errorf("expected -300 to be denied")
	}
}

func TestHandleCommands(t *testing.T) {
	var mu sync.Mutex
	var sentMessages []SendMessageReq

	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/getMe":
			_ = json.NewEncoder(w).Encode(APIResponse[User]{
				Ok:     true,
				Result: User{ID: 1, FirstName: "NafazBot", Username: "nafaz_bot"},
			})
		case "/setMyCommands":
			_ = json.NewEncoder(w).Encode(APIResponse[bool]{Ok: true, Result: true})
		case "/sendMessage":
			var req SendMessageReq
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				mu.Lock()
				sentMessages = append(sentMessages, req)
				mu.Unlock()
			}
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}

	bot, server, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initial scan so server has latest scan results
	_, err := server.TriggerScan(ctx)
	if err != nil {
		t.Fatalf("trigger scan: %v", err)
	}

	testCases := []struct {
		name string
		text string
	}{
		{"menu command", "/menu"},
		{"help command", "/help"},
		{"help command with bot mention", "/help@nafaz_bot"},
		{"status command", "/status"},
		{"latest command", "/latest"},
		{"pair command with symbol", "/pair BTC"},
		{"pair command without arg", "/pair"},
		{"active command", "/active"},
		{"frozen command", "/frozen"},
		{"mute command", "/mute 30m"},
		{"unmute command", "/unmute"},
		{"metrics command", "/metrics"},
		{"scan command", "/scan"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			countBefore := len(sentMessages)
			mu.Unlock()

			u := Update{
				UpdateID: 1,
				Message: &Message{
					MessageID: 10,
					Chat:      Chat{ID: -100123456789, Type: "supergroup"},
					Text:      tc.text,
				},
			}
			bot.handleUpdate(ctx, u)

			mu.Lock()
			countAfter := len(sentMessages)
			mu.Unlock()

			if countAfter <= countBefore {
				t.Errorf("expected message to be sent for command %s", tc.text)
			}
		})
	}
}

func TestHandleCallbackQuery(t *testing.T) {
	var mu sync.Mutex
	var sentMessages []SendMessageReq
	var answeredCallbacks []string

	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/answerCallbackQuery":
			var req AnswerCallbackQueryReq
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				mu.Lock()
				answeredCallbacks = append(answeredCallbacks, req.CallbackQueryID)
				mu.Unlock()
			}
			_ = json.NewEncoder(w).Encode(APIResponse[bool]{Ok: true, Result: true})
		case "/sendMessage":
			var req SendMessageReq
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				mu.Lock()
				sentMessages = append(sentMessages, req)
				mu.Unlock()
			}
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}

	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	callbackTests := []string{"status", "active", "frozen", "pair:ETH", "menu", "help"}
	for _, cbData := range callbackTests {
		u := Update{
			UpdateID: 2,
			CallbackQuery: &CallbackQuery{
				ID:   "cb_" + cbData,
				Data: cbData,
				Message: &Message{
					MessageID: 11,
					Chat:      Chat{ID: -100123456789, Type: "supergroup"},
				},
			},
		}
		bot.handleUpdate(context.Background(), u)
	}

	mu.Lock()
	msgs := len(sentMessages)
	ans := len(answeredCallbacks)
	mu.Unlock()

	if msgs != len(callbackTests) {
		t.Errorf("expected %d reply messages, got %d", len(callbackTests), msgs)
	}
	if ans != len(callbackTests) {
		t.Errorf("expected %d answered callbacks, got %d", len(callbackTests), ans)
	}
}

func TestCommandForDifferentBotIgnored(t *testing.T) {
	var sentCount int
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			sentCount++
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}

	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	u := Update{
		UpdateID: 1,
		Message: &Message{
			MessageID: 10,
			Chat:      Chat{ID: -100123456789, Type: "supergroup"},
			Text:      "/scan@other_bot",
		},
	}
	bot.handleUpdate(context.Background(), u)

	if sentCount != 0 {
		t.Errorf("expected 0 messages sent, got %d", sentCount)
	}
}

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

func TestScanRejectionSummaryNotification(t *testing.T) {
	alerts := make(chan SendMessageReq, 2)
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var req SendMessageReq
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil { alerts <- req }
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.cfg.Telegram.AlertCooldownMinutes = 15
	bot.onScanCompleted(context.Background(), domain.ScanResult{
		ScanID: "rejected-scan", TotalMarkets: 1800, MatchedPairs: 416,
		ActivePairs: 416, ExcludedPairs: 416,
		Opportunities: []domain.Opportunity{{
			BaseAsset: "BTC", TradingEligible: false,
			ExclusionReasons: []string{"ENTRY_SPREAD_BELOW_MINIMUM", "NEGATIVE_FUNDING_RATE"},
		}},
	})
	select {
	case alert := <-alerts:
		if !strings.Contains(alert.Text, "ENTRY_SPREAD_BELOW_MINIMUM") { t.Fatalf("missing rejection reason: %s", alert.Text) }
		if !strings.Contains(alert.Text, "416") { t.Fatalf("missing matched count: %s", alert.Text) }
	case <-time.After(time.Second):
		t.Fatal("expected Telegram rejection summary")
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

func TestHandlePositionsAndForceClose(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rc := redishandler.NewWithClient(rdb)
	defer rc.Close()

	sentMsgs := make(chan SendMessageReq, 10)
	const allowedChatID = -100123456789
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/sendMessage" {
			var req SendMessageReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			sentMsgs <- req
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
			return
		}
		if r.URL.Path == "/answerCallbackQuery" {
			_ = json.NewEncoder(w).Encode(APIResponse[bool]{Ok: true})
			return
		}
	}

	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.SetRedisClient(rc)

	ctx := context.Background()

	// 1. Test /positions when no active positions exist
	bot.handleCommand(ctx, &Message{Chat: Chat{ID: allowedChatID}, MessageID: 1}, "/positions", []string{"/positions"})
	msg1 := <-sentMsgs
	if !strings.Contains(msg1.Text, "tidak ada posisi") {
		t.Errorf("expected no active positions message, got: %s", msg1.Text)
	}

	// 2. Set an active position in Redis
	posKey := redishandler.KeyPrefixActivePos + "indodax:beatidr"
	posJSON := `{"local_exchange":"indodax","global_exchange":"okx","symbol":"beatidr","entry_price_idr":1762,"short_entry_usdt":0.09787,"qty":5130.53,"accumulated_funding_usdt":0.1165}`
	_ = rdb.Set(ctx, posKey, posJSON, 0).Err()

	// 3. Test /positions when active position exists
	bot.handleCommand(ctx, &Message{Chat: Chat{ID: allowedChatID}, MessageID: 2}, "/positions", []string{"/positions"})
	msg2 := <-sentMsgs
	if !strings.Contains(msg2.Text, "BEATIDR") || !strings.Contains(strings.ToUpper(msg2.Text), "INDODAX") {
		t.Errorf("expected active position details, got: %s", msg2.Text)
	}
	if msg2.ReplyMarkup == nil || len(msg2.ReplyMarkup.InlineKeyboard) == 0 {
		t.Fatal("expected inline keyboard with force close button")
	}

	// 4. Test force close callback with 2-step confirmation
	sub := rdb.Subscribe(ctx, redishandler.ChannelForceClose)
	defer sub.Close()
	time.Sleep(10 * time.Millisecond)

	bot.handleCallbackQuery(ctx, &CallbackQuery{
		ID:      "cb1",
		From:    User{ID: allowedChatID},
		Message: &Message{Chat: Chat{ID: allowedChatID}},
		Data:    "force_close:indodax:beatidr",
	})
	msg3 := <-sentMsgs
	if !strings.Contains(msg3.Text, "Konfirmasi Tutup Paksa (Preview)") {
		t.Errorf("expected force close preview confirmation, got: %s", msg3.Text)
	}

	// Confirm force close
	bot.handleCallbackQuery(ctx, &CallbackQuery{
		ID:      "cb2",
		From:    User{ID: allowedChatID},
		Message: &Message{Chat: Chat{ID: allowedChatID}},
		Data:    "confirm_fc:indodax:beatidr",
	})
	msg4 := <-sentMsgs
	if !strings.Contains(msg4.Text, "Perintah Tutup Paksa Dikonfirmasi & Terkirim") {
		t.Errorf("expected confirmed force close, got: %s", msg4.Text)
	}

	// 5. Test /killswitch authorization with user allowlist in group chat
	// Group chat ID is negative (-100123456789), whereas user ID is positive.
	// When AllowedUserIDs is configured, only listed users can execute sensitive actions.
	bot.cfg.Telegram.AllowedUserIDs = []int64{12345678}

	// Unauthorized user in group
	bot.handleCommand(ctx, &Message{
		Chat:      Chat{ID: allowedChatID},
		From:      &User{ID: 99999999},
		MessageID: 5,
	}, "/killswitch", []string{"/killswitch", "on"})
	msg5 := <-sentMsgs
	if !strings.Contains(msg5.Text, "Akses ditolak") {
		t.Errorf("expected access denied for unauthorized user on /killswitch, got: %s", msg5.Text)
	}

	// Authorized user in group
	bot.handleCommand(ctx, &Message{
		Chat:      Chat{ID: allowedChatID},
		From:      &User{ID: 12345678},
		MessageID: 6,
	}, "/killswitch", []string{"/killswitch", "on"})
	msg6 := <-sentMsgs
	if !strings.Contains(msg6.Text, "KILL SWITCH AKTIF") {
		t.Errorf("expected kill switch activated for authorized user, got: %s", msg6.Text)
	}
}

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

type fakeOpportunityHistoryReader struct {
	scans []domain.ScanResult
	err error
	limit int
}

func (f *fakeOpportunityHistoryReader) LatestOpportunityScans(_ context.Context, limit int) ([]domain.ScanResult, error) {
	f.limit = limit
	return f.scans, f.err
}

func TestLatestCommandLoadsTenRecentScansFromHistoryReader(t *testing.T) {
	messages := make(chan SendMessageReq, 12)
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var req SendMessageReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			messages <- req
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	reader := &fakeOpportunityHistoryReader{}
	for i := 0; i < 10; i++ {
		reader.scans = append(reader.scans, domain.ScanResult{
			ScanID: fmt.Sprintf("scan-%02d", 10-i),
			CompletedAt: time.Date(2026, 11, 9, 0, i, 0, 0, time.UTC),
			Opportunities: []domain.Opportunity{{
				BaseAsset: "BTC", QuoteAsset: "IDR", SpotSymbol: "btc_idr",
				PerpSymbol: "BTC/USDT:USDT", NetROIPct: float64(i),
				LastScannedAt: time.Date(2026, 11, 9, 0, i, 0, 0, time.UTC),
			}},
		})
	}
	bot.SetOpportunityHistoryReader(reader)
	bot.handleCommand(context.Background(),
		&Message{Chat: Chat{ID: -100123456789}, MessageID: 21},
		"/latest", []string{"/latest"})

	if reader.limit != 10 {
		t.Fatalf("reader limit=%d, want 10", reader.limit)
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-messages:
			if !strings.Contains(msg.Text, fmt.Sprintf("Scan Peluang Terbaru %d/10", i+1)) {
				t.Fatalf("message %d missing scan sequence: %s", i+1, msg.Text)
			}
			if !strings.Contains(msg.Text, fmt.Sprintf("scan-%02d", 10-i)) {
				t.Fatalf("message %d missing scan ID: %s", i+1, msg.Text)
			}
		case <-time.After(time.Second):
			t.Fatalf("only received %d of 10 scan messages", i)
		}
	}
}

func TestLatestCommandReportsHistoryReadFailure(t *testing.T) {
	messages := make(chan SendMessageReq, 1)
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var req SendMessageReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			messages <- req
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.SetOpportunityHistoryReader(&fakeOpportunityHistoryReader{err: fmt.Errorf("database unavailable")})
	bot.handleCommand(context.Background(),
		&Message{Chat: Chat{ID: -100123456789}, MessageID: 22},
		"/latest", []string{"/latest"})
	msg := <-messages
	if !strings.Contains(msg.Text, "Gagal mengambil 10 scan") {
		t.Fatalf("unexpected history failure message: %s", msg.Text)
	}
}


func TestOpportunityAlertShowsSpotAndFuturesCEX(t *testing.T) {
	bot := New(config.Config{}, nil)
	message := bot.formatOpportunityAlert(domain.Opportunity{
		BaseAsset: "ATH", QuoteAsset: "USDT", LocalExchange: "tokocrypto", GlobalExchange: "okx",
		ExecutionMode: "MAKER_TAKER", SpotSymbol: "ATH/USDT", PerpSymbol: "ATH-USDT-SWAP",
	})
	if !strings.Contains(message, "Spot CEX:</b> TOKOCRYPTO") {
		t.Fatalf("spot CEX missing from opportunity alert: %s", message)
	}
	if !strings.Contains(message, "Futures CEX:</b> OKX") {
		t.Fatalf("futures CEX missing from opportunity alert: %s", message)
	}
}

func TestPendingOrdersCommandShowsExecutorSnapshot(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil { t.Fatal(err) }
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	payload := `{"status":"RUNNING","mock_mode":false,"timestamp_ms":1794220000000,"pending_entries":1,"pending_exits":0,"pending_orders":[{"pair_tag":"toko:ath_usdt","leg":"SPOT_ENTRY","order_id":"order-123","symbol":"ATH/USDT","local_exchange":"tokocrypto","global_exchange":"okx","price":0.0044,"qty":2200,"filled":200,"remaining":2000,"status":"PARTIALLY_FILLED","created_at_ms":1794219900000}]}`
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, payload, time.Minute).Err(); err != nil { t.Fatal(err) }
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&sent)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.handlePendingOrdersCommand(context.Background(), -100123456789, 0)
	if !strings.Contains(sent.Text, "PARTIALLY_FILLED") || !strings.Contains(sent.Text, "TOKOCRYPTO") || !strings.Contains(sent.Text, "OKX") || !strings.Contains(sent.Text, "order-123") {
		t.Fatalf("pending order details missing: %s", sent.Text)
	}
}

func TestSendReplyWithMarkupTruncatesOversizedMessages(t *testing.T) {
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&sent)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	oversized := "<b>Error:</b> " + strings.Repeat("x", 5000)
	bot.sendReplyWithMarkup(context.Background(), -100123456789, 0, oversized, nil)
	if len([]rune(sent.Text)) > 4096 {
		t.Fatalf("expected truncated message <= 4096 runes, got %d", len([]rune(sent.Text)))
	}
	if sent.ParseMode != "" {
		t.Fatalf("expected ParseMode cleared after HTML truncation, got %q", sent.ParseMode)
	}
}

func TestFormatScanResultTenItemsFitsTelegramAndFormatsMicroPrices(t *testing.T) {
	bot := New(config.Config{}, nil)
	ops := make([]domain.Opportunity, 0, 12)
	samples := []struct {
		base   string
		spot   float64
		perp   float64
		floor  float64
		queue  float64
		status string
		elig   bool
		excl   []string
	}{
		{"SOON", 0.2298, 0.2403, 0.2312, 258, "PROFITABLE_IN_RANGE", true, nil},
		{"FLOKI", 0.00002815, 0.00002850, 0.00002820, 42655279, "PROFITABLE_IN_RANGE", true, nil},
		{"ATH", 0.004312, 0.004405, 0.004310, 13577.56, "BELOW_MIN_TARGET", false, []string{"BELOW_MIN_TARGET"}},
		{"MOODENG", 0.0300, 0.0303, 0.0302, 2512.24, "BELOW_MIN_TARGET", false, []string{"BELOW_MIN_TARGET"}},
		{"JELLYJELLY", 0.0403, 0.0407, 0.0405, 7925, "BELOW_MIN_TARGET", false, []string{"BELOW_MIN_TARGET"}},
		{"FARTCOIN", 0.1090, 0.1098, 0.1097, 1496.40, "BELOW_MIN_TARGET", false, []string{"BELOW_MIN_TARGET", "ENTRY_SPREAD_BELOW_MINIMUM"}},
		{"SPX", 0.3419, 0.3438, 0.3441, 376.48, "NOT_PROFITABLE", false, []string{"NOT_PROFITABLE", "MIN_NET_PROFIT_NOT_MET"}},
		{"LUNA", 0.0950, 0.0955, 0.0951, 800, "NOT_PROFITABLE", false, []string{"NOT_PROFITABLE"}},
		{"PEPE", 0.00000842, 0.00000851, 0.00000843, 9100000, "NOT_PROFITABLE", false, []string{"NOT_PROFITABLE"}},
		{"BONK", 0.00001420, 0.00001435, 0.00001422, 5200000, "NOT_PROFITABLE", false, []string{"NOT_PROFITABLE"}},
		{"DOGE", 0.1500, 0.1501, 0.1500, 1000, "NOT_PROFITABLE", false, []string{"NOT_PROFITABLE"}},
	}
	for _, s := range samples {
		ops = append(ops, domain.Opportunity{
			BaseAsset:              s.base,
			QuoteAsset:             "USDT",
			LocalExchange:          "tokocrypto",
			GlobalExchange:         "okx",
			ExecutionMode:          "MAKER_TAKER",
			SpotSymbol:             "toko:" + s.base + "_IDR",
			PerpSymbol:             s.base + "-USDT-SWAP",
			SpotEntryPrice:         s.spot,
			PerpEntryPrice:         s.perp,
			MinAcceptablePerpPrice: s.floor,
			SpotQueueDepth:         s.queue,
			MaxOrderTTLSeconds:     30,
			IsSynthetic:            true,
			OriginalSpotQuote:      "IDR",
			SyntheticFXRate:        18282,
			ProfitabilityStatus:    s.status,
			TradingEligible:        s.elig,
			ExclusionReasons:       s.excl,
			NetProfit:              0.2148,
			NetROIPct:              1.94,
			SpreadPnL:              0.2483,
			FundingPnL:             0.0020,
			TradingFees:            0.0355,
			CurrentFundingRate:     0.000348,
		})
	}

	formatted := bot.formatScanResult(domain.ScanResult{
		ScanID:        "706a0d21b1ee722f",
		MatchedPairs:  216,
		EligiblePairs: 2,
		Opportunities: ops,
	}, "Scan Peluang Terbaru 10/10 • 16:22:59 UTC")

	if runes := len([]rune(formatted)); runes > 4000 {
		t.Fatalf("expected formatted output <= 4000 runes to preserve HTML mode, got %d:\n%s", runes, formatted)
	}
	for _, want := range []string{
		"10. 🔴 <b>BONK/USDT</b>",
		"0.00002815",
		"0.004312",
		"42.66M",
		"ENTRY_SPREAD_BELOW_MINIMUM",
		"Menampilkan top 10 dari 11",
	} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("missing %q in formatted output:\n%s", want, formatted)
		}
	}
	if strings.Contains(formatted, "<code>0.0000</code>") {
		t.Fatalf("unexpected 0.0000 micro-price truncation:\n%s", formatted)
	}
}

func TestSetConfigCommandUpdatesMemoryAndFile(t *testing.T) {
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&sent)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	tmpCfg := t.TempDir() + "/config.json"
	initialData := []byte(`{"capital_total": 100, "allocation_per_pair": 0.5}`)
	if err := os.WriteFile(tmpCfg, initialData, 0644); err != nil {
		t.Fatalf("write tmp config: %v", err)
	}
	bot.SetConfigPath(tmpCfg)

	msg := &Message{
		Chat: Chat{ID: -100123456789},
		From: &User{ID: 1001},
	}

	bot.handleSetConfigCommand(context.Background(), msg, []string{"capital", "250"})
	if bot.cfg.CapitalTotal != 250 {
		t.Fatalf("expected CapitalTotal 250, got %f", bot.cfg.CapitalTotal)
	}
	if !strings.Contains(sent.Text, "250.00") {
		t.Fatalf("expected confirmation message, got: %s", sent.Text)
	}

	bot.handleSetConfigCommand(context.Background(), msg, []string{"allocation", "0.25"})
	if bot.cfg.AllocationPerPair != 0.25 {
		t.Fatalf("expected AllocationPerPair 0.25, got %f", bot.cfg.AllocationPerPair)
	}

	readBack, err := os.ReadFile(tmpCfg)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(readBack, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw["capital_total"] != 250.0 {
		t.Fatalf("expected file capital_total 250, got %v", raw["capital_total"])
	}
	if raw["allocation_per_pair"] != 0.25 {
		t.Fatalf("expected file allocation_per_pair 0.25, got %v", raw["allocation_per_pair"])
	}
}

func TestNotificationSwitchFiltersAlerts(t *testing.T) {
	var sentMessages []SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var sent SendMessageReq
			_ = json.NewDecoder(r.Body).Decode(&sent)
			sentMessages = append(sentMessages, sent)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	tmpCfg := t.TempDir() + "/config.json"
	_ = os.WriteFile(tmpCfg, []byte(`{}`), 0644)
	bot.SetConfigPath(tmpCfg)

	// 1. Set mode to trades_only (only fulfilled pending orders & closed positions)
	bot.setNotifMode(context.Background(), -100123456789, "trades_only")
	if bot.cfg.Telegram.NotifyOpportunitiesEnabled() || bot.cfg.Telegram.NotifyRejectionsEnabled() || bot.cfg.Telegram.NotifyErrorsEnabled() {
		t.Fatalf("opps, rejections, errors should be disabled in trades_only mode")
	}
	if !bot.cfg.Telegram.NotifyFillsEnabled() || !bot.cfg.Telegram.NotifyClosesEnabled() {
		t.Fatalf("fills and closes should be enabled in trades_only mode")
	}

	sentMessages = nil

	// 2. An execution failure should be suppressed
	bot.broadcastTradeEvent(context.Background(), redishandler.TradeEvent{
		Event:   "EXECUTION_FAILURE",
		Symbol:  "BTC",
		Reason:  "INSUFFICIENT_FUNDS",
		Message: "test failure",
	})
	if len(sentMessages) != 0 {
		t.Fatalf("expected 0 messages for suppressed error, got %d", len(sentMessages))
	}

	// 3. POSITION_OPENED (pending order filled) should be delivered
	bot.broadcastTradeEvent(context.Background(), redishandler.TradeEvent{
		Event:         "POSITION_OPENED",
		Symbol:        "BTC",
		LocalExchange: "indodax",
		GlobalExchange: "okx",
		Qty:           0.001,
	})
	if len(sentMessages) != 1 || !strings.Contains(sentMessages[0].Text, "POSISI ARBITRASE TERBUKA") {
		t.Fatalf("expected POSITION_OPENED alert sent, got: %+v", sentMessages)
	}

	// 4. POSITION_CLOSED should be delivered
	sentMessages = nil
	bot.broadcastTradeEvent(context.Background(), redishandler.TradeEvent{
		Event:         "POSITION_CLOSED",
		Symbol:        "BTC",
		Qty:           0.001,
		NetProfitUSDT: 0.5,
	})
	if len(sentMessages) != 1 || !strings.Contains(sentMessages[0].Text, "POSISI BERHASIL DITUTUP") {
		t.Fatalf("expected POSITION_CLOSED alert sent, got: %+v", sentMessages)
	}

	// 5. Toggle fills OFF -> now POSITION_OPENED should be suppressed
	bot.toggleNotifKey(context.Background(), -100123456789, "fills")
	if bot.cfg.Telegram.NotifyFillsEnabled() {
		t.Fatalf("expected fills disabled after toggle")
	}
	sentMessages = nil
	bot.broadcastTradeEvent(context.Background(), redishandler.TradeEvent{
		Event:  "POSITION_OPENED",
		Symbol: "BTC",
	})
	if len(sentMessages) != 0 {
		t.Fatalf("expected POSITION_OPENED suppressed when fills=false, got %d", len(sentMessages))
	}
}

type testStrategyStore struct {
	saved map[string]interface{}
}

func (m *testStrategyStore) SaveStrategyConfig(ctx context.Context, name string, configData map[string]interface{}, executionMode string) error {
	m.saved = configData
	return nil
}

func (m *testStrategyStore) LoadStrategyConfig(ctx context.Context, name string) (map[string]interface{}, error) {
	return m.saved, nil
}

func TestSetConfigCommandReadOnlyDiskPersistsToDBAndRedis(t *testing.T) {
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&sent)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()

	// Setup MiniRedis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	bot.SetRedisClient(redishandler.NewWithClient(rdb))

	// Setup Mock Strategy Store (Supabase)
	store := &testStrategyStore{}
	bot.SetStrategyConfigStore(store)

	// Simulate read-only config path (e.g. /app/config.json with 0444 permission)
	tmpCfg := t.TempDir() + "/readonly_config.json"
	_ = os.WriteFile(tmpCfg, []byte(`{"capital_total": 100}`), 0444)
	bot.SetConfigPath(tmpCfg)

	msg := &Message{
		Chat: Chat{ID: -100123456789},
		From: &User{ID: 1001},
	}

	bot.handleSetConfigCommand(context.Background(), msg, []string{"minroi", "0.45"})

	// Verify RAM updated
	if bot.cfg.MinNetROIPct != 0.45 {
		t.Fatalf("expected MinNetROIPct 0.45 in RAM, got %f", bot.cfg.MinNetROIPct)
	}

	// Verify DB (Supabase) persisted
	if store.saved == nil || store.saved["min_net_roi_pct"] != 0.45 {
		t.Fatalf("expected MinNetROIPct 0.45 saved to DB, got %+v", store.saved)
	}

	// Verify Redis persisted
	cachedCfg, err := bot.redisClient.LoadConfig(context.Background())
	if err != nil || cachedCfg["min_net_roi_pct"] != 0.45 {
		t.Fatalf("expected MinNetROIPct 0.45 in Redis, got %+v (err: %v)", cachedCfg, err)
	}

	// Verify Telegram response is success and graceful about read-only disk
	if !strings.Contains(sent.Text, "Konfigurasi Berhasil Diperbarui") {
		t.Fatalf("expected success message, got: %s", sent.Text)
	}
	if !strings.Contains(sent.Text, "read-only") {
		t.Fatalf("expected read-only acknowledgment, got: %s", sent.Text)
	}
}


