package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/api"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/history"
	"github.com/nafaz/spot-perp-scanner/internal/scanner"


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

