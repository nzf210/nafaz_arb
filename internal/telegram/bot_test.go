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

