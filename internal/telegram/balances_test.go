package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func TestBalanceCommandShowsLiveExchangeAssets(t *testing.T) {
	var received SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&received)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.cfg.Telegram.AllowedUserIDs = []int64{42}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	bot.SetRedisClient(redishandler.NewWithClient(rdb))

	snapshot := redishandler.ExchangeBalanceSnapshot{
		Mode: "LIVE", TimestampMS: time.Now().UnixMilli(),
		Exchanges: []redishandler.ExchangeBalanceVenue{
			{Name: "indodax", Role: "spot", Status: "OK", Assets: []redishandler.ExchangeAssetBalance{
				{Asset: "IDR", Total: 1500000, Free: 1200000, Used: 300000},
				{Asset: "BTC", Total: 0.01, Free: 0.008, Used: 0.002},
			}},
			{Name: "okx", Role: "futures", Status: "OK", Assets: []redishandler.ExchangeAssetBalance{
				{Asset: "USDT", Total: 50, Free: 45, Used: 5},
			}},
		},
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), redishandler.KeyExchangeBalances, raw, 2*time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	bot.handleBalanceCommand(context.Background(), -100123456789, 10, 42)
	for _, want := range []string{"LIVE Exchange Balances", "INDODAX", "BTC", "OKX", "USDT", "Total:", "Free:", "Used:"} {
		if !strings.Contains(received.Text, want) {
			t.Errorf("balance response missing %q: %s", want, received.Text)
		}
	}
}

func TestBalanceCommandRejectsUnauthorizedUser(t *testing.T) {
	var received SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&received)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.cfg.Telegram.AllowedUserIDs = []int64{99}

	bot.handleBalanceCommand(context.Background(), -100123456789, 10, 42)
	if !strings.Contains(received.Text, "Akses ditolak") {
		t.Fatalf("expected access denial, got: %s", received.Text)
	}
}
