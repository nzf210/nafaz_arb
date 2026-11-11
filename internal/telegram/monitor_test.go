package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func TestOrdersCommandUsesExecutorOrderRegistry(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil { t.Fatal(err) }
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	payload := map[string]interface{}{
		"status": "RUNNING",
		"active_orders": []map[string]interface{}{{
			"internal_order_id": "internal-42", "order_id": "exchange-42",
			"pair": "BTC/USDT", "exchange": "okx", "status": "PARTIALLY_FILLED",
		}},
	}
	raw, _ := json.Marshal(payload)
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, raw, 0).Err(); err != nil { t.Fatal(err) }

	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&sent)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.handleOrdersCommand(context.Background(), -100123456789, 0)
	for _, want := range []string{"internal-42", "BTC/USDT", "okx", "PARTIALLY_FILLED"} {
		if !strings.Contains(sent.Text, want) { t.Fatalf("orders response missing %q: %s", want, sent.Text) }
	}
}

func TestOrdersCommandDoesNotClaimEmptyWhenRegistryMissing(t *testing.T) {
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			_ = json.NewDecoder(r.Body).Decode(&sent)
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true})
		}
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	mr, err := miniredis.Run()
	if err != nil { t.Fatal(err) }
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, `{"status":"RUNNING","pending_orders":[]}`, 0).Err(); err != nil { t.Fatal(err) }
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.handleOrdersCommand(context.Background(), -100123456789, 0)
	if !strings.Contains(sent.Text, "belum mempublikasikan all-order registry") {
		t.Fatalf("expected explicit incomplete-registry warning, got: %s", sent.Text)
	}
}

func TestUnauthorizedChatCannotRunMonitoringCommand(t *testing.T) {
	var sent int
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" { sent++; _ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true}) }
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.handleTextMessage(context.Background(), &Message{Chat:Chat{ID:-999}, Text:"/orders"})
	if sent != 0 { t.Fatalf("unauthorized chat triggered %d replies", sent) }
}
