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
		"recovery": map[string]interface{}{"complete": true, "tracked_orders": 1},
	}
	raw, _ := json.Marshal(payload)
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, raw, 0).Err(); err != nil { t.Fatal(err) }
	if err := rdb.HSet(context.Background(), redishandler.KeyOrderRecoveryIndex, "internal-42", "1").Err(); err != nil { t.Fatal(err) }
	if err := rdb.Set(context.Background(), redishandler.KeyPrefixOrderRecord+"internal-42", `{"local_order_id":"internal-42","exchange_order_id":"exchange-42","pair_key":"BTC/USDT","exchange":"okx","status":"PARTIALLY_FILLED","filled_quantity":1}`, 0).Err(); err != nil { t.Fatal(err) }

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
	var sent []SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" {
			var message SendMessageReq; _ = json.NewDecoder(r.Body).Decode(&message); sent = append(sent, message)
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
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, `{"status":"RUNNING","recovery":{"complete":false,"tracked_orders":0},"pending_orders":[]}`, 0).Err(); err != nil { t.Fatal(err) }
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.handleOrdersCommand(context.Background(), -100123456789, 0)
	if len(sent) < 2 || !strings.Contains(sent[0].Text, "INVENTORY BELUM TERVERIFIKASI") {
		t.Fatalf("expected explicit incomplete-recovery warning, got: %+v", sent)
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
