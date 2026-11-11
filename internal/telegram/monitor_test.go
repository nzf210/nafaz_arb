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
		"recovery": map[string]interface{}{"complete": true, "tracked_orders": 2},
	}
	raw, _ := json.Marshal(payload)
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, raw, 0).Err(); err != nil { t.Fatal(err) }
	if err := rdb.HSet(context.Background(), redishandler.KeyOrderRecoveryIndex, "internal-42", "1", "internal-43", "1").Err(); err != nil { t.Fatal(err) }
	if err := rdb.Set(context.Background(), redishandler.KeyPrefixOrderRecord+"internal-42", `{"local_order_id":"internal-42","exchange_order_id":"exchange-42","pair_key":"BTC/USDT","exchange":"okx","status":"PARTIALLY_FILLED","filled_quantity":1}`, 0).Err(); err != nil { t.Fatal(err) }
	if err := rdb.Set(context.Background(), redishandler.KeyPrefixOrderRecord+"internal-43", `{"local_order_id":"internal-43","exchange_order_id":"exchange-43","pair_key":"ETH/USDT","exchange":"mexc","status":"OPEN","filled_quantity":0}`, 0).Err(); err != nil { t.Fatal(err) }

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
	for _, want := range []string{"internal-42", "BTC/USDT", "okx", "PARTIALLY_FILLED", "internal-43", "ETH/USDT", "mexc"} {
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

func TestOrdersCommandReportsMissingRedis(t *testing.T) {
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sendMessage" { _ = json.NewDecoder(r.Body).Decode(&sent); _ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true}) }
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.handleOrdersCommand(context.Background(), -100123456789, 0)
	if !strings.Contains(sent.Text, "Redis tidak terhubung") { t.Fatalf("expected missing Redis warning, got %s", sent.Text) }
}

func TestOrphansCommandReadsOrphanRegistry(t *testing.T) {
	mr, err := miniredis.Run(); if err != nil { t.Fatal(err) }; defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()}); defer rdb.Close()
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, `{"status":"RUNNING","recovery":{"complete":true,"orphan_orders":[{"exchange_order_id":"orphan-9"}]}}`, 0).Err(); err != nil { t.Fatal(err) }
	if err := rdb.HSet(context.Background(), redishandler.KeyOrphanOrderIndex, "orphan-9", "1").Err(); err != nil { t.Fatal(err) }
	if err := rdb.Set(context.Background(), redishandler.KeyPrefixOrphanOrder+"orphan-9", `{"exchange_order_id":"orphan-9","exchange":"okx","symbol":"BTC/USDT","status":"ORPHAN_ORDER"}`, 0).Err(); err != nil { t.Fatal(err) }
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) { if r.URL.Path == "/sendMessage" { _ = json.NewDecoder(r.Body).Decode(&sent); _ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true}) } }
	bot, _, ts := setupTestBot(t, tsHandler); defer ts.Close()
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.sendMonitorSection(context.Background(), -100123456789, 0, "ORPHAN ORDERS", "orphans")
	if !strings.Contains(sent.Text, "orphan-9") { t.Fatalf("orphan detail missing: %s", sent.Text) }
}

func TestStartupRecoveryFailureNeverReportsReady(t *testing.T) {
	mr, err := miniredis.Run(); if err != nil { t.Fatal(err) }; defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()}); defer rdb.Close()
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, `{"status":"RUNNING","open_positions":1,"recovery":{"complete":true,"ready":false,"tracked_orders":1,"pending_orders":{"o1":"venue unavailable"},"orphan_orders":[]},"trading_halted":true}`, 0).Err(); err != nil { t.Fatal(err) }
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) { if r.URL.Path == "/sendMessage" { _ = json.NewDecoder(r.Body).Decode(&sent); _ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true}) } }
	bot, _, ts := setupTestBot(t, tsHandler); defer ts.Close()
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.sendStartupSummary(context.Background())
	if !strings.Contains(sent.Text, "NOT READY") || strings.Contains(sent.Text, "Global trading: <b>READY</b>") { t.Fatalf("unsafe startup status: %s", sent.Text) }
}

func TestMonitorSectionTruncatesLongTelegramMessages(t *testing.T) {
	mr, err := miniredis.Run(); if err != nil { t.Fatal(err) }; defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()}); defer rdb.Close()
	large := strings.Repeat("mismatch-detail-", 1000)
	payload, _ := json.Marshal(map[string]interface{}{"reconciliation":map[string]interface{}{"latest":large}})
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, payload, 0).Err(); err != nil { t.Fatal(err) }
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) { if r.URL.Path == "/sendMessage" { _ = json.NewDecoder(r.Body).Decode(&sent); _ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true}) } }
	bot, _, ts := setupTestBot(t, tsHandler); defer ts.Close()
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.sendMonitorSection(context.Background(), -100123456789, 0, "RECONCILIATION MISMATCHES", "reconciliation")
	if len([]rune(sent.Text)) > 4096 { t.Fatalf("Telegram message exceeds 4096 characters: %d", len([]rune(sent.Text))) }
}

func TestPositionDetailExplainsMissingSupabaseFillLedger(t *testing.T) {
	mr, err := miniredis.Run(); if err != nil { t.Fatal(err) }; defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()}); defer rdb.Close()
	if err := rdb.Set(context.Background(), redishandler.KeyPrefixActivePos+"okx:btc_usdt", `{"local_exchange":"okx","global_exchange":"okx","symbol":"BTC/USDT","qty":0.25,"state":"HEDGED_ACTIVE"}`, 0).Err(); err != nil { t.Fatal(err) }
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) { if r.URL.Path == "/sendMessage" { _ = json.NewDecoder(r.Body).Decode(&sent); _ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true}) } }
	bot, _, ts := setupTestBot(t, tsHandler); defer ts.Close()
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.handlePositionDetail(context.Background(), -100123456789, 0, "BTC/USDT")
	if !strings.Contains(sent.Text, "Supabase belum terhubung") { t.Fatalf("expected explicit fill-ledger limitation, got: %s", sent.Text) }
}

func TestOrdersCommandEmptyRegistryAfterCompletedRecovery(t *testing.T) {
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) { if r.URL.Path == "/sendMessage" { _ = json.NewDecoder(r.Body).Decode(&sent); _ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok:true}) } }
	bot, _, ts := setupTestBot(t, tsHandler); defer ts.Close()
	mr, err := miniredis.Run(); if err != nil { t.Fatal(err) }; defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()}); defer rdb.Close()
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, `{"status":"RUNNING","recovery":{"complete":true,"tracked_orders":0}}`, 0).Err(); err != nil { t.Fatal(err) }
	bot.SetRedisClient(redishandler.NewWithClient(rdb))
	bot.handleOrdersCommand(context.Background(), -100123456789, 0)
	if !strings.Contains(sent.Text, "ACTIVE BOT ORDERS (0)") { t.Fatalf("expected verified empty registry, got: %s", sent.Text) }
}
