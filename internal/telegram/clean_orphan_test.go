package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	redis "github.com/redis/go-redis/v9"
)

func TestCleanOrphanCommand(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rc := redishandler.NewWithClient(rdb)

	var sentMu sync.Mutex
	var sentBodies []string
	bot, _, ts := setupTestBot(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if txt, ok := body["text"].(string); ok {
			sentMu.Lock()
			sentBodies = append(sentBodies, txt)
			sentMu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"result":{}}`))
	})
	defer ts.Close()
	bot.SetRedisClient(rc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Subscribe to recovery request channel and simulate executor reply
	sub := rdb.Subscribe(ctx, redishandler.ChannelRecoveryRequest)
	defer sub.Close()
	go func() {
		ch := sub.Channel()
		msg := <-ch
		if msg == nil {
			return
		}
		var req map[string]string
		_ = json.Unmarshal([]byte(msg.Payload), &req)
		reply := redishandler.RecoveryRetryReply{
			RequestID:    req["request_id"],
			Action:       req["action"],
			Resolved:     true,
			DeletedRedis: 3,
			DeletedDB:    2,
			ClearedPairs: []string{"tokocrypto:toko:soon_idr__okx:soon_usdt_swap"},
			Complete:     true,
			TimestampMS:  time.Now().UnixMilli(),
		}
		payload, _ := json.Marshal(reply)
		_ = rdb.Set(ctx, redishandler.KeyPrefixRecoveryReply+req["request_id"], payload, 30*time.Second).Err()
	}()

	// Wait a bit for subscription to be active
	time.Sleep(50 * time.Millisecond)

	bot.handleUpdate(ctx, Update{
		Message: &Message{
			MessageID: 1,
			Chat:      Chat{ID: -100123456789},
			From:      &User{ID: 339959699},
			Text:      "/cleanorphan c7c9c437-00b6-5cf2-8a9a-ef6e6e2cf1c8",
		},
	})

	sentMu.Lock()
	defer sentMu.Unlock()
	if len(sentBodies) == 0 {
		t.Fatal("expected telegram reply message")
	}
	last := sentBodies[len(sentBodies)-1]
	if !strings.Contains(last, "ORPHAN FORCE CLEANED") {
		t.Errorf("expected ORPHAN FORCE CLEANED in reply, got: %s", last)
	}
	if !strings.Contains(last, "c7c9c437-00b6-5cf2-8a9a-ef6e6e2cf1c8") {
		t.Errorf("expected ID in reply, got: %s", last)
	}
}

func TestCleanOrphanPickerAndMenuRegistration(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rc := redishandler.NewWithClient(rdb)

	_ = rdb.HSet(context.Background(), redishandler.KeyPendingOrderRecovery,
		"5d7a6ee0-92b0-582a-94e1-33579522f704", "absent from venue open-order list",
	).Err()

	var sentMu sync.Mutex
	var sentReqs []SendMessageReq
	var registeredCmds []BotCommand
	bot, _, ts := setupTestBot(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/getMe":
			_ = json.NewEncoder(w).Encode(APIResponse[User]{
				Ok:     true,
				Result: User{ID: 1, FirstName: "NafazBot", Username: "nafaz_bot"},
			})
		case "/setMyCommands":
			var req SetMyCommandsReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			sentMu.Lock()
			registeredCmds = req.Commands
			sentMu.Unlock()
			_ = json.NewEncoder(w).Encode(APIResponse[bool]{Ok: true, Result: true})
		case "/sendMessage":
			var req SendMessageReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			sentMu.Lock()
			sentReqs = append(sentReqs, req)
			sentMu.Unlock()
			_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
		}
	})
	defer ts.Close()
	bot.SetRedisClient(rc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := bot.Start(ctx); err != nil {
		t.Fatalf("bot start: %v", err)
	}

	sentMu.Lock()
	foundClean, foundHapus := false, false
	for _, c := range registeredCmds {
		if c.Command == "cleanorphan" {
			foundClean = true
		}
		if c.Command == "hapusorphan" {
			foundHapus = true
		}
	}
	sentMu.Unlock()
	if !foundClean || !foundHapus {
		t.Fatalf("expected cleanorphan and hapusorphan in setMyCommands, got clean=%v hapus=%v", foundClean, foundHapus)
	}

	bot.handleUpdate(ctx, Update{
		Message: &Message{
			MessageID: 2,
			Chat:      Chat{ID: -100123456789},
			From:      &User{ID: 339959699},
			Text:      "/hapusorphan",
		},
	})

	sentMu.Lock()
	defer sentMu.Unlock()
	if len(sentReqs) == 0 {
		t.Fatal("expected picker reply")
	}
	last := sentReqs[len(sentReqs)-1]
	if last.ReplyMarkup == nil || len(last.ReplyMarkup.InlineKeyboard) == 0 {
		t.Fatal("expected inline clean buttons on /hapusorphan picker")
	}
	if last.ReplyMarkup.InlineKeyboard[0][0].CallbackData != "clean_orphan:5d7a6ee0-92b0-582a-94e1-33579522f704" {
		t.Errorf("unexpected callback_data: %s", last.ReplyMarkup.InlineKeyboard[0][0].CallbackData)
	}
}
