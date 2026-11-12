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
