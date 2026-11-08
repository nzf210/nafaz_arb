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

