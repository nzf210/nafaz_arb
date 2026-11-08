package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestBalanceCommandRejectsUnauthorizedUser(t *testing.T) {
	var sent SendMessageReq
	tsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sendMessage" {
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
			t.Errorf("decode Telegram request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(APIResponse[Message]{Ok: true})
	}
	bot, _, ts := setupTestBot(t, tsHandler)
	defer ts.Close()
	bot.cfg.Telegram.AllowedUserIDs = []int64{77}

	bot.handleBalanceCommand(context.Background(), -100123456789, 10, 42)

	if !strings.Contains(sent.Text, "Akses ditolak") {
		t.Fatalf("expected balance access denial, got %q", sent.Text)
	}
}
