package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"
	"log"
)

func (b *Bot) handleUpdate(ctx context.Context, u Update) {
	// 1. Handle interactive inline button clicks (Callback Queries)
	if u.CallbackQuery != nil {
		b.handleCallbackQuery(ctx, u.CallbackQuery)
		return
	}

	// 2. Handle text messages
	if u.Message == nil || u.Message.Text == "" {
		return
	}

	b.handleTextMessage(ctx, u.Message)
}

func (b *Bot) handleTextMessage(ctx context.Context, msg *Message) {
	if !b.isChatAllowed(msg.Chat.ID) {
		log.Printf("telegram update ignored: chat_id %d not authorized", msg.Chat.ID)
		return
	}

	text := strings.TrimSpace(msg.Text)
	if !strings.HasPrefix(text, "/") {
		return
	}

	parts := strings.Fields(text)
	cmd := strings.ToLower(parts[0])

	if atIdx := strings.Index(cmd, "@"); atIdx != -1 {
		username := cmd[atIdx+1:]
		if b.botUsername != "" && !strings.EqualFold(username, b.botUsername) {
			return
		}
		cmd = cmd[:atIdx]
	}

	b.handleCommand(ctx, msg, cmd, parts)
}

func (b *Bot) handleCommand(ctx context.Context, msg *Message, cmd string, parts []string) {
	switch cmd {
	case "/start", "/menu":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatMenu(), b.defaultKeyboard())
	case "/help":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatHelp(), b.defaultKeyboard())
	case "/status":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatStatus(), b.defaultKeyboard())
	case "/scan":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "🔍 <i>Sedang menjalankan pemindaian pasar...</i>", nil)
		r, err := b.server.TriggerScan(ctx)
		if err != nil {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, fmt.Sprintf("❌ <b>Scan gagal:</b> %s", html.EscapeString(err.Error())), b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, 0, b.formatScanResult(r, "Hasil Pemindaian On-Demand"), b.defaultKeyboard())
	case "/latest":
		latest := b.server.LatestScan()
		if latest.ScanID == "" {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "ℹ️ <i>Belum ada hasil scan. Tekan tombol Scan untuk mulai.</i>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatScanResult(latest, "Hasil Pemindaian Terakhir"), b.defaultKeyboard())
	case "/pair":
		if len(parts) < 2 {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
				"ℹ️ <i>Gunakan:</i> <code>/pair &lt;simbol&gt;</code>\n<i>Contoh:</i> <code>/pair BTC</code> atau <code>/pair ETH</code>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatPairDetail(parts[1]), b.defaultKeyboard())
	case "/active":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatActiveOpportunities(), b.defaultKeyboard())
	case "/positions":
		b.handlePositionsCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/balance":
		userID := int64(0)
		if msg.From != nil {
			userID = msg.From.ID
		}
		b.handleBalanceCommand(ctx, msg.Chat.ID, msg.MessageID, userID)
	case "/history":
		b.handleHistoryCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/frozen":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatFrozenPairs(), b.defaultKeyboard())
	case "/metrics":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatMetrics(), b.defaultKeyboard())
	case "/mute":
		dur := 1 * time.Hour
		if len(parts) > 1 {
			if d, err := time.ParseDuration(parts[1]); err == nil && d > 0 {
				dur = d
			}
		}
		b.mu.Lock()
		b.mutedUntil = time.Now().UTC().Add(dur)
		mutedUntil := b.mutedUntil
		b.mu.Unlock()
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			fmt.Sprintf("🔕 <b>Alert Dibisukan</b>\n\nNotifikasi peluang tidak akan dikirim hingga <b>%s</b> (~%s ke depan).\nGunakan /unmute untuk mengaktifkan kembali.",
				mutedUntil.Format(TIME_FORMAT), dur.String()), b.defaultKeyboard())
	case "/unmute":
		b.mu.Lock()
		b.mutedUntil = time.Time{}
		b.mu.Unlock()
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			"🔔 <b>Alert Diaktifkan Kembali</b>\n\nNotifikasi peluang baru akan dikirimkan normal sesuai konfigurasi.", b.defaultKeyboard())
	case "/killswitch":
		userID := int64(0)
		if msg.From != nil {
			userID = msg.From.ID
		}
		b.handleKillSwitchCommand(ctx, msg.Chat.ID, userID, msg.MessageID, parts)
	default:
	}
}

func (b *Bot) handleCallbackQuery(ctx context.Context, cb *CallbackQuery) {
	if cb == nil || cb.Message == nil {
		return
	}
	chatID := cb.Message.Chat.ID
	if !b.isChatAllowed(chatID) {
		return
	}

	b.answerCallbackQuery(ctx, cb.ID, "Memproses...")

	switch cb.Data {
	case "scan":
		b.sendReplyWithMarkup(ctx, chatID, 0, "🔍 <i>Sedang menjalankan pemindaian pasar...</i>", nil)
		r, err := b.server.TriggerScan(ctx)
		if err != nil {
			b.sendReplyWithMarkup(ctx, chatID, 0, fmt.Sprintf("❌ <b>Scan gagal:</b> %s", html.EscapeString(err.Error())), b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatScanResult(r, "Hasil Pemindaian On-Demand"), b.defaultKeyboard())
	case "latest":
		latest := b.server.LatestScan()
		if latest.ScanID == "" {
			b.sendReplyWithMarkup(ctx, chatID, 0, "ℹ️ <i>Belum ada hasil scan. Tekan tombol Scan untuk mulai.</i>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatScanResult(latest, "Hasil Pemindaian Terakhir"), b.defaultKeyboard())
	case "status":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatStatus(), b.defaultKeyboard())
	case "metrics":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatMetrics(), b.defaultKeyboard())
	case "active":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatActiveOpportunities(), b.defaultKeyboard())
	case "positions":
		b.handlePositionsCommand(ctx, chatID, 0)
	case "balance":
		b.handleBalanceCommand(ctx, chatID, 0, cb.From.ID)
	case "history":
		b.handleHistoryCommand(ctx, chatID, 0)
	case "frozen":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatFrozenPairs(), b.defaultKeyboard())
	case "menu":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatMenu(), b.defaultKeyboard())
	case "help":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatHelp(), b.defaultKeyboard())
	default:
		b.handleExtendedCallback(ctx, chatID, cb)
	}
}

func (b *Bot) handleExtendedCallback(ctx context.Context, chatID int64, cb *CallbackQuery) {
	if sym, ok := strings.CutPrefix(cb.Data, "pair:"); ok {
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatPairDetail(sym), b.defaultKeyboard())
		return
	}
	if pairKey, ok := strings.CutPrefix(cb.Data, "force_close:"); ok {
		b.handleForceCloseCallback(ctx, chatID, cb.From.ID, pairKey)
		return
	}
	if pairKey, ok := strings.CutPrefix(cb.Data, "confirm_fc:"); ok {
		b.handleConfirmForceCloseCallback(ctx, chatID, cb.From.ID, pairKey)
		return
	}
	if cb.Data == "cancel_fc" {
		b.sendReplyWithMarkup(ctx, chatID, 0, "❌ Tutup paksa dibatalkan.", b.defaultKeyboard())
		return
	}
}


type balanceAsset struct { Asset string `json:"asset"`; Free float64 `json:"free"`; Used float64 `json:"used"`; Total float64 `json:"total"` }
type balanceExchange struct { Name string `json:"name"`; Market string `json:"market"`; Status string `json:"status"`; Error string `json:"error,omitempty"`; Assets []balanceAsset `json:"assets"` }
type balanceReport struct { TimestampMS int64 `json:"timestamp_ms"`; MockMode bool `json:"mock_mode"`; Exchanges []balanceExchange `json:"exchanges"` }
