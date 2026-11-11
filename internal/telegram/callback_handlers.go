package telegram

import (
	"context"
	"fmt"
	"html"
	"strings"
)

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
		b.sendLatestOpportunityScans(ctx, chatID, 0)
	case "status":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatStatus(), b.defaultKeyboard())
	case "metrics":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatMetrics(), b.defaultKeyboard())
	case "active":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatActiveOpportunities(), b.defaultKeyboard())
	case "positions":
		b.handlePositionsCommand(ctx, chatID, 0)
	case "pending_orders":
		b.handlePendingOrdersCommand(ctx, chatID, 0)
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


const latestOpportunityScanLimit = 10

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

func (b *Bot) handleForceCloseCallback(ctx context.Context, chatID int64, userID int64, pairKey string) {
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, chatID, 0, "⛔ <b>Akses ditolak:</b> User tidak diizinkan mengeksekusi force close.", b.defaultKeyboard())
		return
	}

	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, 0,
			"❌ <b>Gagal Tutup Paksa:</b> <i>Redis tidak terhubung.</i>", b.defaultKeyboard())
		return
	}

	previewMsg := fmt.Sprintf("⚠️ <b>Konfirmasi Tutup Paksa (Preview)</b>\n\n"+
		"• Pasangan: <code>%s</code>\n"+
		"• Tindakan yang akan diambil:\n"+
		"  1. Batalkan semua order lokal (entry/exit) yang terbuka\n"+
		"  2. Market Buy (reduceOnly) untuk menutup short futures\n"+
		"  3. Market Sell spot untuk melikuidasi koin spot\n\n"+
		"Apakah Anda yakin ingin mengeksekusi force close sekarang?", html.EscapeString(pairKey))

	confirmKb := &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "✅ Ya, Eksekusi Sekarang", CallbackData: "confirm_fc:" + pairKey},
				{Text: "❌ Batalkan", CallbackData: "cancel_fc"},
			},
		},
	}
	b.sendReplyWithMarkup(ctx, chatID, 0, previewMsg, confirmKb)
}

func (b *Bot) handleConfirmForceCloseCallback(ctx context.Context, chatID int64, userID int64, pairKey string) {
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, chatID, 0, "⛔ <b>Akses ditolak:</b> User tidak diizinkan mengeksekusi force close.", b.defaultKeyboard())
		return
	}

	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, 0,
			"❌ <b>Gagal Tutup Paksa:</b> <i>Redis tidak terhubung.</i>", b.defaultKeyboard())
		return
	}

	err := rc.PublishForceClose(ctx, "", "", pairKey, fmt.Sprintf("user_%d", userID))
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, 0,
			fmt.Sprintf("❌ <b>Gagal mengirim perintah tutup paksa:</b> %s", html.EscapeString(err.Error())),
			b.defaultKeyboard())
		return
	}

	msg := fmt.Sprintf("🚨 <b>Perintah Tutup Paksa Dikonfirmasi & Terkirim!</b>\n\n"+
		"• Pasangan: <code>%s</code>\n"+
		"• Operator: <code>user_%d</code>\n"+
		"• Aksi Executor:\n"+
		"  1. Membatalkan antrean Limit Sell/Buy di bursa lokal\n"+
		"  2. Mengeksekusi Market Buy (<code>reduceOnly=True</code>) untuk menutup Short di Futures Global\n"+
		"  3. Likuidasi leg spot dan verifikasi exposure bersih\n\n"+
		"<i>Periksa /positions dalam beberapa detik untuk melihat status terbaru.</i>",
		html.EscapeString(pairKey), userID)

	b.sendReplyWithMarkup(ctx, chatID, 0, msg, b.defaultKeyboard())
}

