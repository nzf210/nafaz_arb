package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
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

func (b *Bot) handleKillSwitchCommand(ctx context.Context, chatID int64, userID int64, replyToID int64, parts []string) {
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⛔ <b>Akses ditolak:</b> User tidak diizinkan mengubah status kill switch.", b.defaultKeyboard())
		return
	}

	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ <b>Kill switch tidak tersedia:</b> Redis belum terhubung.", b.defaultKeyboard())
		return
	}

	action := "on"
	if len(parts) > 1 {
		switch strings.ToLower(parts[1]) {
		case "off", "resume", "clear":
			action = "off"
		case "on":
			action = "on"
		default:
			b.sendReplyWithMarkup(ctx, chatID, replyToID,
				"ℹ️ <i>Gunakan:</i> <code>/killswitch on</code> atau <code>/killswitch off</code>", b.defaultKeyboard())
			return
		}
	}

	if err := rc.PublishKillSwitch(ctx, action, "TELEGRAM"); err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			fmt.Sprintf("❌ <b>Kill switch gagal:</b> %s", html.EscapeString(err.Error())), b.defaultKeyboard())
		return
	}
	if action == "on" {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"🛑 <b>KILL SWITCH AKTIF</b>\n\nSemua entry baru di Executor dihentikan. Posisi terbuka tetap dikelola sampai dipulihkan.\n<i>Gunakan /killswitch off untuk melanjutkan trading.</i>", b.defaultKeyboard())
		return
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID,
		"🟢 <b>Kill Switch Dimatikan</b>\n\nEntry baru di Executor dilanjutkan.", b.defaultKeyboard())
}

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

func (b *Bot) listenTradeEvents(ctx context.Context) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		return
	}

	pubsub := rc.SubscribeTradeEvents(ctx)
	if pubsub == nil {
		return
	}
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if msg != nil && msg.Payload != "" {
				var ev redishandler.TradeEvent
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err == nil {
					b.broadcastTradeEvent(ctx, ev)
				}
			}
		}
	}
}

func (b *Bot) broadcastTradeEvent(ctx context.Context, ev redishandler.TradeEvent) {
	modeTag := "REAL"
	if ev.IsMock {
		modeTag = "SIMULASI"
	}

	var text string
	switch ev.Event {
	case "POSITION_OPENED":
		text = fmt.Sprintf(
			"🟢 <b>[%s] POSISI ARBITRASE TERBUKA!</b>\n\n"+
				"• Pasangan: <b>%s</b>\n"+
				"• Bursa: <code>%s (Spot)</code> vs <code>%s (Futures 1x)</code>\n"+
				"• Kuantitas: <code>%.6f koin</code>\n"+
				"• Beli Spot (Maker): <code>Rp %.2f</code>\n"+
				"• Short Futures (1x): <code>$%.4f USDT</code>\n"+
				"• Hedge: <code>Target delta-neutral; risiko eksekusi tetap ada</code>\n\n"+
				"<i>Gunakan /positions untuk memantau posisi dan opsi tutup paksa.</i>",
			modeTag,
			html.EscapeString(strings.ToUpper(ev.Symbol)),
			html.EscapeString(strings.ToUpper(ev.LocalExchange)),
			html.EscapeString(strings.ToUpper(ev.GlobalExchange)),
			ev.Qty,
			ev.EntryPriceIDR,
			ev.ShortEntryUSDT,
		)
	case "PARTIAL_FILL":
		pairLabel := ev.PairTag
		if pairLabel == "" {
			pairLabel = ev.PairKey
		}
		if pairLabel == "" {
			pairLabel = ev.Symbol
		}
		alertKey := "partial:" + pairLabel
		b.mu.Lock()
		lastAlert, alreadyAlerted := b.alertedKeys[alertKey]
		if alreadyAlerted && time.Since(lastAlert) < 2*time.Minute {
			b.mu.Unlock()
			return
		}
		b.alertedKeys[alertKey] = time.Now()
		b.mu.Unlock()

		filledQty := ev.FilledQty
		if filledQty <= 0 {
			filledQty = ev.Qty
		}
		text = fmt.Sprintf(
			"⚠️ <b>[%s] SPOT PARTIAL FILL</b>\n\n"+
				"• Pair: <code>%s</code>\n"+
				"• Exchange: <code>%s (Spot)</code> → <code>%s (Perpetual)</code>\n"+
				"• Terisi: <code>%.8f / %.8f</code>\n"+
				"• State: <code>%s</code>\n"+
				"• Detail: %s\n\n"+
				"<i>Jangan anggap posisi sudah delta-neutral sampai hedge Futures terkonfirmasi. Pantau /positions dan /status.</i>",
			modeTag,
			html.EscapeString(pairLabel),
			html.EscapeString(ev.LocalExchange),
			html.EscapeString(ev.GlobalExchange),
			filledQty,
			ev.TotalQty,
			html.EscapeString(ev.State),
			html.EscapeString(ev.Message),
		)
	case "EXECUTION_FAILURE", "HEDGE_FAILED":
		pairLabel := ev.PairTag
		if pairLabel == "" {
			pairLabel = ev.PairKey
		}
		if pairLabel == "" {
			pairLabel = ev.Symbol
		}
		reason := ev.Reason
		if reason == "" {
			reason = "EXECUTION_FAILURE"
		}
		message := ev.Message
		if message == "" {
			message = "Proses entry atau hedge tidak berhasil."
		}
		// Avoid flooding Telegram when the same pair repeatedly rejects the
		// same signal or balance snapshot.
		alertKey := "failure:" + pairLabel + ":" + reason
		b.mu.Lock()
		lastAlert, alreadyAlerted := b.alertedKeys[alertKey]
		if alreadyAlerted && time.Since(lastAlert) < 2*time.Minute {
			b.mu.Unlock()
			return
		}
		b.alertedKeys[alertKey] = time.Now()
		b.mu.Unlock()

		text = fmt.Sprintf(
			"🚨 <b>[%s] ENTRY / EXECUTION GAGAL</b>\n\n"+
				"• Pair: <code>%s</code>\n"+
				"• Exchange: <code>%s</code> → <code>%s</code>\n"+
				"• Alasan: <b>%s</b>\n"+
				"• Detail: %s\n"+
				"• State: <code>%s</code>\n\n"+
				"<i>Entry baru untuk kondisi ini ditolak. Periksa saldo, order, dan posisi di kedua exchange sebelum mencoba lagi.</i>",
			modeTag,
			html.EscapeString(pairLabel),
			html.EscapeString(ev.LocalExchange),
			html.EscapeString(ev.GlobalExchange),
			html.EscapeString(reason),
			html.EscapeString(message),
			html.EscapeString(ev.State),
		)
	case "POSITION_CLOSED":
		if strings.EqualFold(ev.PnlStatus, "unverified") {
			text = fmt.Sprintf(
				"⚠️ <b>[%s] POSISI DITUTUP — PNL BELUM TERVERIFIKASI</b>\n\n"+
					"• Pasangan: <b>%s</b>\n"+
					"• Kuantitas: <code>%.6f koin</code>\n"+
					"• Jual Spot: <code>Rp %.2f</code>\n"+
					"• Tutup Short: <code>$%.4f USDT</code>\n"+
					"• Alasan: FX USDT/IDR live tidak tersedia saat rekonsiliasi.\n"+
					"• PnL tidak dimasukkan ke statistik sampai diverifikasi manual.\n\n"+
					"<i>Periksa riwayat order dan fee di kedua exchange.</i>",
				modeTag,
				html.EscapeString(strings.ToUpper(ev.Symbol)),
				ev.Qty,
				ev.ExitPriceIDR,
				ev.ShortExitUSDT,
			)
			break
		}
		emoji := "🎉"
		statusWord := "ESTIMATED NET PROFIT"
		estimatedNet := ev.EstimatedNetProfitUSDT
		if estimatedNet == 0 {
			estimatedNet = ev.NetProfitUSDT
		}
		if estimatedNet < 0 {
			emoji = "⚠️"
			statusWord = "ESTIMATED NET LOSS"
		}
		text = fmt.Sprintf(
			"%s <b>[%s] POSISI BERHASIL DITUTUP (%s)!</b>\n\n"+
				"• Pasangan: <b>%s</b>\n"+
				"• Kuantitas: <code>%.6f koin</code>\n"+
				"• Jual Spot: <code>Rp %.2f</code>\n"+
				"• Tutup Short: <code>$%.4f USDT</code>\n"+
				"• Akumulasi Funding: <code>+$%.4f USDT</code>\n"+
				separatorLine+
				"• Gross PnL: <code>%+.4f USDT</code>\n"+
				"• Estimasi Fee: <code>%.4f USDT</code>\n"+
				"💰 <b>Net PnL (estimasi):</b> <b>%+.4f USDT (Rp %+.2f)</b>\n"+
				"📈 <b>Net ROI (estimasi):</b> <b>%+.3f%%</b>\n\n"+
				"<i>Belum merupakan PnL terealisasi yang dikonfirmasi oleh fee fill exchange.</i>",
			emoji, modeTag, statusWord,
			html.EscapeString(strings.ToUpper(ev.Symbol)),
			ev.Qty,
			ev.ExitPriceIDR,
			ev.ShortExitUSDT,
			ev.AccumulatedFundingUSDT,
			ev.GrossProfitUSDT,
			ev.EstimatedFeesUSDT,
			estimatedNet,
			ev.NetProfitIDR,
			ev.NetROIPct,
		)
	default:
		return
	}

	chatIDs := make([]int64, 0, len(b.cfg.Telegram.AllowedChatIDs)+1)
	if b.cfg.Telegram.ChatID != 0 {
		chatIDs = append(chatIDs, b.cfg.Telegram.ChatID)
	} else {
		chatIDs = append(chatIDs, b.cfg.Telegram.AllowedChatIDs...)
	}
	for _, chatID := range chatIDs {
		b.sendReplyWithMarkup(ctx, chatID, 0, text, b.defaultKeyboard())
	}
}
