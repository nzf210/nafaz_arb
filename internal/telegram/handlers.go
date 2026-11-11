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
		b.sendLatestOpportunityScans(ctx, msg.Chat.ID, msg.MessageID)
	case "/pair":
		if len(parts) < 2 {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
				"ℹ️ <i>Gunakan:</i> <code>/pair &lt;simbol&gt;</code>\n<i>Contoh:</i> <code>/pair BTC</code> atau <code>/pair ETH</code>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatPairDetail(parts[1]), b.defaultKeyboard())
	case "/active":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatActiveOpportunities(), b.defaultKeyboard())
	case "/orders":
		b.handleOrdersCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/recovery":
		b.sendMonitorSection(ctx, msg.Chat.ID, msg.MessageID, "RECOVERY STATUS", "recovery", "recovery_status", "startup_recovery")
	case "/orphans":
		b.sendMonitorSection(ctx, msg.Chat.ID, msg.MessageID, "ORPHAN ORDERS", "orphans", "orphan_orders", "orphan_order_details")
	case "/reconcile":
		b.sendMonitorSection(ctx, msg.Chat.ID, msg.MessageID, "RECONCILIATION MISMATCHES", "reconciliation", "reconciliation_mismatches", "mismatches")
	case "/risk":
		b.sendMonitorSection(ctx, msg.Chat.ID, msg.MessageID, "RISK HALT", "risk", "risk_status", "risk_halt")
	case "/order":
		if len(parts) < 2 { b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "Gunakan: <code>/order &lt;internal_order_id&gt;</code>", b.defaultKeyboard()); return }
		b.handleOrderDetail(ctx, msg.Chat.ID, msg.MessageID, parts[1])
	case "/position":
		if len(parts) < 2 { b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "Gunakan: <code>/position &lt;position_id&gt;</code>", b.defaultKeyboard()); return }
		b.handlePositionDetail(ctx, msg.Chat.ID, msg.MessageID, parts[1])
	case "/positions":
		b.handlePositionsCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/pending":
		b.handlePendingOrdersCommand(ctx, msg.Chat.ID, msg.MessageID)
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

func (b *Bot) sendLatestOpportunityScans(ctx context.Context, chatID, replyToID int64) {
	b.mu.Lock()
	reader := b.opportunityReader
	b.mu.Unlock()
	if reader == nil {
		latest := b.server.LatestScan()
		if latest.ScanID == "" {
			b.sendReplyWithMarkup(ctx, chatID, replyToID,
				"ℹ️ <i>Histori peluang Supabase belum terhubung dan belum ada hasil scan di memori.</i>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"⚠️ <i>Histori Supabase belum terhubung. Menampilkan satu scan terakhir dari memori proses.</i>\n\n"+
				b.formatScanResult(latest, "Hasil Pemindaian Terakhir"), b.defaultKeyboard())
		return
	}
	scans, err := reader.LatestOpportunityScans(ctx, latestOpportunityScanLimit)
	if err != nil {
		log.Printf("telegram latest opportunity history failed: %v", err)
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"❌ <b>Gagal mengambil 10 scan peluang terakhir dari Supabase.</b>\n"+
				"<i>Data tidak diganti dengan cache satu scan agar hasil tidak menyesatkan.</i>", b.defaultKeyboard())
		return
	}
	if len(scans) == 0 {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"ℹ️ <i>Belum ada peluang yang tersimpan di histori scan Supabase.</i>", b.defaultKeyboard())
		return
	}
	for i, scan := range scans {
		scanTime := scan.CompletedAt
		if scanTime.IsZero() && len(scan.Opportunities) > 0 {
			scanTime = scan.Opportunities[0].LastScannedAt
		}
		title := fmt.Sprintf("Scan Peluang Terbaru %d/%d", i+1, len(scans))
		if !scanTime.IsZero() {
			title += " • " + scanTime.UTC().Format(TIME_FORMAT)
		}
		b.sendReplyWithMarkup(ctx, chatID, replyToID, b.formatScanResult(scan, title), b.defaultKeyboard())
		replyToID = 0
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

func (b *Bot) handleBalanceCommand(ctx context.Context, chatID, replyToID, userID int64) {
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⛔ <b>Akses ditolak:</b> User tidak diizinkan melihat saldo exchange.", b.defaultKeyboard())
		return
	}
	b.mu.Lock(); rc := b.redisClient; b.mu.Unlock()
	if rc == nil { b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ <b>Balance tidak tersedia:</b> Redis belum terhubung.", b.defaultKeyboard()); return }
	b.sendReplyWithMarkup(ctx, chatID, replyToID, "⏳ <i>Mengambil saldo terbaru langsung dari exchange...</i>", nil)
	raw, err := rc.RequestLiveBalances(ctx)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, 0, "❌ <b>Gagal mengambil balance:</b> "+html.EscapeString(err.Error())+"\n<i>Pastikan Executor aktif dan Redis berjalan.</i>", b.defaultKeyboard()); return
	}
	var report balanceReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		b.sendReplyWithMarkup(ctx, chatID, 0, "❌ <b>Respons balance tidak valid:</b> "+html.EscapeString(err.Error()), b.defaultKeyboard()); return
	}
	var out strings.Builder
	out.WriteString("💰 <b>BALANCE EXCHANGE TERKINI</b>\n")
	if report.MockMode { out.WriteString("⚠️ <b>MODE SIMULASI</b> — bukan saldo akun exchange.\n") } else { out.WriteString("🟢 <b>LIVE API</b> — data dibaca langsung dari exchange.\n") }
	if report.TimestampMS > 0 { out.WriteString("🕒 "+time.UnixMilli(report.TimestampMS).UTC().Format("2006-01-02 15:04:05 UTC")+"\n") }
	out.WriteString("\n")
	for _, ex := range report.Exchanges {
		out.WriteString("🏦 <b>"+html.EscapeString(strings.ToUpper(ex.Name))+" ("+html.EscapeString(strings.ToUpper(ex.Market))+")</b>\n")
		if ex.Status != "ok" { out.WriteString("❌ Gagal: "+html.EscapeString(ex.Error)+"\n\n"); continue }
		if len(ex.Assets) == 0 { out.WriteString("<i>Tidak ada saldo aset.</i>\n\n"); continue }
		for _, asset := range ex.Assets {
			if asset.Total == 0 && asset.Free == 0 && asset.Used == 0 { continue }
			out.WriteString(fmt.Sprintf("• <code>%s</code> | Free: <code>%.8g</code> | Used: <code>%.8g</code> | Total: <b><code>%.8g</code></b>\n", html.EscapeString(asset.Asset), asset.Free, asset.Used, asset.Total))
		}
		out.WriteString("\n")
	}
	if len(report.Exchanges) == 0 { out.WriteString("❌ Executor tidak melaporkan exchange aktif.\n") }
	out.WriteString("<i>Hanya membaca saldo; tidak mengubah order atau posisi.</i>")
	message := out.String()
	if len([]rune(message)) > 3800 {
		runes := []rune(message)
		message = string(runes[:3700]) + "\n<i>Daftar dipotong karena batas panjang pesan Telegram.</i>"
	}
	b.sendReplyWithMarkup(ctx, chatID, 0, message, b.defaultKeyboard())
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
		emoji := "🎉"
		statusWord := "PROFIT"
		if ev.NetProfitUSDT < 0 {
			emoji = "⚠️"
			statusWord = "LOSS"
		}
		text = fmt.Sprintf(
			"%s <b>[%s] POSISI BERHASIL DITUTUP (%s)!</b>\n\n"+
				"• Pasangan: <b>%s</b>\n"+
				"• Kuantitas: <code>%.6f koin</code>\n"+
				"• Jual Spot (Maker): <code>Rp %.2f</code>\n"+
				"• Tutup Short (1x): <code>$%.4f USDT</code>\n"+
				"• Akumulasi Funding: <code>+$%.4f USDT</code>\n"+
				separatorLine+
				"💰 <b>Net Realized Profit:</b> <b>%+.4f USDT (Rp %+.2f)</b>\n"+
				"📈 <b>Net ROI:</b> <b>%+.3f%%</b>\n\n"+
				"<i>Ketik /history untuk melihat rekap riwayat transaksi yang telah selesai.</i>",
			emoji, modeTag, statusWord,
			html.EscapeString(strings.ToUpper(ev.Symbol)),
			ev.Qty,
			ev.ExitPriceIDR,
			ev.ShortExitUSDT,
			ev.AccumulatedFundingUSDT,
			ev.NetProfitUSDT,
			ev.NetProfitIDR,
			ev.NetROIPct,
		)
	case "ORDER_NOT_FOUND_AFTER_RESTART", "UNKNOWN_EXCHANGE_ORDER", "ORPHAN_ORDER", "CANCEL_FAILED", "REPLACEMENT_BLOCKED", "PARTIAL_FILL_IMBALANCE", "EXPOSURE_MISMATCH", "RECOVERY_EXCHANGE_API_FAILED", "DATABASE_PERSISTENCE_FAILED", "POSITION_RECOVERY_FAILED", "RECONCILIATION_MISMATCH", "RISK_HALT", "TRADE_FILL_RECONCILIATION_REQUIRED", "EXPOSURE_EXPECTED_QTY_INVALID", "EXPOSURE_MAPPING_UNAVAILABLE", "EXPOSURE_BALANCE_UNAVAILABLE", "EXPOSURE_BALANCE_INVALID", "EXPOSURE_POSITION_UNAVAILABLE":
		pair := ev.PairTag
		if pair == "" { pair = ev.PairKey }
		if pair == "" { pair = ev.Symbol }
		orderID, positionID, action := "n/a", "n/a", "Periksa exchange dan rekonsiliasi secara manual sebelum mengizinkan entry baru."
		if ev.Detail != nil {
			if v, ok := ev.Detail["order_id"]; ok { orderID = fmt.Sprint(v) }
			if v, ok := ev.Detail["internal_order_id"]; ok && orderID == "n/a" { orderID = fmt.Sprint(v) }
			if v, ok := ev.Detail["position_id"]; ok { positionID = fmt.Sprint(v) }
			if v, ok := ev.Detail["required_action"]; ok { action = fmt.Sprint(v) }
		}
		if ev.Message != "" { action = ev.Message }
		text = fmt.Sprintf("🚨 <b>CRITICAL EXECUTION / RECOVERY ALERT</b>\n\n• Event: <code>%s</code>\n• Pair: <code>%s</code>\n• Exchange: <code>%s</code> / <code>%s</code>\n• Order ID: <code>%s</code>\n• Position ID: <code>%s</code>\n• Status: <code>%s</code>\n• Reason: <code>%s</code>\n\n<b>Required action:</b> %s\n\n<i>Jangan mengasumsikan order/posisi aman sampai diverifikasi pada exchange.</i>",
			html.EscapeString(ev.Event), html.EscapeString(pair), html.EscapeString(ev.LocalExchange), html.EscapeString(ev.GlobalExchange),
			html.EscapeString(orderID), html.EscapeString(positionID), html.EscapeString(ev.State), html.EscapeString(ev.Reason), html.EscapeString(action))
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
