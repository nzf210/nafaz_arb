package telegram

import (
"context"
"encoding/json"
"fmt"
"html"
"log"
"strconv"
"strings"
"time"
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

func (b *Bot) handleScanCommand(ctx context.Context, msg *Message) {
	b.sendReplyWithMarkup(
		ctx, msg.Chat.ID, msg.MessageID,
		"🔍 <i>Sedang menjalankan pemindaian pasar...</i>", nil,
	)
	result, err := b.server.TriggerScan(ctx)
	if err != nil {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			fmt.Sprintf("❌ <b>Scan gagal:</b> %s", html.EscapeString(err.Error())),
			b.defaultKeyboard(),
		)
		return
	}
	b.sendReplyWithMarkup(
		ctx, msg.Chat.ID, 0,
		b.formatScanResult(result, "Hasil Pemindaian On-Demand"),
		b.defaultKeyboard(),
	)
}

func (b *Bot) handlePairCommand(
	ctx context.Context, msg *Message, parts []string,
) {
	if len(parts) < 2 {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"ℹ️ <i>Gunakan:</i> <code>/pair &lt;simbol&gt;</code>\n<i>Contoh:</i> <code>/pair BTC</code> atau <code>/pair ETH</code>",
			b.defaultKeyboard(),
		)
		return
	}
	b.sendReplyWithMarkup(
		ctx, msg.Chat.ID, msg.MessageID,
		b.formatPairDetail(parts[1]), b.defaultKeyboard(),
	)
}

func (b *Bot) handleOrderDetailCommand(
	ctx context.Context, msg *Message, parts []string,
) {
	if len(parts) < 2 {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"Gunakan: <code>/order &lt;internal_order_id&gt;</code>",
			b.defaultKeyboard(),
		)
		return
	}
	b.handleOrderDetail(ctx, msg.Chat.ID, msg.MessageID, parts[1])
}

func (b *Bot) handlePositionDetailCommand(
	ctx context.Context, msg *Message, parts []string,
) {
	if len(parts) < 2 {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"Gunakan: <code>/position &lt;position_id&gt;</code>",
			b.defaultKeyboard(),
		)
		return
	}
	b.handlePositionDetail(ctx, msg.Chat.ID, msg.MessageID, parts[1])
}

func (b *Bot) handleMuteCommand(ctx context.Context, msg *Message, parts []string) {
	duration := time.Hour
	if len(parts) > 1 {
		if parsed, err := time.ParseDuration(parts[1]); err == nil && parsed > 0 {
			duration = parsed
		}
	}
	b.mu.Lock()
	b.mutedUntil = time.Now().UTC().Add(duration)
	mutedUntil := b.mutedUntil
	b.mu.Unlock()
	b.sendReplyWithMarkup(
		ctx, msg.Chat.ID, msg.MessageID,
		fmt.Sprintf(
			"🔕 <b>Alert Dibisukan</b>\n\nNotifikasi peluang tidak akan dikirim hingga <b>%s</b> (~%s ke depan).\nGunakan /unmute untuk mengaktifkan kembali.",
			mutedUntil.Format(TIME_FORMAT), duration.String(),
		),
		b.defaultKeyboard(),
	)
}

func (b *Bot) handleUnmuteCommand(ctx context.Context, msg *Message) {
	b.mu.Lock()
	b.mutedUntil = time.Time{}
	b.mu.Unlock()
	b.sendReplyWithMarkup(
		ctx, msg.Chat.ID, msg.MessageID,
		"🔔 <b>Alert Diaktifkan Kembali</b>\n\nNotifikasi peluang baru akan dikirimkan normal sesuai konfigurasi.",
		b.defaultKeyboard(),
	)
}

func (b *Bot) handleCommand(
	ctx context.Context, msg *Message, cmd string, parts []string,
) {
	switch cmd {
	case "/start", "/menu":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatMenu(), b.defaultKeyboard())
	case "/help":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatHelp(), b.defaultKeyboard())
	case "/status":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatStatus(), b.defaultKeyboard())
	case "/scan":
		b.handleScanCommand(ctx, msg)
	case "/latest":
		b.sendLatestOpportunityScans(ctx, msg.Chat.ID, msg.MessageID)
	case "/pair":
		b.handlePairCommand(ctx, msg, parts)
	case "/active":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatActiveOpportunities(), b.defaultKeyboard())
	case "/orders":
		b.handleOrdersCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/recovery":
		b.sendMonitorSection(ctx, msg.Chat.ID, msg.MessageID, "RECOVERY STATUS", "recovery", "recovery_status", "startup_recovery")
	case "/orphans":
		b.sendMonitorSection(ctx, msg.Chat.ID, msg.MessageID, "ORPHAN ORDERS", "orphans", "orphan_orders", "orphan_order_details")
	case "/reconcile":
		b.handleReconcileCommand(ctx, msg, parts)
	case "/risk":
		b.sendMonitorSection(ctx, msg.Chat.ID, msg.MessageID, "RISK HALT", "risk", "risk_status", "risk_halt")
	case "/order":
		b.handleOrderDetailCommand(ctx, msg, parts)
	case "/position":
		b.handlePositionDetailCommand(ctx, msg, parts)
	case "/positions":
		b.handlePositionsCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/pending":
		b.handlePendingOrdersCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/spotmax":
		b.handleSpotMaxCommand(ctx, msg, parts)
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
		b.handleMuteCommand(ctx, msg, parts)
	case "/unmute":
		b.handleUnmuteCommand(ctx, msg)
	case "/killswitch":
		userID := int64(0)
		if msg.From != nil {
			userID = msg.From.ID
		}
		b.handleKillSwitchCommand(ctx, msg.Chat.ID, userID, msg.MessageID, parts)
	default:
	}
}

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

func (b *Bot) handleSpotMaxCommand(ctx context.Context, msg *Message, parts []string) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "❌ Redis tidak terhubung; pengaturan max spot order tidak tersedia.", b.defaultKeyboard())
		return
	}
	if len(parts) == 1 {
		limits, err := rc.GetSpotOrderLimits(ctx)
		if err != nil {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "❌ Gagal membaca spot order limits: "+html.EscapeString(err.Error()), b.defaultKeyboard())
			return
		}
		var out strings.Builder
		out.WriteString("⚙️ <b>MAX OPEN SPOT ORDERS PER CEX</b>\n\nDefault untuk CEX tanpa override: <b>2</b>\n")
		if len(limits) == 0 { out.WriteString("\nBelum ada override; semua CEX memakai default 2.\n") }
		for exchange, limit := range limits { out.WriteString(fmt.Sprintf("\n• <b>%s</b>: <code>%d</code>", html.EscapeString(strings.ToUpper(exchange)), limit)) }
		out.WriteString("\n\nAtur: <code>/spotmax &lt;cex&gt; &lt;jumlah&gt;</code>\nContoh: <code>/spotmax indodax 3</code>")
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, out.String(), b.defaultKeyboard())
		return
	}
	userID := int64(0)
	if msg.From != nil { userID = msg.From.ID }
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "⛔ <b>Akses ditolak:</b> hanya operator terotorisasi yang boleh mengubah batas order Spot.", b.defaultKeyboard())
		return
	}
	if len(parts) != 3 {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "ℹ️ Gunakan <code>/spotmax &lt;cex&gt; &lt;jumlah&gt;</code>. Rentang jumlah 1–100; default 2.", b.defaultKeyboard())
		return
	}
	limit, err := strconv.Atoi(parts[2])
	if err != nil || limit < 1 || limit > 100 {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "❌ Jumlah harus berupa bilangan bulat 1–100.", b.defaultKeyboard())
		return
	}
	if err := rc.SetSpotOrderLimit(ctx, parts[1], limit); err != nil {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "❌ Gagal mengubah batas: "+html.EscapeString(err.Error()), b.defaultKeyboard())
		return
	}
	b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
		fmt.Sprintf("✅ <b>BATAS OPEN SPOT ORDER DIPERBARUI</b>\n\n• CEX: <b>%s</b>\n• Maksimum: <b>%d</b> order/posisi Spot\n\nPerubahan disimpan di Redis dan dikirim ke Executor tanpa restart. Order/posisi yang sudah terbuka tidak dibatalkan; batas baru hanya mencegah entry baru sampai jumlah turun di bawah batas.", html.EscapeString(strings.ToUpper(parts[1])), limit), b.defaultKeyboard())
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
