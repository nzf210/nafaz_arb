package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"
)

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
