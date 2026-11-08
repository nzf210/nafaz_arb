package telegram

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

const liveBalanceSnapshotMaxAge = 90 * time.Second

func (b *Bot) handleBalanceCommand(ctx context.Context, chatID, replyToID, userID int64) {
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"⛔ <b>Akses ditolak.</b> Perintah saldo hanya dapat digunakan oleh user yang diizinkan.", nil)
		return
	}

	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"⚠️ <b>Saldo LIVE tidak tersedia.</b> Koneksi Redis Executor belum aktif.", nil)
		return
	}

	snapshot, err := rc.GetExchangeBalanceSnapshot(ctx)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			fmt.Sprintf("❌ <b>Gagal membaca saldo LIVE:</b> %s", html.EscapeString(err.Error())), nil)
		return
	}
	if snapshot == nil || snapshot.Mode != "LIVE" || snapshot.TimestampMS <= 0 {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"⚠️ <b>Saldo LIVE belum tersedia.</b> Pastikan Executor LIVE aktif dan sudah melakukan refresh saldo.", nil)
		return
	}

	age := time.Since(time.UnixMilli(snapshot.TimestampMS))
	if age < 0 || age > liveBalanceSnapshotMaxAge {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			fmt.Sprintf("⚠️ <b>Data saldo LIVE kedaluwarsa.</b> Snapshot terakhir %s lalu. Tidak menampilkan data lama sebagai saldo terkini.", age.Round(time.Second)), nil)
		return
	}

	var sb strings.Builder
	sb.WriteString("💰 <b>LIVE Exchange Balances</b>\n")
	sb.WriteString(fmt.Sprintf("🕒 Update: <code>%s UTC</code> (umur %s)\n\n",
		time.UnixMilli(snapshot.TimestampMS).UTC().Format("2006-01-02 15:04:05"), age.Round(time.Second)))
	for _, venue := range snapshot.Exchanges {
		sb.WriteString(fmt.Sprintf("🏦 <b>%s</b> <i>(%s)</i>\n",
			html.EscapeString(strings.ToUpper(venue.Name)), html.EscapeString(strings.ToUpper(venue.Role))))
		if venue.Status != "OK" {
			sb.WriteString(fmt.Sprintf("❌ <b>Gagal mengambil saldo:</b> %s\n\n", html.EscapeString(venue.Error)))
			continue
		}
		if len(venue.Assets) == 0 {
			sb.WriteString("• Tidak ada aset dengan saldo\n\n")
			continue
		}
		for _, asset := range venue.Assets {
			sb.WriteString(fmt.Sprintf("• <b>%s</b> Total: <code>%.8f</code> | Free: <code>%.8f</code> | Used: <code>%.8f</code>\n",
				html.EscapeString(asset.Asset), asset.Total, asset.Free, asset.Used))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("<i>Data dibaca dari API private exchange melalui Executor; tidak menggunakan saldo simulasi.</i>")
	b.sendReplyWithMarkup(ctx, chatID, replyToID, sb.String(), &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{{Text: "🔄 Refresh Balance", CallbackData: "balance"}},
			{{Text: MenuUtamaText, CallbackData: "menu"}},
		},
	})
}
