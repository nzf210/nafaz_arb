package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"
)

func (b *Bot) handleReconcileCommand(ctx context.Context, msg *Message, parts []string) {
	if len(parts) > 1 && strings.EqualFold(parts[1], "retry") {
		b.handleReconcileRetryCommand(ctx, msg)
		return
	}
	if len(parts) > 1 && strings.EqualFold(parts[1], "resolve") {
		b.handleOrphanResolveCommand(ctx, msg, parts)
		return
	}
	if len(parts) > 1 && !strings.EqualFold(parts[1], "status") {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"ℹ️ Gunakan <code>/reconcile</code>, <code>/reconcile retry</code>, atau <code>/reconcile resolve ORPHAN_ID [PAIR]</code>.",
			b.defaultKeyboard(),
		)
		return
	}
	b.sendMonitorSection(
		ctx, msg.Chat.ID, msg.MessageID,
		"RECONCILIATION INCIDENTS",
		"reconciliation", "reconciliation_mismatches", "mismatches",
	)
}

func (b *Bot) handleReconcileRetryCommand(ctx context.Context, msg *Message) {
	userID := int64(0)
	if msg.From != nil {
		userID = msg.From.ID
	}
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"⛔ <b>Akses ditolak:</b> User tidak diizinkan menjalankan recovery operator.",
			b.defaultKeyboard(),
		)
		return
	}
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"❌ Redis tidak terhubung; permintaan reconciliation tidak dapat dikirim.",
			b.defaultKeyboard(),
		)
		return
	}
	b.sendReplyWithMarkup(
		ctx, msg.Chat.ID, msg.MessageID,
		"⏳ <b>Reconciliation retry dimulai.</b> Executor akan memeriksa ulang order, fill, posisi, dan exposure. Tidak ada order trading yang dikirim atau dibatalkan.",
		nil,
	)
	reply, err := rc.RequestRecoveryRetry(ctx)
	if err != nil {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, 0,
			"⚠️ <b>Reconciliation belum terkonfirmasi:</b> "+html.EscapeString(err.Error())+
				"\nPeriksa <code>/reconcile</code> dan <code>/recovery</code>; jangan menganggap insiden sudah selesai.",
			b.defaultKeyboard(),
		)
		return
	}
	status := "⚠️ <b>MASIH ADA INSIDEN</b>"
	if reply.Complete {
		status = "🟢 <b>SEMUA CHECK RECOVERY SELESAI</b>"
	}
	message := fmt.Sprintf(
		"🧾 <b>HASIL RECONCILIATION RETRY</b>\n\n%s\n• Pending order: <code>%d</code>\n• Scope halt: <code>%d</code>\n• Waktu: <code>%s</code>\n\n<i>Read-only: tidak ada order yang dibuat, diubah, atau dibatalkan.</i>",
		status, len(reply.PendingOrders), len(reply.ScopeHalts),
		time.UnixMilli(reply.TimestampMS).UTC().Format("2006-01-02 15:04:05 UTC"),
	)
	if len(reply.PendingOrders) > 0 {
		message += "\n\n<b>Pending:</b> <code>" + html.EscapeString(prettyJSON(mustJSONRaw(reply.PendingOrders))) + "</code>"
	}
	if len(reply.ScopeHalts) > 0 {
		message += "\n\n<b>Halt:</b> <code>" + html.EscapeString(prettyJSON(mustJSONRaw(reply.ScopeHalts))) + "</code>"
	}
	if reply.Error != "" {
		message += "\n\n<b>Catatan:</b> " + html.EscapeString(reply.Error)
	}
	b.sendReplyWithMarkup(ctx, msg.Chat.ID, 0, message, b.defaultKeyboard())
}

func (b *Bot) handleOrphanResolveCommand(ctx context.Context, msg *Message, parts []string) {
	if len(parts) < 3 {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"ℹ️ Gunakan <code>/reconcile resolve ORPHAN_ID [PAIR_TAG]</code>. Resolusi hanya berhasil jika exchange membuktikan order terminal dengan fill nol.",
			b.defaultKeyboard(),
		)
		return
	}
	userID := int64(0)
	if msg.From != nil { userID = msg.From.ID }
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			"⛔ <b>Akses ditolak:</b> User tidak diizinkan menyelesaikan orphan order.",
			b.defaultKeyboard())
		return
	}
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			"❌ Redis tidak terhubung; resolusi orphan tidak dapat dikirim.",
			b.defaultKeyboard())
		return
	}
	pairTag := ""
	if len(parts) > 3 { pairTag = parts[3] }
	reply, err := rc.RequestOrphanResolution(ctx, parts[2], pairTag)
	if err != nil {
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			"⚠️ <b>Resolusi orphan tidak terkonfirmasi:</b> "+html.EscapeString(err.Error()),
			b.defaultKeyboard())
		return
	}
	if !reply.Resolved {
		detail := "Exchange belum membuktikan order terminal dengan fill nol, atau masih ada insiden lain pada scope yang sama."
		if reply.Error != "" { detail = reply.Error }
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			"🛑 <b>ORPHAN TETAP DIBLOKIR</b>\n"+html.EscapeString(detail)+" Halt tetap dipertahankan. Periksa <code>/orphans</code> dan <code>/reconcile</code>.",
			b.defaultKeyboard())
		return
	}
	status := "⚠️ Scope masih memiliki halt lain."
	if reply.Complete { status = "🟢 Tidak ada pending recovery atau scope halt tersisa." }
	b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
		"✅ <b>ORPHAN TERVERIFIKASI DAN DICATAT SELESAI</b>\n• Orphan: <code>"+html.EscapeString(parts[2])+"</code>\n• Pair: <code>"+html.EscapeString(pairTag)+"</code>\n• "+status+"\n\n<i>Resolusi hanya dilakukan setelah exact venue lookup memastikan status terminal dan filled quantity nol. Tidak ada order yang dikirim atau dibatalkan.</i>",
		b.defaultKeyboard())
}

func mustJSONRaw(value interface{}) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("{}")
	}
	return encoded
}

