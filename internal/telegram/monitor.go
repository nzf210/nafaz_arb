package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

type monitorSnapshot map[string]json.RawMessage

func (b *Bot) readMonitorSnapshot(ctx context.Context) (monitorSnapshot, string) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		return nil, "Redis belum terhubung; data monitoring tidak dapat diverifikasi."
	}
	raw, found, err := rc.GetExecutorStatus(ctx)
	if err != nil {
		return nil, "Gagal membaca status executor: " + err.Error()
	}
	if !found {
		return nil, "Snapshot executor tidak tersedia/kedaluwarsa. Status tidak boleh dianggap kosong atau READY."
	}
	var snapshot monitorSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return nil, "Snapshot executor tidak valid: " + err.Error()
	}
	return snapshot, ""
}

func rawField(m monitorSnapshot, names ...string) json.RawMessage {
	for _, name := range names {
		if value := m[name]; len(value) > 0 && string(value) != "null" {
			return value
		}
	}
	return nil
}

func decodeList(raw json.RawMessage) []json.RawMessage {
	var items []json.RawMessage
	if len(raw) == 0 { return nil }
	_ = json.Unmarshal(raw, &items)
	return items
}

func prettyJSON(raw json.RawMessage) string {
	if len(raw) == 0 { return "Data belum dipublikasikan oleh executor." }
	var value interface{}
	if json.Unmarshal(raw, &value) != nil { return string(raw) }
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil { return string(raw) }
	return string(encoded)
}

func (b *Bot) sendMonitorSection(ctx context.Context, chatID, replyToID int64, title string, fields ...string) {
	snapshot, problem := b.readMonitorSnapshot(ctx)
	if problem != "" {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ <b>"+html.EscapeString(title)+"</b>\n\n"+html.EscapeString(problem), b.defaultKeyboard())
		return
	}
	var raw json.RawMessage
	for _, field := range fields {
		if raw = rawField(snapshot, field); len(raw) > 0 { break }
	}
	body := prettyJSON(raw)
	if len([]rune(body)) > 3300 {
		runes := []rune(body)
		body = string(runes[:3200]) + "\n... dipotong; gunakan detail spesifik."
	}
	msg := "📡 <b>" + html.EscapeString(title) + "</b>\n\n<pre>" + html.EscapeString(body) + "</pre>\n\n<i>Read-only. Sumber: snapshot executor terakhir.</i>"
	b.sendReplyWithMarkup(ctx, chatID, replyToID, msg, b.defaultKeyboard())
}

func (b *Bot) handleOrdersCommand(ctx context.Context, chatID, replyToID int64) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ <b>Orders</b>\\n\\nRedis tidak terhubung; registry order tidak dapat diverifikasi.", b.defaultKeyboard())
		return
	}
	items, err := rc.ListActiveOrderRecords(ctx)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ <b>Gagal membaca active order registry:</b> "+html.EscapeString(err.Error()), b.defaultKeyboard())
		return
	}
	b.sendOrderList(ctx, chatID, replyToID, items)
}

func (b *Bot) sendOrderList(ctx context.Context, chatID, replyToID int64, items []json.RawMessage) {
	var out strings.Builder
	out.WriteString(fmt.Sprintf("📋 <b>ACTIVE BOT ORDERS (%d)</b>\n\n", len(items)))
	if len(items) == 0 { out.WriteString("<i>Tidak ada active order pada snapshot.</i>\n") }
	for i, item := range items {
		if i >= 20 { out.WriteString("\n<i>Daftar dibatasi 20 order.</i>"); break }
		var m map[string]interface{}
		_ = json.Unmarshal(item, &m)
		id := firstMapValue(m, "internal_order_id", "order_id", "id")
		pair := firstMapValue(m, "pair", "pair_tag", "symbol", "pair_id")
		exchange := firstMapValue(m, "exchange", "local_exchange", "venue")
		status := firstMapValue(m, "status", "state")
		out.WriteString(fmt.Sprintf("<b>[%d] %s</b>\n• Pair: <code>%s</code>\n• Exchange: <code>%s</code>\n• Status: <code>%s</code>\n\n",
			i+1, html.EscapeString(id), html.EscapeString(pair), html.EscapeString(exchange), html.EscapeString(status)))
	}
	b.sendReplyWithMarkup(ctx, chatID, replyTo, out.String()+"<i>Read-only; tidak ada order yang diubah.</i>", b.defaultKeyboard())
}

func firstMapValue(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key]; ok && value != nil && fmt.Sprint(value) != "" { return fmt.Sprint(value) }
	}
	return "n/a"
}

func (b *Bot) handleOrderDetail(ctx context.Context, chatID, replyToID int64, id string) {
	snapshot, problem := b.readMonitorSnapshot(ctx)
	if problem != "" {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ "+html.EscapeString(problem), b.defaultKeyboard()); return
	}
	for _, field := range []string{"order_registry", "all_orders", "active_orders", "orders", "pending_orders"} {
		for _, item := range decodeList(rawField(snapshot, field)) {
			var m map[string]interface{}
			_ = json.Unmarshal(item, &m)
			if firstMapValue(m, "internal_order_id", "order_id", "id") == id {
				b.sendReplyWithMarkup(ctx, chatID, replyToID, "🔎 <b>ORDER "+html.EscapeString(id)+"</b>\n<pre>"+html.EscapeString(prettyJSON(item))+"</pre>", b.defaultKeyboard()); return
			}
		}
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Order ID tidak ditemukan pada snapshot executor. Jangan menyimpulkan order tidak ada di exchange; lakukan rekonsiliasi.", b.defaultKeyboard())
}

func (b *Bot) handlePositionDetail(ctx context.Context, chatID, replyToID int64, id string) {
	snapshot, problem := b.readMonitorSnapshot(ctx)
	if problem != "" { b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ "+html.EscapeString(problem), b.defaultKeyboard()); return }
	for _, field := range []string{"positions", "active_positions", "position_registry"} {
		for _, item := range decodeList(rawField(snapshot, field)) {
			var m map[string]interface{}
			_ = json.Unmarshal(item, &m)
			if firstMapValue(m, "position_id", "id", "pair_tag", "symbol") == id {
				b.sendReplyWithMarkup(ctx, chatID, replyToID, "🔎 <b>POSITION "+html.EscapeString(id)+"</b>\n<pre>"+html.EscapeString(prettyJSON(item))+"</pre>", b.defaultKeyboard()); return
			}
		}
	}
	// Redis active-position records remain a supported fallback, but are not
	// misrepresented as a complete historical position/order/fill ledger.
	b.mu.Lock(); rc := b.redisClient; b.mu.Unlock()
	if rc != nil {
		positions, err := rc.ListActivePositions(ctx)
		if err == nil {
			for _, pos := range positions {
				if strings.EqualFold(pos.Symbol, id) || redishandler.CleanPairKey(pos.LocalExchange, pos.Symbol) == id {
					raw, _ := json.Marshal(pos)
					b.sendReplyWithMarkup(ctx, chatID, replyToID, "🔎 <b>ACTIVE POSITION</b>\n<pre>"+html.EscapeString(prettyJSON(raw))+"</pre>\n<i>Executor snapshot tidak menyediakan ledger order/fill historis untuk posisi ini.</i>", b.defaultKeyboard()); return
				}
			}
		}
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Position ID tidak ditemukan. Data order/fills historis tidak tersedia dalam snapshot.", b.defaultKeyboard())
}

func (b *Bot) sendStartupSummary(ctx context.Context) {
	var snapshot monitorSnapshot
	var problem string
	for attempt := 0; attempt < 30; attempt++ {
		snapshot, problem = b.readMonitorSnapshot(ctx)
		if problem == "" { break }
		select { case <-ctx.Done(): return; case <-time.After(time.Second): }
	}
	if problem != "" {
		message := "🚨 <b>EXECUTOR STARTUP RECOVERY — NOT READY</b>\n\n• Global trading: <b>NOT READY / UNKNOWN</b>\n• Verification: " + html.EscapeString(problem) + "\n\n<i>Jangan aktifkan trading sampai order, posisi, dan exchange berhasil diverifikasi.</i>"
		targets := append([]int64(nil), b.cfg.Telegram.AllowedChatIDs...)
		if b.cfg.Telegram.ChatID != 0 { targets = []int64{b.cfg.Telegram.ChatID} }
		for _, chatID := range targets { if b.isChatAllowed(chatID) { b.sendReplyWithMarkup(ctx, chatID, 0, message, nil) } }
		return
	}
	started := time.Now().UTC().Format(time.RFC3339)
	if raw := rawField(snapshot, "startup_time", "executor_startup_time"); len(raw) > 0 {
		var value string; if json.Unmarshal(raw, &value) == nil && value != "" { started = value }
	}
	safetyReady := false
	_ = json.Unmarshal(rawField(snapshot, "safety_conditions_met"), &safetyReady)
	globalStatus := "NOT READY / UNKNOWN"
	if safetyReady && strings.EqualFold(string(rawField(snapshot, "global_trading_status")), "\"READY\"") {
		globalStatus = "READY"
	}
	count := func(names ...string) string {
		raw := rawField(snapshot, names...)
		if len(raw)==0 { return "unknown" }
		return strings.Trim(string(raw), "\"")
	}
	message := fmt.Sprintf("🚦 <b>EXECUTOR STARTUP RECOVERY</b>\n\n• Startup: <code>%s</code>\n• Recovered positions: <code>%s</code>\n• Recovered open orders: <code>%s</code>\n• Partial-fill orders: <code>%s</code>\n• Orphan orders: <code>%s</code>\n• Reconciliation mismatches: <code>%s</code>\n• Failed exchanges: <code>%s</code>\n• Global trading: <b>%s</b>\n\n<i>READY hanya jika safety_conditions_met=true dan global_trading_status=READY.</i>",
		html.EscapeString(started), count("recovered_positions"), count("recovered_open_orders"), count("partial_fill_orders"), count("orphan_orders_count"), count("reconciliation_mismatch_count"), count("failed_exchanges"), globalStatus)
	targets := append([]int64(nil), b.cfg.Telegram.AllowedChatIDs...)
	if b.cfg.Telegram.ChatID != 0 { targets = []int64{b.cfg.Telegram.ChatID} }
	for _, chatID := range targets { if b.isChatAllowed(chatID) { b.sendReplyWithMarkup(ctx, chatID, 0, message, nil) } }
}
