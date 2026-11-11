package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
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
	if title == "ORPHAN ORDERS" {
		b.mu.Lock(); rc := b.redisClient; b.mu.Unlock()
		if rc == nil {
			b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Redis tidak terhubung; orphan registry tidak dapat diverifikasi.", b.defaultKeyboard()); return
		}
		items, err := rc.ListOrphanOrderRecords(ctx)
		if err != nil { b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Gagal membaca orphan registry: "+html.EscapeString(err.Error()), b.defaultKeyboard()); return }
		raw, _ = json.Marshal(items)
	} else if title == "RISK HALT" {
		view := monitorSnapshot{}
		for _, key := range []string{"status", "trading_halted", "halt_reason", "exchange_halted", "failed_pairs"} {
			if value := rawField(snapshot, key); len(value) > 0 { view[key] = value }
		}
		raw, _ = json.Marshal(view)
	} else {
		for _, field := range fields {
			if raw = rawField(snapshot, field); len(raw) > 0 { break }
		}
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
	statusRaw, found, statusErr := rc.GetExecutorStatus(ctx)
	if statusErr != nil || !found {
		problem := "Executor status snapshot tidak tersedia; registry saja tidak cukup untuk membuktikan seluruh exchange order telah direkonsiliasi."
		if statusErr != nil { problem = statusErr.Error() }
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ <b>Orders — verifikasi belum lengkap</b>\\n\\n"+html.EscapeString(problem), b.defaultKeyboard())
		return
	}
	var status monitorSnapshot
	if err := json.Unmarshal([]byte(statusRaw), &status); err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Snapshot executor tidak valid: "+html.EscapeString(err.Error()), b.defaultKeyboard()); return
	}
	var recovery monitorSnapshot
	_ = json.Unmarshal(rawField(status, "recovery"), &recovery)
	var recoveryComplete bool
	_ = json.Unmarshal(rawField(recovery, "complete"), &recoveryComplete)
	var expected int
	_ = json.Unmarshal(rawField(recovery, "tracked_orders"), &expected)
	items, err := rc.ListActiveOrderRecords(ctx)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ <b>Gagal membaca active order registry:</b> "+html.EscapeString(err.Error()), b.defaultKeyboard())
		return
	}
	if !recoveryComplete || expected != len(items) {
		warning := fmt.Sprintf("⚠️ <b>INVENTORY BELUM TERVERIFIKASI</b>\\nRecovery complete: <code>%t</code> | Snapshot tracked: <code>%d</code> | Registry terbaca: <code>%d</code>\\nDaftar di bawah hanya order yang berhasil dibaca; jangan menganggapnya sebagai seluruh order exchange.", recoveryComplete, expected, len(items))
		b.sendReplyWithMarkup(ctx, chatID, replyToID, warning, nil)
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
	b.mu.Lock(); rc := b.redisClient; b.mu.Unlock()
	if rc == nil { b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Redis tidak terhubung; order registry tidak dapat diverifikasi.", b.defaultKeyboard()); return }
	records, err := rc.ListActiveOrderRecords(ctx)
	if err != nil { b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Gagal membaca order registry: "+html.EscapeString(err.Error()), b.defaultKeyboard()); return }
	for _, item := range records {
		var m map[string]interface{}
		_ = json.Unmarshal(item, &m)
		if firstMapValue(m, "local_order_id", "internal_order_id", "exchange_order_id", "id") == id {
			b.sendReplyWithMarkup(ctx, chatID, replyToID, "🔎 <b>ORDER "+html.EscapeString(id)+"</b>\\n<pre>"+html.EscapeString(prettyJSON(item))+"</pre>", b.defaultKeyboard()); return
		}
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Order ID tidak ditemukan dalam active order registry. Order terminal mungkin sudah dipindahkan dari index aktif; jangan menyimpulkan order exchange tidak ada.", b.defaultKeyboard())
}

func (b *Bot) handlePositionDetail(ctx context.Context, chatID, replyToID int64, id string) {
	b.mu.Lock(); rc := b.redisClient; b.mu.Unlock()
	if rc == nil { b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Redis tidak terhubung; posisi dan order terkait tidak dapat diverifikasi.", b.defaultKeyboard()); return }
	positions, posErr := rc.ListActivePositions(ctx)
	if posErr != nil { b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Gagal membaca active positions: "+html.EscapeString(posErr.Error()), b.defaultKeyboard()); return }
	var matched *domain.ActivePosition
	for i := range positions {
		if strings.EqualFold(positions[i].Symbol, id) || redishandler.CleanPairKey(positions[i].LocalExchange, positions[i].Symbol) == id {
			matched = &positions[i]; break
		}
	}
	orders, orderErr := rc.ListActiveOrderRecords(ctx)
	if orderErr != nil { b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Gagal membaca order registry terkait posisi: "+html.EscapeString(orderErr.Error()), b.defaultKeyboard()); return }
	var related []json.RawMessage
	for _, item := range orders {
		var record map[string]interface{}
		_ = json.Unmarshal(item, &record)
		if firstMapValue(record, "position_id") == id || firstMapValue(record, "pair_key", "pair_tag") == id {
			related = append(related, item)
		}
	}
	if matched == nil && len(related) == 0 {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Position ID tidak ditemukan pada active position store maupun active order registry. Ini tidak membuktikan tidak ada exposure di exchange.", b.defaultKeyboard()); return
	}
	var out strings.Builder
	out.WriteString("🔎 <b>POSITION "+html.EscapeString(id)+"</b>\\n\\n")
	if matched != nil {
		raw, _ := json.Marshal(matched)
		out.WriteString("<b>Active position snapshot</b>\\n<pre>"+html.EscapeString(prettyJSON(raw))+"</pre>\\n")
	}
	out.WriteString(fmt.Sprintf("<b>Active orders terkait (%d)</b>\\n", len(related)))
	if len(related) == 0 { out.WriteString("<i>Tidak ada active order terkait yang ditemukan.</i>\\n") }
	for i, item := range related {
		if i >= 10 { out.WriteString("<i>Daftar order dibatasi 10.</i>"); break }
		out.WriteString("<pre>"+html.EscapeString(prettyJSON(item))+"</pre>\\n")
	}
	out.WriteString("<i>Fill ledger historis tidak dipublikasikan oleh status Redis Telegram saat ini.</i>")
	b.sendReplyWithMarkup(ctx, chatID, replyToID, out.String(), b.defaultKeyboard())
}

func (b *Bot) sendStartupSummary(ctx context.Context) {
	var snapshot monitorSnapshot
	var problem string
	for attempt := 0; attempt < 30; attempt++ {
		snapshot, problem = b.readMonitorSnapshot(ctx)
		if problem == "" { break }
		select { case <-ctx.Done(): return; case <-time.After(time.Second): }
	}
	targets := append([]int64(nil), b.cfg.Telegram.AllowedChatIDs...)
	if b.cfg.Telegram.ChatID != 0 { targets = []int64{b.cfg.Telegram.ChatID} }
	if problem != "" {
		message := "🚨 <b>EXECUTOR STARTUP RECOVERY — NOT READY</b>\\n\\n• Global trading: <b>NOT READY / UNKNOWN</b>\\n• Verification: " + html.EscapeString(problem) + "\\n\\n<i>Jangan aktifkan trading sampai order, posisi, dan exchange berhasil diverifikasi.</i>"
		for _, chatID := range targets { if b.isChatAllowed(chatID) { b.sendReplyWithMarkup(ctx, chatID, 0, message, nil) } }
		return
	}

	var recovery, reconciliation monitorSnapshot
	_ = json.Unmarshal(rawField(snapshot, "recovery"), &recovery)
	_ = json.Unmarshal(rawField(snapshot, "reconciliation"), &reconciliation)
	countMap := func(raw json.RawMessage) int { var value map[string]json.RawMessage; _ = json.Unmarshal(raw, &value); return len(value) }
	countList := func(raw json.RawMessage) int { return len(decodeList(raw)) }
	orphanCount := countList(rawField(recovery, "orphan_orders"))
	pendingRecoveryCount := countMap(rawField(recovery, "pending_orders"))
	scopeHaltCount := countMap(rawField(recovery, "scope_halts"))
	failedExchangeCount := countMap(rawField(snapshot, "exchange_halted"))
	mismatchCount := countMap(rawField(reconciliation, "exposure_mismatches")) + countMap(rawField(reconciliation, "trade_fill_mismatches"))
	pendingOrders := decodeList(rawField(snapshot, "pending_orders"))
	partialFillCount := 0
	for _, item := range pendingOrders {
		var order map[string]interface{}
		_ = json.Unmarshal(item, &order)
		status := strings.ToUpper(firstMapValue(order, "status", "state"))
		filled := firstMapValue(order, "filled", "filled_quantity", "filled_qty")
		if strings.Contains(status, "PARTIAL") || (filled != "n/a" && filled != "0" && filled != "0.0") { partialFillCount++ }
	}
	ready := false
	var recoveryComplete, recoveryReady, tradingHalted bool
	_ = json.Unmarshal(rawField(recovery, "complete"), &recoveryComplete)
	_ = json.Unmarshal(rawField(recovery, "ready"), &recoveryReady)
	_ = json.Unmarshal(rawField(snapshot, "trading_halted"), &tradingHalted)
	ready = recoveryComplete && recoveryReady && !tradingHalted && orphanCount == 0 && pendingRecoveryCount == 0 && scopeHaltCount == 0 && failedExchangeCount == 0 && mismatchCount == 0
	globalStatus := "NOT READY"
	if ready { globalStatus = "READY" }

	started := "unknown"
	if raw := rawField(snapshot, "executor_startup_time_ms"); len(raw) > 0 {
		var value int64
		if json.Unmarshal(raw, &value) == nil && value > 0 { started = time.UnixMilli(value).UTC().Format(time.RFC3339) }
	}
	message := fmt.Sprintf("🚦 <b>EXECUTOR STARTUP RECOVERY</b>\\n\\n• Startup: <code>%s</code>\\n• Recovered positions: <code>%s</code>\\n• Recovered open orders: <code>%s</code>\\n• Partial-fill orders: <code>%d</code>\\n• Orphan orders: <code>%d</code>\\n• Reconciliation mismatches: <code>%d</code>\\n• Exchanges halted / failed verification: <code>%d</code>\\n• Global trading: <b>%s</b>\\n\\n<i>READY hanya jika recovery complete, semua registry/venue terverifikasi, tidak ada orphan, mismatch, scope halt, atau risk halt.</i>",
		html.EscapeString(started), strings.Trim(string(rawField(snapshot, "open_positions")), "\\""), strings.Trim(string(rawField(recovery, "tracked_orders")), "\\""),
		partialFillCount, orphanCount, mismatchCount, failedExchangeCount, globalStatus)
	for _, chatID := range targets { if b.isChatAllowed(chatID) { b.sendReplyWithMarkup(ctx, chatID, 0, message, nil) } }
}
