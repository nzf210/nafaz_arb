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

const htmlPreClose = "</pre>"

func (b *Bot) readMonitorSnapshot(ctx context.Context) (monitorSnapshot, string, bool) {
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
	return snapshot, "", false
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

func safeRecordJSON(raw json.RawMessage) json.RawMessage {
	var record map[string]json.RawMessage
	if json.Unmarshal(raw, &record) != nil { return json.RawMessage("{}") }
	allowed := []string{
		"local_order_id", "internal_order_id", "exchange_order_id", "client_order_id",
		"position_id", "pair_key", "exchange", "exchange_name", "market_type", "leg",
		"order_role", "symbol", "side", "order_type", "status", "original_quantity",
		"requested_qty", "filled_quantity", "filled_qty", "remaining_quantity",
		"remaining_qty", "price", "limit_price", "average_fill_price", "hedged_quantity",
		"recovery_status", "reduce_only", "created_at", "updated_at", "submitted_at",
		"completed_at", "id", "order_id", "exchange_fill_id", "fill_source", "qty",
		"quote_qty", "fee_amount", "fee_asset", "fee_verified", "filled_at",
		"orphan_id", "reason", "scope",
	}
	view := make(map[string]json.RawMessage, len(allowed))
	for _, key := range allowed { if value, ok := record[key]; ok { view[key] = value } }
	encoded, err := json.Marshal(view)
	if err != nil { return json.RawMessage("{}") }
	return encoded
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
		var recovery monitorSnapshot
		_ = json.Unmarshal(rawField(snapshot, "recovery"), &recovery)
		reported := decodeList(rawField(recovery, "orphan_orders"))
		if len(items) == 0 && len(reported) > 0 { items = reported }
		if len(items) != len(reported) {
			b.sendReplyWithMarkup(ctx, chatID, replyToID, fmt.Sprintf("⚠️ <b>ORPHAN REGISTRY MISMATCH</b>\nRedis index: <code>%d</code> | Recovery snapshot: <code>%d</code>\nGunakan daftar berikut sebagai informasi parsial; inventory belum konsisten.", len(items), len(reported)), b.defaultKeyboard())
		}
		b.sendJSONPages(ctx, chatID, replyToID, "ORPHAN EXCHANGE ORDERS", items)
		return
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
	msg := "📡 <b>" + html.EscapeString(title) + "</b>\n\n<pre>" + html.EscapeString(body) + htmlPreClose + "\n\n<i>Read-only. Sumber: snapshot executor terakhir.</i>"
	b.sendReplyWithMarkup(ctx, chatID, replyToID, msg, b.defaultKeyboard())
}

func (b *Bot) handleOrdersCommand(ctx context.Context, chatID, replyToID int64) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ <b>Orders</b>\n\nRedis tidak terhubung; registry order tidak dapat diverifikasi.", b.defaultKeyboard())
		return
	}
	statusRaw, found, statusErr := rc.GetExecutorStatus(ctx)
	if statusErr != nil || !found {
		problem := "Executor status snapshot tidak tersedia; registry saja tidak cukup untuk membuktikan seluruh exchange order telah direkonsiliasi."
		if statusErr != nil { problem = statusErr.Error() }
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ <b>Orders — verifikasi belum lengkap</b>\n\n"+html.EscapeString(problem), b.defaultKeyboard())
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
	inventoryKnown := len(rawField(recovery, "tracked_orders")) > 0
	if !recoveryComplete || !inventoryKnown || expected != len(items) {
		warning := fmt.Sprintf("⚠️ <b>INVENTORY BELUM TERVERIFIKASI</b>\nRecovery complete: <code>%t</code> | Snapshot tracked: <code>%d</code> | Registry terbaca: <code>%d</code>\nDaftar di bawah hanya order yang berhasil dibaca; jangan menganggapnya sebagai seluruh order exchange.", recoveryComplete, expected, len(items))
		b.sendReplyWithMarkup(ctx, chatID, replyToID, warning, nil)
	}
	b.sendOrderList(ctx, chatID, replyToID, items)
}

func (b *Bot) sendOrderList(ctx context.Context, chatID, replyToID int64, items []json.RawMessage) {
	if len(items) == 0 {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "📋 <b>ACTIVE BOT ORDERS (0)</b>\n\n<i>Tidak ada active order pada registry setelah recovery terverifikasi.</i>", b.defaultKeyboard())
		return
	}
	const pageSize = 20
	pages := (len(items) + pageSize - 1) / pageSize
	for page, start := 0, 0; start < len(items); page, start = page+1, start+pageSize {
		end := start + pageSize
		if end > len(items) { end = len(items) }
		var out strings.Builder
		out.WriteString(fmt.Sprintf("📋 <b>ACTIVE BOT ORDERS (%d) — %d/%d</b>\n\n", len(items), page+1, pages))
		for i, item := range items[start:end] {
			var m map[string]interface{}
			_ = json.Unmarshal(item, &m)
			id := firstMapValue(m, "local_order_id", "internal_order_id", "order_id", "id")
			pair := firstMapValue(m, "pair", "pair_key", "pair_tag", "symbol", "pair_id")
			exchange := firstMapValue(m, "exchange", "exchange_name", "local_exchange", "venue")
			status := firstMapValue(m, "status", "state")
			out.WriteString(fmt.Sprintf("<b>[%d] %s</b>\n• Pair: <code>%s</code>\n• Exchange: <code>%s</code>\n• Status: <code>%s</code>\n\n",
				start+i+1, html.EscapeString(id), html.EscapeString(pair), html.EscapeString(exchange), html.EscapeString(status)))
		}
		b.sendReplyWithMarkup(ctx, chatID, replyToID, out.String()+"<i>Read-only; tidak ada order yang diubah.</i>", b.defaultKeyboard())
		replyToID = 0
	}
}

func firstMapValue(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key]; ok && value != nil && fmt.Sprint(value) != "" { return fmt.Sprint(value) }
	}
	return "n/a"
}

func (b *Bot) handleOrderDetail(ctx context.Context, chatID, replyToID int64, id string) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Redis tidak terhubung; order registry tidak dapat diverifikasi.", b.defaultKeyboard())
		return
	}
	record, found, err := rc.GetOrderRecord(ctx, id)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Gagal membaca order record: "+html.EscapeString(err.Error()), b.defaultKeyboard())
		return
	}
	if found {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "🔎 <b>ORDER "+html.EscapeString(id)+"</b>\n<pre>"+html.EscapeString(prettyJSON(safeRecordJSON(record)))+htmlPreClose, b.defaultKeyboard())
		return
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Internal order ID tidak ditemukan pada registry Redis. Jika order sudah retired, periksa Supabase order_registry; ketiadaan record bukan bukti order exchange tidak ada.", b.defaultKeyboard())
}

func (b *Bot) sendJSONPages(ctx context.Context, chatID, replyToID int64, title string, rows []json.RawMessage) {
	if len(rows) == 0 {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "<b>"+html.EscapeString(title)+" (0)</b>\n<i>Tidak ada record.</i>", b.defaultKeyboard())
		return
	}
	const pageSize = 3
	pages := (len(rows) + pageSize - 1) / pageSize
	for page, start := 0, 0; start < len(rows); page, start = page+1, start+pageSize {
		end := start + pageSize
		if end > len(rows) { end = len(rows) }
		var out strings.Builder
		out.WriteString(fmt.Sprintf("<b>%s (%d) — %d/%d</b>\n\n", html.EscapeString(title), len(rows), page+1, pages))
		for _, row := range rows[start:end] {
			value := prettyJSON(safeRecordJSON(row))
			if len([]rune(value)) > 1000 { value = string([]rune(value)[:950]) + "\n... record dipotong" }
			out.WriteString("<pre>"+html.EscapeString(value)+htmlPreClose + "\n")
		}
		b.sendReplyWithMarkup(ctx, chatID, replyToID, out.String(), b.defaultKeyboard())
		replyToID = 0
	}
}

func (b *Bot) handlePositionDetail(ctx context.Context, chatID, replyToID int64, id string) {
	b.mu.Lock()
	rc := b.redisClient
	ledgerReader := b.positionLedgerReader
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Redis tidak terhubung; posisi dan active order terkait tidak dapat diverifikasi.", b.defaultKeyboard())
		return
	}
	positions, posErr := rc.ListActivePositions(ctx)
	if posErr != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Gagal membaca active positions: "+html.EscapeString(posErr.Error()), b.defaultKeyboard())
		return
	}
	var matched *domain.ActivePosition
	for i := range positions {
		if strings.EqualFold(positions[i].Symbol, id) || redishandler.CleanPairKey(positions[i].LocalExchange, positions[i].Symbol) == id {
			matched = &positions[i]
			break
		}
	}
	orders, orderErr := rc.ListActiveOrderRecords(ctx)
	if orderErr != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ Gagal membaca order registry terkait posisi: "+html.EscapeString(orderErr.Error()), b.defaultKeyboard())
		return
	}
	var related []json.RawMessage
	for _, item := range orders {
		var record map[string]interface{}
		_ = json.Unmarshal(item, &record)
		if firstMapValue(record, "position_id") == id || firstMapValue(record, "pair_key", "pair_tag") == id {
			related = append(related, item)
		}
	}
	var ledger map[string][]json.RawMessage
	ledgerAvailable := ledgerReader != nil
	if ledgerReader != nil {
		var err error
		ledger, err = ledgerReader.PositionOrderFills(ctx, id)
		if err != nil {
			b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Supabase order/fill ledger tidak dapat dibaca: "+html.EscapeString(err.Error())+"\nData Redis tetap tersedia, tetapi fills historis belum terverifikasi.", b.defaultKeyboard())
			return
		}
	}
	ledgerOrders, ledgerFills := ledger["orders"], ledger["fills"]
	if matched == nil && len(related) == 0 && len(ledgerOrders) == 0 {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ Position ID tidak ditemukan pada active position store maupun durable order ledger. Ini tidak membuktikan tidak ada exposure di exchange.", b.defaultKeyboard())
		return
	}
	var summary strings.Builder
	summary.WriteString("🔎 <b>POSITION "+html.EscapeString(id)+"</b>\n")
	if matched != nil {
		raw, _ := json.Marshal(matched)
		summary.WriteString("<b>Active position snapshot</b>\n<pre>"+html.EscapeString(prettyJSON(raw))+htmlPreClose)
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, summary.String(), b.defaultKeyboard())
	b.sendJSONPages(ctx, chatID, 0, "Redis active orders terkait", related)
	if ledgerAvailable {
		b.sendJSONPages(ctx, chatID, 0, "Supabase durable orders", ledgerOrders)
		b.sendJSONPages(ctx, chatID, 0, "Immutable exchange fills", ledgerFills)
	} else {
		b.sendReplyWithMarkup(ctx, chatID, 0, "<b>Fills historis:</b> Supabase belum terhubung; fill ledger tidak dapat diverifikasi.", b.defaultKeyboard())
	}
}

type startupRecoverySummary struct {
	started               string
	recoveredPositions    int
	trackedOrders         int
	partialFillCount      int
	orphanCount           int
	mismatchCount         int
	failedExchangeCount   int
	globalStatus          string
}

func (b *Bot) waitForStartupRecovery(
	ctx context.Context,
) (monitorSnapshot, string) {
	var snapshot monitorSnapshot
	problem := ""
	for attempt := 0; attempt < 60; attempt++ {
		snapshot, problem = b.readMonitorSnapshot(ctx)
		if problem == "" {
			var recovery monitorSnapshot
			var recoveryComplete bool
			_ = json.Unmarshal(rawField(snapshot, "recovery"), &recovery)
			_ = json.Unmarshal(rawField(recovery, "complete"), &recoveryComplete)
			if recoveryComplete {
				return snapshot, ""
			}
			problem = "Executor startup recovery is still in progress."
		}
		select {
		case <-ctx.Done():
			return nil, "", true
		case <-time.After(time.Second):
		}
	}
	return snapshot, problem, false
}

func countSnapshotMap(raw json.RawMessage) int {
	var value map[string]json.RawMessage
	_ = json.Unmarshal(raw, &value)
	return len(value)
}

func (b *Bot) buildStartupRecoverySummary(
	snapshot monitorSnapshot,
) startupRecoverySummary {
	var recovery, reconciliation monitorSnapshot
	_ = json.Unmarshal(rawField(snapshot, "recovery"), &recovery)
	_ = json.Unmarshal(rawField(snapshot, "reconciliation"), &reconciliation)
	orphanCount := len(decodeList(rawField(recovery, "orphan_orders")))
	pendingRecoveryCount := countSnapshotMap(rawField(recovery, "pending_orders"))
	scopeHaltCount := countSnapshotMap(rawField(recovery, "scope_halts"))
	failedExchangeCount := countSnapshotMap(rawField(snapshot, "exchange_halted"))
	mismatchCount := countSnapshotMap(
		rawField(reconciliation, "exposure_mismatches"),
	) + countSnapshotMap(rawField(reconciliation, "trade_fill_mismatches"))

	pendingOrders := decodeList(rawField(snapshot, "pending_orders"))
	partialFillCount := 0
	for _, item := range pendingOrders {
		var order map[string]interface{}
		_ = json.Unmarshal(item, &order)
		status := strings.ToUpper(firstMapValue(order, "status", "state"))
		filled := firstMapValue(order, "filled", "filled_quantity", "filled_qty")
		if strings.Contains(status, "PARTIAL") ||
			(filled != "n/a" && filled != "0" && filled != "0.0") {
			partialFillCount++
		}
	}

	var recoveryComplete, recoveryReady, tradingHalted bool
	_ = json.Unmarshal(rawField(recovery, "complete"), &recoveryComplete)
	_ = json.Unmarshal(rawField(recovery, "ready"), &recoveryReady)
	_ = json.Unmarshal(rawField(snapshot, "trading_halted"), &tradingHalted)
	safetySchemaComplete := len(rawField(recovery, "complete")) > 0 &&
		len(rawField(recovery, "ready")) > 0 &&
		len(rawField(recovery, "pending_orders")) > 0 &&
		len(rawField(recovery, "scope_halts")) > 0 &&
		len(rawField(recovery, "orphan_orders")) > 0 &&
		len(rawField(snapshot, "exchange_halted")) > 0 &&
		len(rawField(snapshot, "reconciliation")) > 0
	ready := safetySchemaComplete && recoveryComplete && recoveryReady &&
		!tradingHalted && orphanCount == 0 && pendingRecoveryCount == 0 &&
		scopeHaltCount == 0 && failedExchangeCount == 0 && mismatchCount == 0
	globalStatus := "NOT READY"
	if ready {
		globalStatus = "READY"
	}

	var summary startupRecoverySummary
	_ = json.Unmarshal(rawField(snapshot, "open_positions"), &summary.recoveredPositions)
	_ = json.Unmarshal(rawField(recovery, "tracked_orders"), &summary.trackedOrders)
	summary.started = "unknown"
	if raw := rawField(snapshot, "executor_startup_time_ms"); len(raw) > 0 {
		var value int64
		if json.Unmarshal(raw, &value) == nil && value > 0 {
			summary.started = time.UnixMilli(value).UTC().Format(time.RFC3339)
		}
	}
	summary.partialFillCount = partialFillCount
	summary.orphanCount = orphanCount
	summary.mismatchCount = mismatchCount
	summary.failedExchangeCount = failedExchangeCount
	summary.globalStatus = globalStatus
	return summary
}

func formatStartupRecoveryMessage(summary startupRecoverySummary) string {
	return fmt.Sprintf("🚦 <b>EXECUTOR STARTUP RECOVERY</b>\n\n• Startup: <code>%s</code>\n• Recovered positions: <code>%s</code>\n• Recovered open orders: <code>%s</code>\n• Partial-fill orders: <code>%d</code>\n• Orphan orders: <code>%d</code>\n• Reconciliation mismatches: <code>%d</code>\n• Exchanges halted / failed verification: <code>%d</code>\n• Global trading: <b>%s</b>\n\n<i>READY hanya jika recovery complete, semua registry/venue terverifikasi, tidak ada orphan, mismatch, scope halt, atau risk halt.</i>",
		html.EscapeString(summary.started),
		fmt.Sprint(summary.recoveredPositions),
		fmt.Sprint(summary.trackedOrders),
		summary.partialFillCount,
		summary.orphanCount,
		summary.mismatchCount,
		summary.failedExchangeCount,
		summary.globalStatus,
	)
}

func (b *Bot) sendStartupNotReady(
	ctx context.Context, targets []int64, problem string,
) {
	message := "🚨 <b>EXECUTOR STARTUP RECOVERY — NOT READY</b>\n\n• Global trading: <b>NOT READY / UNKNOWN</b>\n• Verification: " +
		html.EscapeString(problem) +
		"\n\n<i>Jangan aktifkan trading sampai order, posisi, dan exchange berhasil diverifikasi.</i>"
	for _, chatID := range targets {
		if b.isChatAllowed(chatID) {
			b.sendReplyWithMarkup(ctx, chatID, 0, message, nil)
		}
	}
}

func (b *Bot) sendStartupSummary(ctx context.Context) {
	targets := append([]int64(nil), b.cfg.Telegram.AllowedChatIDs...)
	if b.cfg.Telegram.ChatID != 0 {
		targets = []int64{b.cfg.Telegram.ChatID}
	}
	snapshot, problem, cancelled := b.waitForStartupRecovery(ctx)
	if cancelled {
		return
	}
	if problem != "" {
		b.sendStartupNotReady(ctx, targets, problem)
		return
	}
	message := formatStartupRecoveryMessage(
		b.buildStartupRecoverySummary(snapshot),
	)
	for _, chatID := range targets {
		if b.isChatAllowed(chatID) {
			b.sendReplyWithMarkup(ctx, chatID, 0, message, nil)
		}
	}
}
