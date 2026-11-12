package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

type pendingOrderSnapshot struct {
	PairTag       string  `json:"pair_tag"`
	Leg           string  `json:"leg"`
	OrderID       string  `json:"order_id"`
	Symbol        string  `json:"symbol"`
	QuoteAsset    string  `json:"quote_asset"`
	LocalExchange string  `json:"local_exchange"`
	GlobalExchange string `json:"global_exchange"`
	Price         float64 `json:"price"`
	Qty           float64 `json:"qty"`
	Filled        float64 `json:"filled"`
	Remaining     float64 `json:"remaining"`
	Status        string  `json:"status"`
	CreatedAtMS   int64   `json:"created_at_ms"`
}

type executorPendingSnapshot struct {
	Status       string                 `json:"status"`
	MockMode     bool                   `json:"mock_mode"`
	TimestampMS  int64                  `json:"timestamp_ms"`
	PendingOrders []pendingOrderSnapshot `json:"pending_orders"`
	PendingEntries int                  `json:"pending_entries"`
	PendingExits   int                  `json:"pending_exits"`
}

// handlePendingOrdersCommand shows only executor-managed orders still awaiting
// resolution. It reads the executor's latest status snapshot and never mutates orders.
func (b *Bot) readPendingOrderSnapshot(
	ctx context.Context,
) (executorPendingSnapshot, string, bool) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		return executorPendingSnapshot{},
			"🕒 <b>Pending Orders</b>\n\n❌ Redis belum terhubung; status order tidak dapat dibaca.",
			false
	}
	raw, found, err := rc.GetExecutorStatus(ctx)
	if err != nil {
		return executorPendingSnapshot{},
			"❌ <b>Gagal membaca status executor:</b> "+html.EscapeString(err.Error()),
			false
	}
	if !found {
		return executorPendingSnapshot{},
			"🕒 <b>Pending Orders</b>\n\n⚠️ Executor offline atau status terakhir sudah kedaluwarsa. Tidak ada data pending yang dapat diverifikasi.",
			false
	}
	var snapshot executorPendingSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return executorPendingSnapshot{},
			"❌ <b>Format status executor tidak valid:</b> "+html.EscapeString(err.Error()),
			false
	}
	if len(snapshot.PendingOrders) == 0 &&
		snapshot.PendingEntries+snapshot.PendingExits > 0 {
		return executorPendingSnapshot{},
			"⚠️ <b>Executor belum mengirim rincian pending order.</b>\n\nPerbarui image executor agar menu ini dapat menampilkan ID order, CEX, harga, dan kuantitas. Jumlah tercatat: "+
				fmt.Sprintf("%d entry / %d exit", snapshot.PendingEntries, snapshot.PendingExits),
			false
	}
	return snapshot, "", true
}

func sortPendingOrderSnapshots(
	orders []pendingOrderSnapshot,
) []pendingOrderSnapshot {
	sorted := append([]pendingOrderSnapshot(nil), orders...)
	sort.SliceStable(sorted, func(i, j int) bool {
		left := strings.ToLower(
			sorted[i].LocalExchange + "|" + sorted[i].Symbol,
		)
		right := strings.ToLower(
			sorted[j].LocalExchange + "|" + sorted[j].Symbol,
		)
		return left < right
	})
	return sorted
}

func formatPendingOrderPage(
	snapshot executorPendingSnapshot, orders []pendingOrderSnapshot,
	start, end, page, pages int,
) string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf(
		"🕒 <b>PENDING ORDERS (%d) — %d/%d</b> — grouped by exchange / pair\n",
		len(orders), page+1, pages,
	))
	if snapshot.MockMode {
		out.WriteString("⚠️ <b>MODE SIMULASI</b>\n")
	} else {
		out.WriteString("🟢 <b>EXECUTOR LIVE</b>\n")
	}
	if snapshot.TimestampMS > 0 {
		out.WriteString(
			"🕓 Snapshot: " +
				time.UnixMilli(snapshot.TimestampMS).UTC().Format("2006-01-02 15:04:05 UTC") +
				"\n",
		)
	}
	out.WriteString("\n")
	lastGroup := ""
	for i, order := range orders[start:end] {
		group := strings.ToUpper(order.LocalExchange) + " / " +
			strings.ToUpper(order.Symbol)
		if group != lastGroup {
			out.WriteString("<b>🏦 " + html.EscapeString(group) + "</b>\n")
			lastGroup = group
		}
		status := order.Status
		if status == "" {
			status = "UNKNOWN"
		}
		out.WriteString(fmt.Sprintf(
			"<b>[%d] %s — %s</b>\n",
			start+i+1, html.EscapeString(strings.ToUpper(order.Symbol)),
			html.EscapeString(status),
		))
		out.WriteString(fmt.Sprintf(
			"• Leg: <code>%s</code> | Order ID: <code>%s</code>\n",
			html.EscapeString(order.Leg), html.EscapeString(order.OrderID),
		))
		out.WriteString(fmt.Sprintf(
			"• CEX Spot: <b>%s</b> | CEX Futures: <b>%s</b>\n",
			html.EscapeString(strings.ToUpper(order.LocalExchange)),
			html.EscapeString(strings.ToUpper(order.GlobalExchange)),
		))
		out.WriteString(fmt.Sprintf(
			"• Harga: <code>%.10g %s</code> | Qty: <code>%.8g</code>\n",
			order.Price, html.EscapeString(order.QuoteAsset), order.Qty,
		))
		out.WriteString(fmt.Sprintf(
			"• Filled: <code>%.8g</code> | Sisa: <code>%.8g</code>\n",
			order.Filled, order.Remaining,
		))
		if order.CreatedAtMS > 0 {
			age := time.Since(time.UnixMilli(order.CreatedAtMS))
			if age < 0 {
				age = 0
			}
			out.WriteString(fmt.Sprintf(
				"• Dibuka: %s (umur %s)\n",
				time.UnixMilli(order.CreatedAtMS).UTC().Format(TIME_FORMAT),
				age.Round(time.Second),
			))
		} else {
			out.WriteString("• Waktu pembukaan: <i>tidak tersedia</i>\n")
		}
		out.WriteString("\n")
	}
	out.WriteString("<i>Read-only: status berasal dari snapshot executor NAFAZ.</i>")
	return out.String()
}

func (b *Bot) handlePendingOrdersCommand(
	ctx context.Context, chatID, replyToID int64,
) {
	snapshot, problem, ok := b.readPendingOrderSnapshot(ctx)
	if !ok {
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID, problem, b.defaultKeyboard(),
		)
		return
	}
	orders := sortPendingOrderSnapshots(snapshot.PendingOrders)
	if len(orders) == 0 {
		message := "🕒 <b>PENDING ORDERS (0)</b>\n<i>Tidak ada pending order pada snapshot executor.</i>"
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID, message, b.defaultKeyboard(),
		)
		return
	}
	const pageSize = 6
	pages := (len(orders) + pageSize - 1) / pageSize
	for page, start := 0, 0; start < len(orders); page, start = page+1, start+pageSize {
		end := min(start+pageSize, len(orders))
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID,
			formatPendingOrderPage(snapshot, orders, start, end, page, pages),
			b.defaultKeyboard(),
		)
		replyToID = 0
	}
}

func (b *Bot) handlePositionsCommand(ctx context.Context, chatID int64, replyToID int64) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"💼 <b>Status Posisi Executor</b>\n\n⚠️ <i>Integrasi Redis tidak aktif atau belum terhubung. Posisi Executor tidak dapat dibaca saat ini.</i>",
			b.defaultKeyboard())
		return
	}

	positions, err := rc.ListActivePositions(ctx)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			fmt.Sprintf("❌ <b>Gagal membaca posisi:</b> %s", html.EscapeString(err.Error())),
			b.defaultKeyboard())
		return
	}

	stats, _ := rc.GetTradeStats(ctx)
	balances, _ := rc.GetBalances(ctx)
	orderRecords, orderRecordsErr := rc.ListActiveOrderRecords(ctx)

	if len(positions) == 0 {
		b.sendEmptyPositionsReply(ctx, chatID, replyToID, balances, stats)
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("💼 <b>Posisi Aktif Executor (%d Posisi Terbuka)</b>\n\n", len(positions)))

	if balances != nil && balances.TotalEquity > 0 {
		sb.WriteString("💰 <b>Saldo Bursa Executor:</b>\n")
		sb.WriteString(fmt.Sprintf("• Spot: <code>$%.2f USDT</code> (Tersedia: <code>$%.2f</code> | Terkunci: <code>$%.2f</code>)\n",
			balances.SpotTotal, balances.SpotAvailable, balances.SpotLocked))
		sb.WriteString(fmt.Sprintf("• Futures: <code>$%.2f USDT</code> (Tersedia: <code>$%.2f</code> | Terkunci: <code>$%.2f</code>)\n",
			balances.FuturesTotal, balances.FuturesAvailable, balances.FuturesLocked))
		sb.WriteString(fmt.Sprintf("• Total Ekuitas: <b>$%.2f USDT</b>\n\n", balances.TotalEquity))
		sb.WriteString(separatorLine + "\n")
	}

	var kbRows [][]InlineKeyboardButton
	if orderRecordsErr != nil {
		sb.WriteString("⚠️ <i>Active order registry tidak dapat dibaca; status order per leg belum diverifikasi.</i>\n\n")
	}
	for i, pos := range positions {
		b.formatPositionBlock(&sb, i+1, pos, rc, ctx)
		if orderRecordsErr == nil { b.formatPositionLegOrders(&sb, pos, orderRecords) }
		tag := redishandler.CleanPairKey(pos.LocalExchange, pos.Symbol)
		btnText := fmt.Sprintf("🚨 Tutup Paksa %s", strings.ToUpper(pos.Symbol))
		kbRows = append(kbRows, []InlineKeyboardButton{
			{Text: btnText, CallbackData: "force_close:" + tag},
		})
	}

	if stats.TotalClosed > 0 {
		sb.WriteString(separatorLine)
		sb.WriteString("📊 <b>Rekap Posisi Berhasil Ditutup:</b>\n")
		sb.WriteString(fmt.Sprintf("• Total Ditutup: <code>%d transaksi</code> (Menang: <code>%d</code>)\n",
			stats.TotalClosed, stats.WinCount))
		sb.WriteString(fmt.Sprintf("• Total Realized Profit: <b>%+.4f USDT</b> (Rp %+.2f)\n\n",
			stats.TotalPnLUSDT, stats.TotalPnLIDR))
	}

	kbRows = append(kbRows, []InlineKeyboardButton{
		{Text: HistoryText, CallbackData: "history"},
		{Text: "🔄 Refresh Posisi", CallbackData: "positions"},
	})
	kbRows = append(kbRows, []InlineKeyboardButton{
		{Text: MenuUtamaText, CallbackData: "menu"},
	})

	markup := &InlineKeyboardMarkup{InlineKeyboard: kbRows}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, sb.String(), markup)
}


func positionOrderRecordMatches(
	raw json.RawMessage, prefix string,
) (map[string]interface{}, bool) {
	var record map[string]interface{}
	if json.Unmarshal(raw, &record) != nil {
		return nil, false
	}
	pairKey := strings.ToLower(fmt.Sprint(record["pair_key"]))
	if pairKey == "" || pairKey == "<nil>" {
		return nil, false
	}
	if !strings.HasPrefix(pairKey, prefix) && !strings.Contains(pairKey, prefix) {
		return nil, false
	}
	return record, true
}

func formatPositionOrderRecord(
	record map[string]interface{},
) (string, string, bool) {
	market := strings.ToLower(fmt.Sprint(record["market_type"]))
	if market != "spot" && market != "perpetual" {
		return "", "", false
	}
	id := fmt.Sprint(record["exchange_order_id"])
	if id == "" || id == "<nil>" {
		id = fmt.Sprint(record["local_order_id"])
	}
	line := fmt.Sprintf(
		"<code>%s</code> %s (filled %s / remaining %s)",
		html.EscapeString(id),
		html.EscapeString(fmt.Sprint(record["status"])),
		html.EscapeString(fmt.Sprint(record["filled_quantity"])),
		html.EscapeString(fmt.Sprint(record["remaining_quantity"])),
	)
	return market, line, true
}

func writePositionLegLines(sb *strings.Builder, label string, lines []string) {
	if len(lines) == 0 {
		sb.WriteString(
			"• " + label +
				": <i>tidak ada active order record; exposure harus diverifikasi dari exchange</i>\n",
		)
		return
	}
	for i, item := range lines {
		if i >= 3 {
			sb.WriteString(
				"• " + label + ": <i>additional active orders omitted</i>\n",
			)
			break
		}
		sb.WriteString("• " + label + ": " + item + "\n")
	}
}

func (b *Bot) formatPositionLegOrders(
	sb *strings.Builder, pos domain.ActivePosition, records []json.RawMessage,
) {
	prefix := strings.ToLower(
		redishandler.CleanPairKey(pos.LocalExchange, pos.Symbol),
	)
	var spot, futures []string
	for _, raw := range records {
		record, ok := positionOrderRecordMatches(raw, prefix)
		if !ok {
			continue
		}
		market, line, ok := formatPositionOrderRecord(record)
		if !ok {
			continue
		}
		if market == "spot" {
			spot = append(spot, line)
		} else {
			futures = append(futures, line)
		}
	}
	sb.WriteString("<b>Active order status per leg:</b>\n")
	writePositionLegLines(sb, "Spot", spot)
	writePositionLegLines(sb, "Futures", futures)
	sb.WriteString("\n")
}

func (b *Bot) sendEmptyPositionsReply(ctx context.Context, chatID int64, replyToID int64, balances *domain.BalancesRecord, stats redishandler.TradeStats) {
	var noPos strings.Builder
	noPos.WriteString("💼 <b>Status Posisi Executor</b>\n\n")
	noPos.WriteString("<i>Saat ini tidak ada posisi arbitrase yang sedang aktif (semua posisi closed/idle).</i>\n\n")

	if balances != nil && balances.TotalEquity > 0 {
		noPos.WriteString("💰 <b>Saldo Bursa Executor (Simulasi $100/Exchange):</b>\n")
		noPos.WriteString(fmt.Sprintf("• Spot: <code>$%.2f USDT</code> (Tersedia: <code>$%.2f</code>)\n",
			balances.SpotTotal, balances.SpotAvailable))
		noPos.WriteString(fmt.Sprintf("• Futures: <code>$%.2f USDT</code> (Tersedia: <code>$%.2f</code>)\n",
			balances.FuturesTotal, balances.FuturesAvailable))
		noPos.WriteString(fmt.Sprintf("• Total Ekuitas: <b>$%.2f USDT</b>\n\n", balances.TotalEquity))
	}

	if stats.TotalClosed > 0 {
		noPos.WriteString("📊 <b>Statistik Realisasi Arbitrase:</b>\n")
		noPos.WriteString(fmt.Sprintf("• Berhasil Ditutup: <code>%d transaksi</code> (Menang: <code>%d</code>)\n",
			stats.TotalClosed, stats.WinCount))
		noPos.WriteString(fmt.Sprintf("• Total Realized Profit: <b>%+.4f USDT</b> (Rp %+.2f)\n",
			stats.TotalPnLUSDT, stats.TotalPnLIDR))
	}

	kb := &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: HistoryText, CallbackData: "history"},
				{Text: "🔄 Refresh", CallbackData: "positions"},
			},
			{
				{Text: MenuUtamaText, CallbackData: "menu"},
			},
		},
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, noPos.String(), kb)
}

type positionFloatingPnL struct {
	hasFloating   bool
	staleData     bool
	shortDiffUSDT float64
	spotDiffUSDT  float64
	floatingUSDT  float64
	floatingIDR   float64
}

func (b *Bot) handleHistoryCommand(ctx context.Context, chatID int64, replyToID int64) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"📜 <b>Riwayat Transaksi</b>\n\n⚠️ <i>Integrasi Redis tidak aktif atau belum terhubung.</i>",
			b.defaultKeyboard())
		return
	}

	history, err := rc.GetTradeHistory(ctx, 10)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			fmt.Sprintf("❌ <b>Gagal membaca histori:</b> %s", html.EscapeString(err.Error())),
			b.defaultKeyboard())
		return
	}

	stats, _ := rc.GetTradeStats(ctx)

	if len(history) == 0 {
		msg := "📜 <b>Riwayat Transaksi Berhasil Ditutup</b>\n\n<i>Belum ada posisi arbitrase yang selesai ditutup di database.</i>"
		if stats.TotalClosed > 0 {
			msg += fmt.Sprintf("\n\n📊 Total Ditutup: <code>%d</code> | Realized Profit: <b>%+.4f USDT</b>",
				stats.TotalClosed, stats.TotalPnLUSDT)
		}
		b.sendReplyWithMarkup(ctx, chatID, replyToID, msg, b.defaultKeyboard())
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📜 <b>Riwayat Transaksi Selesai (%d Terakhir)</b>\n\n", len(history)))

	for i, t := range history {
		modeBadge := "REAL"
		if t.IsMock {
			modeBadge = "SIMULASI"
		}
		closedTime := time.UnixMilli(t.ClosedAtMS).UTC().Format(TIME_FORMAT)
		sb.WriteString(fmt.Sprintf("<b>[%d] %s (%s)</b> - %s\n",
			i+1, html.EscapeString(strings.ToUpper(t.Symbol)), modeBadge, closedTime))
		sb.WriteString(fmt.Sprintf("• Kuantitas: <code>%.6f koin</code>\n", t.Qty))
		sb.WriteString(fmt.Sprintf("• Beli Spot: <code>Rp %.2f</code> | Jual: <code>Rp %.2f</code>\n",
			t.EntryPriceIDR, t.ExitPriceIDR))
		sb.WriteString(fmt.Sprintf("• Short: <code>$%.4f</code> | Tutup: <code>$%.4f USDT</code>\n",
			t.ShortEntryUSDT, t.ShortExitUSDT))
		sb.WriteString(fmt.Sprintf("• Funding Diterima: <code>+$%.4f USDT</code>\n", t.AccumulatedFundingUSDT))
		sb.WriteString(fmt.Sprintf("💰 <b>Net Profit:</b> <b>%+.4f USDT</b> (Rp %+.2f)\n",
			t.NetProfitUSDT, t.NetProfitIDR))
		sb.WriteString(fmt.Sprintf("📈 <b>Net ROI:</b> <b>%+.3f%%</b>\n\n", t.NetROIPct))
	}

	if stats.TotalClosed > 0 {
		sb.WriteString(separatorLine)
		sb.WriteString(fmt.Sprintf("📊 <b>Akumulasi Total (%d Transaksi):</b> <b>%+.4f USDT</b> (Rp %+.2f)\n",
			stats.TotalClosed, stats.TotalPnLUSDT, stats.TotalPnLIDR))
	}

	kb := &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "💼 Cek Posisi Aktif", CallbackData: "positions"},
				{Text: "🔄 Refresh Riwayat", CallbackData: "history"},
			},
			{
				{Text: "📱 Menu Utama", CallbackData: "menu"},
			},
		},
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID, sb.String(), kb)
}
