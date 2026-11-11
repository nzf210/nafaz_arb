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
func (b *Bot) handlePendingOrdersCommand(ctx context.Context, chatID, replyToID int64) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "🕒 <b>Pending Orders</b>\n\n❌ Redis belum terhubung; status order tidak dapat dibaca.", b.defaultKeyboard())
		return
	}
	raw, found, err := rc.GetExecutorStatus(ctx)
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ <b>Gagal membaca status executor:</b> "+html.EscapeString(err.Error()), b.defaultKeyboard())
		return
	}
	if !found {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "🕒 <b>Pending Orders</b>\n\n⚠️ Executor offline atau status terakhir sudah kedaluwarsa. Tidak ada data pending yang dapat diverifikasi.", b.defaultKeyboard())
		return
	}
	var snapshot executorPendingSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ <b>Format status executor tidak valid:</b> "+html.EscapeString(err.Error()), b.defaultKeyboard())
		return
	}
	if len(snapshot.PendingOrders) == 0 && snapshot.PendingEntries+snapshot.PendingExits > 0 {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "⚠️ <b>Executor belum mengirim rincian pending order.</b>\n\nPerbarui image executor agar menu ini dapat menampilkan ID order, CEX, harga, dan kuantitas. Jumlah tercatat: "+fmt.Sprintf("%d entry / %d exit", snapshot.PendingEntries, snapshot.PendingExits), b.defaultKeyboard())
		return
	}
	orders := append([]pendingOrderSnapshot(nil), snapshot.PendingOrders...)
	sort.SliceStable(orders, func(i, j int) bool {
		left := strings.ToLower(orders[i].LocalExchange + "|" + orders[i].Symbol)
		right := strings.ToLower(orders[j].LocalExchange + "|" + orders[j].Symbol)
		return left < right
	})
	var out strings.Builder
	out.WriteString(fmt.Sprintf("🕒 <b>PENDING ORDERS (%d)</b> — grouped by exchange / pair\n", len(orders)))
	if snapshot.MockMode {
		out.WriteString("⚠️ <b>MODE SIMULASI</b>\n")
	} else {
		out.WriteString("🟢 <b>EXECUTOR LIVE</b>\n")
	}
	if snapshot.TimestampMS > 0 {
		out.WriteString("🕓 Snapshot: "+time.UnixMilli(snapshot.TimestampMS).UTC().Format("2006-01-02 15:04:05 UTC")+"\n")
	}
	out.WriteString("\n")
	if len(orders) == 0 {
		out.WriteString("<i>Tidak ada order Spot yang masih pending.</i>\n")
	} else {
		lastGroup := ""
		for i, order := range orders {
			if i >= 20 {
				out.WriteString("\n<i>Daftar dibatasi hingga 20 order.</i>")
				break
			}
			group := strings.ToUpper(order.LocalExchange) + " / " + strings.ToUpper(order.Symbol)
			if group != lastGroup { out.WriteString("<b>🏦 " + html.EscapeString(group) + "</b>\n"); lastGroup = group }
			status := order.Status
			if status == "" { status = "UNKNOWN" }
			out.WriteString(fmt.Sprintf("<b>[%d] %s — %s</b>\n", i+1, html.EscapeString(strings.ToUpper(order.Symbol)), html.EscapeString(status)))
			out.WriteString(fmt.Sprintf("• Leg: <code>%s</code> | Order ID: <code>%s</code>\n", html.EscapeString(order.Leg), html.EscapeString(order.OrderID)))
			out.WriteString(fmt.Sprintf("• CEX Spot: <b>%s</b> | CEX Futures: <b>%s</b>\n", html.EscapeString(strings.ToUpper(order.LocalExchange)), html.EscapeString(strings.ToUpper(order.GlobalExchange))))
			out.WriteString(fmt.Sprintf("• Harga: <code>%.10g %s</code> | Qty: <code>%.8g</code>\n", order.Price, html.EscapeString(order.QuoteAsset), order.Qty))
			out.WriteString(fmt.Sprintf("• Filled: <code>%.8g</code> | Sisa: <code>%.8g</code>\n", order.Filled, order.Remaining))
			if order.CreatedAtMS > 0 {
				age := time.Since(time.UnixMilli(order.CreatedAtMS))
				if age < 0 { age = 0 }
				out.WriteString(fmt.Sprintf("• Dibuka: %s (umur %s)\n", time.UnixMilli(order.CreatedAtMS).UTC().Format(TIME_FORMAT), age.Round(time.Second)))
			} else {
				out.WriteString("• Waktu pembukaan: <i>tidak tersedia</i>\n")
			}
			out.WriteString("\n")
		}
	}
	out.WriteString("<i>Read-only: menu ini tidak membatalkan atau mengubah order. Status berasal dari order yang dikelola executor NAFAZ.</i>")
	b.sendReplyWithMarkup(ctx, chatID, replyToID, out.String(), &InlineKeyboardMarkup{InlineKeyboard: [][]InlineKeyboardButton{
		{{Text: "🔄 Refresh Pending", CallbackData: "pending_orders"}, {Text: "💼 Posisi Aktif", CallbackData: "positions"}},
		{{Text: MenuUtamaText, CallbackData: "menu"}},
	}})
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
	for i, pos := range positions {
		b.formatPositionBlock(&sb, i+1, pos, rc, ctx)
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

func calculatePositionPnL(ctx context.Context, pos domain.ActivePosition, rc *redishandler.Client) positionFloatingPnL {
	const staleThresholdMS = 120000 // must match executor LIVE_PRICE_STALE_MS

	if pos.CurrentPerpAskUSDT > 0 || pos.FuturesUnrealizedPnLUSDT != 0 {
		if pos.UpdatedAt > 0 && time.Now().UnixMilli()-pos.UpdatedAt > staleThresholdMS {
			return positionFloatingPnL{staleData: true}
		}
		shortDiffUSDT := pos.FuturesUnrealizedPnLUSDT
		floatingUSDT := pos.NetUnrealizedPnLUSDT
		floatingIDR := pos.NetUnrealizedPnLIDR
		spotDiffUSDT := floatingUSDT - shortDiffUSDT - pos.AccumulatedFundingUSDT
		return positionFloatingPnL{
			hasFloating:   true,
			shortDiffUSDT: shortDiffUSDT,
			spotDiffUSDT:  spotDiffUSDT,
			floatingUSDT:  floatingUSDT,
			floatingIDR:   floatingIDR,
		}
	}

	live, errLive := rc.GetLivePrice(ctx, pos.LocalExchange, pos.Symbol)
	if errLive == nil && live != nil && live.USDTIDRRate > 0 && live.TimestampMS > 0 && time.Now().UnixMilli()-live.TimestampMS <= staleThresholdMS {
		spotDiffIDR := (live.LocalBestBid - pos.EntryPriceIDR) * pos.Qty
		spotDiffUSDT := spotDiffIDR / live.USDTIDRRate
		shortDiffUSDT := (pos.ShortEntryUSDT - live.GlobalBestAskUSDT) * pos.Qty
		floatingUSDT := spotDiffUSDT + shortDiffUSDT + pos.AccumulatedFundingUSDT
		floatingIDR := floatingUSDT * live.USDTIDRRate
		return positionFloatingPnL{
			hasFloating:   true,
			shortDiffUSDT: shortDiffUSDT,
			spotDiffUSDT:  spotDiffUSDT,
			floatingUSDT:  floatingUSDT,
			floatingIDR:   floatingIDR,
		}
	}

	return positionFloatingPnL{staleData: true}
}

func (b *Bot) formatPositionBlock(sb *strings.Builder, idx int, pos domain.ActivePosition, rc *redishandler.Client, ctx context.Context) {
	sb.WriteString(fmt.Sprintf("<b>[%d] %s</b>\n", idx, html.EscapeString(strings.ToUpper(pos.Symbol))))
	sb.WriteString(fmt.Sprintf("• Bursa: <code>%s (Spot)</code> vs <code>%s (Futures 1x)</code>\n",
		html.EscapeString(strings.ToUpper(pos.LocalExchange)),
		html.EscapeString(strings.ToUpper(pos.GlobalExchange))))
	sb.WriteString(fmt.Sprintf("• Kuantitas: <code>%.6f koin</code>\n", pos.Qty))
	sb.WriteString(fmt.Sprintf("• Entry Spot: <code>Rp %.2f</code>\n", pos.EntryPriceIDR))
	sb.WriteString(fmt.Sprintf("• Entry Short: <code>$%.4f USDT</code>\n", pos.ShortEntryUSDT))
	sb.WriteString(fmt.Sprintf("• Akumulasi Funding: <code>+$%.4f USDT</code>\n", pos.AccumulatedFundingUSDT))

	pnl := calculatePositionPnL(ctx, pos, rc)
	if pnl.staleData {
		sb.WriteString("• ⚠️ <i>Harga live tidak tersedia/stale — floating PnL disembunyikan (jangan diandalkan).</i>\n")
	}

	if pnl.hasFloating {
		futuresTag := "PROFIT"
		if pnl.shortDiffUSDT < 0 {
			futuresTag = "MINUS"
		}
		spotTag := "PROFIT"
		if pnl.spotDiffUSDT < 0 {
			spotTag = "MINUS"
		}
		sb.WriteString(fmt.Sprintf("• PnL Short Futures: <b>%+.4f USDT (%s)</b>\n", pnl.shortDiffUSDT, futuresTag))
		sb.WriteString(fmt.Sprintf("• PnL Spot Lokal: <b>%+.4f USDT (%s)</b>\n", pnl.spotDiffUSDT, spotTag))
		sb.WriteString(fmt.Sprintf("• Estimasi Floating PnL: <b>%+.4f USDT</b> (Rp %+.2f)\n", pnl.floatingUSDT, pnl.floatingIDR))
	}

	statusStr := "UNKNOWN — status kedua leg belum dipublikasikan secara independen"
	if pos.State == "HEDGED_ACTIVE" || pos.State == "HEDGED" {
		statusStr = "HEDGED_ACTIVE (state agregat executor; bukan verifikasi live kedua exchange)"
	} else if pos.State == "RECONCILIATION_REQUIRED" {
		statusStr = "RECONCILIATION_REQUIRED"
	} else if pos.State != "" {
		statusStr = pos.State
	}
	sb.WriteString(fmt.Sprintf("• Status agregat: <code>%s</code>\n", html.EscapeString(statusStr)))
	sb.WriteString("• Status leg Spot: <i>tidak tersedia pada schema posisi Redis</i>\n• Status leg Futures: <i>tidak tersedia pada schema posisi Redis</i>\n\n")
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
