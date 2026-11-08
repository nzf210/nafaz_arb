package telegram

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

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
		sb.WriteString(fmt.Sprintf("• Total Net PnL (fee estimasi): <b>%+.4f USDT</b> (Rp %+.2f)\n• PnL belum terverifikasi: <code>%d transaksi</code>\n\n",
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
		noPos.WriteString("📊 <b>Statistik Net PnL Estimasi Arbitrase:</b>\n")
		noPos.WriteString(fmt.Sprintf("• Berhasil Ditutup: <code>%d transaksi</code> (Menang: <code>%d</code>)\n",
			stats.TotalClosed, stats.WinCount))
		noPos.WriteString(fmt.Sprintf("• Total Net PnL (fee estimasi): <b>%+.4f USDT</b> (Rp %+.2f)\n• PnL belum terverifikasi: <code>%d transaksi</code>\n",
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

	statusStr := "HEDGED (Delta Neutral 100%)"
	if pos.State == "RECONCILIATION_REQUIRED" {
		statusStr = "⚠️ RECONCILIATION REQUIRED (Unhedged / Leg Failed)"
	}
	sb.WriteString(fmt.Sprintf("• Status: <code>%s</code>\n\n", statusStr))
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
		if strings.EqualFold(t.PnlStatus, "unverified") {
			sb.WriteString("⚠️ <b>PnL belum terverifikasi</b> — periksa FX dan fee aktual di exchange.\n\n")
		} else {
			estimatedNet := t.EstimatedNetProfitUSDT
			if estimatedNet == 0 {
				estimatedNet = t.NetProfitUSDT
			}
			sb.WriteString(fmt.Sprintf("• Gross PnL: <code>%+.4f USDT</code> | Estimasi fee: <code>%.4f USDT</code>\n",
				t.GrossProfitUSDT, t.EstimatedFeesUSDT))
			sb.WriteString(fmt.Sprintf("💰 <b>Net PnL (estimasi):</b> <b>%+.4f USDT</b> (Rp %+.2f)\n",
				estimatedNet, t.NetProfitIDR))
			sb.WriteString(fmt.Sprintf("📈 <b>Net ROI (estimasi):</b> <b>%+.3f%%</b>\n\n", t.NetROIPct))
		}
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
