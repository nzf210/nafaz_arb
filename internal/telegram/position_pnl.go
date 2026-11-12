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
