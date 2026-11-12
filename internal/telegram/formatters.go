package telegram

import (
	"fmt"
	"html"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func (b *Bot) formatMenu() string {
	return `🤖 <b>Menu Interaktif Scanner NAFAZ</b>

Gunakan tombol interaktif di bawah untuk navigasi cepat:`
}

func (b *Bot) formatHelp() string {
	return `🤖 <b>NAFAZ Spot-Perp Scanner Bot</b>

<b>Perintah Keyboard:</b>
/menu - <i>Buka menu tombol interaktif</i>
/orders - <i>Lihat active bot orders</i>
/recovery - <i>Status recovery setelah restart</i>
/orphans - <i>Order exchange belum teridentifikasi</i>
/cleanorphan [id] - <i>Force clean orphan/stuck order dari cache Redis &amp; DB</i>
/reconcile - <i>Mismatch reconciliation terbaru</i>
/risk - <i>Status risk halt</i>
/order [internal_order_id] - <i>Detail order dan fills terkait</i>
/position [position_id] - <i>Detail posisi</i>
/positions - <i>Lihat posisi aktif Executor</i>
/pending - <i>Lihat pending order Spot yang belum selesai</i>
/spotmax [cex] [jumlah] - <i>Atur maksimum open order/posisi Spot per CEX (default 2)</i>
/balance - <i>Cek saldo live seluruh aset Spot & Futures</i>
/scan - <i>Jalankan scan pasar on-demand</i>
/latest - <i>Lihat peluang dari 10 scan terakhir</i>
/pair [simbol] - <i>Cek rincian koin tertentu (misal: /pair BTC)</i>
/active - <i>Lihat peluang yang sedang aktif berkelanjutan</i>
/frozen - <i>Lihat daftar pasangan yang sedang dibekukan</i>
/status - <i>Lihat status scanner, mode & bursa aktif</i>
/metrics - <i>Lihat statistik durasi & histori arbitrase</i>
/mute [durasi] - <i>Bisukan alert sementara (misal: /mute 1h, /mute 30m)</i>
/unmute - <i>Aktifkan kembali notifikasi alert</i>
/killswitch [on|off] - <i>Hentikan atau lanjutkan semua entry baru Executor</i>
/help - <i>Tampilkan panduan ini</i>

<i>Tip: Anda juga dapat menggunakan tombol interaktif di bawah pesan untuk navigasi cepat.</i>`
}

func (b *Bot) formatStatus() string {
	latest := b.server.LatestScan()
	operational := b.server.OperationalStatus()
	lastScanStr := "Belum pernah"
	if !latest.CompletedAt.IsZero() {
		lastScanStr = latest.CompletedAt.Format(TIME_FORMAT)
	}
	lastSuccessStr := "Belum pernah"
	if operational.LastSuccessfulAt != nil {
		lastSuccessStr = operational.LastSuccessfulAt.Format(TIME_FORMAT)
	}
	lastError := "Tidak ada"
	if operational.LastError != "" {
		errStr := operational.LastError
		if len(errStr) > 300 {
			errStr = errStr[:300] + "... (dipotong)"
		}
		lastError = html.EscapeString(errStr)
	}
	exchangeMetrics := formatExchangeMetrics(latest.ExchangeMetrics)

	b.mu.Lock()
	mutedUntil := b.mutedUntil
	b.mu.Unlock()
	now := time.Now().UTC()
	alertState := "Aktif"
	if !mutedUntil.IsZero() && now.Before(mutedUntil) {
		alertState = fmt.Sprintf("Dibisukan hingga %s", mutedUntil.Format(TIME_FORMAT))
	}

	return fmt.Sprintf(`⚡ <b>Status NAFAZ Scanner</b>

• <b>Kondisi:</b> %s
• <b>Mode:</b> <code>%s</code>
• <b>Bursa Spot:</b> Tokocrypto, Indodax, MEXC
• <b>Bursa Perp:</b> OKX SWAP, MEXC Contract
• <b>Scan Interval:</b> %ds
• <b>Scan Terakhir:</b> %s
• <b>Scan Sukses Terakhir:</b> %s
• <b>Umur Data Sukses:</b> %s
• <b>Durasi Scan Terakhir:</b> %dms
• <b>Metrik Bursa:</b> %s
• <b>Scan Berjalan:</b> %t
• <b>Data Stale:</b> %t
• <b>Status Alert:</b> %s
• <b>Error Terakhir:</b> %s
• <b>Total Pasar:</b> %d
• <b>Pasangan Cocok:</b> %d (Aktif: %d, Dibekukan: %d)
• <b>Koin Orphan (Tanpa Future):</b> %d
• <b>Pasangan Eligible:</b> %d
• <b>Pasangan Excluded:</b> %d
• <b>Target Net ROI:</b> %.2f%% - %.2f%%
• <b>Perp Leverage:</b> %.0fx`,
		html.EscapeString(operational.Status),
		html.EscapeString(b.cfg.Mode),
		b.cfg.ScanIntervalSeconds,
		lastScanStr,
		lastSuccessStr,
		formatScanAge(operational.SuccessfulScanAgeSeconds),
		operational.LatestScanDurationMS,
		exchangeMetrics,
		operational.ScanInProgress,
		operational.Stale,
		html.EscapeString(alertState),
		lastError,
		latest.TotalMarkets,
		latest.MatchedPairs,
		latest.ActivePairs,
		latest.FrozenPairs,
		latest.OrphanSpotMarkets,
		latest.EligiblePairs,
		latest.ExcludedPairs,
		b.cfg.MinNetROIPct,
		b.cfg.MaxNetROIPct,
		b.cfg.PerpLeverage,
	)
}

func formatScanAge(age *int64) string {
	if age == nil {
		return "Belum tersedia"
	}
	return fmt.Sprintf("%ds", *age)
}

func formatExchangeMetrics(metrics []domain.ExchangeScanMetrics) string {
	if len(metrics) == 0 {
		return "Belum tersedia"
	}
	parts := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		parts = append(parts, fmt.Sprintf("%s %d req/%d err/%dus",
			html.EscapeString(metric.Exchange), metric.Requests, metric.Errors, metric.DurationMicros))
	}
	res := strings.Join(parts, "; ")
	if len(res) > 300 {
		res = res[:300] + "..."
	}
	return res
}

func (b *Bot) formatMetrics() string {
	store := b.server.Store()
	metrics := store.GetMetrics(time.Time{}, time.Time{}, time.Now().UTC())

	return fmt.Sprintf(`📊 <b>Metrik Histori Peluang Arbitrase</b>

• <b>Peluang Aktif:</b> %d
• <b>Peluang Baru:</b> %d
• <b>Peluang Berakhir:</b> %d
• <b>Total Observasi Scan:</b> %d
• <b>Rata-rata Durasi Teramati:</b> %.1fs
• <b>Maksimum Durasi Teramati:</b> %ds`,
		metrics.ActiveOpportunities,
		metrics.NewOpportunities,
		metrics.EndedOpportunities,
		metrics.TotalScanObservations,
		metrics.AverageObservedDurationSeconds,
		metrics.MaxObservedDurationSeconds,
	)
}

func formatPrice(p float64) string {
	if p == 0 || math.IsNaN(p) || math.IsInf(p, 0) {
		return "0"
	}
	abs := math.Abs(p)
	var decimals int
	switch {
	case abs >= 1000:
		return strconv.FormatFloat(p, 'f', 2, 64)
	case abs >= 1:
		return strconv.FormatFloat(p, 'f', 4, 64)
	case abs >= 0.01:
		decimals = 5
	case abs >= 0.0001:
		decimals = 6
	case abs >= 0.000001:
		decimals = 8
	default:
		decimals = 10
	}
	s := strconv.FormatFloat(p, 'f', decimals, 64)
	dot := strings.IndexByte(s, '.')
	if dot == -1 {
		return s
	}
	intPart, frac := s[:dot], s[dot+1:]
	for len(frac) > 4 && frac[len(frac)-1] == '0' {
		frac = frac[:len(frac)-1]
	}
	return intPart + "." + frac
}

func formatCompactNumber(v float64) string {
	abs := math.Abs(v)
	switch {
	case abs >= 1_000_000:
		return fmt.Sprintf("%.2fM", v/1_000_000)
	case abs >= 10_000:
		return fmt.Sprintf("%.2fK", v/1_000)
	case abs >= 100:
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}

func (b *Bot) formatScanResult(r domain.ScanResult, title string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📡 <b>%s</b>\n", html.EscapeString(title)))
	sb.WriteString(fmt.Sprintf("🆔 <code>%s</code> • 🔗 Cocok: <b>%d</b>", html.EscapeString(r.ScanID), r.MatchedPairs))
	if r.FrozenPairs > 0 {
		sb.WriteString(fmt.Sprintf(" (⚡%d | ❄️%d)", r.ActivePairs, r.FrozenPairs))
	}
	sb.WriteString(fmt.Sprintf(" | ✅ Eligible: <b>%d</b>\n\n", r.EligiblePairs))

	if len(r.Opportunities) == 0 {
		sb.WriteString("<i>Tidak ada peluang yang dievaluasi pada scan ini.</i>\n")
		return sb.String()
	}

	maxDisplay := 10
	displayOps := r.Opportunities
	if len(displayOps) > maxDisplay {
		displayOps = displayOps[:maxDisplay]
	}

	const maxTelegramHTMLRunes = 3900
	shown := 0
	for i, op := range displayOps {
		item := formatScanOpportunity(i, op)
		if i > 0 && utf8.RuneCountInString(sb.String())+utf8.RuneCountInString(item)+120 > maxTelegramHTMLRunes {
			break
		}
		sb.WriteString(item)
		shown++
	}
	if len(r.Opportunities) > shown {
		sb.WriteString(fmt.Sprintf("<i>📌 Menampilkan top %d dari %d pasangan (urut Net ROI tertinggi)</i>\n",
			shown, len(r.Opportunities)))
	}
	return strings.TrimSpace(sb.String())
}

func formatScanOpportunity(i int, op domain.Opportunity) string {
	var sb strings.Builder
	statusEmoji := "🔴"
	switch {
	case op.TradingEligible:
		statusEmoji = "🟢"
	case op.ProfitabilityStatus == "PROFITABLE_IN_RANGE":
		statusEmoji = "🟡"
	case op.ProfitabilityStatus == "BELOW_MIN_TARGET":
		statusEmoji = "🟠"
	}

	sb.WriteString(fmt.Sprintf("%d. %s <b>%s/%s</b> · <b>%s</b>\n",
		i+1, statusEmoji,
		html.EscapeString(op.BaseAsset), html.EscapeString(op.QuoteAsset),
		html.EscapeString(op.ProfitabilityStatus),
	))

	if op.LocalExchange != "" || op.GlobalExchange != "" || op.IsSynthetic {
		sb.WriteString("   🏦 ")
		if op.LocalExchange != "" || op.GlobalExchange != "" {
			sb.WriteString(fmt.Sprintf("<b>%s</b> ➔ <b>%s</b>",
				html.EscapeString(strings.ToUpper(op.LocalExchange)),
				html.EscapeString(strings.ToUpper(op.GlobalExchange))))
			if op.ExecutionMode == "MAKER_TAKER" {
				sb.WriteString(" (Maker)")
			}
		}
		if op.IsSynthetic {
			if op.LocalExchange != "" || op.GlobalExchange != "" {
				sb.WriteString(" • ")
			}
			sb.WriteString(fmt.Sprintf("🔄 %s Rp%.0f", html.EscapeString(op.OriginalSpotQuote), op.SyntheticFXRate))
		}
		sb.WriteString("\n")
	}

	if op.ExecutionMode == "MAKER_TAKER" {
		sb.WriteString(fmt.Sprintf("   📈 Spot: <code>%s</code> (%s · Q:%s)\n",
			formatPrice(op.SpotEntryPrice), html.EscapeString(op.SpotSymbol), formatCompactNumber(op.SpotQueueDepth)))
		sb.WriteString(fmt.Sprintf("   📉 Short: <code>%s</code> (%s)",
			formatPrice(op.PerpEntryPrice), html.EscapeString(op.PerpSymbol)))
		if op.MinAcceptablePerpPrice > 0 {
			sb.WriteString(fmt.Sprintf(" • 🛡️ <code>%s</code> (%ds)",
				formatPrice(op.MinAcceptablePerpPrice), op.MaxOrderTTLSeconds))
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString(fmt.Sprintf("   📈 Spot: <code>%s</code> (%s)\n",
			formatPrice(op.SpotEntryPrice), html.EscapeString(op.SpotSymbol)))
		sb.WriteString(fmt.Sprintf("   📉 Short: <code>%s</code> (%s)\n",
			formatPrice(op.PerpEntryPrice), html.EscapeString(op.PerpSymbol)))
	}

	if op.CapacityPct > 0 && op.CapacityPct < 99.9 {
		sb.WriteString(fmt.Sprintf("   📊 Book: %.1f%% ($%.2f)\n", op.CapacityPct, op.AllocatedCapital))
	}

	sb.WriteString(fmt.Sprintf("   💰 Net: <b>%+.4f %s</b> (<b>%+.2f%%</b>) | Spr: %+.4f | Fund: %+.4f",
		op.NetProfit, html.EscapeString(op.QuoteAsset), op.NetROIPct, op.SpreadPnL, op.FundingPnL))
	if op.CurrentFundingRate != 0 {
		sb.WriteString(fmt.Sprintf(" (%+.4f%%)", op.CurrentFundingRate*100))
	}
	sb.WriteString(fmt.Sprintf(" | Fee: %.4f\n", op.TradingFees))

	var extraReasons []string
	for _, reason := range op.ExclusionReasons {
		if reason != "" && reason != op.ProfitabilityStatus {
			extraReasons = append(extraReasons, reason)
		}
	}
	if len(extraReasons) > 0 {
		sb.WriteString(fmt.Sprintf("   ⛔ <i>%s</i>\n",
			html.EscapeString(strings.Join(extraReasons, ", "))))
	}
	sb.WriteString("\n")
	return sb.String()
}

func (b *Bot) formatOpportunityAlert(op domain.Opportunity) string {
	modeBadge := ""
	spotEntryLabel := "Buy Spot"
	extraGuardrail := ""
	if op.ExecutionMode == "MAKER_TAKER" {
		modeBadge = " [MAKER-TAKER]"
		spotEntryLabel = "Limit Buy Spot (Maker)"
		if op.MinAcceptablePerpPrice > 0 {
			extraGuardrail = fmt.Sprintf("\n🛡️ <b>Auto-Cancel Perp Floor:</b> <code>%s</code> (TTL: %ds)", formatPrice(op.MinAcceptablePerpPrice), op.MaxOrderTTLSeconds)
		}
	}
	if op.IsSynthetic {
		modeBadge += " [SYNTH-IDR]"
		extraGuardrail += fmt.Sprintf("\n🔄 <b>Sintetis IDR:</b> Spot %s @ Rp %.0f/USDT", html.EscapeString(op.OriginalSpotQuote), op.SyntheticFXRate)
	}
	capacityInfo := ""
	if op.CapacityPct > 0 && op.CapacityPct < 99.9 {
		capacityInfo = fmt.Sprintf(" (Kapasitas: %.1f%%)", op.CapacityPct)
	}
	executionInfo := ""
	switch op.ExecutionSignalStatus {
	case "published":
		executionInfo = "\n🟢 <b>Signal Executor:</b> berhasil dipublikasikan ke Redis"
	case "disabled":
		executionInfo = "\n⚪ <b>Signal Executor:</b> dinonaktifkan pada konfigurasi scanner"
	case "redis_unavailable":
		executionInfo = "\n🔴 <b>Signal Executor:</b> Redis tidak tersedia; posisi tidak dikirim"
	case "live_price_publish_failed":
		executionInfo = "\n🔴 <b>Signal Executor:</b> gagal memperbarui live price; posisi tidak dikirim"
	case "publish_failed":
		executionInfo = "\n🔴 <b>Signal Executor:</b> gagal dipublikasikan; periksa error scan"
	}

	return fmt.Sprintf(`🚨 <b>PROFITABLE ARBITRAGE OPPORTUNITY%s</b> 🚨

Pair: <b>%s/%s</b>
🏦 <b>Spot CEX:</b> %s
🏦 <b>Futures CEX:</b> %s
Net ROI: <b>%+.2f%%</b> | Net Profit: <b>%+.4f %s</b>

📈 <b>%s:</b> <code>%s</code> (%s)
📉 <b>Short Perp:</b> <code>%s</code> (%s)%s
📦 <b>Quantity:</b> %.4f %s
💼 <b>Allocated Capital:</b> %.2f %s%s%s

⚡ <b>Rincian:</b>
• Spread PnL: %+.4f %s
• Funding PnL: %+.4f %s
• Funding Rate: %+.4f%% (%+.6f)
• Trading Fees: %.4f %s
• Max Slippage: %.4f %s

🕒 <i>Terdeteksi: %s</i>`,
		modeBadge,
		html.EscapeString(op.BaseAsset), html.EscapeString(op.QuoteAsset),
		html.EscapeString(strings.ToUpper(op.LocalExchange)), html.EscapeString(strings.ToUpper(op.GlobalExchange)),
		op.NetROIPct, op.NetProfit, html.EscapeString(op.QuoteAsset),
		spotEntryLabel, formatPrice(op.SpotEntryPrice), html.EscapeString(op.SpotSymbol),
		formatPrice(op.PerpEntryPrice), html.EscapeString(op.PerpSymbol),
		extraGuardrail,
		op.Quantity, html.EscapeString(op.BaseAsset),
		op.AllocatedCapital, html.EscapeString(op.QuoteAsset), capacityInfo, executionInfo,
		op.SpreadPnL, html.EscapeString(op.QuoteAsset),
		op.FundingPnL, html.EscapeString(op.QuoteAsset),
		op.CurrentFundingRate*100, op.CurrentFundingRate,
		op.TradingFees, html.EscapeString(op.QuoteAsset),
		op.Slippage, html.EscapeString(op.QuoteAsset),
		op.LastScannedAt.Format(TIME_FORMAT),
	)
}

func (b *Bot) formatFrozenPairs() string {
	frozen := b.server.Scanner().FrozenPairs()
	if len(frozen) == 0 {
		return "❄️ <b>Daftar Pasangan Dibekukan</b>\n\n<i>Tidak ada pasangan yang dibekukan saat ini. Semua pasangan aktif dipindai.</i>"
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("❄️ <b>Daftar Pasangan Dibekukan (%d)</b>\n\n", len(frozen)))
	for i, f := range frozen {
		remaining := time.Until(f.FrozenUntil).Round(time.Minute)
		if remaining < 0 {
			remaining = 0
		}
		sb.WriteString(fmt.Sprintf("%d. <code>%s</code>\n   • Dibekukan hingga: %s (sisa ~%s)\n",
			i+1, html.EscapeString(f.PairKey), f.FrozenUntil.Format(TIME_FORMAT), remaining.String()))
	}
	return strings.TrimSpace(sb.String())
}

func (b *Bot) formatActiveOpportunities() string {
	store := b.server.Store()
	items := store.List(time.Time{}, time.Time{}, "ACTIVE")
	if len(items) == 0 {
		return "🔥 <b>Peluang Aktif Berkelanjutan</b>\n\n<i>Saat ini belum ada peluang arbitrase berstatus ACTIVE yang tercatat di histori.</i>"
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🔥 <b>Peluang Aktif Berkelanjutan (%d)</b>\n\n", len(items)))
	now := time.Now().UTC()
	for i, e := range items {
		dur := now.Sub(e.FirstSeenAt).Round(time.Second)
		sb.WriteString(fmt.Sprintf("%d. <b>%s/%s</b> (<code>%s</code>)\n",
			i+1, html.EscapeString(e.BaseAsset), html.EscapeString(e.QuoteAsset), html.EscapeString(e.OpportunityKey)))
		sb.WriteString(fmt.Sprintf("   • Pertama teramati: %s (aktif %s)\n",
			e.FirstSeenAt.Format(TIME_FORMAT), dur.String()))
		sb.WriteString(fmt.Sprintf("   • Observasi scan: %d kali berturut-turut\n\n", e.ScanObservationCount))
	}
	return strings.TrimSpace(sb.String())
}

func (b *Bot) formatPairDetail(symbol string) string {
	latest := b.server.LatestScan()
	if latest.ScanID == "" {
		return "ℹ️ <i>Belum ada data scan. Jalankan /scan terlebih dahulu.</i>"
	}
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	sym = strings.TrimPrefix(sym, "/")
	var matches []domain.Opportunity
	for _, op := range latest.Opportunities {
		if strings.EqualFold(op.BaseAsset, sym) ||
			strings.EqualFold(op.SpotSymbol, sym) ||
			strings.EqualFold(op.PerpSymbol, sym) ||
			strings.Contains(strings.ToUpper(op.OpportunityKey), sym) {
			matches = append(matches, op)
		}
	}
	if len(matches) == 0 {
		return fmt.Sprintf("🔍 <b>Hasil Pencarian Pasangan:</b> <code>%s</code>\n\n<i>Tidak ditemukan peluang yang cocok dengan simbol tersebut pada hasil scan terakhir.</i>", html.EscapeString(sym))
	}
	return b.formatScanResult(domain.ScanResult{
		ScanID:        latest.ScanID,
		Opportunities: matches,
		MatchedPairs:  len(matches),
		EligiblePairs: countEligible(matches),
	}, fmt.Sprintf("Detail Pasangan: %s", sym))
}

func countEligible(ops []domain.Opportunity) int {
	c := 0
	for _, op := range ops {
		if op.TradingEligible {
			c++
		}
	}
	return c
}
