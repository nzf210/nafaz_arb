package telegram

import (
	"fmt"
	"html"
	"strings"
	"time"

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
/positions - <i>Lihat posisi aktif Executor & opsi tutup paksa</i>
/scan - <i>Jalankan scan pasar on-demand</i>
/latest - <i>Lihat hasil scan & peluang ROI tertinggi</i>
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
• <b>Bursa Spot:</b> Indodax, Tokocrypto
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

func (b *Bot) formatScanSummary(r domain.ScanResult) string {
	status := "SELESAI"
	icon := "✅"
	note := ""
	if !r.Complete {
		status = "TIDAK LENGKAP"
		icon = "⚠️"
		note = "Sebagian data scan tidak dapat diverifikasi; peluang dari scan ini tidak dikirim sebagai alert entry."
	}
	if isFXScanFailure(r) {
		status = "FX USDT/IDR TIDAK VALID"
		icon = "🚨"
		note = "Scan IDR dihentikan. Tidak ada hardcoded atau stale FX fallback yang digunakan."
	}

	return fmt.Sprintf(
		"%s <b>Ringkasan Scan NAFAZ — %s</b>\n\n"+
			"• Mode: <code>%s</code>\n"+
			"• Scan ID: <code>%s</code>\n"+
			"• Durasi: <code>%d ms</code>\n"+
			"• Total pasar: <b>%d</b>\n"+
			"• Pair cocok / aktif: <b>%d / %d</b>\n"+
			"• Eligible / excluded / failed: <b>%d / %d / %d</b>\n"+
			"• Opportunity tercatat: <b>%d</b>\n\n"+
			"%s",
		icon, status,
		html.EscapeString(r.Mode),
		html.EscapeString(r.ScanID),
		r.DurationMS,
		r.TotalMarkets,
		r.MatchedPairs, r.ActivePairs,
		r.EligiblePairs, r.ExcludedPairs, r.FailedPairs,
		len(r.Opportunities),
		html.EscapeString(note),
	)
}

func (b *Bot) formatScanResult(r domain.ScanResult, title string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📋 <b>%s</b>\n", html.EscapeString(title)))
	sb.WriteString(fmt.Sprintf("<i>Scan ID:</i> <code>%s</code>\n", html.EscapeString(r.ScanID)))
	if r.FrozenPairs > 0 {
		sb.WriteString(fmt.Sprintf("<i>Pasangan Cocok:</i> %d (Aktif: %d | Dibekukan: %d) | <i>Eligible:</i> %d\n\n",
			r.MatchedPairs, r.ActivePairs, r.FrozenPairs, r.EligiblePairs))
	} else {
		sb.WriteString(fmt.Sprintf("<i>Pasangan Cocok:</i> %d | <i>Eligible:</i> %d\n\n", r.MatchedPairs, r.EligiblePairs))
	}

	if len(r.Opportunities) == 0 {
		sb.WriteString("<i>Tidak ada peluang yang dievaluasi pada scan ini.</i>\n")
		return sb.String()
	}

	maxDisplay := 10
	displayOps := r.Opportunities
	if len(displayOps) > maxDisplay {
		displayOps = displayOps[:maxDisplay]
	}

	for i, op := range displayOps {
		sb.WriteString(formatScanOpportunity(i, op))
	}

	if len(r.Opportunities) > maxDisplay {
		sb.WriteString(fmt.Sprintf("<i>... menampilkan top %d dari %d total pasangan pasar (diurutkan berdasarkan Net ROI tertinggi)</i>\n",
			maxDisplay, len(r.Opportunities)))
	}

	return strings.TrimSpace(sb.String())
}

func formatScanOpportunity(i int, op domain.Opportunity) string {
	var sb strings.Builder
	statusEmoji := "🔴"
	if op.TradingEligible {
		statusEmoji = "🟢"
	} else if op.ProfitabilityStatus == "PROFITABLE_IN_RANGE" {
		statusEmoji = "🟡"
	}

	sb.WriteString(fmt.Sprintf("%d. %s <b>%s/%s</b> - <b>%s</b>\n",
		i+1, statusEmoji,
		html.EscapeString(op.BaseAsset), html.EscapeString(op.QuoteAsset),
		html.EscapeString(op.ProfitabilityStatus),
	))

	if op.ExecutionMode == "MAKER_TAKER" {
		sb.WriteString(fmt.Sprintf("   • Spot Limit (Maker): <code>%.4f</code> (%s) [Antrean: %.2f]\n", op.SpotEntryPrice, html.EscapeString(op.SpotSymbol), op.SpotQueueDepth))
		sb.WriteString(fmt.Sprintf("   • Perp Short: <code>%.4f</code> (%s)\n", op.PerpEntryPrice, html.EscapeString(op.PerpSymbol)))
		if op.MinAcceptablePerpPrice > 0 {
			sb.WriteString(fmt.Sprintf("   • Auto-Cancel Perp Floor: <code>%.4f</code> (TTL: %ds)\n", op.MinAcceptablePerpPrice, op.MaxOrderTTLSeconds))
		}
	} else {
		sb.WriteString(fmt.Sprintf("   • Spot Buy: <code>%.4f</code> (%s)\n", op.SpotEntryPrice, html.EscapeString(op.SpotSymbol)))
		sb.WriteString(fmt.Sprintf("   • Perp Short: <code>%.4f</code> (%s)\n", op.PerpEntryPrice, html.EscapeString(op.PerpSymbol)))
	}
	if op.IsSynthetic {
		sb.WriteString(fmt.Sprintf("   • 🔄 Sintetis IDR: Spot %s (Kurs: Rp %.0f/USDT)\n", html.EscapeString(op.OriginalSpotQuote), op.SyntheticFXRate))
	}
	if op.CapacityPct > 0 && op.CapacityPct < 99.9 {
		sb.WriteString(fmt.Sprintf("   • 📊 Kapasitas Book (Adaptive): %.1f%% ($%.2f)\n", op.CapacityPct, op.AllocatedCapital))
	}
	sb.WriteString(fmt.Sprintf("   • Net Profit: <b>%+.4f %s</b> (<b>%+.2f%% ROI</b>)\n",
		op.NetProfit, html.EscapeString(op.QuoteAsset), op.NetROIPct))
	sb.WriteString(fmt.Sprintf("   • Spread: %+.4f | Funding PnL: %+.4f %s | Fees: %.4f\n",
		op.SpreadPnL, op.FundingPnL, html.EscapeString(op.QuoteAsset), op.TradingFees))
	if op.CurrentFundingRate != 0 {
		sb.WriteString(fmt.Sprintf("   • Funding Rate: %+.4f%% (%+.6f)\n",
			op.CurrentFundingRate*100, op.CurrentFundingRate))
	}

	if len(op.ExclusionReasons) > 0 {
		sb.WriteString(fmt.Sprintf("   ⚠️ <i>Excluded: %s</i>\n",
			html.EscapeString(strings.Join(op.ExclusionReasons, ", "))))
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
			extraGuardrail = fmt.Sprintf("\n🛡️ <b>Auto-Cancel Perp Floor:</b> <code>%.4f</code> (TTL: %ds)", op.MinAcceptablePerpPrice, op.MaxOrderTTLSeconds)
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

	return fmt.Sprintf(`🚨 <b>PROFITABLE ARBITRAGE OPPORTUNITY%s</b> 🚨

Pair: <b>%s/%s</b>
Net ROI: <b>%+.2f%%</b> | Net Profit: <b>%+.4f %s</b>

📈 <b>%s:</b> <code>%.4f</code> (%s)
📉 <b>Short Perp:</b> <code>%.4f</code> (%s)%s
📦 <b>Quantity:</b> %.4f %s
💼 <b>Allocated Capital:</b> %.2f %s%s

⚡ <b>Rincian:</b>
• Spread PnL: %+.4f %s
• Funding PnL: %+.4f %s
• Funding Rate: %+.4f%% (%+.6f)
• Trading Fees: %.4f %s
• Max Slippage: %.4f %s

🕒 <i>Terdeteksi: %s</i>`,
		modeBadge,
		html.EscapeString(op.BaseAsset), html.EscapeString(op.QuoteAsset),
		op.NetROIPct, op.NetProfit, html.EscapeString(op.QuoteAsset),
		spotEntryLabel, op.SpotEntryPrice, html.EscapeString(op.SpotSymbol),
		op.PerpEntryPrice, html.EscapeString(op.PerpSymbol),
		extraGuardrail,
		op.Quantity, html.EscapeString(op.BaseAsset),
		op.AllocatedCapital, html.EscapeString(op.QuoteAsset), capacityInfo,
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
