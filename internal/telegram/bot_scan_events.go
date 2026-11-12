package telegram

import (
	"context"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func (b *Bot) formatScanRejectionSummary(r domain.ScanResult) string {
	reasonCounts := make(map[string]int)
	for _, op := range r.Opportunities {
		if op.TradingEligible { continue }
		if len(op.ExclusionReasons) == 0 && op.ProfitabilityStatus != "" {
			reasonCounts[op.ProfitabilityStatus]++
			continue
		}
		for _, reason := range op.ExclusionReasons {
			if reason != "" { reasonCounts[reason]++ }
		}
	}
	type reasonCount struct { reason string; count int }
	reasons := make([]reasonCount, 0, len(reasonCounts))
	for reason, count := range reasonCounts {
		reasons = append(reasons, reasonCount{reason: reason, count: count})
	}
	sort.Slice(reasons, func(i, j int) bool {
		if reasons[i].count == reasons[j].count { return reasons[i].reason < reasons[j].reason }
		return reasons[i].count > reasons[j].count
	})
	var message strings.Builder
	message.WriteString("⚠️ <b>SCAN SELESAI — TIDAK ADA ENTRY YANG DILAKUKAN</b>\n\n")
	message.WriteString(fmt.Sprintf("• Pasar: <code>%d</code>\n• Pasangan cocok: <code>%d</code>\n• Pasangan aktif dievaluasi: <code>%d</code>\n• Eligible: <code>%d</code>\n• Excluded: <code>%d</code>\n• Gagal evaluasi: <code>%d</code>\n",
		r.TotalMarkets, r.MatchedPairs, r.ActivePairs, r.EligiblePairs, r.ExcludedPairs, r.FailedPairs))
	if len(reasons) > 0 {
		message.WriteString("\n<b>Alasan utama tidak eligible:</b>\n")
		limit := len(reasons); if limit > 6 { limit = 6 }
		for _, item := range reasons[:limit] {
			message.WriteString(fmt.Sprintf("• <code>%s</code>: <b>%d</b>\n", html.EscapeString(item.reason), item.count))
		}
	}
	if len(r.Errors) > 0 {
		message.WriteString("\n<b>Kesalahan evaluasi terbaru:</b>\n")
		limit := len(r.Errors); if limit > 3 { limit = 3 }
		for _, scanErr := range r.Errors[:limit] {
			message.WriteString("• " + html.EscapeString(scanErr) + "\n")
		}
	}
	message.WriteString("\n<i>Entry tidak dipaksakan. Periksa alasan di atas; filter profit dan proteksi risiko tetap aktif.</i>")
	return message.String()
}

func (b *Bot) onScanCompleted(_ context.Context, r domain.ScanResult) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UTC()
	if !b.mutedUntil.IsZero() && now.Before(b.mutedUntil) {
		return
	}
	cooldown := time.Duration(b.cfg.Telegram.AlertCooldownMinutes) * time.Minute
	if cooldown <= 0 {
		cooldown = 15 * time.Minute
	}
	hasEligibleOpportunity := r.EligiblePairs > 0
	for _, op := range r.Opportunities {
		if op.TradingEligible {
			hasEligibleOpportunity = true
			break
		}
	}
	if !hasEligibleOpportunity && (r.MatchedPairs > 0 || len(r.Opportunities) > 0 || r.FailedPairs > 0) {
		const rejectionAlertKey = "scan-rejection-summary"
		lastAlert, alreadyAlerted := b.alertedKeys[rejectionAlertKey]
		if !alreadyAlerted || now.Sub(lastAlert) >= cooldown {
			b.alertedKeys[rejectionAlertKey] = now
			message := b.formatScanRejectionSummary(r)
			go b.sendReplyWithMarkup(context.Background(), b.cfg.Telegram.ChatID, 0, message, b.defaultKeyboard())
		}
	}
	for _, op := range r.Opportunities {
		if !op.TradingEligible ||
			op.NetROIPct < b.cfg.Telegram.AlertMinNetROIPct ||
			op.NetProfit < b.cfg.Telegram.AlertMinNetProfitQuote {
			continue
		}

		lastAlert, exists := b.alertedKeys[op.OpportunityKey]
		if !exists || now.Sub(lastAlert) >= cooldown {
			b.alertedKeys[op.OpportunityKey] = now
			alertMsg := b.formatOpportunityAlert(op)
			go b.sendReplyWithMarkup(context.Background(), b.cfg.Telegram.ChatID, 0, alertMsg, b.alertKeyboard(op.BaseAsset))
		}
	}
}
