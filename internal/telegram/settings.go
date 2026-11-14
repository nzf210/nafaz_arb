package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"os"
	"strconv"
	"strings"
)

func (b *Bot) formatNotificationSettings() string {
	b.mu.Lock()
	tc := b.cfg.Telegram
	b.mu.Unlock()

	statusIcon := func(enabled bool) string {
		if enabled {
			return "🟢 <b>AKTIF</b>"
		}
		return "🔴 <i>NONAKTIF</i>"
	}

	return fmt.Sprintf(`🔔 <b>Pengaturan Filter Notifikasi</b>

• <b>Peluang Baru (Opportunities):</b> %s
• <b>Alasan Scan Ditolak (Rejection):</b> %s
• <b>Order Pending Terisi (Fill / Open):</b> %s
• <b>Posisi Selesai Ditutup (Closed Spot-Perp):</b> %s
• <b>Alert Gagal / Error:</b> %s

<i>Perintah Cepat:</i>
• <code>/notif trades</code> — Hanya order fulfilled & closed
• <code>/notif all</code> — Nyalakan semua notifikasi
• <code>/notif pause</code> — Pause semua notifikasi
• <code>/notif toggle &lt;opps|rejection|fills|closes|errors&gt;</code>`,
		statusIcon(tc.NotifyOpportunitiesEnabled()),
		statusIcon(tc.NotifyRejectionsEnabled()),
		statusIcon(tc.NotifyFillsEnabled()),
		statusIcon(tc.NotifyClosesEnabled()),
		statusIcon(tc.NotifyErrorsEnabled()),
	)
}

func (b *Bot) notifKeyboard() *InlineKeyboardMarkup {
	b.mu.Lock()
	tc := b.cfg.Telegram
	b.mu.Unlock()

	toggleText := func(label string, enabled bool) string {
		if enabled {
			return label + ": ✅ ON"
		}
		return label + ": ❌ OFF"
	}

	return &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "🎯 Hanya Fill & Close", CallbackData: "notif:trades_only"},
				{Text: "🔔 Nyalakan Semua", CallbackData: "notif:all_on"},
			},
			{
				{Text: toggleText("Peluang", tc.NotifyOpportunitiesEnabled()), CallbackData: "notif:toggle:opps"},
				{Text: toggleText("Scan Reject", tc.NotifyRejectionsEnabled()), CallbackData: "notif:toggle:rejection"},
			},
			{
				{Text: toggleText("Order Fill", tc.NotifyFillsEnabled()), CallbackData: "notif:toggle:fills"},
				{Text: toggleText("Posisi Closed", tc.NotifyClosesEnabled()), CallbackData: "notif:toggle:closes"},
			},
			{
				{Text: toggleText("Error Alert", tc.NotifyErrorsEnabled()), CallbackData: "notif:toggle:errors"},
				{Text: "🔕 Pause Semua", CallbackData: "notif:all_off"},
			},
			{
				{Text: "⚙️ Pengaturan Setup", CallbackData: "settings"},
				{Text: MenuUtamaText, CallbackData: "menu"},
			},
		},
	}
}

func (b *Bot) settingsKeyboard() *InlineKeyboardMarkup {
	return &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "🔔 Filter Notifikasi", CallbackData: "notif"},
				{Text: "🛡️ Risk Status", CallbackData: "risk"},
			},
			{
				{Text: "🎯 Hanya Fill & Close", CallbackData: "notif:trades_only"},
				{Text: "🔔 Semua Notif ON", CallbackData: "notif:all_on"},
			},
			{
				{Text: "⚡ Status", CallbackData: "status"},
				{Text: MenuUtamaText, CallbackData: "menu"},
			},
		},
	}
}

func (b *Bot) saveConfigFileUpdate(updater func(map[string]interface{}) error) error {
	path := b.configPath
	if path == "" {
		path = "config.json"
	}
	data, err := os.ReadFile(path)
	var raw map[string]interface{}
	if err == nil {
		_ = json.Unmarshal(data, &raw)
	}
	if raw == nil {
		raw = make(map[string]interface{})
	}
	if err := updater(raw); err != nil {
		return err
	}
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0644)
}

func (b *Bot) setNotifMode(ctx context.Context, chatID int64, mode string) {
	b.mu.Lock()
	switch mode {
	case "trades_only":
		f := false
		t := true
		b.cfg.Telegram.NotifyOpportunities = &f
		b.cfg.Telegram.NotifyRejections = &f
		b.cfg.Telegram.NotifyFills = &t
		b.cfg.Telegram.NotifyCloses = &t
		b.cfg.Telegram.NotifyErrors = &f
	case "all_on":
		t := true
		b.cfg.Telegram.NotifyOpportunities = &t
		b.cfg.Telegram.NotifyRejections = &t
		b.cfg.Telegram.NotifyFills = &t
		b.cfg.Telegram.NotifyCloses = &t
		b.cfg.Telegram.NotifyErrors = &t
	case "all_off":
		f := false
		b.cfg.Telegram.NotifyOpportunities = &f
		b.cfg.Telegram.NotifyRejections = &f
		b.cfg.Telegram.NotifyFills = &f
		b.cfg.Telegram.NotifyCloses = &f
		b.cfg.Telegram.NotifyErrors = &f
	}
	_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
		tg, ok := raw["telegram"].(map[string]interface{})
		if !ok {
			tg = make(map[string]interface{})
			raw["telegram"] = tg
		}
		tg["notify_opportunities"] = *b.cfg.Telegram.NotifyOpportunities
		tg["notify_rejections"] = *b.cfg.Telegram.NotifyRejections
		tg["notify_fills"] = *b.cfg.Telegram.NotifyFills
		tg["notify_closes"] = *b.cfg.Telegram.NotifyCloses
		tg["notify_errors"] = *b.cfg.Telegram.NotifyErrors
		return nil
	})
	b.mu.Unlock()

	b.sendReplyWithMarkup(ctx, chatID, 0, b.formatNotificationSettings(), b.notifKeyboard())
}

func (b *Bot) toggleNotifKey(ctx context.Context, chatID int64, key string) {
	b.mu.Lock()
	switch strings.ToLower(key) {
	case "opps", "opportunities":
		val := !b.cfg.Telegram.NotifyOpportunitiesEnabled()
		b.cfg.Telegram.NotifyOpportunities = &val
	case "rejection", "rejections":
		val := !b.cfg.Telegram.NotifyRejectionsEnabled()
		b.cfg.Telegram.NotifyRejections = &val
	case "fills", "fill", "open":
		val := !b.cfg.Telegram.NotifyFillsEnabled()
		b.cfg.Telegram.NotifyFills = &val
	case "closes", "close", "closed":
		val := !b.cfg.Telegram.NotifyClosesEnabled()
		b.cfg.Telegram.NotifyCloses = &val
	case "errors", "error", "fails":
		val := !b.cfg.Telegram.NotifyErrorsEnabled()
		b.cfg.Telegram.NotifyErrors = &val
	}
	_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
		tg, ok := raw["telegram"].(map[string]interface{})
		if !ok {
			tg = make(map[string]interface{})
			raw["telegram"] = tg
		}
		if b.cfg.Telegram.NotifyOpportunities != nil {
			tg["notify_opportunities"] = *b.cfg.Telegram.NotifyOpportunities
		}
		if b.cfg.Telegram.NotifyRejections != nil {
			tg["notify_rejections"] = *b.cfg.Telegram.NotifyRejections
		}
		if b.cfg.Telegram.NotifyFills != nil {
			tg["notify_fills"] = *b.cfg.Telegram.NotifyFills
		}
		if b.cfg.Telegram.NotifyCloses != nil {
			tg["notify_closes"] = *b.cfg.Telegram.NotifyCloses
		}
		if b.cfg.Telegram.NotifyErrors != nil {
			tg["notify_errors"] = *b.cfg.Telegram.NotifyErrors
		}
		return nil
	})
	b.mu.Unlock()

	b.sendReplyWithMarkup(ctx, chatID, 0, b.formatNotificationSettings(), b.notifKeyboard())
}

func (b *Bot) handleNotifCommand(ctx context.Context, msg *Message, parts []string) {
	if len(parts) == 0 {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			b.formatNotificationSettings(),
			b.notifKeyboard(),
		)
		return
	}

	sub := strings.ToLower(strings.TrimSpace(parts[0]))
	switch sub {
	case "trades", "trade", "fills_only", "fill_only":
		b.setNotifMode(ctx, msg.Chat.ID, "trades_only")
	case "all", "on":
		b.setNotifMode(ctx, msg.Chat.ID, "all_on")
	case "pause", "off", "mute":
		b.setNotifMode(ctx, msg.Chat.ID, "all_off")
	case "toggle":
		if len(parts) >= 2 {
			b.toggleNotifKey(ctx, msg.Chat.ID, parts[1])
		} else {
			b.sendReplyWithMarkup(
				ctx, msg.Chat.ID, msg.MessageID,
				"ℹ️ <i>Gunakan:</i> <code>/notif toggle &lt;opps|rejection|fills|closes|errors&gt;</code>",
				b.notifKeyboard(),
			)
		}
	default:
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			b.formatNotificationSettings(),
			b.notifKeyboard(),
		)
	}
}

func (b *Bot) handleSetConfigCommand(ctx context.Context, msg *Message, parts []string) {
	userID := int64(0)
	if msg.From != nil {
		userID = msg.From.ID
	}
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"⛔ <b>Akses ditolak:</b> User tidak diizinkan mengubah konfigurasi.",
			b.defaultKeyboard(),
		)
		return
	}

	if len(parts) >= 1 && strings.Contains(parts[0], "=") {
		split := strings.SplitN(parts[0], "=", 2)
		parts = []string{split[0], split[1]}
	}

	if len(parts) < 2 {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			"ℹ️ <b>Format:</b> <code>/set &lt;parameter&gt; &lt;nilai&gt;</code>\n\n"+
				"<b>Parameter tersedia:</b>\n"+
				"• <code>capital &lt;usd&gt;</code> (misal: <code>/set capital 100</code>)\n"+
				"• <code>allocation &lt;0.1-1.0&gt;</code> (misal: <code>/set allocation 0.5</code>)\n"+
				"• <code>minroi &lt;persen&gt;</code> (misal: <code>/set minroi 0.3</code>)\n"+
				"• <code>maxroi &lt;persen&gt;</code> (misal: <code>/set maxroi 20</code>)\n"+
				"• <code>leverage &lt;1-10&gt;</code> (misal: <code>/set leverage 1</code>)\n"+
				"• <code>mincap &lt;usd&gt;</code> (misal: <code>/set mincap 10</code>)\n"+
				"• <code>scan &lt;detik&gt;</code> (misal: <code>/set scan 30</code>)\n"+
				"• <code>mode &lt;MOCK|LIVE&gt;</code>\n"+
				"• <code>adaptive &lt;true|false&gt;</code>\n"+
				"• <code>synthetic_idr &lt;true|false&gt;</code>\n\n"+
				"<i>Ketik <code>/settings</code> untuk melihat status live.</i>",
			b.defaultKeyboard(),
		)
		return
	}

	param := strings.ToLower(strings.TrimSpace(parts[0]))
	valStr := strings.TrimSpace(parts[1])

	b.mu.Lock()
	defer b.mu.Unlock()

	var desc string
	var err error

	switch param {
	case "capital", "capital_total":
		v, parseErr := strconv.ParseFloat(valStr, 64)
		if parseErr != nil || v <= 0 {
			err = fmt.Errorf("capital harus angka positif > 0 (contoh: 100)")
			break
		}
		old := b.cfg.CapitalTotal
		b.cfg.CapitalTotal = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["capital_total"] = v
			return nil
		})
		desc = fmt.Sprintf("Total Capital Budget: $%.2f → <b>$%.2f</b>", old, v)

	case "allocation", "allocation_per_pair":
		v, parseErr := strconv.ParseFloat(strings.TrimSuffix(valStr, "%"), 64)
		if parseErr != nil {
			err = fmt.Errorf("allocation harus angka antara 0.01 - 1.0 atau persen (misal: 0.5 atau 50%%)")
			break
		}
		if v > 1.0 {
			v = v / 100.0
		}
		if v <= 0 || v > 1.0 {
			err = fmt.Errorf("allocation harus > 0 dan <= 1.0 (misal: 0.5)")
			break
		}
		old := b.cfg.AllocationPerPair
		b.cfg.AllocationPerPair = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["allocation_per_pair"] = v
			return nil
		})
		desc = fmt.Sprintf("Alokasi Per Pair: %.1f%% → <b>%.1f%% ($%.2f)</b>", old*100, v*100, b.cfg.CapitalTotal*v)

	case "minroi", "min_net_roi_pct":
		v, parseErr := strconv.ParseFloat(strings.TrimSuffix(valStr, "%"), 64)
		if parseErr != nil || v < 0 {
			err = fmt.Errorf("minroi harus angka >= 0 (contoh: 0.3)")
			break
		}
		old := b.cfg.MinNetROIPct
		b.cfg.MinNetROIPct = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["min_net_roi_pct"] = v
			return nil
		})
		desc = fmt.Sprintf("Min Net ROI: %.2f%% → <b>%.2f%%</b>", old, v)

	case "maxroi", "max_net_roi_pct":
		v, parseErr := strconv.ParseFloat(strings.TrimSuffix(valStr, "%"), 64)
		if parseErr != nil || v <= b.cfg.MinNetROIPct {
			err = fmt.Errorf("maxroi harus angka > minroi (saat ini minroi: %.2f%%)", b.cfg.MinNetROIPct)
			break
		}
		old := b.cfg.MaxNetROIPct
		b.cfg.MaxNetROIPct = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["max_net_roi_pct"] = v
			return nil
		})
		desc = fmt.Sprintf("Max Net ROI: %.2f%% → <b>%.2f%%</b>", old, v)

	case "leverage", "perp_leverage":
		v, parseErr := strconv.ParseFloat(strings.TrimSuffix(strings.ToLower(valStr), "x"), 64)
		if parseErr != nil || v < 1 || v > 20 {
			err = fmt.Errorf("leverage harus angka antara 1 dan 20 (contoh: 1 atau 2)")
			break
		}
		old := b.cfg.PerpLeverage
		b.cfg.PerpLeverage = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["perp_leverage"] = v
			return nil
		})
		desc = fmt.Sprintf("Perp Leverage: %.0fx → <b>%.0fx</b>", old, v)

	case "mincap", "min_allocated_capital":
		v, parseErr := strconv.ParseFloat(valStr, 64)
		if parseErr != nil || v <= 0 {
			err = fmt.Errorf("mincap harus angka > 0 (contoh: 10)")
			break
		}
		old := b.cfg.MinAllocatedCapital
		b.cfg.MinAllocatedCapital = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["min_allocated_capital"] = v
			return nil
		})
		desc = fmt.Sprintf("Min Allocated Capital: $%.2f → <b>$%.2f</b>", old, v)

	case "scan", "scan_interval", "scan_interval_seconds":
		v, parseErr := strconv.Atoi(strings.TrimSuffix(strings.ToLower(valStr), "s"))
		if parseErr != nil || v < 5 {
			err = fmt.Errorf("scan interval harus integer >= 5 detik (contoh: 30)")
			break
		}
		old := b.cfg.ScanIntervalSeconds
		b.cfg.ScanIntervalSeconds = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["scan_interval_seconds"] = v
			return nil
		})
		desc = fmt.Sprintf("Scan Interval: %ds → <b>%ds</b>", old, v)

	case "mode":
		m := strings.ToUpper(valStr)
		if m != "LIVE" && m != "MOCK" {
			err = fmt.Errorf("mode harus 'LIVE' atau 'MOCK'")
			break
		}
		old := b.cfg.Mode
		b.cfg.Mode = m
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["mode"] = m
			return nil
		})
		desc = fmt.Sprintf("Trading Mode: <code>%s</code> → <b>%s</b>", old, m)

	case "adaptive", "enable_adaptive_sizing":
		v, parseErr := strconv.ParseBool(valStr)
		if parseErr != nil {
			err = fmt.Errorf("adaptive harus 'true' atau 'false'")
			break
		}
		b.cfg.EnableAdaptiveSizing = &v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["enable_adaptive_sizing"] = v
			return nil
		})
		desc = fmt.Sprintf("Adaptive Sizing: <b>%t</b>", v)

	case "synthetic_idr", "enable_synthetic_idr":
		v, parseErr := strconv.ParseBool(valStr)
		if parseErr != nil {
			err = fmt.Errorf("synthetic_idr harus 'true' atau 'false'")
			break
		}
		b.cfg.EnableSyntheticIDR = v
		_ = b.saveConfigFileUpdate(func(raw map[string]interface{}) error {
			raw["enable_synthetic_idr"] = v
			return nil
		})
		desc = fmt.Sprintf("Synthetic IDR: <b>%t</b>", v)

	default:
		err = fmt.Errorf("parameter '%s' tidak dikenali. Ketik /set untuk daftar parameter", param)
	}

	if err != nil {
		b.sendReplyWithMarkup(
			ctx, msg.Chat.ID, msg.MessageID,
			fmt.Sprintf("❌ <b>Gagal update config:</b> %s", html.EscapeString(err.Error())),
			b.defaultKeyboard(),
		)
		return
	}

	b.sendReplyWithMarkup(
		ctx, msg.Chat.ID, msg.MessageID,
		fmt.Sprintf("✅ <b>Konfigurasi Berhasil Diperbarui</b>\n\n• %s\n• Tersimpan ke: <code>%s</code>\n\n<i>Ketik /settings untuk melihat konfigurasi aktif.</i>",
			desc, b.configPath),
		b.defaultKeyboard(),
	)
}
