package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/api"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

type Update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *Message       `json:"message,omitempty"`
	CallbackQuery *CallbackQuery `json:"callback_query,omitempty"`
}

type Message struct {
	MessageID int64  `json:"message_id"`
	From      *User  `json:"from,omitempty"`
	Chat      Chat   `json:"chat"`
	Date      int64  `json:"date"`
	Text      string `json:"text,omitempty"`
}

type User struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	Username  string `json:"username,omitempty"`
}

type Chat struct {
	ID    int64  `json:"id"`
	Type  string `json:"type"`
	Title string `json:"title,omitempty"`
}

type CallbackQuery struct {
	ID      string   `json:"id"`
	From    User     `json:"from"`
	Message *Message `json:"message,omitempty"`
	Data    string   `json:"data"`
}

type InlineKeyboardButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data,omitempty"`
	URL          string `json:"url,omitempty"`
}

type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard"`
}

type BotCommand struct {
	Command     string `json:"command"`
	Description string `json:"description"`
}

type SetMyCommandsReq struct {
	Commands []BotCommand `json:"commands"`
}

type SendMessageReq struct {
	ChatID           int64                 `json:"chat_id"`
	Text             string                `json:"text"`
	ParseMode        string                `json:"parse_mode,omitempty"`
	ReplyToMessageID int64                 `json:"reply_to_message_id,omitempty"`
	ReplyMarkup      *InlineKeyboardMarkup `json:"reply_markup,omitempty"`
}

type AnswerCallbackQueryReq struct {
	CallbackQueryID string `json:"callback_query_id"`
	Text            string `json:"text,omitempty"`
	ShowAlert       bool   `json:"show_alert,omitempty"`
}

type APIResponse[T any] struct {
	Ok          bool   `json:"ok"`
	Result      T      `json:"result"`
	Description string `json:"description,omitempty"`
}

type Bot struct {
	cfg         config.Config
	server      *api.Server
	client      *http.Client
	baseURL     string
	botUsername string
	lastOffset  int64
	redisClient *redishandler.Client
	opportunityReader interface { LatestOpportunityScans(context.Context, int) ([]domain.ScanResult, error) }
	positionLedgerReader interface { PositionOrderFills(context.Context, string) (map[string][]json.RawMessage, error) }

	mu          sync.Mutex
	alertedKeys map[string]time.Time
	mutedUntil  time.Time
}

const (
	CT            = "Content-Type"
	AJ            = "application/json"
	TIME_FORMAT   = "15:04:05 UTC"
	HistoryText   = "📜 Riwayat Selesai"
	MenuUtamaText = "📱 Menu Utama"
	separatorLine = "----------------------------------------\n"
)

func New(cfg config.Config, server *api.Server) *Bot {
	baseURL := ""
	if cfg.Telegram.BotToken != "" {
		baseURL = fmt.Sprintf("https://api.telegram.org/bot%s", cfg.Telegram.BotToken)
	}
	return &Bot{
		cfg:         cfg,
		server:      server,
		client:      &http.Client{Timeout: 35 * time.Second},
		baseURL:     baseURL,
		alertedKeys: make(map[string]time.Time),
	}
}

// SetRedisClient configures the Redis client for active position queries and commands.
func (b *Bot) SetRedisClient(r *redishandler.Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.redisClient = r
}


// SetOpportunityHistoryReader wires the durable source used by /latest and
// the "Peluang Terbaru" button. The reader is read-only.
func (b *Bot) SetOpportunityHistoryReader(reader interface {
	LatestOpportunityScans(context.Context, int) ([]domain.ScanResult, error)
}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opportunityReader = reader
}

// SetPositionLedgerReader wires the read-only durable order/fill ledger.
func (b *Bot) SetPositionLedgerReader(reader interface {
	PositionOrderFills(context.Context, string) (map[string][]json.RawMessage, error)
}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.positionLedgerReader = reader
}

// SetBaseURL allows overriding Telegram API URL (useful for testing with mock server).
func (b *Bot) SetBaseURL(url string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baseURL = url
}

// Start launches the Telegram bot polling and hooks up scanner alerts.
func (b *Bot) Start(ctx context.Context) error {
	if !b.cfg.Telegram.Enabled {
		log.Println("telegram bot is disabled")
		return nil
	}
	if b.cfg.Telegram.BotToken == "" {
		return fmt.Errorf("telegram bot token is required when enabled")
	}
	if b.cfg.Telegram.ChatID == 0 && len(b.cfg.Telegram.AllowedChatIDs) == 0 {
		return fmt.Errorf("telegram fail-closed: chat_id or allowed_chat_ids must be configured when enabled")
	}

	// Verify bot token and fetch bot info
	user, err := b.getMe(ctx)
	if err != nil {
		return fmt.Errorf("verify telegram bot token: %w", err)
	}
	b.botUsername = user.Username
	log.Printf("telegram bot started as @%s (group alerts enabled: %v, target chat: %d)",
		b.botUsername, b.cfg.Telegram.AlertOnNew, b.cfg.Telegram.ChatID)

	// Register bot commands with Telegram so typing "/" pops up all commands
	commands := []BotCommand{
		{Command: "menu", Description: "📱 Buka menu tombol interaktif"},
		{Command: "orders", Description: "📋 Semua active bot orders"},
		{Command: "positions", Description: "💼 Status posisi terbuka"},
		{Command: "recovery", Description: "🔄 Status recovery setelah restart"},
		{Command: "orphans", Description: "👻 Order exchange yang belum teridentifikasi"},
		{Command: "reconcile", Description: "🧾 Insiden reconciliation; /reconcile retry untuk verifikasi ulang"},
		{Command: "risk", Description: "🛡️ Status risk halt"},
		{Command: "order", Description: "🔎 Detail order berdasarkan internal ID"},
		{Command: "position", Description: "🔎 Detail posisi berdasarkan ID"},
		{Command: "pending", Description: "🕒 Lihat pending order Spot yang belum selesai"},
		{Command: "balance", Description: "💰 Cek saldo live Spot & Futures semua aset"},
		{Command: "history", Description: "📜 Riwayat transaksi yang berhasil ditutup"},
		{Command: "scan", Description: "🔍 Jalankan scan pasar & cari peluang baru"},
		{Command: "latest", Description: "📋 Lihat peluang dari 10 scan terakhir"},
		{Command: "pair", Description: "🔎 Cek peluang aset tertentu (misal: /pair BTC)"},
		{Command: "active", Description: "🔥 Lihat peluang aktif yang sedang berjalan"},
		{Command: "frozen", Description: "❄️ Lihat daftar pasangan yang dibekukan"},
		{Command: "status", Description: "⚡ Status scanner, mode & bursa aktif"},
		{Command: "metrics", Description: "📊 Statistik durasi & histori arbitrase"},
		{Command: "mute", Description: "🔕 Bisukan alert sementara (misal: /mute 1h)"},
		{Command: "unmute", Description: "🔔 Aktifkan kembali notifikasi alert"},
		{Command: "killswitch", Description: "🛑 Hentikan/lanjutkan semua entry baru (on/off)"},
		{Command: "help", Description: "ℹ️ Petunjuk lengkap & bantuan"},
	}
	if err := b.setMyCommands(ctx, commands); err != nil {
		log.Printf("warning: telegram setMyCommands: %v", err)
	}

	// Register scanner notification hook for group alerts
	if b.cfg.Telegram.AlertOnNew && b.cfg.Telegram.ChatID != 0 {
		b.server.RegisterScanHook(func(r domain.ScanResult) {
			b.onScanCompleted(ctx, r)
		})
	}

	go b.pollLoop(ctx)
	go b.listenTradeEvents(ctx)
	go b.sendStartupSummary(ctx)
	return nil
}

func (b *Bot) getMe(ctx context.Context) (User, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/getMe", nil)
	if err != nil {
		return User{}, err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return User{}, err
	}
	defer resp.Body.Close()

	var apiResp APIResponse[User]
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return User{}, err
	}
	if !apiResp.Ok {
		return User{}, fmt.Errorf("telegram API error: %s", apiResp.Description)
	}
	return apiResp.Result, nil
}

func (b *Bot) setMyCommands(ctx context.Context, commands []BotCommand) error {
	reqBody := SetMyCommandsReq{Commands: commands}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+"/setMyCommands", bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set(CT, AJ)
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var apiResp APIResponse[bool]
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return err
	}
	if !apiResp.Ok {
		return fmt.Errorf("telegram API error: %s", apiResp.Description)
	}
	return nil
}

func (b *Bot) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updates, err := b.getUpdates(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("telegram getUpdates error: %v", err)
				time.Sleep(3 * time.Second)
				continue
			}
			for _, u := range updates {
				if u.UpdateID >= b.lastOffset {
					b.lastOffset = u.UpdateID + 1
				}
				go b.handleUpdate(ctx, u)
			}
		}
	}
}

func (b *Bot) getUpdates(ctx context.Context) ([]Update, error) {
	url := fmt.Sprintf("%s/getUpdates?offset=%d&timeout=20", b.baseURL, b.lastOffset)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResp APIResponse[[]Update]
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}
	if !apiResp.Ok {
		return nil, fmt.Errorf("telegram API error: %s", apiResp.Description)
	}
	return apiResp.Result, nil
}

func (b *Bot) isChatAllowed(chatID int64) bool {
	if b.cfg.Telegram.ChatID != 0 && b.cfg.Telegram.ChatID == chatID {
		return true
	}
	if len(b.cfg.Telegram.AllowedChatIDs) > 0 {
		for _, id := range b.cfg.Telegram.AllowedChatIDs {
			if id == chatID {
				return true
			}
		}
		return false
	}
	return false
}

func (b *Bot) isUserAllowedForSensitiveActions(userID int64) bool {
	if len(b.cfg.Telegram.AllowedUserIDs) > 0 {
		for _, id := range b.cfg.Telegram.AllowedUserIDs {
			if id == userID {
				return true
			}
		}
		return false
	}
	// The caller has already verified the chat. When no user allow-list is
	// configured, that authorized chat is the operator trust boundary.
	return true
}

func (b *Bot) defaultKeyboard() *InlineKeyboardMarkup {
	return &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "🔍 Scan Pasar", CallbackData: "scan"},
				{Text: "📋 Peluang Terbaru", CallbackData: "latest"},
			},
			{
				{Text: "💼 Posisi Aktif", CallbackData: "positions"},
				{Text: "🕒 Pending Orders", CallbackData: "pending_orders"},
				{Text: "💰 Cek Balance", CallbackData: "balance"},
				{Text: HistoryText, CallbackData: "history"},
			},
			{
				{Text: "⚡ Status", CallbackData: "status"},
				{Text: "📊 Statistik Metrik", CallbackData: "metrics"},
			},
			{
				{Text: "🔥 Peluang Aktif", CallbackData: "active"},
				{Text: "❄️ Pasangan Freeze", CallbackData: "frozen"},
			},
			{
				{Text: "🔄 Refresh Menu", CallbackData: "menu"},
				{Text: "ℹ️ Bantuan", CallbackData: "help"},
			},
		},
	}
}

func (b *Bot) alertKeyboard(baseAsset string) *InlineKeyboardMarkup {
	return &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: fmt.Sprintf("🔎 Detail %s", baseAsset), CallbackData: "pair:" + baseAsset},
				{Text: "🔍 Scan Baru", CallbackData: "scan"},
			},
			{
				{Text: "⚡ Status", CallbackData: "status"},
				{Text: MenuUtamaText, CallbackData: "menu"},
			},
		},
	}
}

func (b *Bot) answerCallbackQuery(ctx context.Context, callbackID string, toastText string) {
	reqBody := AnswerCallbackQueryReq{
		CallbackQueryID: callbackID,
		Text:            toastText,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+"/answerCallbackQuery", bytes.NewReader(bodyBytes))
	if err != nil {
		return
	}
	req.Header.Set(CT, AJ)
	resp, err := b.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (b *Bot) sendReplyWithMarkup(ctx context.Context, chatID int64, replyToID int64, text string, markup *InlineKeyboardMarkup) {
	reqBody := SendMessageReq{
		ChatID:           chatID,
		Text:             text,
		ParseMode:        "HTML",
		ReplyToMessageID: replyToID,
		ReplyMarkup:      markup,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		log.Printf("marshal telegram message: %v", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+"/sendMessage", bytes.NewReader(bodyBytes))
	if err != nil {
		log.Printf("create send message request: %v", err)
		return
	}
	req.Header.Set(CT, AJ)

	resp, err := b.client.Do(req)
	if err != nil {
		log.Printf("send telegram message error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var apiResp APIResponse[any]
		_ = json.NewDecoder(resp.Body).Decode(&apiResp)
		log.Printf("telegram API error sending message (status %d): %s", resp.StatusCode, apiResp.Description)

		// Fallback: if HTML parsing failed, retry sending as plain text
		if strings.Contains(apiResp.Description, "can't parse entities") {
			reqBody.ParseMode = ""
			if fallbackBytes, fbErr := json.Marshal(reqBody); fbErr == nil {
				if fbReq, fbReqErr := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+"/sendMessage", bytes.NewReader(fallbackBytes)); fbReqErr == nil {
					fbReq.Header.Set(CT, AJ)
					if fbResp, fbDoErr := b.client.Do(fbReq); fbDoErr == nil {
						defer fbResp.Body.Close()
					}
				}
			}
		}
	}
}

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
