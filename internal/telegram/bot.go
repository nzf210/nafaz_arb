package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"net/http"
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
		{Command: "positions", Description: "💼 Status posisi terbuka & opsi tutup paksa"},
		{Command: "history", Description: "📜 Riwayat transaksi yang berhasil ditutup"},
		{Command: "scan", Description: "🔍 Jalankan scan pasar & cari peluang baru"},
		{Command: "latest", Description: "📋 Lihat hasil scan & peluang ROI tertinggi"},
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
	// Fallback to chat authorization if user list is empty
	return b.isChatAllowed(userID)
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

func (b *Bot) handleUpdate(ctx context.Context, u Update) {
	// 1. Handle interactive inline button clicks (Callback Queries)
	if u.CallbackQuery != nil {
		b.handleCallbackQuery(ctx, u.CallbackQuery)
		return
	}

	// 2. Handle text messages
	if u.Message == nil || u.Message.Text == "" {
		return
	}

	b.handleTextMessage(ctx, u.Message)
}

func (b *Bot) handleTextMessage(ctx context.Context, msg *Message) {
	if !b.isChatAllowed(msg.Chat.ID) {
		log.Printf("telegram update ignored: chat_id %d not authorized", msg.Chat.ID)
		return
	}

	text := strings.TrimSpace(msg.Text)
	if !strings.HasPrefix(text, "/") {
		return
	}

	parts := strings.Fields(text)
	cmd := strings.ToLower(parts[0])

	if atIdx := strings.Index(cmd, "@"); atIdx != -1 {
		username := cmd[atIdx+1:]
		if b.botUsername != "" && !strings.EqualFold(username, b.botUsername) {
			return
		}
		cmd = cmd[:atIdx]
	}

	b.handleCommand(ctx, msg, cmd, parts)
}

func (b *Bot) handleCommand(ctx context.Context, msg *Message, cmd string, parts []string) {
	switch cmd {
	case "/start", "/menu":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatMenu(), b.defaultKeyboard())
	case "/help":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatHelp(), b.defaultKeyboard())
	case "/status":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatStatus(), b.defaultKeyboard())
	case "/scan":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "🔍 <i>Sedang menjalankan pemindaian pasar...</i>", nil)
		r, err := b.server.TriggerScan(ctx)
		if err != nil {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, fmt.Sprintf("❌ <b>Scan gagal:</b> %s", html.EscapeString(err.Error())), b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, 0, b.formatScanResult(r, "Hasil Pemindaian On-Demand"), b.defaultKeyboard())
	case "/latest":
		latest := b.server.LatestScan()
		if latest.ScanID == "" {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, "ℹ️ <i>Belum ada hasil scan. Tekan tombol Scan untuk mulai.</i>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatScanResult(latest, "Hasil Pemindaian Terakhir"), b.defaultKeyboard())
	case "/pair":
		if len(parts) < 2 {
			b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
				"ℹ️ <i>Gunakan:</i> <code>/pair &lt;simbol&gt;</code>\n<i>Contoh:</i> <code>/pair BTC</code> atau <code>/pair ETH</code>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatPairDetail(parts[1]), b.defaultKeyboard())
	case "/active":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatActiveOpportunities(), b.defaultKeyboard())
	case "/positions":
		b.handlePositionsCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/history":
		b.handleHistoryCommand(ctx, msg.Chat.ID, msg.MessageID)
	case "/frozen":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatFrozenPairs(), b.defaultKeyboard())
	case "/metrics":
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID, b.formatMetrics(), b.defaultKeyboard())
	case "/mute":
		dur := 1 * time.Hour
		if len(parts) > 1 {
			if d, err := time.ParseDuration(parts[1]); err == nil && d > 0 {
				dur = d
			}
		}
		b.mu.Lock()
		b.mutedUntil = time.Now().UTC().Add(dur)
		mutedUntil := b.mutedUntil
		b.mu.Unlock()
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			fmt.Sprintf("🔕 <b>Alert Dibisukan</b>\n\nNotifikasi peluang tidak akan dikirim hingga <b>%s</b> (~%s ke depan).\nGunakan /unmute untuk mengaktifkan kembali.",
				mutedUntil.Format(TIME_FORMAT), dur.String()), b.defaultKeyboard())
	case "/unmute":
		b.mu.Lock()
		b.mutedUntil = time.Time{}
		b.mu.Unlock()
		b.sendReplyWithMarkup(ctx, msg.Chat.ID, msg.MessageID,
			"🔔 <b>Alert Diaktifkan Kembali</b>\n\nNotifikasi peluang baru akan dikirimkan normal sesuai konfigurasi.", b.defaultKeyboard())
	case "/killswitch":
		b.handleKillSwitchCommand(ctx, msg.Chat.ID, msg.MessageID, parts)
	default:
	}
}

func (b *Bot) handleCallbackQuery(ctx context.Context, cb *CallbackQuery) {
	if cb == nil || cb.Message == nil {
		return
	}
	chatID := cb.Message.Chat.ID
	if !b.isChatAllowed(chatID) {
		return
	}

	b.answerCallbackQuery(ctx, cb.ID, "Memproses...")

	switch cb.Data {
	case "scan":
		b.sendReplyWithMarkup(ctx, chatID, 0, "🔍 <i>Sedang menjalankan pemindaian pasar...</i>", nil)
		r, err := b.server.TriggerScan(ctx)
		if err != nil {
			b.sendReplyWithMarkup(ctx, chatID, 0, fmt.Sprintf("❌ <b>Scan gagal:</b> %s", html.EscapeString(err.Error())), b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatScanResult(r, "Hasil Pemindaian On-Demand"), b.defaultKeyboard())
	case "latest":
		latest := b.server.LatestScan()
		if latest.ScanID == "" {
			b.sendReplyWithMarkup(ctx, chatID, 0, "ℹ️ <i>Belum ada hasil scan. Tekan tombol Scan untuk mulai.</i>", b.defaultKeyboard())
			return
		}
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatScanResult(latest, "Hasil Pemindaian Terakhir"), b.defaultKeyboard())
	case "status":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatStatus(), b.defaultKeyboard())
	case "metrics":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatMetrics(), b.defaultKeyboard())
	case "active":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatActiveOpportunities(), b.defaultKeyboard())
	case "positions":
		b.handlePositionsCommand(ctx, chatID, 0)
	case "history":
		b.handleHistoryCommand(ctx, chatID, 0)
	case "frozen":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatFrozenPairs(), b.defaultKeyboard())
	case "menu":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatMenu(), b.defaultKeyboard())
	case "help":
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatHelp(), b.defaultKeyboard())
	default:
		b.handleExtendedCallback(ctx, chatID, cb)
	}
}

func (b *Bot) handleExtendedCallback(ctx context.Context, chatID int64, cb *CallbackQuery) {
	if sym, ok := strings.CutPrefix(cb.Data, "pair:"); ok {
		b.sendReplyWithMarkup(ctx, chatID, 0, b.formatPairDetail(sym), b.defaultKeyboard())
		return
	}
	if pairKey, ok := strings.CutPrefix(cb.Data, "force_close:"); ok {
		b.handleForceCloseCallback(ctx, chatID, cb.From.ID, pairKey)
		return
	}
	if pairKey, ok := strings.CutPrefix(cb.Data, "confirm_fc:"); ok {
		b.handleConfirmForceCloseCallback(ctx, chatID, cb.From.ID, pairKey)
		return
	}
	if cb.Data == "cancel_fc" {
		b.sendReplyWithMarkup(ctx, chatID, 0, "❌ Tutup paksa dibatalkan.", b.defaultKeyboard())
		return
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

func (b *Bot) handleKillSwitchCommand(ctx context.Context, chatID int64, replyToID int64, parts []string) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID, "❌ <b>Kill switch tidak tersedia:</b> Redis belum terhubung.", b.defaultKeyboard())
		return
	}

	action := "on"
	if len(parts) > 1 {
		switch strings.ToLower(parts[1]) {
		case "off", "resume", "clear":
			action = "off"
		case "on":
			action = "on"
		default:
			b.sendReplyWithMarkup(ctx, chatID, replyToID,
				"ℹ️ <i>Gunakan:</i> <code>/killswitch on</code> atau <code>/killswitch off</code>", b.defaultKeyboard())
			return
		}
	}

	if err := rc.PublishKillSwitch(ctx, action, "TELEGRAM"); err != nil {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			fmt.Sprintf("❌ <b>Kill switch gagal:</b> %s", html.EscapeString(err.Error())), b.defaultKeyboard())
		return
	}
	if action == "on" {
		b.sendReplyWithMarkup(ctx, chatID, replyToID,
			"🛑 <b>KILL SWITCH AKTIF</b>\n\nSemua entry baru di Executor dihentikan. Posisi terbuka tetap dikelola sampai dipulihkan.\n<i>Gunakan /killswitch off untuk melanjutkan trading.</i>", b.defaultKeyboard())
		return
	}
	b.sendReplyWithMarkup(ctx, chatID, replyToID,
		"🟢 <b>Kill Switch Dimatikan</b>\n\nEntry baru di Executor dilanjutkan.", b.defaultKeyboard())
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

	sb.WriteString("• Status: <code>HEDGED (Delta Neutral 100%)</code>\n\n")
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

func (b *Bot) listenTradeEvents(ctx context.Context) {
	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		return
	}

	pubsub := rc.SubscribeTradeEvents(ctx)
	if pubsub == nil {
		return
	}
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if msg != nil && msg.Payload != "" {
				var ev redishandler.TradeEvent
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err == nil {
					b.broadcastTradeEvent(ctx, ev)
				}
			}
		}
	}
}

func (b *Bot) broadcastTradeEvent(ctx context.Context, ev redishandler.TradeEvent) {
	if b.cfg.Telegram.ChatID == 0 {
		return
	}

	modeTag := "REAL"
	if ev.IsMock {
		modeTag = "SIMULASI"
	}

	var text string
	switch ev.Event {
	case "POSITION_OPENED":
		text = fmt.Sprintf(
			"🟢 <b>[%s] POSISI ARBITRASE TERBUKA!</b>\n\n"+
				"• Pasangan: <b>%s</b>\n"+
				"• Bursa: <code>%s (Spot)</code> vs <code>%s (Futures 1x)</code>\n"+
				"• Kuantitas: <code>%.6f koin</code>\n"+
				"• Beli Spot (Maker): <code>Rp %.2f</code>\n"+
				"• Short Futures (1x): <code>$%.4f USDT</code>\n"+
				"• Proteksi: <code>100%% Delta Neutral (Nol Risiko Arah Pasar)</code>\n\n"+
				"<i>Gunakan /positions untuk memantau posisi dan opsi tutup paksa.</i>",
			modeTag,
			html.EscapeString(strings.ToUpper(ev.Symbol)),
			html.EscapeString(strings.ToUpper(ev.LocalExchange)),
			html.EscapeString(strings.ToUpper(ev.GlobalExchange)),
			ev.Qty,
			ev.EntryPriceIDR,
			ev.ShortEntryUSDT,
		)
	case "POSITION_CLOSED":
		emoji := "🎉"
		statusWord := "PROFIT"
		if ev.NetProfitUSDT < 0 {
			emoji = "⚠️"
			statusWord = "LOSS"
		}
		text = fmt.Sprintf(
			"%s <b>[%s] POSISI BERHASIL DITUTUP (%s)!</b>\n\n"+
				"• Pasangan: <b>%s</b>\n"+
				"• Kuantitas: <code>%.6f koin</code>\n"+
				"• Jual Spot (Maker): <code>Rp %.2f</code>\n"+
				"• Tutup Short (1x): <code>$%.4f USDT</code>\n"+
				"• Akumulasi Funding: <code>+$%.4f USDT</code>\n"+
				separatorLine+
				"💰 <b>Net Realized Profit:</b> <b>%+.4f USDT (Rp %+.2f)</b>\n"+
				"📈 <b>Net ROI:</b> <b>%+.3f%%</b>\n\n"+
				"<i>Ketik /history untuk melihat rekap riwayat transaksi yang telah selesai.</i>",
			emoji, modeTag, statusWord,
			html.EscapeString(strings.ToUpper(ev.Symbol)),
			ev.Qty,
			ev.ExitPriceIDR,
			ev.ShortExitUSDT,
			ev.AccumulatedFundingUSDT,
			ev.NetProfitUSDT,
			ev.NetProfitIDR,
			ev.NetROIPct,
		)
	default:
		return
	}

	b.sendReplyWithMarkup(ctx, b.cfg.Telegram.ChatID, 0, text, b.defaultKeyboard())
}

func (b *Bot) handleForceCloseCallback(ctx context.Context, chatID int64, userID int64, pairKey string) {
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, chatID, 0, "⛔ <b>Akses ditolak:</b> User tidak diizinkan mengeksekusi force close.", b.defaultKeyboard())
		return
	}

	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, 0,
			"❌ <b>Gagal Tutup Paksa:</b> <i>Redis tidak terhubung.</i>", b.defaultKeyboard())
		return
	}

	previewMsg := fmt.Sprintf("⚠️ <b>Konfirmasi Tutup Paksa (Preview)</b>\n\n"+
		"• Pasangan: <code>%s</code>\n"+
		"• Tindakan yang akan diambil:\n"+
		"  1. Batalkan semua order lokal (entry/exit) yang terbuka\n"+
		"  2. Market Buy (reduceOnly) untuk menutup short futures\n"+
		"  3. Market Sell spot untuk melikuidasi koin spot\n\n"+
		"Apakah Anda yakin ingin mengeksekusi force close sekarang?", html.EscapeString(pairKey))

	confirmKb := &InlineKeyboardMarkup{
		InlineKeyboard: [][]InlineKeyboardButton{
			{
				{Text: "✅ Ya, Eksekusi Sekarang", CallbackData: "confirm_fc:" + pairKey},
				{Text: "❌ Batalkan", CallbackData: "cancel_fc"},
			},
		},
	}
	b.sendReplyWithMarkup(ctx, chatID, 0, previewMsg, confirmKb)
}

func (b *Bot) handleConfirmForceCloseCallback(ctx context.Context, chatID int64, userID int64, pairKey string) {
	if !b.isUserAllowedForSensitiveActions(userID) {
		b.sendReplyWithMarkup(ctx, chatID, 0, "⛔ <b>Akses ditolak:</b> User tidak diizinkan mengeksekusi force close.", b.defaultKeyboard())
		return
	}

	b.mu.Lock()
	rc := b.redisClient
	b.mu.Unlock()

	if rc == nil {
		b.sendReplyWithMarkup(ctx, chatID, 0,
			"❌ <b>Gagal Tutup Paksa:</b> <i>Redis tidak terhubung.</i>", b.defaultKeyboard())
		return
	}

	err := rc.PublishForceClose(ctx, "", "", pairKey, fmt.Sprintf("user_%d", userID))
	if err != nil {
		b.sendReplyWithMarkup(ctx, chatID, 0,
			fmt.Sprintf("❌ <b>Gagal mengirim perintah tutup paksa:</b> %s", html.EscapeString(err.Error())),
			b.defaultKeyboard())
		return
	}

	msg := fmt.Sprintf("🚨 <b>Perintah Tutup Paksa Dikonfirmasi & Terkirim!</b>\n\n"+
		"• Pasangan: <code>%s</code>\n"+
		"• Operator: <code>user_%d</code>\n"+
		"• Aksi Executor:\n"+
		"  1. Membatalkan antrean Limit Sell/Buy di bursa lokal\n"+
		"  2. Mengeksekusi Market Buy (<code>reduceOnly=True</code>) untuk menutup Short di Futures Global\n"+
		"  3. Likuidasi leg spot dan verifikasi exposure bersih\n\n"+
		"<i>Periksa /positions dalam beberapa detik untuk melihat status terbaru.</i>",
		html.EscapeString(pairKey), userID)

	b.sendReplyWithMarkup(ctx, chatID, 0, msg, b.defaultKeyboard())
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
