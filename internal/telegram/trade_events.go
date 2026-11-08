package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

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
				"• Hedge: <code>Target delta-neutral; risiko eksekusi tetap ada</code>\n\n"+
				"<i>Gunakan /positions untuk memantau posisi dan opsi tutup paksa.</i>",
			modeTag,
			html.EscapeString(strings.ToUpper(ev.Symbol)),
			html.EscapeString(strings.ToUpper(ev.LocalExchange)),
			html.EscapeString(strings.ToUpper(ev.GlobalExchange)),
			ev.Qty,
			ev.EntryPriceIDR,
			ev.ShortEntryUSDT,
		)
	case "EXECUTION_FAILURE", "HEDGE_FAILED":
		pairLabel := ev.PairTag
		if pairLabel == "" {
			pairLabel = ev.PairKey
		}
		if pairLabel == "" {
			pairLabel = ev.Symbol
		}
		reason := ev.Reason
		if reason == "" {
			reason = "EXECUTION_FAILURE"
		}
		message := ev.Message
		if message == "" {
			message = "Proses entry atau hedge tidak berhasil."
		}
		// Avoid flooding Telegram when the same pair repeatedly rejects the
		// same signal or balance snapshot.
		alertKey := "failure:" + pairLabel + ":" + reason
		b.mu.Lock()
		lastAlert, alreadyAlerted := b.alertedKeys[alertKey]
		if alreadyAlerted && time.Since(lastAlert) < 2*time.Minute {
			b.mu.Unlock()
			return
		}
		b.alertedKeys[alertKey] = time.Now()
		b.mu.Unlock()

		text = fmt.Sprintf(
			"🚨 <b>[%s] ENTRY / EXECUTION GAGAL</b>\n\n"+
				"• Pair: <code>%s</code>\n"+
				"• Exchange: <code>%s</code> → <code>%s</code>\n"+
				"• Alasan: <b>%s</b>\n"+
				"• Detail: %s\n"+
				"• State: <code>%s</code>\n\n"+
				"<i>Entry baru untuk kondisi ini ditolak. Periksa saldo, order, dan posisi di kedua exchange sebelum mencoba lagi.</i>",
			modeTag,
			html.EscapeString(pairLabel),
			html.EscapeString(ev.LocalExchange),
			html.EscapeString(ev.GlobalExchange),
			html.EscapeString(reason),
			html.EscapeString(message),
			html.EscapeString(ev.State),
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

	chatIDs := make([]int64, 0, len(b.cfg.Telegram.AllowedChatIDs)+1)
	if b.cfg.Telegram.ChatID != 0 {
		chatIDs = append(chatIDs, b.cfg.Telegram.ChatID)
	} else {
		chatIDs = append(chatIDs, b.cfg.Telegram.AllowedChatIDs...)
	}
	for _, chatID := range chatIDs {
		b.sendReplyWithMarkup(ctx, chatID, 0, text, b.defaultKeyboard())
	}
}
