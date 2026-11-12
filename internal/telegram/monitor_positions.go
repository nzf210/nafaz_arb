package telegram

import (
	"context"
	"encoding/json"
	"html"
	"strings"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

func findActivePosition(positions []domain.ActivePosition, id string) *domain.ActivePosition {
	for i := range positions {
		if strings.EqualFold(positions[i].Symbol, id) ||
			redishandler.CleanPairKey(positions[i].LocalExchange, positions[i].Symbol) == id {
			return &positions[i]
		}
	}
	return nil
}

func relatedPositionOrders(
	orders []json.RawMessage, id string,
) []json.RawMessage {
	var related []json.RawMessage
	for _, item := range orders {
		var record map[string]interface{}
		_ = json.Unmarshal(item, &record)
		if firstMapValue(record, "position_id") == id ||
			firstMapValue(record, "pair_key", "pair_tag") == id {
			related = append(related, item)
		}
	}
	return related
}

type positionLedgerReader interface {
	PositionOrderFills(context.Context, string) (map[string][]json.RawMessage, error)
}

func readPositionLedger(
	ctx context.Context, reader positionLedgerReader, id string,
) ([]json.RawMessage, []json.RawMessage, bool, error) {
	if reader == nil {
		return nil, nil, false, nil
	}
	ledger, err := reader.PositionOrderFills(ctx, id)
	if err != nil {
		return nil, nil, true, err
	}
	return ledger["orders"], ledger["fills"], true, nil
}

func (b *Bot) handlePositionDetail(
	ctx context.Context, chatID, replyToID int64, id string,
) {
	b.mu.Lock()
	rc := b.redisClient
	ledgerReader := b.positionLedgerReader
	b.mu.Unlock()
	if rc == nil {
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID,
			"⚠️ Redis tidak terhubung; posisi dan active order terkait tidak dapat diverifikasi.",
			b.defaultKeyboard(),
		)
		return
	}
	positions, err := rc.ListActivePositions(ctx)
	if err != nil {
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID,
			"❌ Gagal membaca active positions: "+html.EscapeString(err.Error()),
			b.defaultKeyboard(),
		)
		return
	}
	matched := findActivePosition(positions, id)
	orders, err := rc.ListActiveOrderRecords(ctx)
	if err != nil {
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID,
			"❌ Gagal membaca order registry terkait posisi: "+html.EscapeString(err.Error()),
			b.defaultKeyboard(),
		)
		return
	}
	related := relatedPositionOrders(orders, id)
	ledgerOrders, ledgerFills, ledgerAvailable, err := readPositionLedger(
		ctx, ledgerReader, id,
	)
	if err != nil {
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID,
			"⚠️ Supabase order/fill ledger tidak dapat dibaca: "+
				html.EscapeString(err.Error())+
				"\nData Redis tetap tersedia, tetapi fills historis belum terverifikasi.",
			b.defaultKeyboard(),
		)
		return
	}
	if matched == nil && len(related) == 0 && len(ledgerOrders) == 0 {
		b.sendReplyWithMarkup(
			ctx, chatID, replyToID,
			"⚠️ Position ID tidak ditemukan pada active position store maupun durable order ledger. Ini tidak membuktikan tidak ada exposure di exchange.",
			b.defaultKeyboard(),
		)
		return
	}

	var summary strings.Builder
	summary.WriteString("🔎 <b>POSITION " + html.EscapeString(id) + "</b>\n")
	if matched != nil {
		raw, _ := json.Marshal(matched)
		summary.WriteString(
			"<b>Active position snapshot</b>\n<pre>" +
				html.EscapeString(prettyJSON(raw)) + htmlPreClose,
		)
	}
	b.sendReplyWithMarkup(
		ctx, chatID, replyToID, summary.String(), b.defaultKeyboard(),
	)
	b.sendJSONPages(ctx, chatID, 0, "Redis active orders terkait", related)
	if ledgerAvailable {
		b.sendJSONPages(ctx, chatID, 0, "Supabase durable orders", ledgerOrders)
		b.sendJSONPages(ctx, chatID, 0, "Immutable exchange fills", ledgerFills)
		return
	}
	b.sendReplyWithMarkup(
		ctx, chatID, 0,
		"<b>Fills historis:</b> Supabase belum terhubung; fill ledger tidak dapat diverifikasi.",
		b.defaultKeyboard(),
	)
}

type startupRecoverySummary struct {
	started               string
	recoveredPositions    int
	trackedOrders         int
	partialFillCount      int
	orphanCount           int
	mismatchCount         int
	failedExchangeCount   int
	globalStatus          string
}
