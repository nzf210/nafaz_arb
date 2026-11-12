package scanner

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func activePositionExposures(positions []domain.ActivePosition) ([]domain.OpenExposure, error) {
	out := make([]domain.OpenExposure, 0, len(positions))
	for _, position := range positions {
		if position.Qty <= 0 { continue }
		parts := splitPairSymbol(position.Symbol)
		if len(parts) != 2 || strings.TrimSpace(position.LocalExchange) == "" {
			return nil, fmt.Errorf("active position has incomplete pair identity: exchange=%q symbol=%q", position.LocalExchange, position.Symbol)
		}
		out = append(out, domain.OpenExposure{
			Exchange: strings.ToLower(strings.TrimSpace(position.LocalExchange)),
			Symbol: position.Symbol, BaseAsset: parts[0], QuoteAsset: parts[1],
			MarketType: "SPOT", Kind: "POSITION", Quantity: position.Qty,
			Active: true, Managed: true,
		})
	}
	return out, nil
}

func splitPairSymbol(symbol string) []string {
	value := strings.ToUpper(strings.TrimSpace(symbol))
	if index := strings.LastIndex(value, ":"); index >= 0 { value = value[index+1:] }
	value = strings.ReplaceAll(value, "/", "_")
	parts := strings.Split(value, "_")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" { return nil }
	return parts
}

type activeOrderRecord struct {
	Exchange string `json:"exchange"`
	MarketType string `json:"market_type"`
	Symbol string `json:"symbol"`
	Status string `json:"status"`
	RecoveryStatus string `json:"recovery_status"`
	RemainingQuantity float64 `json:"remaining_quantity"`
	OriginalQuantity float64 `json:"original_quantity"`
	FilledQuantity float64 `json:"filled_quantity"`
}

func activeOrderExposures(records []json.RawMessage) ([]domain.OpenExposure, error) {
	out := make([]domain.OpenExposure, 0, len(records))
	for index, raw := range records {
		var record activeOrderRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, fmt.Errorf("decode active order record %d: %w", index, err)
		}
		status := strings.ToLower(strings.TrimSpace(record.Status))
		switch status {
		case "filled", "closed", "canceled", "cancelled", "rejected", "expired":
			continue
		}
		remaining := record.RemainingQuantity
		if remaining <= 0 { remaining = record.OriginalQuantity - record.FilledQuantity }
		if remaining <= 0 {
			return nil, fmt.Errorf("active order has unknown/non-positive remaining quantity: exchange=%q symbol=%q status=%q", record.Exchange, record.Symbol, record.Status)
		}
		if strings.TrimSpace(record.Exchange) == "" || strings.TrimSpace(record.Symbol) == "" {
			return nil, fmt.Errorf("active order record %d has incomplete venue identity", index)
		}
		recovery := strings.ToUpper(strings.TrimSpace(record.RecoveryStatus))
		managed := recovery == "MANAGED" || recovery == "ADOPTED"
		out = append(out, domain.OpenExposure{
			Exchange: strings.ToLower(strings.TrimSpace(record.Exchange)),
			Symbol: record.Symbol, MarketType: strings.ToUpper(strings.TrimSpace(record.MarketType)),
			Kind: "OPEN_ORDER", Quantity: remaining, Active: true, Managed: managed,
		})
	}
	return out, nil
}
