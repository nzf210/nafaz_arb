package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// FundingRate retrieves current funding rate from OKX or MEXC.
func (l *LiveAdapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	if isMEXCSymbol(symbol) {
		return l.fetchMEXCFundingRate(ctx, symbol)
	}
	return l.fetchOKXFundingRate(ctx, symbol)
}

func (l *LiveAdapter) fetchOKXFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	l.mu.RLock(); client := l.okxClient; l.mu.RUnlock()
	if client == nil { return domain.FundingRate{}, fmt.Errorf("okx client is not configured") }
	rate, err := client.FundingRate(ctx, symbol)
	if err != nil && strings.Contains(err.Error(), "status") { return domain.FundingRate{}, err }
	return rate, err
}

func (l *LiveAdapter) fetchMEXCFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	l.mu.RLock()
	client := l.mexcClient
	l.mu.RUnlock()
	if client == nil { return domain.FundingRate{}, fmt.Errorf("mexc client is not configured") }
	rate, err := client.FundingRate(ctx, symbol)
	if err != nil { return domain.FundingRate{}, err }
	return rate, nil
}

func nextSettlementTime(t time.Time, intervalHours int) time.Time {
	if intervalHours <= 0 {
		intervalHours = 8
	}
	utc := t.UTC()
	year, month, day := utc.Date()
	hour := utc.Hour()
	nextSlot := ((hour / intervalHours) + 1) * intervalHours
	if nextSlot >= 24 {
		return time.Date(year, month, day+1, nextSlot-24, 0, 0, 0, time.UTC)
	}
	return time.Date(year, month, day, nextSlot, 0, 0, 0, time.UTC)
}

// OpenExposures returns public scanner open exposure state. Read-only by design.
func (l *LiveAdapter) OpenExposures(ctx context.Context) ([]domain.OpenExposure, error) {
	return []domain.OpenExposure{}, nil
}

// parseLevels decodes Indodax's depth payload. Levels arrive as
// [price, quantity] where both may be strings or numbers.
func parseLevels(raw [][]any, multiplier float64) []domain.PriceLevel {
	var levels []domain.PriceLevel
	for _, item := range raw {
		if len(item) < 2 {
			continue
		}
		p := toFloat(item[0])
		q := toFloat(item[1]) * multiplier
		if p > 0 && q > 0 {
			levels = append(levels, domain.PriceLevel{Price: p, Quantity: q})
		}
	}
	return levels
}

func parseStringLevels(raw [][]string, multiplier float64) []domain.PriceLevel {
	var levels []domain.PriceLevel
	for _, item := range raw {
		if len(item) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(item[0], 64)
		q, _ := strconv.ParseFloat(item[1], 64)
		q = q * multiplier
		if p > 0 && q > 0 {
			levels = append(levels, domain.PriceLevel{Price: p, Quantity: q})
		}
	}
	return levels
}

func toFloat(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}
