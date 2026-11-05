package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
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
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v5/public/funding-rate?instId=%s", l.okxBaseURL, url.QueryEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("okx funding req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("okx funding request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.FundingRate{}, fmt.Errorf("%w: okx funding status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.FundingRate{}, fmt.Errorf("okx funding status: %d", resp.StatusCode)
	}

	var okxResp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			FundingRate     string `json:"fundingRate"`
			FundingTime     string `json:"fundingTime"`
			PrevFundingTime string `json:"prevFundingTime"`
			Ts              string `json:"ts"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&okxResp); err != nil {
		return domain.FundingRate{}, fmt.Errorf("decode okx funding: %w", err)
	}
	if okxResp.Code != "0" || len(okxResp.Data) == 0 {
		return domain.FundingRate{}, fmt.Errorf("okx funding error: %s (%s)", okxResp.Msg, okxResp.Code)
	}

	item := okxResp.Data[0]
	rate, _ := strconv.ParseFloat(item.FundingRate, 64)

	intervalHours := 8
	ft, _ := strconv.ParseInt(item.FundingTime, 10, 64)
	pft, _ := strconv.ParseInt(item.PrevFundingTime, 10, 64)
	if ft > 0 && pft > 0 && ft > pft {
		diffHours := int((ft - pft) / (1000 * 3600))
		if diffHours > 0 {
			intervalHours = diffHours
		}
	}

	ts := time.Now().UTC()
	if ms, err := strconv.ParseInt(item.Ts, 10, 64); err == nil && ms > 0 {
		ts = time.UnixMilli(ms).UTC()
	}

	var nextFunding time.Time
	if ft > 0 {
		nextFunding = time.UnixMilli(ft).UTC()
	}

	return domain.FundingRate{
		Rate:            rate,
		IntervalHours:   intervalHours,
		Timestamp:       ts,
		NextFundingTime: nextFunding,
	}, nil
}

func (l *LiveAdapter) fetchMEXCFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v1/contract/funding_rate/%s", l.mexcBaseURL, url.PathEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("mexc funding req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("mexc funding request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.FundingRate{}, fmt.Errorf("%w: mexc funding status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.FundingRate{}, fmt.Errorf("mexc funding status: %d", resp.StatusCode)
	}

	var mexcResp struct {
		Success bool `json:"success"`
		Code    int  `json:"code"`
		Data    struct {
			FundingRate  float64 `json:"fundingRate"`
			CollectCycle int     `json:"collectCycle"`
			Timestamp    int64   `json:"timestamp"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&mexcResp); err != nil {
		return domain.FundingRate{}, fmt.Errorf("decode mexc funding: %w", err)
	}
	if !mexcResp.Success {
		return domain.FundingRate{}, fmt.Errorf("mexc funding error code: %d", mexcResp.Code)
	}

	interval := mexcResp.Data.CollectCycle
	if interval <= 0 {
		interval = 8
	}

	ts := time.Now().UTC()
	if mexcResp.Data.Timestamp > 0 {
		ts = time.UnixMilli(mexcResp.Data.Timestamp).UTC()
	}

	return domain.FundingRate{
		Rate:            mexcResp.Data.FundingRate,
		IntervalHours:   interval,
		Timestamp:       ts,
		NextFundingTime: nextSettlementTime(ts, interval),
	}, nil
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
