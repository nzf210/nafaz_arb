package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// FundingRate retrieves current funding rate from OKX or MEXC.
func (l *LiveAdapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	if isMEXCSymbol(symbol) {
		return l.mexcVenue().FetchFundingRate(ctx, symbol)
	}
	return l.okxVenue().FetchFundingRate(ctx, symbol)
}

func (l *LiveAdapter) fetchOKXFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	rawSymbol := cleanSymbol(symbol)
	path := fmt.Sprintf("/api/v5/public/funding-rate?instId=%s", url.QueryEscape(rawSymbol))

	resp, err := l.okxDo(ctx, path, func() (*http.Request, error) {
		return l.newOKXRequest(ctx, path)
	})
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
			NextFundingTime string `json:"nextFundingTime"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&okxResp); err != nil {
		return domain.FundingRate{}, fmt.Errorf("decode okx funding: %w", err)
	}
	if okxResp.Code != "0" || len(okxResp.Data) == 0 {
		return domain.FundingRate{}, fmt.Errorf("okx funding error: %s (%s)", okxResp.Msg, okxResp.Code)
	}

	item := okxResp.Data[0]
	rate, err := strconv.ParseFloat(item.FundingRate, 64)
	if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) { return domain.FundingRate{}, fmt.Errorf("invalid OKX funding rate %q", item.FundingRate) }
	intervalHours := 8
	ft, ftErr := strconv.ParseInt(item.FundingTime, 10, 64)
	if ftErr != nil || ft <= 0 { return domain.FundingRate{}, fmt.Errorf("invalid OKX fundingTime %q", item.FundingTime) }
	nextMS, nextErr := strconv.ParseInt(item.NextFundingTime, 10, 64)
	if nextErr != nil || nextMS <= ft { return domain.FundingRate{}, fmt.Errorf("invalid OKX nextFundingTime %q", item.NextFundingTime) }
	if nextMS > ft {
		diffHours := int((nextMS - ft) / (1000 * 3600))
		if diffHours > 0 {
			intervalHours = diffHours
		}
	}

	tsMS, tsErr := strconv.ParseInt(item.Ts, 10, 64)
	if tsErr != nil || tsMS <= 0 { return domain.FundingRate{}, fmt.Errorf("invalid OKX funding response timestamp %q", item.Ts) }
	ts := time.UnixMilli(tsMS).UTC()
	nextFunding := time.UnixMilli(nextMS).UTC()

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
			FundingRate  *float64 `json:"fundingRate"`
			CollectCycle int      `json:"collectCycle"`
			NextSettleTime int64  `json:"nextSettleTime"`
			Timestamp    int64    `json:"timestamp"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&mexcResp); err != nil {
		return domain.FundingRate{}, fmt.Errorf("decode mexc funding: %w", err)
	}
	if !mexcResp.Success { return domain.FundingRate{}, fmt.Errorf("mexc funding error code: %d", mexcResp.Code) }
	if mexcResp.Data.FundingRate == nil || math.IsNaN(*mexcResp.Data.FundingRate) || math.IsInf(*mexcResp.Data.FundingRate, 0) { return domain.FundingRate{}, fmt.Errorf("mexc funding response has invalid or missing fundingRate") }
	if mexcResp.Data.Timestamp <= 0 || mexcResp.Data.NextSettleTime <= 0 { return domain.FundingRate{}, fmt.Errorf("mexc funding response missing authoritative timestamp or nextSettleTime") }
	interval := mexcResp.Data.CollectCycle
	if interval <= 0 { return domain.FundingRate{}, fmt.Errorf("mexc funding response has invalid collectCycle %d", interval) }
	ts := time.UnixMilli(mexcResp.Data.Timestamp).UTC()
	next := time.UnixMilli(mexcResp.Data.NextSettleTime).UTC()
	if !next.After(ts) { return domain.FundingRate{}, fmt.Errorf("mexc nextSettleTime is not after response timestamp") }
	return domain.FundingRate{Rate:*mexcResp.Data.FundingRate, IntervalHours:interval, Timestamp:ts, NextFundingTime:next}, nil
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
		if p > 0 && q > 0 && !math.IsNaN(p) && !math.IsInf(p, 0) && !math.IsNaN(q) && !math.IsInf(q, 0) {
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
		if p > 0 && q > 0 && !math.IsNaN(p) && !math.IsInf(p, 0) && !math.IsNaN(q) && !math.IsInf(q, 0) {
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
