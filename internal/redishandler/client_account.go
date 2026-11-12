package redishandler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

// DynamicFeeProfile is an authenticated, short-lived account fee snapshot
// published by the executor. Rates are percentages, not decimal fractions.
type DynamicFeeProfile struct {
	MakerFeePct  float64 `json:"maker_fee_pct"`
	TakerFeePct  float64 `json:"taker_fee_pct"`
	Source       string  `json:"source"`
	Verified     bool    `json:"verified"`
	FetchedAtMS  int64   `json:"fetched_at_ms"`
}

func (c *Client) GetDynamicFeeProfile(ctx context.Context, exchange, market, symbol string) (*DynamicFeeProfile, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	normalize := func(value string) string {
		value = strings.ToLower(strings.TrimSpace(value))
		value = strings.ReplaceAll(value, "/", "_")
		value = strings.ReplaceAll(value, "-", "_")
		return strings.ReplaceAll(value, ":", "_")
	}
	key := "arb:fee_profile:" + normalize(exchange) + ":" + normalize(market) + ":" + normalize(symbol)
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get fee profile %s: %w", key, err)
	}
	var profile DynamicFeeProfile
	if err := json.Unmarshal(raw, &profile); err != nil {
		return nil, fmt.Errorf("decode fee profile %s: %w", key, err)
	}
	age := time.Now().UnixMilli() - profile.FetchedAtMS
	if !profile.Verified || profile.Source != "EXCHANGE_API" ||
		profile.FetchedAtMS <= 0 || age < 0 || age > int64(5*time.Minute/time.Millisecond) ||
		profile.MakerFeePct < 0 || profile.MakerFeePct > 100 ||
		profile.TakerFeePct < 0 || profile.TakerFeePct > 100 {
		return nil, nil
	}
	return &profile, nil
}

// GetTradeStats retrieves aggregate statistics of closed arbitrage trades.

func (c *Client) GetTradeStats(ctx context.Context) (TradeStats, error) {
	if c == nil || c.rdb == nil {
		return TradeStats{}, nil
	}
	vals, err := c.rdb.HGetAll(ctx, KeyTradeStats).Result()
	if err != nil {
		return TradeStats{}, err
	}
	var stats TradeStats
	if tc, ok := vals["total_closed"]; ok {
		fmt.Sscanf(tc, "%d", &stats.TotalClosed)
	}
	if wc, ok := vals["win_count"]; ok {
		fmt.Sscanf(wc, "%d", &stats.WinCount)
	}
	if pu, ok := vals["total_pnl_usdt"]; ok {
		fmt.Sscanf(pu, "%f", &stats.TotalPnLUSDT)
	}
	if pi, ok := vals["total_pnl_idr"]; ok {
		fmt.Sscanf(pi, "%f", &stats.TotalPnLIDR)
	}
	return stats, nil
}

// GetTradeHistory retrieves the list of recently closed trades.

func (c *Client) GetTradeHistory(ctx context.Context, limit int64) ([]ClosedTradeRecord, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	items, err := c.rdb.LRange(ctx, KeyTradeHistory, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	var out []ClosedTradeRecord
	for _, it := range items {
		var rec ClosedTradeRecord
		if json.Unmarshal([]byte(it), &rec) == nil {
			out = append(out, rec)
		}
	}
	return out, nil
}

// GetBalances retrieves the simulation or live balances stored in Redis.

func (c *Client) GetBalances(ctx context.Context) (*domain.BalancesRecord, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	raw, err := c.rdb.Get(ctx, KeyBalances).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	var b domain.BalancesRecord
	if err := json.Unmarshal([]byte(raw), &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// SubscribeTradeEvents subscribes to the arb:trade_events channel.

func (c *Client) SubscribeTradeEvents(ctx context.Context) *redis.PubSub {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Subscribe(ctx, ChannelTradeEvents)
}
