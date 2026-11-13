package redishandler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"encoding/json"
	"math"
	"strconv"
	"fmt"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

const (
	DefaultTTL = 60 * time.Second
	// SignalTTL bounds how long an actionable signal stays readable. A signal
	// older than this is stale by definition: the executor pegs a maker order
	// against a quote that no longer exists, so it must expire rather than be
	// replayed on the next notify/pubsub restart.
	SignalTTL              = 10 * time.Second
	ChannelSignalNotify    = "arb:signal_notify"
	ChannelEmergencyCancel = "arb:emergency_cancel"
	ChannelForceClose      = "arb:force_close"
	ChannelKillSwitch      = "arb:kill_switch"
	ChannelSpotOrderLimitUpdate = "arb:executor:spot_order_limit_update"
	KeySpotOrderLimits = "arb:executor:spot_order_limits"
	ChannelTradeEvents     = "arb:trade_events"
	KeyPrefixSignal        = "arb:signal:"
	KeySignalPriority      = "arb:signal_priority"
	KeyPrefixLivePrice     = "arb:live_price:"
	KeyPrefixActivePos     = "arb:active_pos:"
	KeyTradeHistory        = "arb:trade_history"
	KeyTradeStats          = "arb:stats"
	KeyBalances            = "arb:balances"
	KeyExecutorStatus      = "arb:executor:status"
	ChannelBalanceRequest  = "arb:balance_request"
	KeyPrefixBalanceReply  = "arb:balance_reply:"
	ChannelRecoveryRequest = "arb:recovery_request"
	KeyPrefixRecoveryReply = "arb:recovery_reply:"
	KeyOrderRecoveryIndex   = "arb:order_recovery_idx"
	KeyOrphanOrderIndex     = "arb:orphan_idx"
	KeyPendingOrderRecovery = "arb:order_pending_recovery"
	KeyPrefixOrderRecord    = "arb:order:"
	KeyPrefixOrphanOrder    = "arb:orphan_order:"
)

type TradeStats struct {
	TotalClosed  int     `json:"total_closed"`
	WinCount     int     `json:"win_count"`
	TotalPnLUSDT float64 `json:"total_pnl_usdt"`
	TotalPnLIDR  float64 `json:"total_pnl_idr"`
}

type ClosedTradeRecord struct {
	TradeID                string  `json:"trade_id"`
	LocalExchange          string  `json:"local_exchange"`
	GlobalExchange         string  `json:"global_exchange"`
	Symbol                 string  `json:"symbol"`
	Qty                    float64 `json:"qty"`
	EntryPriceIDR          float64 `json:"entry_price_idr"`
	ExitPriceIDR           float64 `json:"exit_price_idr"`
	ShortEntryUSDT         float64 `json:"short_entry_usdt"`
	ShortExitUSDT          float64 `json:"short_exit_usdt"`
	NetProfitUSDT          float64 `json:"net_profit_usdt"`
	NetProfitIDR           float64 `json:"net_profit_idr"`
	NetROIPct              float64 `json:"net_roi_pct"`
	AccumulatedFundingUSDT float64 `json:"accumulated_funding_usdt"`
	IsMock                 bool    `json:"is_mock"`
	ClosedAtMS             int64   `json:"closed_at_ms"`
}

type TradeEvent struct {
	Event                  string                 `json:"event"`
	PairTag                string                 `json:"pair_tag,omitempty"`
	PairKey                string                 `json:"pair_key,omitempty"`
	Reason                 string                 `json:"reason,omitempty"`
	Message                string                 `json:"message,omitempty"`
	State                  string                 `json:"state,omitempty"`
	Detail                 map[string]interface{} `json:"detail,omitempty"`
	LocalExchange          string                 `json:"local_exchange"`
	GlobalExchange         string  `json:"global_exchange"`
	Symbol                 string  `json:"symbol"`
	Qty                    float64 `json:"qty"`
	EntryPriceIDR          float64 `json:"entry_price_idr"`
	ExitPriceIDR           float64 `json:"exit_price_idr,omitempty"`
	ShortEntryUSDT         float64 `json:"short_entry_usdt"`
	ShortExitUSDT          float64 `json:"short_exit_usdt,omitempty"`
	NetProfitUSDT          float64 `json:"net_profit_usdt,omitempty"`
	NetProfitIDR           float64 `json:"net_profit_idr,omitempty"`
	NetROIPct              float64 `json:"net_roi_pct,omitempty"`
	AccumulatedFundingUSDT float64 `json:"accumulated_funding_usdt,omitempty"`
	IsMock                 bool    `json:"is_mock"`
	TimestampMS            int64   `json:"timestamp_ms"`
}

type PairUpdate struct {
	LocalEx       string
	Symbol        string
	GlobalEx      string
	GlobalSymbol  string
	LivePrice     domain.PricingSnapshot
	Payload       *domain.ExecutorPayload // non-nil if eligible for entry signal
}

type Client struct {
	rdb *redis.Client
}

func New(cfg config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

func NewWithClient(rdb *redis.Client) *Client {
	return &Client{rdb: rdb}
}

// CleanPairKey formats local exchange and symbol into a normalized {local_ex}:{symbol} tag.
func CleanPairKey(localEx, symbol string, globalPair ...string) string {
	clean := func(value string) string {
		value = strings.ToLower(strings.TrimSpace(value))
		value = strings.ReplaceAll(value, "/", "_")
		return strings.ReplaceAll(value, "-", "_")
	}
	tag := clean(localEx) + ":" + clean(symbol)
	if len(globalPair) >= 2 && strings.TrimSpace(globalPair[0]) != "" && strings.TrimSpace(globalPair[1]) != "" {
		tag += "__" + clean(globalPair[0]) + ":" + clean(globalPair[1])
	}
	return tag
}

// HasActivePosition checks if arb:active_pos:{local_ex}:{symbol} exists.
func (c *Client) HasActivePosition(ctx context.Context, localEx, symbol string, globalPair ...string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	key := KeyPrefixActivePos + CleanPairKey(localEx, symbol, globalPair...)
	val, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists %s: %w", key, err)
	}
	if val > 0 || len(globalPair) < 2 {
		return val > 0, nil
	}
	// Legacy keys do not identify the futures venue. Treat one as active for
	// every matching spot pair until the executor migrates its stored metadata.
	legacyKey := KeyPrefixActivePos + CleanPairKey(localEx, symbol)
	legacy, err := c.rdb.Exists(ctx, legacyKey).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists legacy %s: %w", legacyKey, err)
	}
	return legacy > 0, nil
}

// GetActivePosition retrieves active position info if present.
func (c *Client) GetActivePosition(ctx context.Context, localEx, symbol string, globalPair ...string) (*domain.ActivePosition, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	key := KeyPrefixActivePos + CleanPairKey(localEx, symbol, globalPair...)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get %s: %w", key, err)
	}
	var pos domain.ActivePosition
	if err := json.Unmarshal(data, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal active pos: %w", err)
	}
	return &pos, nil
}

// UpdateLivePrice sets arb:live_price:{local_ex}:{symbol} with TTL 3s.
func (c *Client) UpdateLivePrice(ctx context.Context, localEx, symbol string, snapshot domain.PricingSnapshot, globalPair ...string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	key := KeyPrefixLivePrice + CleanPairKey(localEx, symbol, globalPair...)
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal live price: %w", err)
	}
	return c.rdb.Set(ctx, key, data, DefaultTTL).Err()
}

// PublishSignal writes arb:signal:{local_ex}:{symbol} with SignalTTL and notifies arb:signal_notify.
func (c *Client) PublishSignal(ctx context.Context, payload domain.ExecutorPayload) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	tag := CleanPairKey(payload.PairInfo.LocalExchange, payload.PairInfo.LocalSymbol, payload.PairInfo.GlobalExchange, payload.PairInfo.GlobalSymbol)
	signalKey := KeyPrefixSignal + tag
	liveKey := KeyPrefixLivePrice + tag

	signalData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal signal: %w", err)
	}
	liveData, err := json.Marshal(payload.PricingSnapshot)
	if err != nil {
		return fmt.Errorf("marshal live price: %w", err)
	}

	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, liveKey, liveData, DefaultTTL)
	profit := payload.ExecutionDirectives.EstimatedNetProfitUSDT
	if !math.IsNaN(profit) && !math.IsInf(profit, 0) && profit > 0 {
		pipe.ZAdd(ctx, KeySignalPriority, redis.Z{Score: profit, Member: tag})
		pipe.Expire(ctx, KeySignalPriority, SignalTTL+5*time.Second)
	}
	pipe.Set(ctx, signalKey, signalData, SignalTTL)
	pipe.Publish(ctx, ChannelSignalNotify, tag)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline publish signal: %w", err)
	}
	return nil
}

// PublishEmergencyCancel publishes cancel directive to arb:emergency_cancel.
func (c *Client) PublishEmergencyCancel(ctx context.Context, localEx, symbol, reason string, fundingRate float64, globalPair ...string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	tag := CleanPairKey(localEx, symbol, globalPair...)
	payload := map[string]interface{}{
		"action":         "CANCEL_ALL_NEGATIVE_FUNDING",
		"local_exchange": localEx,
		"symbol":         symbol,
		"pair_key":       tag,
		"funding_rate":   fundingRate,
		"reason":         reason,
		"timestamp_ms":   time.Now().UnixMilli(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal emergency cancel: %w", err)
	}
	return c.rdb.Publish(ctx, ChannelEmergencyCancel, string(data)).Err()
}

// ExecuteBatchUpdates applies live price updates and signals in a single pipeline.
func (c *Client) ExecuteBatchUpdates(ctx context.Context, updates []PairUpdate) error {
	if c == nil || c.rdb == nil || len(updates) == 0 {
		return nil
	}

	pipe := c.rdb.Pipeline()
	for _, u := range updates {
		globalEx, globalSymbol := u.GlobalEx, u.GlobalSymbol
		if u.Payload != nil {
			globalEx = u.Payload.PairInfo.GlobalExchange
			globalSymbol = u.Payload.PairInfo.GlobalSymbol
		}
		tag := CleanPairKey(u.LocalEx, u.Symbol, globalEx, globalSymbol)
		liveKey := KeyPrefixLivePrice + tag
		liveData, err := json.Marshal(u.LivePrice)
		if err == nil {
			pipe.Set(ctx, liveKey, liveData, DefaultTTL)
		}

		if u.Payload != nil {
			signalKey := KeyPrefixSignal + tag
			signalData, err := json.Marshal(u.Payload)
			if err == nil {
				profit := u.Payload.ExecutionDirectives.EstimatedNetProfitUSDT
				if !math.IsNaN(profit) && !math.IsInf(profit, 0) && profit > 0 {
					pipe.ZAdd(ctx, KeySignalPriority, redis.Z{Score: profit, Member: tag})
					pipe.Expire(ctx, KeySignalPriority, SignalTTL+5*time.Second)
				}
				pipe.Set(ctx, signalKey, signalData, SignalTTL)
				pipe.Publish(ctx, ChannelSignalNotify, tag)
			}
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis batch exec: %w", err)
	}
	return nil
}

// GetExecutorStatus returns the executor's most recent self-reported status.
// found is false when the executor has never published (not running, or its
// last snapshot already expired), which callers must report as "OFFLINE"
// rather than as a failure.
func (c *Client) GetExecutorStatus(ctx context.Context) (raw string, found bool, err error) {
	if c == nil || c.rdb == nil {
		return "", false, nil
	}
	raw, err = c.rdb.Get(ctx, KeyExecutorStatus).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("redis get executor status: %w", err)
	}
	return raw, true, nil
}

// ListActivePositions retrieves all currently active positions stored in Redis.
func (c *Client) ListActivePositions(ctx context.Context) ([]domain.ActivePosition, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	keys, err := c.rdb.Keys(ctx, KeyPrefixActivePos+"*").Result()
	if err != nil {
		return nil, fmt.Errorf("redis keys active pos: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	var out []domain.ActivePosition
	for _, k := range keys {
		data, err := c.rdb.Get(ctx, k).Bytes()
		if err == nil {
			var pos domain.ActivePosition
			if json.Unmarshal(data, &pos) == nil && pos.Qty > 0 {
				out = append(out, pos)
			}
		}
	}
	return out, nil
}


 
// ListActiveOrderRecords reads the executor's durable-in-Redis active order
// registry. The index is maintained by UniversalOrderRegistry and excludes
// terminal orders. An unavailable index is an error, never an empty registry.
func (c *Client) ListActiveOrderRecords(ctx context.Context) ([]json.RawMessage, error) {
	if c == nil || c.rdb == nil { return nil, errors.New("redis is not connected") }
	ids, err := c.rdb.HKeys(ctx, KeyOrderRecoveryIndex).Result()
	if err != nil { return nil, fmt.Errorf("read active order registry index: %w", err) }
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		raw, getErr := c.rdb.Get(ctx, KeyPrefixOrderRecord+id).Bytes()
		if getErr != nil {
			if getErr == redis.Nil { continue }
			return nil, fmt.Errorf("read order record %s: %w", id, getErr)
		}
		if !json.Valid(raw) { return nil, fmt.Errorf("order record %s contains invalid JSON", id) }
		out = append(out, json.RawMessage(raw))
	}
	return out, nil
}

// GetOrderRecord reads one canonical local order ID, including terminal
// records retained outside the active-order index.
func (c *Client) GetOrderRecord(ctx context.Context, localOrderID string) (json.RawMessage, bool, error) {
	if c == nil || c.rdb == nil { return nil, false, errors.New("redis is not connected") }
	localOrderID = strings.TrimSpace(localOrderID)
	if localOrderID == "" { return nil, false, errors.New("local order ID is required") }
	raw, err := c.rdb.Get(ctx, KeyPrefixOrderRecord+localOrderID).Bytes()
	if err == redis.Nil { return nil, false, nil }
	if err != nil { return nil, false, fmt.Errorf("read order record %s: %w", localOrderID, err) }
	if !json.Valid(raw) { return nil, false, fmt.Errorf("order record %s contains invalid JSON", localOrderID) }
	return json.RawMessage(raw), true, nil
}

// ListOrphanOrderRecords returns every indexed exchange order that recovery
// could not safely identify. Redis errors are surfaced to avoid false-empty UI.
func (c *Client) ListOrphanOrderRecords(ctx context.Context) ([]json.RawMessage, error) {
	if c == nil || c.rdb == nil { return nil, errors.New("redis is not connected") }
	ids, err := c.rdb.HKeys(ctx, KeyOrphanOrderIndex).Result()
	if err != nil { return nil, fmt.Errorf("read orphan order index: %w", err) }
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		raw, getErr := c.rdb.Get(ctx, KeyPrefixOrphanOrder+id).Bytes()
		if getErr != nil {
			if getErr == redis.Nil { continue }
			return nil, fmt.Errorf("read orphan order %s: %w", id, getErr)
		}
		if !json.Valid(raw) { return nil, fmt.Errorf("orphan order %s contains invalid JSON", id) }
		out = append(out, json.RawMessage(raw))
	}
	return out, nil
}

// GetPendingOrderRecovery returns order IDs and reasons currently blocked by
// recovery. It is read-only and does not change the recovery gate.
func (c *Client) GetPendingOrderRecovery(ctx context.Context) (map[string]string, error) {
	if c == nil || c.rdb == nil { return nil, errors.New("redis is not connected") }
	values, err := c.rdb.HGetAll(ctx, KeyPendingOrderRecovery).Result()
	if err != nil { return nil, fmt.Errorf("read pending order recovery: %w", err) }
	return values, nil
}

// PublishForceClose broadcasts a force-close command to the Executor via Redis Pub/Sub.
func (c *Client) PublishForceClose(ctx context.Context, localEx, symbol, pairKey, requestedBy string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	if pairKey == "" {
		pairKey = CleanPairKey(localEx, symbol)
	}
	payload := map[string]interface{}{
		"action":         "FORCE_CLOSE",
		"local_exchange": localEx,
		"symbol":         symbol,
		"pair_key":       pairKey,
		"requested_by":   requestedBy,
		"timestamp_ms":   time.Now().UnixMilli(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal force close: %w", err)
	}
	return c.rdb.Publish(ctx, ChannelForceClose, string(data)).Err()
}

// PublishKillSwitch broadcasts an operator kill-switch directive (action "on"
// or "off") to the Executor via Redis Pub/Sub. ON halts all new entries until
// an explicit OFF; it never auto-resets on a UTC rollover.
func (c *Client) PublishKillSwitch(ctx context.Context, action, reason string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	if action != "on" && action != "off" {
		return fmt.Errorf("kill switch action must be \"on\" or \"off\", got %q", action)
	}
	if reason == "" {
		reason = "OPERATOR"
	}
	payload := map[string]interface{}{
		"action":       action,
		"reason":       reason,
		"requested_by": "telegram_user",
		"timestamp_ms": time.Now().UnixMilli(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal kill switch: %w", err)
	}
	return c.rdb.Publish(ctx, ChannelKillSwitch, string(data)).Err()
}

// SetSpotOrderLimit persists and broadcasts a per-spot-CEX open-order cap.
// The executor applies the update immediately and restores it after restart.
func (c *Client) SetSpotOrderLimit(ctx context.Context, exchange string, limit int) error {
	if c == nil || c.rdb == nil { return errors.New("redis is not connected") }
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	if exchange == "" || len(exchange) > 64 { return errors.New("exchange name is invalid") }
	for _, r := range exchange { if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-') { return errors.New("exchange name may contain only letters, digits, '_' or '-'") } }
	if limit < 1 || limit > 100 { return errors.New("max open spot orders must be between 1 and 100") }

	var requestBytes [16]byte
	if _, err := rand.Read(requestBytes[:]); err != nil {
		return fmt.Errorf("create spot order limit request id: %w", err)
	}
	requestID := hex.EncodeToString(requestBytes[:])
	replyKey := KeyPrefixSpotOrderLimitReply + requestID
	payload, err := json.Marshal(map[string]interface{}{
		"request_id": requestID,
		"exchange": exchange,
		"max_open_spot_orders": limit,
		"updated_at_ms": time.Now().UnixMilli(),
	})
	if err != nil { return fmt.Errorf("marshal spot order limit: %w", err) }
	if err := c.rdb.Publish(ctx, ChannelSpotOrderLimitUpdate, string(payload)).Err(); err != nil {
		return fmt.Errorf("publish spot order limit: %w", err)
	}
	reply, err := c.rdb.BLPop(ctx, 20*time.Second, replyKey).Result()
	if err != nil {
		return fmt.Errorf("executor did not confirm durable Spot capacity update: %w", err)
	}
	if len(reply) < 2 {
		return errors.New("executor returned an empty Spot capacity acknowledgement")
	}
	var acknowledgement struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal([]byte(reply[1]), &acknowledgement); err != nil {
		return fmt.Errorf("decode executor Spot capacity acknowledgement: %w", err)
	}
	if !acknowledgement.Success {
		if acknowledgement.Error == "" { acknowledgement.Error = "Postgres persistence or coordinator validation failed" }
		return fmt.Errorf("executor rejected Spot capacity update: %s", acknowledgement.Error)
	}
	return nil
}

// GetSpotOrderLimits returns operator overrides; venues without an override use default 2.
func (c *Client) GetSpotOrderLimits(ctx context.Context) (map[string]int, error) {
	if c == nil || c.rdb == nil { return nil, errors.New("redis is not connected") }
	stored, err := c.rdb.HGetAll(ctx, KeySpotOrderLimits).Result()
	if err != nil { return nil, fmt.Errorf("read spot order limits: %w", err) }
	limits := make(map[string]int, len(stored))
	for exchange, raw := range stored {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 1 || value > 100 { return nil, fmt.Errorf("stored spot order limit for %s is invalid", exchange) }
		limits[exchange] = value
	}
	return limits, nil
}

// GetLivePrice retrieves the latest PricingSnapshot for a pair.
func (c *Client) GetLivePrice(ctx context.Context, localEx, symbol string, globalPair ...string) (*domain.PricingSnapshot, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	key := KeyPrefixLivePrice + CleanPairKey(localEx, symbol, globalPair...)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap domain.PricingSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// DynamicFeeProfile is an authenticated, short-lived account fee snapshot
// published by the executor. Rates are percentages, not decimal fractions.
type DynamicFeeProfile struct {
	MakerFeePct  float64 `json:"maker_fee_pct"`
	TakerFeePct  float64 `json:"taker_fee_pct"`
	Source       string  `json:"source"`
	Verified     bool    `json:"verified"`
	FetchedAtMS  int64   `json:"fetched_at_ms"`
}

// GetDynamicFeeProfile returns only a fresh, verified executor fee snapshot.
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

func (c *Client) Close() error {
	if c != nil && c.rdb != nil {
		return c.rdb.Close()
	}
	return nil
}


type RecoveryRetryReply struct {
	RequestID string `json:"request_id"`
	Action string `json:"action"`
	Resolved bool `json:"resolved"`
	Error string `json:"error"`
	DeletedRedis  int               `json:"deleted_redis,omitempty"`
	DeletedDB     int               `json:"deleted_db,omitempty"`
	ClearedPairs  []string          `json:"cleared_pairs,omitempty"`
	Complete      bool              `json:"complete"`
	PendingOrders map[string]string `json:"pending_orders"`
	ScopeHalts    map[string]string `json:"scope_halts"`
	TimestampMS   int64             `json:"timestamp_ms"`
}

// RequestRecoveryRetry asks the executor to re-run read-only reconciliation.
func (c *Client) RequestRecoveryRetry(ctx context.Context) (RecoveryRetryReply, error) {
	return c.requestRecoveryAction(ctx, "retry", "", "")
}

// RequestOrphanResolution asks the executor to verify and resolve one orphan.
func (c *Client) RequestOrphanResolution(ctx context.Context, orphanID, pairTag string) (RecoveryRetryReply, error) {
	orphanID = strings.TrimSpace(orphanID)
	if orphanID == "" || len(orphanID) > 256 {
		return RecoveryRetryReply{}, errors.New("orphan id tidak valid")
	}
	return c.requestRecoveryAction(ctx, "resolve_orphan", orphanID, strings.TrimSpace(pairTag))
}

// RequestForceCleanOrphan asks the executor to delete an orphan/stuck order across Redis cache and Supabase DB.
func (c *Client) RequestForceCleanOrphan(ctx context.Context, orphanID string) (RecoveryRetryReply, error) {
	orphanID = strings.TrimSpace(orphanID)
	if orphanID == "" || len(orphanID) > 256 {
		return RecoveryRetryReply{}, errors.New("orphan id tidak valid")
	}
	return c.requestRecoveryAction(ctx, "force_clean_orphan", orphanID, "")
}

func (c *Client) requestRecoveryAction(ctx context.Context, action, orphanID, pairTag string) (RecoveryRetryReply, error) {
	if c == nil || c.rdb == nil {
		return RecoveryRetryReply{}, errors.New("redis is not connected")
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return RecoveryRetryReply{}, fmt.Errorf("generate recovery request id: %w", err)
	}
	requestID := hex.EncodeToString(idBytes)
	request := map[string]string{"request_id": requestID, "action": action}
	if orphanID != "" { request["orphan_id"] = orphanID }
	if pairTag != "" { request["pair_tag"] = pairTag }
	payload, err := json.Marshal(request)
	if err != nil {
		return RecoveryRetryReply{}, fmt.Errorf("marshal recovery request: %w", err)
	}
	if err := c.rdb.Publish(ctx, ChannelRecoveryRequest, payload).Err(); err != nil {
		return RecoveryRetryReply{}, fmt.Errorf("publish recovery request: %w", err)
	}
	key := KeyPrefixRecoveryReply + requestID
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(25 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			return RecoveryRetryReply{}, ctx.Err()
		case <-timeout.C:
			return RecoveryRetryReply{}, errors.New("executor tidak merespons permintaan reconciliation dalam 25 detik")
		case <-ticker.C:
			raw, err := c.rdb.Get(ctx, key).Result()
			if err == redis.Nil { continue }
			if err != nil { return RecoveryRetryReply{}, fmt.Errorf("read recovery response: %w", err) }
			var reply RecoveryRetryReply
			if err := json.Unmarshal([]byte(raw), &reply); err != nil {
				return RecoveryRetryReply{}, fmt.Errorf("decode recovery response: %w", err)
			}
			if reply.RequestID != requestID || reply.Action != action {
				return RecoveryRetryReply{}, errors.New("recovery response request identity mismatch")
			}
			return reply, nil
		}
	}
}
// RequestLiveBalances asks the running Executor to fetch fresh balances from both exchanges.
func (c *Client) RequestLiveBalances(ctx context.Context) (string, error) {
	if c == nil || c.rdb == nil { return "", errors.New("redis is not connected") }
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil { return "", fmt.Errorf("generate balance request id: %w", err) }
	requestID := hex.EncodeToString(idBytes)
	payload, err := json.Marshal(map[string]string{"request_id": requestID})
	if err != nil { return "", fmt.Errorf("marshal balance request: %w", err) }
	if err := c.rdb.Publish(ctx, ChannelBalanceRequest, payload).Err(); err != nil { return "", fmt.Errorf("publish balance request: %w", err) }
	key := KeyPrefixBalanceReply + requestID
	ticker := time.NewTicker(200 * time.Millisecond); defer ticker.Stop()
	timeout := time.NewTimer(15 * time.Second); defer timeout.Stop()
	for {
		select {
		case <-ctx.Done(): return "", ctx.Err()
		case <-timeout.C: return "", errors.New("executor tidak merespons permintaan saldo dalam 15 detik")
		case <-ticker.C:
			raw, err := c.rdb.Get(ctx, key).Result()
			if err == redis.Nil { continue }
			if err != nil { return "", fmt.Errorf("read balance response: %w", err) }
			return raw, nil
		}
	}
}
