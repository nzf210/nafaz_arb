package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type TelegramConfig struct {
	Enabled                bool    `json:"enabled"`
	BotToken               string  `json:"bot_token"`
	ChatID                 int64   `json:"chat_id"`
	AllowedChatIDs         []int64 `json:"allowed_chat_ids"`
	AllowedUserIDs         []int64 `json:"allowed_user_ids"`
	AlertOnNew             bool    `json:"alert_on_new"`
	AlertCooldownMinutes   int     `json:"alert_cooldown_minutes"`
	AlertMinNetROIPct      float64 `json:"alert_min_net_roi_pct"`
	AlertMinNetProfitQuote float64 `json:"alert_min_net_profit_quote"`
}

type RedisConfig struct {
	Enabled  bool   `json:"enabled"`
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type Config struct {
	Mode                          string         `json:"mode"`
	ExecutionMode                 string         `json:"execution_mode"`
	ListenAddr                    string         `json:"listen_addr"`
	APIAuthToken                  string         `json:"api_auth_token"`
	ScanIntervalSeconds           int            `json:"scan_interval_seconds"`
	QuoteAssets                   []string       `json:"quote_assets"`
	CheckOpenPositions            bool           `json:"check_open_positions"`
	ExcludeOpenOrders             bool           `json:"exclude_open_orders"`
	CapitalTotal                  float64        `json:"capital_total"`
	AllocationPerPair             float64        `json:"allocation_per_pair"`
	PerpLeverage                  float64        `json:"perp_leverage"`
	EstimatedHoldingHours         int            `json:"estimated_holding_hours"`
	MinNetROIPct                  float64        `json:"min_net_roi_pct"`
	MaxNetROIPct                  float64        `json:"max_net_roi_pct"`
	MinNetProfitQuote             float64        `json:"min_net_profit_quote"`
	MaxSlippagePct                float64        `json:"max_slippage_pct"`
	SpotTakerFeePct               float64        `json:"spot_taker_fee_pct"`
	SpotMakerFeePct               float64        `json:"spot_maker_fee_pct"`
	PerpTakerFeePct               float64        `json:"perp_taker_fee_pct"`
	// FeeAssetSurchargePct adds a conservative surcharge (in percent of
	// notional) for fee-asset effects that the simple rate model misses:
	// BNB discount changing the effective rate, fees billed in a non-quote
	// asset needing conversion, etc. 0 disables it.
	FeeAssetSurchargePct          float64        `json:"fee_asset_surcharge_pct"`
	MaxOrderTTLSeconds            int            `json:"max_order_ttl_seconds"`
	MaxOrderBookAgeSeconds        int            `json:"max_order_book_age_seconds"`
	MaxCrossExchangeSkewSeconds   int            `json:"max_cross_exchange_skew_seconds"`
	MinTopOfBookQuoteUSDT         float64        `json:"min_top_of_book_quote_usdt"`
	MaxOrderBookImbalance         float64        `json:"max_order_book_imbalance"`
	SpikeSpreadPct                float64        `json:"spike_spread_pct"`
	MinSpikeScans                 int            `json:"min_spike_scans"`
	MaxLastTradeAgeSeconds        int            `json:"max_last_trade_age_seconds"`
	FXRateSanityPct               float64        `json:"fx_rate_sanity_pct"`
	EnableWebSocketFeed           bool           `json:"enable_websocket_feed"`
	WebSocketBookMaxAgeSeconds    int            `json:"websocket_book_max_age_seconds"`
	ExchangeErrorWindowSeconds    int            `json:"exchange_error_window_seconds"`
	ExchangeErrorRateThreshold    float64        `json:"exchange_error_rate_threshold"`
	ExchangeErrorMinRequests      int            `json:"exchange_error_min_requests"`
	HistoryPath                   string         `json:"history_path"`
	HistoryRetentionDays          int            `json:"history_retention_days"`
	MaxMissedSuccessfulScans      int            `json:"max_missed_successful_scans"`
	EmptyBookFreezeHours          int            `json:"empty_book_freeze_hours"`
	EmptyBookFreezeMinutes        int            `json:"empty_book_freeze_minutes"`
	EnableAdaptiveSizing          *bool          `json:"enable_adaptive_sizing,omitempty"`
	MinAllocatedCapital           float64        `json:"min_allocated_capital"`
	EnableSyntheticIDR            bool           `json:"enable_synthetic_idr"`
	SyntheticUSDTIDRRate          float64        `json:"synthetic_usdt_idr_rate"`
	Redis                         RedisConfig    `json:"redis"`
	MinFundingRate                float64        `json:"min_funding_rate"`
	ExcludedSymbols               []string       `json:"excluded_symbols"`
	FundingPollingIntervalSeconds int            `json:"funding_polling_interval_seconds"`
	BaseTargetExitSpreadPct       float64        `json:"base_target_exit_spread_pct"`
	SnipingWindowMinutes          int            `json:"sniping_window_minutes"`
	FundingSubsidyFloorPct        float64        `json:"funding_subsidy_floor_pct"`
	LocalPriceTick                float64        `json:"local_price_tick"`
	GlobalFuturesExchange         string         `json:"global_futures_exchange"`
	EnableExecutorSignals         bool           `json:"enable_executor_signals"`
	Telegram                      TelegramConfig `json:"telegram"`
}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	c.setDefaults()
	c.applyEnvOverrides()
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c *Config) setDefaults() {
	c.Mode = defaultString(c.Mode, "MOCK")
	c.ExecutionMode = defaultString(c.ExecutionMode, "TAKER_TAKER")
	c.ListenAddr = defaultString(c.ListenAddr, ":8080")
	c.ScanIntervalSeconds = defaultInt(c.ScanIntervalSeconds, 30)
	c.MaxOrderTTLSeconds = defaultInt(c.MaxOrderTTLSeconds, 30)
	// Exchange maintenance detection: 0 window or 0 threshold disables it.
	c.ExchangeErrorWindowSeconds = defaultInt(c.ExchangeErrorWindowSeconds, 300)
	c.ExchangeErrorRateThreshold = defaultFloat(c.ExchangeErrorRateThreshold, 0.5)
	c.ExchangeErrorMinRequests = defaultInt(c.ExchangeErrorMinRequests, 5)
	c.WebSocketBookMaxAgeSeconds = defaultInt(c.WebSocketBookMaxAgeSeconds, 2)
	c.MaxCrossExchangeSkewSeconds = defaultInt(c.MaxCrossExchangeSkewSeconds, 1000)
	c.HistoryPath = defaultString(c.HistoryPath, "./data/opportunities.json")
	c.MaxMissedSuccessfulScans = defaultInt(c.MaxMissedSuccessfulScans, 2)
	c.EmptyBookFreezeHours = defaultInt(c.EmptyBookFreezeHours, 6)
	if c.EmptyBookFreezeMinutes <= 0 && c.EmptyBookFreezeHours == 6 {
		c.EmptyBookFreezeMinutes = 15
	}
	if c.EnableAdaptiveSizing == nil {
		t := true
		c.EnableAdaptiveSizing = &t
	}
	c.MinAllocatedCapital = defaultFloat(c.MinAllocatedCapital, 10.0)
	c.SyntheticUSDTIDRRate = defaultFloat(c.SyntheticUSDTIDRRate, 16000.0)
	c.Telegram.AlertCooldownMinutes = defaultInt(c.Telegram.AlertCooldownMinutes, 15)
	c.PerpLeverage = defaultFloat(c.PerpLeverage, 1)
	c.FundingPollingIntervalSeconds = defaultInt(c.FundingPollingIntervalSeconds, 30)
	c.BaseTargetExitSpreadPct = defaultFloat(c.BaseTargetExitSpreadPct, 0.3)
	c.SnipingWindowMinutes = defaultInt(c.SnipingWindowMinutes, 30)
	c.FundingSubsidyFloorPct = defaultFloat(c.FundingSubsidyFloorPct, 0.08)
	c.LocalPriceTick = defaultFloat(c.LocalPriceTick, 1.0)
	c.GlobalFuturesExchange = defaultString(c.GlobalFuturesExchange, "binance")
	c.Redis.Addr = defaultString(c.Redis.Addr, "localhost:6380")
}

func defaultString(val, fallback string) string {
	if val == "" {
		return fallback
	}
	return val
}

func defaultInt(val, fallback int) int {
	if val <= 0 {
		return fallback
	}
	return val
}

func defaultFloat(val, fallback float64) float64 {
	if val <= 0 {
		return fallback
	}
	return val
}

// AdaptiveSizingEnabled returns whether adaptive position sizing is enabled.
func (c *Config) AdaptiveSizingEnabled() bool {
	return c.EnableAdaptiveSizing == nil || *c.EnableAdaptiveSizing
}

func (c *Config) validate() error {
	if strings.EqualFold(c.Mode, "LIVE") && strings.TrimSpace(c.APIAuthToken) == "" {
		return fmt.Errorf("api_auth_token is required when mode=LIVE")
	}
	if c.ExecutionMode != "" && c.ExecutionMode != "TAKER_TAKER" && c.ExecutionMode != "MAKER_TAKER" {
		return fmt.Errorf("execution_mode must be TAKER_TAKER or MAKER_TAKER")
	}
	if c.SpotMakerFeePct < 0 {
		return fmt.Errorf("spot_maker_fee_pct cannot be negative")
	}
	if c.FeeAssetSurchargePct < 0 || c.FeeAssetSurchargePct > 100 {
		return fmt.Errorf("fee_asset_surcharge_pct must be in [0,100]")
	}
	if c.MaxOrderTTLSeconds < 0 {
		return fmt.Errorf("max_order_ttl_seconds cannot be negative")
	}
	if c.MaxOrderBookAgeSeconds < 0 {
		return fmt.Errorf("max_order_book_age_seconds cannot be negative")
	}
	if c.WebSocketBookMaxAgeSeconds < 0 {
		return fmt.Errorf("websocket_book_max_age_seconds cannot be negative")
	}
	if c.MaxCrossExchangeSkewSeconds < 0 {
		return fmt.Errorf("max_cross_exchange_skew_seconds cannot be negative")
	}
	if c.MinTopOfBookQuoteUSDT < 0 {
		return fmt.Errorf("min_top_of_book_quote_usdt cannot be negative")
	}
	if c.MaxOrderBookImbalance < 0 || c.MaxOrderBookImbalance > 1 {
		return fmt.Errorf("max_order_book_imbalance must be in [0,1]")
	}
	if c.SpikeSpreadPct < 0 {
		return fmt.Errorf("spike_spread_pct cannot be negative")
	}
	if c.MinSpikeScans < 0 {
		return fmt.Errorf("min_spike_scans cannot be negative")
	}
	if c.MaxLastTradeAgeSeconds < 0 {
		return fmt.Errorf("max_last_trade_age_seconds cannot be negative")
	}
	if c.FXRateSanityPct < 0 || c.FXRateSanityPct > 100 {
		return fmt.Errorf("fx_rate_sanity_pct must be in [0,100]")
	}
	if c.ExchangeErrorWindowSeconds < 0 {
		return fmt.Errorf("exchange_error_window_seconds cannot be negative")
	}
	if c.ExchangeErrorRateThreshold < 0 || c.ExchangeErrorRateThreshold > 1 {
		return fmt.Errorf("exchange_error_rate_threshold must be in [0,1]")
	}
	if c.ExchangeErrorMinRequests < 0 {
		return fmt.Errorf("exchange_error_min_requests cannot be negative")
	}
	if c.EmptyBookFreezeMinutes < 0 {
		return fmt.Errorf("empty_book_freeze_minutes cannot be negative")
	}
	if c.MinAllocatedCapital < 0 {
		return fmt.Errorf("min_allocated_capital cannot be negative")
	}
	if c.SyntheticUSDTIDRRate < 0 {
		return fmt.Errorf("synthetic_usdt_idr_rate cannot be negative")
	}
	if c.AllocationPerPair <= 0 || c.AllocationPerPair > 1 {
		return fmt.Errorf("allocation_per_pair must be in (0,1]")
	}
	if c.CapitalTotal <= 0 {
		return fmt.Errorf("capital_total must be positive")
	}
	if c.MinNetROIPct > c.MaxNetROIPct {
		return fmt.Errorf("min_net_roi_pct must not exceed max_net_roi_pct")
	}
	if c.EstimatedHoldingHours < 0 {
		return fmt.Errorf("estimated_holding_hours cannot be negative")
	}
	if c.MinFundingRate < 0 {
		return fmt.Errorf("min_funding_rate cannot be negative")
	}
	if c.Telegram.AlertMinNetROIPct < 0 {
		return fmt.Errorf("telegram.alert_min_net_roi_pct cannot be negative")
	}
	if c.Telegram.AlertMinNetProfitQuote < 0 {
		return fmt.Errorf("telegram.alert_min_net_profit_quote cannot be negative")
	}
	return nil
}

func setEnvString(target *string, key string) {
	if v := os.Getenv(key); v != "" {
		*target = v
	}
}

func setEnvBool(target *bool, key string) {
	if v := os.Getenv(key); v != "" {
		*target = v == "true" || v == "1"
	}
}

func setEnvInt(target *int, key string) {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*target = n
		}
	}
}

func setEnvInt64(target *int64, key string) {
	if v := os.Getenv(key); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			*target = id
		}
	}
}

func setEnvFloat(target *float64, key string) {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			*target = f
		}
	}
}

func (c *Config) applyEnvOverrides() {
	setEnvString(&c.APIAuthToken, "API_AUTH_TOKEN")
	setEnvString(&c.ExecutionMode, "EXECUTION_MODE")
	if v := os.Getenv("ENABLE_ADAPTIVE_SIZING"); v != "" {
		b := v == "true" || v == "1"
		c.EnableAdaptiveSizing = &b
	}
	setEnvFloat(&c.MinAllocatedCapital, "MIN_ALLOCATED_CAPITAL")
	setEnvInt(&c.EmptyBookFreezeMinutes, "EMPTY_BOOK_FREEZE_MINUTES")
	setEnvBool(&c.EnableSyntheticIDR, "ENABLE_SYNTHETIC_IDR")
	setEnvFloat(&c.SyntheticUSDTIDRRate, "SYNTHETIC_USDT_IDR_RATE")
	if tok := os.Getenv("TELEGRAM_BOT_TOKEN"); tok != "" {
		c.Telegram.BotToken = tok
		c.Telegram.Enabled = true
	}
	setEnvInt64(&c.Telegram.ChatID, "TELEGRAM_CHAT_ID")
	setEnvBool(&c.Telegram.Enabled, "TELEGRAM_ENABLED")
	setEnvBool(&c.Redis.Enabled, "REDIS_ENABLED")
	setEnvString(&c.Redis.Addr, "REDIS_ADDR")
	setEnvString(&c.Redis.Password, "REDIS_PASSWORD")
	setEnvInt(&c.Redis.DB, "REDIS_DB")
	setEnvFloat(&c.MinFundingRate, "MIN_FUNDING_RATE")
	setEnvFloat(&c.FeeAssetSurchargePct, "FEE_ASSET_SURCHARGE_PCT")
	setEnvFloat(&c.BaseTargetExitSpreadPct, "BASE_TARGET_EXIT_SPREAD_PCT")
	setEnvFloat(&c.MinNetROIPct, "MIN_NET_ROI_PCT")
	setEnvFloat(&c.MinTopOfBookQuoteUSDT, "MIN_TOP_OF_BOOK_QUOTE_USDT")
	setEnvFloat(&c.MaxOrderBookImbalance, "MAX_ORDER_BOOK_IMBALANCE")
	setEnvFloat(&c.SpikeSpreadPct, "SPIKE_SPREAD_PCT")
	setEnvInt(&c.MinSpikeScans, "MIN_SPIKE_SCANS")
	setEnvInt(&c.MaxLastTradeAgeSeconds, "MAX_LAST_TRADE_AGE_SECONDS")
	setEnvFloat(&c.FXRateSanityPct, "FX_RATE_SANITY_PCT")
	setEnvBool(&c.EnableWebSocketFeed, "ENABLE_WEBSOCKET_FEED")
	setEnvInt(&c.WebSocketBookMaxAgeSeconds, "WEBSOCKET_BOOK_MAX_AGE_SECONDS")
	setEnvString(&c.GlobalFuturesExchange, "GLOBAL_FUTURES_EXCHANGE")
	setEnvBool(&c.EnableExecutorSignals, "ENABLE_EXECUTOR_SIGNALS")
	setEnvSlice(&c.ExcludedSymbols, "EXCLUDED_SYMBOLS")
}

func setEnvSlice(target *[]string, key string) {
	if v := os.Getenv(key); v != "" {
		parts := strings.Split(v, ",")
		var cleaned []string
		for _, p := range parts {
			if s := strings.TrimSpace(p); s != "" {
				cleaned = append(cleaned, s)
			}
		}
		if len(cleaned) > 0 {
			*target = cleaned
		}
	}
}
