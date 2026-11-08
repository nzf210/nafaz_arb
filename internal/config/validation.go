package config

import (
	"fmt"
	"strings"
)

func (c *Config) validate() error {
	validators := []func() error{
		c.validateExecutionSettings,
		c.validateFeeSettings,
		c.validateOrderBookSettings,
		c.validateMarketQualitySettings,
		c.validateCapitalSettings,
		c.validateTelegramSettings,
	}
	for _, validate := range validators {
		if err := validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateExecutionSettings() error {
	if strings.EqualFold(c.Mode, "LIVE") && len(strings.TrimSpace(c.APIAuthToken)) < 32 {
		return fmt.Errorf("api_auth_token must contain at least 32 characters when mode=LIVE")
	}
	if c.ExecutionMode != "" && c.ExecutionMode != "TAKER_TAKER" && c.ExecutionMode != "MAKER_TAKER" {
		return fmt.Errorf("execution_mode must be TAKER_TAKER or MAKER_TAKER")
	}
	return nil
}

func (c *Config) validateFeeSettings() error {
	for exchange, profile := range c.FeeProfiles {
		fees := map[string]*float64{
			"spot_maker_fee_pct": profile.SpotMakerFeePct,
			"spot_taker_fee_pct": profile.SpotTakerFeePct,
			"perp_maker_fee_pct": profile.PerpMakerFeePct,
			"perp_taker_fee_pct": profile.PerpTakerFeePct,
		}
		for name, value := range fees {
			if value != nil && (*value < 0 || *value > 100) {
				return fmt.Errorf("fee_profiles.%s.%s must be in [0,100]", exchange, name)
			}
		}
	}
	if c.SpotMakerFeePct < 0 {
		return fmt.Errorf("spot_maker_fee_pct cannot be negative")
	}
	if c.FeeAssetSurchargePct < 0 || c.FeeAssetSurchargePct > 100 {
		return fmt.Errorf("fee_asset_surcharge_pct must be in [0,100]")
	}
	return nil
}

func (c *Config) validateOrderBookSettings() error {
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
	return nil
}

func (c *Config) validateMarketQualitySettings() error {
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
	return nil
}

func (c *Config) validateCapitalSettings() error {
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
	return nil
}

func (c *Config) validateTelegramSettings() error {
	if c.Telegram.AlertMinNetROIPct < 0 {
		return fmt.Errorf("telegram.alert_min_net_roi_pct cannot be negative")
	}
	if c.Telegram.AlertMinNetProfitQuote < 0 {
		return fmt.Errorf("telegram.alert_min_net_profit_quote cannot be negative")
	}
	return nil
}
