package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadTelegramAlertDefaultsAndThresholds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Telegram.AlertCooldownMinutes != 15 {
		t.Fatalf("alert cooldown=%d, want default 15", cfg.Telegram.AlertCooldownMinutes)
	}
	if cfg.Telegram.AlertMinNetROIPct != 0 || cfg.Telegram.AlertMinNetProfitQuote != 0 {
		t.Fatalf("alert thresholds should default to disabled: %+v", cfg.Telegram)
	}

	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"telegram":{
			"alert_cooldown_minutes":7,
			"alert_min_net_roi_pct":1.25,
			"alert_min_net_profit_quote":3
		}
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Telegram.AlertCooldownMinutes != 7 ||
		cfg.Telegram.AlertMinNetROIPct != 1.25 ||
		cfg.Telegram.AlertMinNetProfitQuote != 3 {
		t.Fatalf("unexpected Telegram alert settings: %+v", cfg.Telegram)
	}
}

func TestLoadRejectsNegativeAlertThreshold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"telegram":{"alert_min_net_roi_pct":-0.1}
	}`), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "alert_min_net_roi_pct") {
		t.Fatalf("Load error=%v, want negative alert ROI validation error", err)
	}
}

func TestExecutionModeDefaultsAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExecutionMode != "TAKER_TAKER" {
		t.Fatalf("execution_mode=%s, want default TAKER_TAKER", cfg.ExecutionMode)
	}
	if cfg.MaxOrderTTLSeconds != 30 {
		t.Fatalf("max_order_ttl_seconds=%d, want default 30", cfg.MaxOrderTTLSeconds)
	}

	// Test valid MAKER_TAKER
	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"execution_mode":"MAKER_TAKER",
		"spot_maker_fee_pct":0.1,
		"max_order_ttl_seconds":45
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExecutionMode != "MAKER_TAKER" || cfg.SpotMakerFeePct != 0.1 || cfg.MaxOrderTTLSeconds != 45 {
		t.Fatalf("unexpected settings: %+v", cfg)
	}

	// Test invalid execution mode
	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"execution_mode":"INVALID"
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = Load(path)
	if err == nil || !strings.Contains(err.Error(), "execution_mode") {
		t.Fatalf("want error for invalid execution_mode, got %v", err)
	}
}

func TestOptimizationFeaturesDefaultsAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AdaptiveSizingEnabled() {
		t.Fatal("expected adaptive sizing enabled by default")
	}
	if cfg.MinAllocatedCapital != 10.0 {
		t.Fatalf("min_allocated_capital=%f, want 10.0", cfg.MinAllocatedCapital)
	}
	if cfg.EmptyBookFreezeMinutes != 15 {
		t.Fatalf("empty_book_freeze_minutes=%d, want 15", cfg.EmptyBookFreezeMinutes)
	}
	if cfg.EnableSyntheticIDR {
		t.Fatal("expected enable_synthetic_idr false by default")
	}
	if cfg.SyntheticUSDTIDRRate != 16000.0 {
		t.Fatalf("synthetic_usdt_idr_rate=%f, want 16000.0", cfg.SyntheticUSDTIDRRate)
	}

	// Test custom values and disabling adaptive sizing
	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"enable_adaptive_sizing":false,
		"min_allocated_capital":25.0,
		"empty_book_freeze_minutes":30,
		"enable_synthetic_idr":true,
		"synthetic_usdt_idr_rate":15800.5
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AdaptiveSizingEnabled() {
		t.Fatal("expected adaptive sizing to be disabled")
	}
	if cfg.MinAllocatedCapital != 25.0 || cfg.EmptyBookFreezeMinutes != 30 || !cfg.EnableSyntheticIDR || cfg.SyntheticUSDTIDRRate != 15800.5 {
		t.Fatalf("unexpected custom values: %+v", cfg)
	}
}

func TestMinFundingRateAndExcludedSymbols(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	// Test default is 0 when omitted
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinFundingRate != 0 {
		t.Fatalf("expected MinFundingRate=0 by default, got %f", cfg.MinFundingRate)
	}

	// Test explicit 0 remains 0 and excluded symbols loaded
	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"min_funding_rate": 0,
		"excluded_symbols": ["META", "LPT/USDT"]
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinFundingRate != 0 {
		t.Fatalf("expected MinFundingRate=0, got %f", cfg.MinFundingRate)
	}
	if len(cfg.ExcludedSymbols) != 2 || cfg.ExcludedSymbols[0] != "META" || cfg.ExcludedSymbols[1] != "LPT/USDT" {
		t.Fatalf("unexpected excluded symbols: %v", cfg.ExcludedSymbols)
	}

	// Test negative min_funding_rate rejected
	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"min_funding_rate": -0.001
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = Load(path)
	if err == nil || !strings.Contains(err.Error(), "min_funding_rate") {
		t.Fatalf("expected error for negative min_funding_rate, got %v", err)
	}

	// Test env override
	t.Setenv("EXCLUDED_SYMBOLS", "BTC, ETH,SOL")
	t.Setenv("MIN_FUNDING_RATE", "0.0001")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinFundingRate != 0.0001 {
		t.Fatalf("expected MinFundingRate=0.0001 from env, got %f", cfg.MinFundingRate)
	}
	if len(cfg.ExcludedSymbols) != 3 || cfg.ExcludedSymbols[0] != "BTC" || cfg.ExcludedSymbols[2] != "SOL" {
		t.Fatalf("unexpected excluded symbols from env: %v", cfg.ExcludedSymbols)
	}
}

func TestExchangeHealthDefaultsAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExchangeErrorWindowSeconds != 300 {
		t.Fatalf("window=%d, want default 300", cfg.ExchangeErrorWindowSeconds)
	}
	if cfg.ExchangeErrorRateThreshold != 0.5 {
		t.Fatalf("threshold=%v, want default 0.5", cfg.ExchangeErrorRateThreshold)
	}
	if cfg.ExchangeErrorMinRequests != 5 {
		t.Fatalf("min requests=%d, want default 5", cfg.ExchangeErrorMinRequests)
	}

	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"exchange_error_window_seconds":60,
		"exchange_error_rate_threshold":0.75,
		"exchange_error_min_requests":3
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExchangeErrorWindowSeconds != 60 || cfg.ExchangeErrorRateThreshold != 0.75 || cfg.ExchangeErrorMinRequests != 3 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}

	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"exchange_error_rate_threshold":1.5
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "exchange_error_rate_threshold") {
		t.Fatalf("expected threshold validation error, got %v", err)
	}

	// Negative values are coerced to defaults by setDefaults, so exercise the
	// guard the way a programmatically built Config would hit it.
	invalid := cfg
	invalid.ExchangeErrorWindowSeconds = -1
	if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "exchange_error_window_seconds") {
		t.Fatalf("expected window validation error, got %v", err)
	}
}

func TestTopOfBookLiquidityConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinTopOfBookQuoteUSDT != 0 {
		t.Fatalf("liquidity filter must default to disabled, got %v", cfg.MinTopOfBookQuoteUSDT)
	}

	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"min_top_of_book_quote_usdt":250
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinTopOfBookQuoteUSDT != 250 {
		t.Fatalf("override not applied, got %v", cfg.MinTopOfBookQuoteUSDT)
	}

	// setDefaults coerces negatives to 0 ("disabled"), so hit validate() directly
	// the way a programmatically built Config would.
	invalid := cfg
	invalid.MinTopOfBookQuoteUSDT = -1
	if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "min_top_of_book_quote_usdt") {
		t.Fatalf("expected liquidity validation error, got %v", err)
	}
}

func TestWebSocketFeedConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EnableWebSocketFeed {
		t.Fatal("websocket feed must default to off")
	}
	if cfg.WebSocketBookMaxAgeSeconds != 2 {
		t.Fatalf("max age=%d, want default 2", cfg.WebSocketBookMaxAgeSeconds)
	}

	if err := os.WriteFile(path, []byte(`{
		"capital_total":100,
		"allocation_per_pair":0.5,
		"enable_websocket_feed":true,
		"websocket_book_max_age_seconds":5
	}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.EnableWebSocketFeed || cfg.WebSocketBookMaxAgeSeconds != 5 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}

	invalid := cfg
	invalid.WebSocketBookMaxAgeSeconds = -1
	if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "websocket_book_max_age_seconds") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestFeeAssetSurchargeValidation(t *testing.T) {
	cfg := Config{FeeAssetSurchargePct: -1}
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "fee_asset_surcharge_pct") {
		t.Fatalf("expected error for negative fee_asset_surcharge_pct, got %v", err)
	}
	cfg = Config{FeeAssetSurchargePct: 101}
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "fee_asset_surcharge_pct") {
		t.Fatalf("expected error for fee_asset_surcharge_pct > 100, got %v", err)
	}
	cfg = Config{FeeAssetSurchargePct: 0.5, AllocationPerPair: 0.5, PerpLeverage: 1, CapitalTotal: 100}
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected 0.5 to be valid, got %v", err)
	}
}


func TestLiveModeRejectsWeakAPIAuthToken(t *testing.T) {
    path := filepath.Join(t.TempDir(), "config.json")
    if err := os.WriteFile(path, []byte(`{
        "mode":"LIVE",
        "api_auth_token":"short-token",
        "capital_total":100,
        "allocation_per_pair":0.5
    }`), 0600); err != nil {
        t.Fatal(err)
    }
    if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "32 characters") {
        t.Fatalf("expected weak token validation error, got %v", err)
    }
}

func TestLiveModeRequiresAPIAuthToken(t *testing.T) {
    path := filepath.Join(t.TempDir(), "config.json")
    if err := os.WriteFile(path, []byte(`{
        "mode":"LIVE",
        "capital_total":100,
        "allocation_per_pair":0.5
    }`), 0600); err != nil {
        t.Fatal(err)
    }
    if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "api_auth_token") {
        t.Fatalf("expected live API token validation error, got %v", err)
    }

    token := strings.Repeat("x", 32)
    validConfig := fmt.Sprintf(`{
        "mode":"LIVE",
        "api_auth_token":%q,
        "capital_total":100,
        "allocation_per_pair":0.5
    }`, token)
    if err := os.WriteFile(path, []byte(validConfig), 0600); err != nil {
        t.Fatal(err)
    }
    cfg, err := Load(path)
    if err != nil {
        t.Fatal(err)
    }
    if cfg.APIAuthToken != token {
        t.Fatalf("APIAuthToken=%q, want configured token", cfg.APIAuthToken)
    }
}


func TestExchangeRoleAllowlistDefaultsAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"capital_total":100,"allocation_per_pair":0.5}`), 0600); err != nil { t.Fatal(err) }
	cfg, err := Load(path)
	if err != nil { t.Fatal(err) }
	if cfg.GlobalFuturesExchange != "all" { t.Fatalf("global futures default=%q, want all", cfg.GlobalFuturesExchange) }
	if len(cfg.EnabledSpotExchanges) != 2 || cfg.EnabledSpotExchanges[0] != "indodax" || cfg.EnabledSpotExchanges[1] != "tokocrypto" {
		t.Fatalf("unexpected local spot defaults: %v", cfg.EnabledSpotExchanges)
	}
	invalid := Config{CapitalTotal: 100, AllocationPerPair: 0.5, EnabledSpotExchanges: []string{"mexc"}}
	if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "enabled_spot_exchanges") {
		t.Fatalf("expected foreign Spot venue rejection, got %v", err)
	}
	invalid = Config{CapitalTotal: 100, AllocationPerPair: 0.5, GlobalFuturesExchange: "tokocrypto"}
	if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "global_futures_exchange") {
		t.Fatalf("expected local Futures venue rejection, got %v", err)
	}
	valid := Config{CapitalTotal: 100, AllocationPerPair: 0.5, PerpLeverage: 1, GlobalFuturesExchange: "mexc,okx,binance,bybit", EnabledSpotExchanges: []string{"indodax", "tokocrypto"}}
	if err := valid.validate(); err != nil { t.Fatalf("valid six-venue role configuration rejected: %v", err) }
}
