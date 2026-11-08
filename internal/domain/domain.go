package domain

import "time"

type Market struct {
	Exchange     string  `json:"exchange"`
	Symbol       string  `json:"symbol"`
	BaseAsset    string  `json:"base_asset"`
	QuoteAsset   string  `json:"quote_asset"`
	MarketType   string  `json:"market_type"`
	Active       bool    `json:"active"`
	ContractSize float64 `json:"contract_size,omitempty"`
	TickSize     float64 `json:"tick_size,omitempty"`
	LotSize      float64 `json:"lot_size,omitempty"`
	MinOrder     float64 `json:"min_order,omitempty"`
}

type PriceLevel struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"` // base-asset quantity
}

type OrderBook struct {
	Exchange   string       `json:"exchange"`
	Symbol     string       `json:"symbol"`
	Bids       []PriceLevel `json:"bids"`
	Asks       []PriceLevel `json:"asks"`
	Timestamp  time.Time    `json:"timestamp"`
	SequenceID int64        `json:"sequence_id,omitempty"`
	ReceivedAt time.Time    `json:"received_at,omitempty"`
	Source     string       `json:"source,omitempty"`
	Checksum   string       `json:"checksum,omitempty"`
}

type FundingRate struct {
	Rate            float64   `json:"rate"` // decimal rate per settlement
	IntervalHours   int       `json:"interval_hours"`
	Timestamp       time.Time `json:"timestamp"`
	NextFundingTime time.Time `json:"next_funding_time,omitempty"`
}

type OpenExposure struct {
	Exchange   string  `json:"exchange"`
	Symbol     string  `json:"symbol"`
	BaseAsset  string  `json:"base_asset"`
	QuoteAsset string  `json:"quote_asset"`
	MarketType string  `json:"market_type"` // SPOT or PERPETUAL
	Kind       string  `json:"kind"`        // POSITION or OPEN_ORDER
	Quantity   float64 `json:"quantity"`
	Active     bool    `json:"active"`
	Managed    bool    `json:"managed"` // linked to the scanner's managed trade registry
}

type Opportunity struct {
	OpportunityKey         string    `json:"opportunity_key"`
	SpotExchange           string    `json:"spot_exchange"`
	PerpExchange           string    `json:"perp_exchange"`
	BaseAsset              string    `json:"base_asset"`
	QuoteAsset             string    `json:"quote_asset"`
	SpotSymbol             string    `json:"spot_symbol"`
	PerpSymbol             string    `json:"perp_symbol"`
	ExecutionMode          string    `json:"execution_mode,omitempty"`
	SpotEntryPrice         float64   `json:"spot_entry_price"`
	SpotExitPrice          float64   `json:"spot_exit_price"`
	PerpEntryPrice         float64   `json:"perp_entry_price"`
	PerpExitPrice          float64   `json:"perp_exit_price"`
	Quantity               float64   `json:"quantity"`
	SpreadPnL              float64   `json:"spread_pnl"`
	FundingPnL             float64   `json:"funding_pnl"`
	TradingFees            float64   `json:"trading_fees"`
	Slippage               float64   `json:"slippage"`
	NetProfit              float64   `json:"net_profit"`
	AllocatedCapital       float64   `json:"allocated_capital"`
	NetROIPct              float64   `json:"net_roi_pct"`
	ProfitabilityStatus    string    `json:"profitability_status"`
	TradingEligible        bool      `json:"trading_eligible"`
	ExclusionReasons       []string  `json:"exclusion_reasons,omitempty"`
	LastScannedAt          time.Time `json:"last_scanned_at"`
	SpotTargetLimitPrice   float64   `json:"spot_target_limit_price,omitempty"`
	MinAcceptablePerpPrice float64   `json:"min_acceptable_perp_price,omitempty"`
	SpotQueueDepth         float64   `json:"spot_queue_depth,omitempty"`
	MaxOrderTTLSeconds     int       `json:"max_order_ttl_seconds,omitempty"`
	IsSynthetic            bool      `json:"is_synthetic,omitempty"`
	SyntheticFXRate        float64   `json:"synthetic_fx_rate,omitempty"`
	OriginalSpotQuote      string    `json:"original_spot_quote,omitempty"`
	CapacityPct            float64   `json:"capacity_pct,omitempty"`
	LocalPriceTick         float64   `json:"local_price_tick,omitempty"`
	TargetLimitBuyIDR      float64   `json:"target_limit_buy_idr,omitempty"`
	// Order book imbalance in [-1,1] for the spot and perp books at the traded
	// size. Positive means bid-heavy (buying pressure). Advisory only: it is
	// reported so operators can judge fill risk, and never gates eligibility.
	SpotOBI float64 `json:"spot_obi,omitempty"`
	PerpOBI float64 `json:"perp_obi,omitempty"`
	EntrySpreadPct         float64   `json:"entry_spread_pct,omitempty"`
	MinEntrySpreadPct      float64   `json:"min_entry_spread_pct,omitempty"`
	BaseTargetExitSpreadPct float64  `json:"base_target_exit_spread_pct,omitempty"`
	CancelIfGlobalBidDropsBelowUSDT float64 `json:"cancel_if_global_bid_drops_below_usdt,omitempty"`
	IsSnipingWindow        bool      `json:"is_sniping_window,omitempty"`
	MinutesToNextFunding   float64   `json:"minutes_to_next_funding,omitempty"`
	CurrentFundingRate     float64   `json:"current_funding_rate,omitempty"`
}

type ScanResult struct {
	ScanID                    string                `json:"scan_id"`
	Mode                      string                `json:"mode"`
	StartedAt                 time.Time             `json:"started_at"`
	CompletedAt               time.Time             `json:"completed_at"`
	DurationMS                int64                 `json:"duration_ms"`
	MarketDiscoveryDurationMS int64                 `json:"market_discovery_duration_ms"`
	ExposureCheckDurationMS   int64                 `json:"exposure_check_duration_ms"`
	PairEvaluationDurationMS  int64                 `json:"pair_evaluation_duration_ms"`
	ExchangeMetrics           []ExchangeScanMetrics `json:"exchange_metrics,omitempty"`
	Complete                  bool                  `json:"complete"`
	TotalMarkets              int                   `json:"total_markets"`
	MatchedPairs              int                   `json:"matched_pairs"`
	ActivePairs               int                   `json:"active_pairs"`
	FrozenPairs               int                   `json:"frozen_pairs"`
	EligiblePairs             int                   `json:"eligible_pairs"`
	ExcludedPairs             int                   `json:"excluded_pairs"`
	FailedPairs               int                   `json:"failed_pairs"`
	Opportunities             []Opportunity         `json:"opportunities"`
	Errors                    []string              `json:"errors,omitempty"`
}

type ExchangeScanMetrics struct {
	Exchange       string `json:"exchange"`
	Requests       int    `json:"requests"`
	Errors         int    `json:"errors"`
	DurationMicros int64  `json:"duration_micros"`
}

type PricingSnapshot struct {
	LocalBestBid      float64 `json:"local_best_bid"`
	LocalBestAsk      float64 `json:"local_best_ask"`
	GlobalBestBidUSDT float64 `json:"global_best_bid_usdt"`
	GlobalBestAskUSDT float64 `json:"global_best_ask_usdt"`
	USDTIDRRate       float64 `json:"usdt_idr_rate"`
	TimestampMS       int64   `json:"timestamp_ms"`
}

type FundingMetrics struct {
	CurrentFundingRate   float64 `json:"current_funding_rate"`
	NextFundingTimeMS    int64   `json:"next_funding_time_ms"`
	MinutesToNextFunding float64 `json:"minutes_to_next_funding"`
	IsSnipingWindow      bool    `json:"is_sniping_window"`
}

type PairInfo struct {
	LocalExchange  string  `json:"local_exchange"`
	GlobalExchange string  `json:"global_exchange"`
	LocalSymbol    string  `json:"local_symbol"`
	GlobalSymbol   string  `json:"global_symbol"`
	BaseAsset      string  `json:"base_asset"`
	QuoteAsset     string  `json:"quote_asset"`
	LocalPriceTick float64 `json:"local_price_tick"`
	LocalLotSize   float64 `json:"local_lot_size,omitempty"`
}

type ExecutionDirectives struct {
	Action                          string  `json:"action"` // "PLACE_MAKER_BUY"
	RecommendedLimitBuyIDR          float64 `json:"recommended_limit_buy_idr"`
	EntrySpreadPct                  float64 `json:"entry_spread_pct"`
	MinEntrySpreadPct               float64 `json:"min_entry_spread_pct"`
	BaseTargetExitSpreadPct         float64 `json:"base_target_exit_spread_pct"`
	MaxPositionUSDT                 float64 `json:"max_position_usdt"`
	CancelIfGlobalBidDropsBelowUSDT float64 `json:"cancel_if_global_bid_drops_below_usdt"`
	// True: this payload was validated against live orderbook data in the same
	// scan that published it. Lets the executor distinguish a fresh quote from
	// a replayed Redis key.
	ScannerValidated                bool    `json:"scanner_validated"`
}

type ExecutorPayload struct {
	PairInfo            PairInfo            `json:"pair_info"`
	PricingSnapshot     PricingSnapshot     `json:"pricing_snapshot"`
	FundingMetrics      FundingMetrics      `json:"funding_metrics"`
	ExecutionDirectives ExecutionDirectives `json:"execution_directives"`
}

type ActivePosition struct {
	LocalExchange            string  `json:"local_exchange"`
	GlobalExchange           string  `json:"global_exchange"`
	Symbol                   string  `json:"symbol"`
	EntryPriceIDR            float64 `json:"entry_price_idr"`
	ShortEntryUSDT           float64 `json:"short_entry_usdt"`
	Qty                      float64 `json:"qty"`
	AccumulatedFundingUSDT   float64 `json:"accumulated_funding_usdt"`
	LastFundingTimeMS        int64   `json:"last_funding_time_ms,omitempty"`
	BaseTargetExitSpreadPct  float64 `json:"base_target_exit_spread_pct,omitempty"`
	CurrentFundingRate       float64 `json:"current_funding_rate,omitempty"`
	UpdatedAt                int64   `json:"updated_at,omitempty"`
	CurrentSpotBidIDR        float64 `json:"current_spot_bid_idr,omitempty"`
	CurrentPerpAskUSDT       float64 `json:"current_perp_ask_usdt,omitempty"`
	SpotUnrealizedPnLIDR     float64 `json:"spot_unrealized_pnl_idr,omitempty"`
	FuturesUnrealizedPnLUSDT float64 `json:"futures_unrealized_pnl_usdt,omitempty"`
	NetUnrealizedPnLUSDT     float64 `json:"net_unrealized_pnl_usdt,omitempty"`
	NetUnrealizedPnLIDR      float64 `json:"net_unrealized_pnl_idr,omitempty"`
	NetUnrealizedROIPct      float64 `json:"net_unrealized_roi_pct,omitempty"`
	State                    string  `json:"state,omitempty"`
}

type BalancesRecord struct {
	SpotTotal        float64 `json:"spot_total"`
	SpotAvailable    float64 `json:"spot_available"`
	SpotLocked       float64 `json:"spot_locked"`
	FuturesTotal     float64 `json:"futures_total"`
	FuturesAvailable float64 `json:"futures_available"`
	FuturesLocked    float64 `json:"futures_locked"`
	TotalEquity      float64 `json:"total_equity"`
}

