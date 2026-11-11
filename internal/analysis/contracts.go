package analysis

import (
	"context"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// Snapshot is normalized market data. It contains no execution instructions and
// can be consumed by any registered analysis strategy.
type Snapshot struct {
	At       time.Time
	Markets  []domain.Market
	OrderBooks map[string]domain.OrderBook
	Funding  map[string]domain.FundingRate
}

// SignalSide describes the direction a strategy wants to research, not an order.
type SignalSide string

const (
	SideLong  SignalSide = "LONG"
	SideShort SignalSide = "SHORT"
	SideFlat  SignalSide = "FLAT"
)

// Signal is a strategy-neutral research result. Execution must independently
// validate risk, venue capabilities, balances, and current prices.
type Signal struct {
	Strategy       string            `json:"strategy"`
	Instrument     string            `json:"instrument"`
	Venue          string            `json:"venue"`
	MarketType     string            `json:"market_type"`
	Side           SignalSide        `json:"side"`
	Confidence     float64           `json:"confidence"`
	ExpectedReturn float64           `json:"expected_return_pct,omitempty"`
	StopLoss       float64           `json:"stop_loss,omitempty"`
	TakeProfit     float64           `json:"take_profit,omitempty"`
	RiskReward     float64           `json:"risk_reward,omitempty"`
	Reason         string            `json:"reason,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// Strategy analyzes a normalized snapshot and returns research signals.
// Implementations must not place, cancel, or modify orders.
type Strategy interface {
	Name() string
	Analyze(context.Context, Snapshot) ([]Signal, error)
}
