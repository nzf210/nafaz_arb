package analysis

import (
	"context"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// BookKey returns the stable map key used for normalized order books.
func BookKey(exchange, symbol string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + ":" + strings.ToUpper(strings.TrimSpace(symbol))
}

// FeeKey returns the stable fee-profile key for one venue instrument.
func FeeKey(exchange, symbol string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + ":" + strings.ToUpper(strings.TrimSpace(symbol))
}

// FeeSchedule contains venue-specific account fee percentages.
type FeeSchedule struct {
	MakerPct float64
	TakerPct float64
}

// Snapshot is normalized, read-only market state shared by independent strategies.
type Snapshot struct {
	At                   time.Time
	Markets              []domain.Market
	OrderBooks           map[string]domain.OrderBook
	Funding              map[string]domain.FundingRate
	Fees                 map[string]FeeSchedule
	EstimatedSlippagePct float64
	MaxBookAge           time.Duration
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
	Confidence     float64           `json:"confidence"` // normalized ranking score, not a calibrated probability
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
