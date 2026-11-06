package scanner

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/shopspring/decimal"
)

// validateOrderBookAge rejects books whose exchange-side timestamp is older than
// maxAgeSeconds. A cache-serving exchange happily returns a 200 with hours-old
// depth; pricing an opportunity off that produces a signal that loses money the
// moment the executor fills it. maxAgeSeconds <= 0 disables the check: the live
// adapters currently stamp books with local receive time, so a caller must opt
// in once the venue timestamps are plumbed through.
func validateOrderBookAge(sb, pb domain.OrderBook, now time.Time, maxAgeSeconds int) error {
	if maxAgeSeconds <= 0 {
		return nil
	}
	maxAge := time.Duration(maxAgeSeconds) * time.Second
	for _, book := range []domain.OrderBook{sb, pb} {
		if book.Timestamp.IsZero() {
			// A zero timestamp means the venue did not provide one. Treat as
			// potentially stale rather than silently accepting it.
			return fmt.Errorf("stale order book: %s %s missing timestamp", book.Exchange, book.Symbol)
		}
		// Timestamps slightly in the future mean clock skew, not staleness.
		if age := now.Sub(book.Timestamp); age > maxAge {
			return fmt.Errorf("stale order book: %s %s age=%s exceeds %s", book.Exchange, book.Symbol, age.Round(time.Millisecond), maxAge)
		}
	}
	return nil
}

// validateCrossExchangeTimestampAlignment checks that the timestamps from both
// exchanges are within maxSkew of each other. If one venue's clock is skewed
// relative to the other, the spread calculation can be wrong and the scanner
// may price an opportunity off stale data. A zero maxSkew disables the check.
func validateCrossExchangeTimestampAlignment(sb, pb domain.OrderBook, maxSkew time.Duration) error {
	if maxSkew <= 0 {
		return nil
	}
	if sb.Timestamp.IsZero() || pb.Timestamp.IsZero() {
		return fmt.Errorf("cross-exchange timestamp alignment: %s %s or %s %s missing timestamp", sb.Exchange, sb.Symbol, pb.Exchange, pb.Symbol)
	}
	skew := sb.Timestamp.Sub(pb.Timestamp)
	if skew < 0 {
		skew = -skew
	}
	if skew > maxSkew {
		return fmt.Errorf("cross-exchange timestamp alignment: %s %s vs %s %s skew=%s exceeds %s", sb.Exchange, sb.Symbol, pb.Exchange, pb.Symbol, skew.Round(time.Millisecond), maxSkew)
	}
	return nil
}

// validateOrderBookIntegrity computes a checksum over the order book's levels
// and compares it against the stored checksum. A mismatch means the book was
// corrupted in transit (e.g. a truncated websocket frame) and must not be used
// for pricing. A zero checksum disables the check.
func validateOrderBookIntegrity(sb, pb domain.OrderBook) error {
	for _, book := range []domain.OrderBook{sb, pb} {
		if book.Checksum == "" {
			// No checksum provided by the venue; skip integrity check rather
			// than rejecting valid data that simply doesn't carry one.
			continue
		}
		if computed := orderBookChecksum(book); computed != book.Checksum {
			return fmt.Errorf("order book integrity: %s %s checksum mismatch (computed=%s stored=%s)", book.Exchange, book.Symbol, computed, book.Checksum)
		}
	}
	return nil
}

// orderBookChecksum computes a stable checksum over an order book's levels.
// It is intentionally simple: price+quantity pairs joined by '|', then a
// FNV-1a hash. This catches truncation, reordering, and level corruption.
func orderBookChecksum(book domain.OrderBook) string {
	h := fnv.New32a()
	for _, l := range book.Bids {
		fmt.Fprintf(h, "%f|%f|", l.Price, l.Quantity)
	}
	for _, l := range book.Asks {
		fmt.Fprintf(h, "%f|%f|", l.Price, l.Quantity)
	}
	return fmt.Sprintf("%x", h.Sum32())
}

func validateOrderBooks(sb, pb domain.OrderBook) error {
	if len(sb.Bids) == 0 || len(sb.Asks) == 0 || len(pb.Bids) == 0 || len(pb.Asks) == 0 {
		return fmt.Errorf("empty order book")
	}
	if sb.Asks[0].Price <= 0 || sb.Bids[0].Price <= 0 || pb.Bids[0].Price <= 0 || pb.Asks[0].Price <= 0 {
		return fmt.Errorf("invalid best price")
	}
	return nil
}

type executionPrices struct {
	spotEntry decimal.Decimal
	spotExit  decimal.Decimal
	perpEntry decimal.Decimal
	perpExit  decimal.Decimal
}

func calculateExecutionPrices(sb, pb domain.OrderBook, qty decimal.Decimal, isMakerTaker bool, makerPrice decimal.Decimal) (executionPrices, error) {
	var spotEntry decimal.Decimal
	var err error
	if isMakerTaker {
		spotEntry = makerPrice
	} else {
		spotEntry, err = vwap(sb.Asks, qty)
		if err != nil {
			return executionPrices{}, fmt.Errorf("spot entry depth: %w", err)
		}
	}
	spotExit, err := vwap(sb.Bids, qty)
	if err != nil {
		return executionPrices{}, fmt.Errorf("spot exit depth: %w", err)
	}
	perpEntry, err := vwap(pb.Bids, qty)
	if err != nil {
		return executionPrices{}, fmt.Errorf("perpetual entry depth: %w", err)
	}
	perpExit, err := vwap(pb.Asks, qty)
	if err != nil {
		return executionPrices{}, fmt.Errorf("perpetual exit depth: %w", err)
	}
	return executionPrices{
		spotEntry: spotEntry,
		spotExit:  spotExit,
		perpEntry: perpEntry,
		perpExit:  perpExit,
	}, nil
}

type minAcceptablePerpPriceParams struct {
	spotEntry         decimal.Decimal
	perpExit          decimal.Decimal
	qty               decimal.Decimal
	spotFees          decimal.Decimal
	perpTakerFeeRate  decimal.Decimal
	frRate            decimal.Decimal
	settlementsDec    decimal.Decimal
	slippage          decimal.Decimal
	minNetProfitQuote float64
}

func calculateMinAcceptablePerpPrice(p minAcceptablePerpPriceParams) decimal.Decimal {
	if p.qty.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	perpExitFee := p.perpExit.Mul(p.qty).Mul(p.perpTakerFeeRate)
	fixedCosts := p.spotEntry.Mul(p.qty).Add(p.spotFees).Add(perpExitFee).Add(p.slippage)
	if p.minNetProfitQuote > 0 {
		fixedCosts = fixedCosts.Add(decimal.NewFromFloat(p.minNetProfitQuote))
	}
	perpMultiplier := p.qty.Mul(decimal.NewFromInt(1).Add(p.frRate.Mul(p.settlementsDec)).Sub(p.perpTakerFeeRate))
	if perpMultiplier.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	return fixedCosts.Div(perpMultiplier)
}

func (s *Service) evaluateOpportunity(op *domain.Opportunity, positionReason string, perpBid, spotAsk, frRate float64) {
	if op.ProfitabilityStatus != "PROFITABLE_IN_RANGE" {
		op.ExclusionReasons = append(op.ExclusionReasons, op.ProfitabilityStatus)
	}
	if positionReason != "" {
		op.ExclusionReasons = append(op.ExclusionReasons, positionReason)
	}
	if op.NetProfit < s.cfg.MinNetProfitQuote {
		op.ExclusionReasons = append(op.ExclusionReasons, "MIN_NET_PROFIT_NOT_MET")
	}
	if op.AllocatedCapital > 0 && op.Slippage/op.AllocatedCapital*100 > s.cfg.MaxSlippagePct {
		op.ExclusionReasons = append(op.ExclusionReasons, "MAX_SLIPPAGE_EXCEEDED")
	}
	isUnfavorableRegime := perpBid < spotAsk && frRate <= 0
	if isUnfavorableRegime {
		op.ExclusionReasons = append(op.ExclusionReasons, "UNFAVORABLE_REGIME_NEGATIVE_BASIS_AND_FUNDING")
	}
	op.TradingEligible = len(op.ExclusionReasons) == 0
}

func calculateSettlements(holdingHours, intervalHours int, nextFunding, now time.Time) int {
	if intervalHours <= 0 || holdingHours <= 0 {
		return 0
	}
	if !nextFunding.IsZero() {
		for nextFunding.Before(now) {
			nextFunding = nextFunding.Add(time.Duration(intervalHours) * time.Hour)
		}
		timeUntilNext := nextFunding.Sub(now)
		holdingDuration := time.Duration(holdingHours) * time.Hour
		if holdingDuration < timeUntilNext {
			return 0
		}
		remainingDuration := holdingDuration - timeUntilNext
		intervalDuration := time.Duration(intervalHours) * time.Hour
		return 1 + int(remainingDuration/intervalDuration)
	}
	settlements := holdingHours / intervalHours
	if settlements == 0 && holdingHours >= intervalHours/2 {
		settlements = 1
	}
	return settlements
}

func vwap(levels []domain.PriceLevel, qty decimal.Decimal) (decimal.Decimal, error) {
	remaining := qty
	total := decimal.Zero
	zero := decimal.Zero
	var lastPrice decimal.Decimal
	for _, l := range levels {
		price := decimal.NewFromFloat(l.Price)
		quantity := decimal.NewFromFloat(l.Quantity)
		if price.LessThanOrEqual(zero) || quantity.LessThanOrEqual(zero) {
			continue
		}
		lastPrice = price
		take := decimal.Min(remaining, quantity)
		total = total.Add(take.Mul(price))
		remaining = remaining.Sub(take)
		if remaining.LessThanOrEqual(zero) {
			return total.Div(qty), nil
		}
	}
	// Tolerate fractional remaining dust due to float rounding (within 0.05% of qty)
	if remaining.GreaterThan(zero) && lastPrice.GreaterThan(zero) {
		if remaining.Div(qty).LessThan(decimal.NewFromFloat(0.0005)) {
			total = total.Add(remaining.Mul(lastPrice))
			return total.Div(qty), nil
		}
	}
	return decimal.Zero, fmt.Errorf("insufficient depth for quantity %s", qty.String())
}
func id() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err == nil {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func matchingExposureReason(items []domain.OpenExposure, spot, perp domain.Market, excludeOrders bool) string {
	for _, e := range items {
		if reason := exposureReason(e, spot, perp, excludeOrders); reason != "" {
			return reason
		}
	}
	return ""
}

func exposureReason(e domain.OpenExposure, spot, perp domain.Market, excludeOrders bool) string {
	if !e.Active {
		return ""
	}
	kind := strings.ToUpper(e.Kind)
	if (kind == "OPEN_ORDER" && !excludeOrders) || (kind == "POSITION" && e.Quantity == 0) {
		return ""
	}
	symbolMatch := (strings.EqualFold(e.Exchange, perp.Exchange) && strings.EqualFold(e.Symbol, perp.Symbol)) ||
		(kind == "OPEN_ORDER" && strings.EqualFold(e.Exchange, spot.Exchange) && strings.EqualFold(e.Symbol, spot.Symbol))
	pairMatch := strings.EqualFold(e.BaseAsset, spot.BaseAsset) && strings.EqualFold(e.QuoteAsset, spot.QuoteAsset) && e.Managed
	if !symbolMatch && !pairMatch {
		return ""
	}
	if e.Managed {
		return "OPEN_POSITION_EXISTS"
	}
	return "RECONCILIATION_REQUIRED"
}
