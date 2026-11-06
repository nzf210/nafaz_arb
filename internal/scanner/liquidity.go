package scanner

import (
	"fmt"
	"math"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/shopspring/decimal"
)

// requiredNotionalUsdt is the quote-currency size one position would commit:
// the per-pair allocation budget, floored at what the configured minimum capital
// implies. It bounds how much top-of-book liquidity the filter demands, so a
// small account is never required to have a large book.
func (s *Service) requiredNotionalUsdt() decimal.Decimal {
	budget := decimal.NewFromFloat(s.cfg.CapitalTotal).Mul(decimal.NewFromFloat(s.cfg.AllocationPerPair))
	if minCap := decimal.NewFromFloat(s.cfg.MinAllocatedCapital); minCap.GreaterThan(budget) {
		return minCap
	}
	return budget
}

// validateLiquidityVelocity rejects pairs whose top of book is too thin to
// absorb one position at the best price. A market order into a book holding only
// a few dollars at the touch fills far worse than the quoted spread implies,
// turning a nominally profitable opportunity into a loss.
//
// requiredNotionalUsdt is the position size in quote currency, so the configured
// floor is capped by it: demanding $10k of liquidity for a $50 position would
// reject everything. Disabled unless MinTopOfBookQuoteUSDT is set.
func (s *Service) validateLiquidityVelocity(sb, pb domain.OrderBook, requiredNotionalUsdt decimal.Decimal) error {
	minQuote := s.cfg.MinTopOfBookQuoteUSDT
	if minQuote <= 0 {
		return nil
	}
	threshold := decimal.NewFromFloat(minQuote)
	if requiredNotionalUsdt.GreaterThan(decimal.Zero) {
		threshold = decimal.Min(threshold, requiredNotionalUsdt)
	}
	if quote := topOfBookQuoteValue(sb.Asks); quote.LessThan(threshold) {
		return fmt.Errorf("insufficient spot top-of-book liquidity: %s < %s", quote.String(), threshold.String())
	}
	if quote := topOfBookQuoteValue(pb.Bids); quote.LessThan(threshold) {
		return fmt.Errorf("insufficient perp top-of-book liquidity: %s < %s", quote.String(), threshold.String())
	}
	return nil
}

// topOfBookQuoteValue returns the notional (price * quantity) resting on the
// best price level only.
func topOfBookQuoteValue(levels []domain.PriceLevel) decimal.Decimal {
	if len(levels) == 0 || levels[0].Price <= 0 || levels[0].Quantity <= 0 {
		return decimal.Zero
	}
	return decimal.NewFromFloat(levels[0].Price).Mul(decimal.NewFromFloat(levels[0].Quantity))
}

// validateOrderBookImbalance rejects pairs whose book is dominated by one side.
// An extreme imbalance usually marks a spike: one venue printing a price the
// other side's book cannot support, and the level that "caused" the opportunity
// is the first to vanish once the dominant side is consumed. maxImbalance is in
// [0,1] measured on the absolute OBI of both books; 0 disables the guard.
func (s *Service) validateOrderBookImbalance(sb, pb domain.OrderBook) error {
	maxImbalance := s.cfg.MaxOrderBookImbalance
	if maxImbalance <= 0 {
		return nil
	}
	if obi := math.Abs(orderBookImbalance(sb)); obi > maxImbalance {
		return fmt.Errorf("spot order book imbalance %.2f exceeds max %.2f", obi, maxImbalance)
	}
	if obi := math.Abs(orderBookImbalance(pb)); obi > maxImbalance {
		return fmt.Errorf("perp order book imbalance %.2f exceeds max %.2f", obi, maxImbalance)
	}
	return nil
}

// spikeKey derives the per-pair tracker key from the two books of a pair.
func spikeKey(sb, pb domain.OrderBook) string {
	return sb.Exchange + "/" + sb.Symbol + ":" + pb.Exchange + "/" + pb.Symbol
}

// SpikeThresholds returns the configured spike-detection parameters: the
// spread multiple that marks an anomalous scan and the number of consecutive
// anomalous scans required before a spike is believed.
func (s *Service) SpikeThresholds() (multiple float64, scans int) {
	multiple = s.cfg.SpikeSpreadPct
	if multiple <= 0 {
		multiple = 0
	}
	scans = s.cfg.MinSpikeScans
	if scans <= 0 {
		scans = 1
	}
	return multiple, scans
}

// validateSpikePersistence tracks consecutive anomalous-spread scans per pair.
// A fresh spike (fewer than MinSpikeScans consecutive anomalies) is rejected as
// noise; once the anomaly persists across the required scans it is accepted as a
// genuine dislocation and the counter lets the signal through. Requires
// SpikeSpreadPct > 0 to be enabled, so default deployments are unchanged.
func (s *Service) validateSpikePersistence(pairID string, spreadPct float64) error {
	spikePct, minScans := s.SpikeThresholds()
	if spikePct <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, exists := s.spikeTrackers[pairID]
	if !exists {
		rec = &spikeRecord{}
		s.spikeTrackers[pairID] = rec
	}
	if spreadPct > spikePct {
		rec.consecutiveCount++
	} else {
		rec.consecutiveCount = 0
	}
	if rec.consecutiveCount > 0 && rec.consecutiveCount < minScans {
		return fmt.Errorf("spike persistence: anomalous spread %.3f%% seen %d/%d scans, waiting for persistence",
			spreadPct, rec.consecutiveCount, minScans)
	}
	return nil
}

// orderBookImbalance computes (bidDepth - askDepth) / (bidDepth + askDepth) over
// the notional resting within 1% of the mid price, which is roughly the band a
// market order of our size walks through. The result is in [-1,1]: positive means
// more bids than asks (buy pressure), negative means sell pressure.
//
// Only meaningful with a two-sided book; a one-sided book returns 1 or -1 and an
// empty one returns 0, which keeps the value total and safe to publish.
func orderBookImbalance(b domain.OrderBook) float64 {
	if len(b.Bids) == 0 && len(b.Asks) == 0 {
		return 0
	}
	if len(b.Bids) == 0 {
		return -1
	}
	if len(b.Asks) == 0 {
		return 1
	}
	bestBid, bestAsk := b.Bids[0].Price, b.Asks[0].Price
	if bestBid <= 0 || bestAsk <= 0 || bestAsk < bestBid {
		return 0
	}
	mid := (bestBid + bestAsk) / 2
	lower, upper := mid*0.99, mid*1.01

	var bidDepth, askDepth float64
	for _, lvl := range b.Bids {
		if lvl.Price <= 0 || lvl.Quantity <= 0 || lvl.Price < lower {
			break
		}
		bidDepth += lvl.Price * lvl.Quantity
	}
	for _, lvl := range b.Asks {
		if lvl.Price <= 0 || lvl.Quantity <= 0 || lvl.Price > upper {
			break
		}
		askDepth += lvl.Price * lvl.Quantity
	}
	total := bidDepth + askDepth
	if total <= 0 {
		return 0
	}
	return (bidDepth - askDepth) / total
}

// validateFXRateSanity rejects a fetched USDT/IDR rate that has moved further
// than sanityPct from the configured baseline (SyntheticUSDTIDRRate). A wildly
// wrong rate silently converts every IDR price level and turns the spread into
// fiction — a bad ticker, a stale cache stamp, or the hardcode fallback firing
// while the market moved all produce the same wrong-answer profit number.
// sanityPct <= 0 disables the guard.
func validateFXRateSanity(fxRate, baseline, sanityPct float64) error {
	if sanityPct <= 0 || baseline <= 0 {
		return nil
	}
	if fxRate <= 0 {
		return fmt.Errorf("fx rate sanity: rate %.2f is not positive", fxRate)
	}
	deviation := math.Abs(fxRate-baseline) / baseline * 100
	if deviation > sanityPct {
		return fmt.Errorf("fx rate sanity: rate %.2f deviates %.1f%% from baseline %.2f (max %.1f%%)",
			fxRate, deviation, baseline, sanityPct)
	}
	return nil
}

// validateLastTradeAge rejects pairs whose books have not changed for a while.
// Book updates are only pushed when quotes or trades move, so an update older
// than maxAgeSeconds proxies for a quiet market with no recent trades — the kind
// of book where the quoted spread is wide and fills slip badly. The check needs
// the adapter to expose activity data (tradeActivityProvider); adapters without
// it (mocks, REST-only) are never blocked. maxAgeSeconds <= 0 disables the guard.
func (s *Service) validateLastTradeAge(now time.Time, sm, pm domain.Market, sb, pb domain.OrderBook) error {
	maxAgeSeconds := s.cfg.MaxLastTradeAgeSeconds
	if maxAgeSeconds <= 0 {
		return nil
	}
	activity, ok := s.adapter.(tradeActivityProvider)
	if !ok {
		return nil
	}
	maxAge := time.Duration(maxAgeSeconds) * time.Second
	for _, c := range []struct {
		name string
		book domain.OrderBook
	}{
		{"spot", sb}, {"perp", pb},
	} {
		last := activity.LastBookUpdate(c.book.Exchange, c.book.Symbol)
		if last.IsZero() {
			// No feed data for this book: cannot prove inactivity, so pass.
			continue
		}
		if age := now.Sub(last); age > maxAge {
			return fmt.Errorf("%s book for %s %s quiet for %s (max %s): no recent trade activity",
				c.name, c.book.Exchange, c.book.Symbol, age.Round(time.Second), maxAge)
		}
	}
	return nil
}
