package scanner

import (
	"context"
	"math"
	"fmt"
	"log"
	"strings"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func (s *Service) applyHybridMakerTakerRules(ctx context.Context, op *domain.Opportunity, sm, pm domain.Market, rawSB, pb domain.OrderBook, fr domain.FundingRate, fxRate float64, isIDRQuote bool, now time.Time) error {
	lastFundingRate := fr.Rate

	var minutesToNextFunding float64
	if !fr.NextFundingTime.IsZero() {
		next := fr.NextFundingTime
		for next.Before(now) {
			next = next.Add(8 * time.Hour)
		}
		minutesToNextFunding = next.Sub(now).Minutes()
	} else {
		minutesToNextFunding = 240.0
	}

	snipingWindow := s.cfg.SnipingWindowMinutes
	if snipingWindow <= 0 {
		snipingWindow = 30
	}
	isSnipingWindow := minutesToNextFunding >= 0 && minutesToNextFunding <= float64(snipingWindow)

	localTick := sm.TickSize
	if localTick <= 0 {
		if isIDRQuote {
			if s.cfg.LocalPriceTick > 0 {
				localTick = s.cfg.LocalPriceTick
			} else {
				localTick = 1.0
			}
		} else {
			if s.cfg.LocalPriceTick > 0 && s.cfg.LocalPriceTick < 1.0 {
				localTick = s.cfg.LocalPriceTick
			} else {
				p := rawSB.Bids[0].Price
				if p > 0 && p < 0.001 {
					localTick = 0.000001
				} else if p > 0 && p < 0.1 {
					localTick = 0.0001
				} else if p > 0 && p < 1.0 {
					localTick = 0.001
				} else {
					localTick = 0.01
				}
			}
		}
	}

	tickIDR := localTick
	if !isIDRQuote && fxRate > 0 {
		tickIDR = localTick * fxRate
	}

	// Direct USDT/USDC pairs share a stable quote unit and must remain
	// comparable even before this process has observed an IDR/USDT ticker.
	// Keep fxRate itself untouched (zero still means no FX observation); use
	// a unit conversion only for the spread comparison and cancellation math.
	comparisonRate := fxRate
	if !isIDRQuote && comparisonRate <= 0 {
		comparisonRate = 1.0
	}

	var targetLimitBuyIDR float64
	if isIDRQuote {
		targetLimitBuyIDR = rawSB.Bids[0].Price + localTick
		if len(rawSB.Asks) > 0 && targetLimitBuyIDR >= rawSB.Asks[0].Price && rawSB.Asks[0].Price > rawSB.Bids[0].Price {
			targetLimitBuyIDR = rawSB.Bids[0].Price
		}
	} else {
		targetUSDT := rawSB.Bids[0].Price + localTick
		if len(rawSB.Asks) > 0 && targetUSDT >= rawSB.Asks[0].Price && rawSB.Asks[0].Price > rawSB.Bids[0].Price {
			targetUSDT = rawSB.Bids[0].Price
		}
		targetLimitBuyIDR = targetUSDT * comparisonRate
	}

	globalBestBidUSDT := pb.Bids[0].Price
	globalBestBidIDR := globalBestBidUSDT * comparisonRate
	var entrySpreadPct float64
	if globalBestBidIDR > 0 {
		entrySpreadPct = ((globalBestBidIDR - targetLimitBuyIDR) / globalBestBidIDR) * 100.0
	}

	// Spike persistence: an anomalous spread must repeat across several scans
	// before the pair is trusted. A first-scan spike is treated as noise.
	if err := s.validateSpikePersistence(spikeKey(rawSB, pb), entrySpreadPct); err != nil {
		op.ExclusionReasons = append(op.ExclusionReasons, "SPIKE_PERSISTENCE_WAIT")
	}

	currentFundingRatePct := lastFundingRate * 100.0
	var fundingSubsidyPct float64
	if isSnipingWindow {
		fundingSubsidyPct = currentFundingRatePct * 1.0
	} else {
		fundingSubsidyPct = currentFundingRatePct * 0.5
	}
	subsidyFloor := s.cfg.FundingSubsidyFloorPct
	if subsidyFloor <= 0 {
		subsidyFloor = 0.08
	}
	if fundingSubsidyPct < subsidyFloor {
		fundingSubsidyPct = subsidyFloor
	}
	minimumEntrySpreadPct := (op.SpotEntryFeePct + op.PerpEntryFeePct) - fundingSubsidyPct

	var cancelIfGlobalBidDropsBelowUSDT float64
	denom := comparisonRate * (1.0 - (minimumEntrySpreadPct / 100.0))
	if denom > 0 {
		cancelIfGlobalBidDropsBelowUSDT = targetLimitBuyIDR / denom
	} else {
		cancelIfGlobalBidDropsBelowUSDT = globalBestBidUSDT * 0.99
	}

	s.mu.Lock()
	rc := s.redisClient
	s.mu.Unlock()

	// Lapis 1: Funding Filter
	if lastFundingRate < 0 {
		op.ExclusionReasons = append(op.ExclusionReasons, "NEGATIVE_FUNDING_RATE")
		if rc != nil {
			_ = rc.PublishEmergencyCancel(ctx, sm.Exchange, sm.Symbol, "CANCEL_ALL_NEGATIVE_FUNDING", lastFundingRate, pm.Exchange, pm.Symbol)
		}
	} else if s.cfg.MinFundingRate > 0 && lastFundingRate < s.cfg.MinFundingRate {
		op.ExclusionReasons = append(op.ExclusionReasons, "FUNDING_RATE_BELOW_MINIMUM")
	}

	// Lapis 2: Maker-Taker Entry Spread Filter
	if entrySpreadPct < minimumEntrySpreadPct {
		op.ExclusionReasons = append(op.ExclusionReasons, "ENTRY_SPREAD_BELOW_MINIMUM")
	}

	// Active Position in Executor check. A failed check must fail closed:
	// publishing without knowing the executor state risks a doubled position.
	hasActive := false
	if rc != nil {
		posExists, errPos := rc.HasActivePosition(ctx, sm.Exchange, sm.Symbol, pm.Exchange, pm.Symbol)
		switch {
		case errPos == nil && posExists:
			hasActive = true
			op.ExclusionReasons = append(op.ExclusionReasons, "ACTIVE_POSITION_EXISTS_IN_EXECUTOR")
		case errPos != nil:
			op.ExclusionReasons = append(op.ExclusionReasons, "POSITION_STATE_UNKNOWN")
		}
	}

	op.TradingEligible = len(op.ExclusionReasons) == 0
	if op.TradingEligible && s.cfg.EnableExecutorSignals && !hasActive &&
		(!validAmountSpec(sm.AmountSpec) || !validAmountSpec(pm.AmountSpec)) {
		op.TradingEligible = false
		op.ExecutionSignalStatus = "amount_rules_missing"
		op.ExclusionReasons = append(op.ExclusionReasons, "AMOUNT_RULES_MISSING")
		log.Printf(
			"execution signal suppressed for %s/%s: spot or perpetual amount rules are missing or invalid",
			sm.Exchange+":"+sm.Symbol, pm.Exchange+":"+pm.Symbol,
		)
	}

	// Populate metadata fields
	op.LocalPriceTick = tickIDR
	op.TargetLimitBuyIDR = targetLimitBuyIDR
	op.SpotOBI = orderBookImbalance(rawSB)
	op.PerpOBI = orderBookImbalance(pb)
	op.EntrySpreadPct = entrySpreadPct
	op.MinEntrySpreadPct = minimumEntrySpreadPct
	baseExitSpread := s.cfg.BaseTargetExitSpreadPct
	if baseExitSpread <= 0 {
		baseExitSpread = 0.3
	}
	op.BaseTargetExitSpreadPct = baseExitSpread
	op.CancelIfGlobalBidDropsBelowUSDT = cancelIfGlobalBidDropsBelowUSDT
	op.IsSnipingWindow = isSnipingWindow
	op.MinutesToNextFunding = minutesToNextFunding
	op.CurrentFundingRate = lastFundingRate

	// Redis updates
	pricingSnapshot := domain.PricingSnapshot{
		LocalBestBid:      rawSB.Bids[0].Price,
		LocalBestAsk:      rawSB.Asks[0].Price,
		GlobalBestBidUSDT: pb.Bids[0].Price,
		GlobalBestAskUSDT: pb.Asks[0].Price,
		USDTIDRRate:       fxRate,
		TimestampMS:       time.Now().UnixMilli(),
	}

	if op.TradingEligible {
		switch {
		case rc == nil:
			op.ExecutionSignalStatus = "redis_unavailable"
		case !s.cfg.EnableExecutorSignals:
			op.ExecutionSignalStatus = "disabled"
		default:
			op.ExecutionSignalStatus = "pending"
		}
	}
	if rc != nil {
		if err := rc.UpdateLivePrice(ctx, sm.Exchange, sm.Symbol, pricingSnapshot, pm.Exchange, pm.Symbol); err != nil {
			if op.TradingEligible {
				op.ExecutionSignalStatus = "live_price_publish_failed"
			}
		}
	}

	if rc != nil {
		if op.TradingEligible && !hasActive && s.cfg.EnableExecutorSignals && op.ExecutionSignalStatus != "live_price_publish_failed" {
			maxPosUSDT := s.cfg.CapitalTotal * s.cfg.AllocationPerPair
			netProfitUSDT := op.NetProfit
			quote := strings.ToUpper(op.QuoteAsset)
			if quote == "IDR" || quote == "BIDR" {
				if fxRate <= 0 || math.IsNaN(fxRate) || math.IsInf(fxRate, 0) {
					return fmt.Errorf("cannot rank %s: valid live USDT/IDR conversion unavailable", sm.Symbol)
				}
				netProfitUSDT /= fxRate
			} else if quote != "USDT" {
				return fmt.Errorf("cannot rank %s: unsupported profit quote asset %s", sm.Symbol, quote)
			}
			if netProfitUSDT <= 0 || math.IsNaN(netProfitUSDT) || math.IsInf(netProfitUSDT, 0) {
				return fmt.Errorf("cannot rank %s: estimated net profit is not finite and positive", sm.Symbol)
			}
			payload := domain.ExecutorPayload{
				PairInfo: domain.PairInfo{
					LocalExchange:  sm.Exchange,
					GlobalExchange: pm.Exchange,
					LocalSymbol:    sm.Symbol,
					GlobalSymbol:   pm.Symbol,
					BaseAsset:      sm.BaseAsset,
					QuoteAsset:     sm.QuoteAsset,
					LocalPriceTick: tickIDR,
					LocalLotSize:   sm.LotSize,
					LocalAmountSpec:  sm.AmountSpec,
					GlobalAmountSpec: pm.AmountSpec,
					GlobalQuoteAsset: pm.QuoteAsset,
				},
				PricingSnapshot: pricingSnapshot,
				FundingMetrics: domain.FundingMetrics{
					CurrentFundingRate:   lastFundingRate,
					NextFundingTimeMS:    fr.NextFundingTime.UnixMilli(),
					MinutesToNextFunding: minutesToNextFunding,
					IsSnipingWindow:      isSnipingWindow,
				},
				ExecutionDirectives: domain.ExecutionDirectives{
					Action:                          "PLACE_MAKER_BUY",
					RecommendedLimitBuyIDR:          targetLimitBuyIDR,
					EntrySpreadPct:                  entrySpreadPct,
					MinEntrySpreadPct:               minimumEntrySpreadPct,
					BaseTargetExitSpreadPct:         baseExitSpread,
					MaxPositionUSDT:                 maxPosUSDT,
					CancelIfGlobalBidDropsBelowUSDT: cancelIfGlobalBidDropsBelowUSDT,
					ScannerValidated:                true,
					EstimatedNetProfitUSDT:           netProfitUSDT,
					SpotEntryFeePct:                  op.SpotEntryFeePct,
					SpotExitFeePct:                   op.SpotExitFeePct,
					PerpEntryFeePct:                  op.PerpEntryFeePct,
					PerpExitFeePct:                   op.PerpExitFeePct,
					FeeSource:                        op.FeeSource,
					FeeVerified:                      op.FeeVerified,
					FeeFetchedAtMS:                   op.FeeFetchedAtMS,
					FeeAssetSurchargePct:             op.FeeAssetSurchargePct,
					EstimatedFeesUSDT:                op.TradingFees,
					Quantity:                         op.Quantity,
					SpotEntryPriceUSDT:               op.SpotEntryPrice,
					SpotExitPriceUSDT:                op.SpotExitPrice,
					PerpEntryPriceUSDT:               op.PerpEntryPrice,
					PerpExitPriceUSDT:                op.PerpExitPrice,
				},
			}

			if err := rc.PublishSignal(ctx, payload); err != nil {
				// A signal that never reached Redis would be silently lost:
				// surface it as a fatal error so the scan is not "complete".
				op.ExecutionSignalStatus = "publish_failed"
				return fmt.Errorf("publish signal %s: %w", sm.Symbol, err)
			}
			op.ExecutionSignalStatus = "published"
		}
	}
	return nil
}
