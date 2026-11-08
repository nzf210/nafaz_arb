package scanner

import (
	"context"
	"fmt"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
)

type entryRuleInput struct {
	spot       domain.Market
	perp       domain.Market
	spotBook   domain.OrderBook
	perpBook   domain.OrderBook
	funding    domain.FundingRate
	fxRate     float64
	isIDRQuote bool
	now        time.Time
}

type entryRuleValues struct {
	localPriceTickIDR             float64
	targetLimitBuyIDR             float64
	entrySpreadPct                float64
	minEntrySpreadPct             float64
	cancelIfGlobalBidDropsBelowUSDT float64
	minutesToNextFunding          float64
	isSnipingWindow               bool
	baseExitSpreadPct             float64
}

func (s *Service) buildEntryRuleValues(input entryRuleInput) entryRuleValues {
	minutes, sniping := fundingWindow(input.funding.NextFundingTime, input.now, s.cfg.SnipingWindowMinutes)
	localTick := resolveLocalPriceTick(input.spot, input.spotBook, input.isIDRQuote, s.cfg.LocalPriceTick)
	tickIDR := localTick
	if !input.isIDRQuote && input.fxRate > 0 {
		tickIDR *= input.fxRate
	}

	targetBuyIDR := targetLimitBuyPrice(input.spotBook, localTick, input.fxRate, input.isIDRQuote)
	globalBidUSDT := input.perpBook.Bids[0].Price
	globalBidIDR := globalBidUSDT * input.fxRate
	entrySpreadPct := calculateEntrySpread(globalBidIDR, targetBuyIDR)
	minimumSpread := s.minimumEntrySpread(input.funding.Rate, sniping)
	cancelThreshold := calculateCancelThreshold(targetBuyIDR, globalBidUSDT, input.fxRate, minimumSpread)

	baseExitSpread := s.cfg.BaseTargetExitSpreadPct
	if baseExitSpread <= 0 {
		baseExitSpread = 0.3
	}

	return entryRuleValues{
		localPriceTickIDR:              tickIDR,
		targetLimitBuyIDR:              targetBuyIDR,
		entrySpreadPct:                 entrySpreadPct,
		minEntrySpreadPct:              minimumSpread,
		cancelIfGlobalBidDropsBelowUSDT: cancelThreshold,
		minutesToNextFunding:           minutes,
		isSnipingWindow:                sniping,
		baseExitSpreadPct:              baseExitSpread,
	}
}

func fundingWindow(nextFunding, now time.Time, configuredWindow int) (float64, bool) {
	minutes := 240.0
	if !nextFunding.IsZero() {
		next := nextFunding
		for next.Before(now) {
			next = next.Add(8 * time.Hour)
		}
		minutes = next.Sub(now).Minutes()
	}
	if configuredWindow <= 0 {
		configuredWindow = 30
	}
	return minutes, minutes >= 0 && minutes <= float64(configuredWindow)
}

func resolveLocalPriceTick(spot domain.Market, book domain.OrderBook, isIDRQuote bool, configuredTick float64) float64 {
	if spot.TickSize > 0 {
		return spot.TickSize
	}
	if isIDRQuote {
		if configuredTick > 0 {
			return configuredTick
		}
		return 1.0
	}
	if configuredTick > 0 && configuredTick < 1.0 {
		return configuredTick
	}
	price := book.Bids[0].Price
	switch {
	case price > 0 && price < 0.001:
		return 0.000001
	case price > 0 && price < 0.1:
		return 0.0001
	case price > 0 && price < 1.0:
		return 0.001
	default:
		return 0.01
	}
}

func targetLimitBuyPrice(book domain.OrderBook, tick, fxRate float64, isIDRQuote bool) float64 {
	bestBid := book.Bids[0].Price
	target := bestBid + tick
	if len(book.Asks) > 0 && target >= book.Asks[0].Price && book.Asks[0].Price > bestBid {
		target = bestBid
	}
	if !isIDRQuote {
		target *= fxRate
	}
	return target
}

func calculateEntrySpread(globalBidIDR, targetBuyIDR float64) float64 {
	if globalBidIDR <= 0 {
		return 0
	}
	return ((globalBidIDR - targetBuyIDR) / globalBidIDR) * 100
}

func (s *Service) minimumEntrySpread(fundingRate float64, isSnipingWindow bool) float64 {
	subsidy := fundingRate * 100
	if !isSnipingWindow {
		subsidy *= 0.5
	}
	floor := s.cfg.FundingSubsidyFloorPct
	if floor <= 0 {
		floor = 0.08
	}
	if subsidy < floor {
		subsidy = floor
	}
	return s.cfg.SpotMakerFeePct + s.cfg.PerpTakerFeePct - subsidy
}

func calculateCancelThreshold(targetBuyIDR, globalBidUSDT, fxRate, minimumSpreadPct float64) float64 {
	threshold := globalBidUSDT * 0.99
	denominator := fxRate * (1 - minimumSpreadPct/100)
	if denominator > 0 {
		threshold = targetBuyIDR / denominator
	}
	return threshold
}

func (s *Service) currentRedisClient() *redishandler.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.redisClient
}

func (s *Service) applyEligibilityFilters(ctx context.Context, op *domain.Opportunity, input entryRuleInput, values entryRuleValues, rc *redishandler.Client) bool {
	if err := s.validateSpikePersistence(spikeKey(input.spotBook, input.perpBook), values.entrySpreadPct); err != nil {
		op.ExclusionReasons = append(op.ExclusionReasons, "SPIKE_PERSISTENCE_WAIT")
	}
	s.applyFundingFilter(ctx, op, input, rc)
	if values.entrySpreadPct < values.minEntrySpreadPct {
		op.ExclusionReasons = append(op.ExclusionReasons, "ENTRY_SPREAD_BELOW_MINIMUM")
	}
	hasActive := s.checkActivePosition(ctx, op, input, rc)
	op.TradingEligible = len(op.ExclusionReasons) == 0
	return hasActive
}

func (s *Service) applyFundingFilter(ctx context.Context, op *domain.Opportunity, input entryRuleInput, rc *redishandler.Client) {
	rate := input.funding.Rate
	if rate < 0 {
		op.ExclusionReasons = append(op.ExclusionReasons, "NEGATIVE_FUNDING_RATE")
		if rc != nil {
			_ = rc.PublishEmergencyCancel(ctx, input.spot.Exchange, input.spot.Symbol, "CANCEL_ALL_NEGATIVE_FUNDING", rate)
		}
		return
	}
	if s.cfg.MinFundingRate > 0 && rate < s.cfg.MinFundingRate {
		op.ExclusionReasons = append(op.ExclusionReasons, "FUNDING_RATE_BELOW_MINIMUM")
	}
}

func (s *Service) checkActivePosition(ctx context.Context, op *domain.Opportunity, input entryRuleInput, rc *redishandler.Client) bool {
	if rc == nil {
		return false
	}
	exists, err := rc.HasActivePosition(ctx, input.spot.Exchange, input.spot.Symbol)
	switch {
	case err != nil:
		op.ExclusionReasons = append(op.ExclusionReasons, "POSITION_STATE_UNKNOWN")
	case exists:
		op.ExclusionReasons = append(op.ExclusionReasons, "ACTIVE_POSITION_EXISTS_IN_EXECUTOR")
	}
	return exists
}

func (s *Service) populateOpportunityMetadata(op *domain.Opportunity, input entryRuleInput, values entryRuleValues) {
	op.LocalPriceTick = values.localPriceTickIDR
	op.TargetLimitBuyIDR = values.targetLimitBuyIDR
	op.SpotOBI = orderBookImbalance(input.spotBook)
	op.PerpOBI = orderBookImbalance(input.perpBook)
	op.EntrySpreadPct = values.entrySpreadPct
	op.MinEntrySpreadPct = values.minEntrySpreadPct
	op.BaseTargetExitSpreadPct = values.baseExitSpreadPct
	op.CancelIfGlobalBidDropsBelowUSDT = values.cancelIfGlobalBidDropsBelowUSDT
	op.IsSnipingWindow = values.isSnipingWindow
	op.MinutesToNextFunding = values.minutesToNextFunding
	op.CurrentFundingRate = input.funding.Rate
}

func (s *Service) publishOpportunity(ctx context.Context, op *domain.Opportunity, input entryRuleInput, values entryRuleValues, rc *redishandler.Client, hasActive bool) error {
	if rc == nil {
		return nil
	}
	snapshot := domain.PricingSnapshot{
		LocalBestBid:      input.spotBook.Bids[0].Price,
		LocalBestAsk:      input.spotBook.Asks[0].Price,
		GlobalBestBidUSDT: input.perpBook.Bids[0].Price,
		GlobalBestAskUSDT: input.perpBook.Asks[0].Price,
		USDTIDRRate:       input.fxRate,
		TimestampMS:       input.perpBook.Timestamp.UnixMilli(),
	}
	if err := rc.UpdateLivePrice(ctx, input.spot.Exchange, input.spot.Symbol, snapshot); err != nil {
		op.ExclusionReasons = append(op.ExclusionReasons, "LIVE_PRICE_PUBLISH_FAILED")
		op.TradingEligible = false
		return nil
	}
	if !op.TradingEligible || hasActive || !s.cfg.EnableExecutorSignals {
		return nil
	}
	return s.publishExecutorSignal(ctx, input, values, snapshot, rc)
}

func (s *Service) publishExecutorSignal(ctx context.Context, input entryRuleInput, values entryRuleValues, snapshot domain.PricingSnapshot, rc *redishandler.Client) error {
	maxPositionUSDT := s.cfg.CapitalTotal * s.cfg.AllocationPerPair
	payload := domain.ExecutorPayload{
		PairInfo: domain.PairInfo{
			LocalExchange:  input.spot.Exchange,
			GlobalExchange: input.perp.Exchange,
			LocalSymbol:    input.spot.Symbol,
			GlobalSymbol:   input.perp.Symbol,
			BaseAsset:      input.spot.BaseAsset,
			QuoteAsset:     input.spot.QuoteAsset,
			LocalPriceTick: values.localPriceTickIDR,
			LocalLotSize:   input.spot.LotSize,
		},
		PricingSnapshot: snapshot,
		FundingMetrics: domain.FundingMetrics{
			CurrentFundingRate:   input.funding.Rate,
			NextFundingTimeMS:    input.funding.NextFundingTime.UnixMilli(),
			MinutesToNextFunding: values.minutesToNextFunding,
			IsSnipingWindow:      values.isSnipingWindow,
		},
		ExecutionDirectives: domain.ExecutionDirectives{
			Action:                          "PLACE_MAKER_BUY",
			RecommendedLimitBuyIDR:          values.targetLimitBuyIDR,
			EntrySpreadPct:                  values.entrySpreadPct,
			MinEntrySpreadPct:               values.minEntrySpreadPct,
			BaseTargetExitSpreadPct:         values.baseExitSpreadPct,
			MaxPositionUSDT:                 maxPositionUSDT,
			CancelIfGlobalBidDropsBelowUSDT: values.cancelIfGlobalBidDropsBelowUSDT,
			ScannerValidated:                true,
		},
	}
	if err := rc.PublishSignal(ctx, payload); err != nil {
		return fmt.Errorf("publish signal %s: %w", input.spot.Symbol, err)
	}
	return nil
}
