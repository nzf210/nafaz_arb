package scanner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
)

type availabilityReporter interface {
	ExchangeAvailable(exchange string) bool
}

// wsHealthReporter is implemented by adapters that track websocket feed health.
// A degraded feed means the cached books cannot be trusted, so pairs on that
// venue fall back to REST or are skipped entirely.
type wsHealthReporter interface {
	WSConnectionHealthy(exchange string) bool
}

// tradeActivityProvider is implemented by adapters that can report when a
// book last changed. Book updates are only pushed when trades or quotes move,
// so an old update time proxies for a quiet (possibly illiquid) market.
type tradeActivityProvider interface {
	LastBookUpdate(exchange, symbol string) time.Time
}

func (s *Service) evaluatePair(ctx context.Context, pair [2]domain.Market, exposures []domain.OpenExposure, positionStateKnown bool, started time.Time) evalResult {
	sm, pm := pair[0], pair[1]
	pairKey := strings.ToLower(sm.Exchange) + ":" + sm.Symbol + "->" + strings.ToLower(pm.Exchange) + ":" + pm.Symbol

	if reporter, ok := s.adapter.(availabilityReporter); ok {
		for _, name := range []string{sm.Exchange, pm.Exchange} {
			if s.exchangeUnavailable(reporter, name) {
				return evalResult{
					err: fmt.Errorf("%s/%s skipped: exchange %s unavailable (maintenance)",
						sm.BaseAsset, sm.QuoteAsset, name),
				}
			}
		}
	}

	// P2-05: skip pairs whose venue wallet is suspended — quoting can look
	// fine while deposit/withdrawal is disabled, which breaks the eventual
	// exit leg. Adapters without the optional interface are treated as
	// healthy; transport errors from the status check also fail open.
	if wallet, ok := s.adapter.(exchange.AccountStatusReporter); ok {
		for _, name := range []string{sm.Exchange, pm.Exchange} {
			if _, err := wallet.AccountAvailable(name); err != nil {
				return evalResult{
					err: fmt.Errorf("%s/%s skipped: exchange %s: %w",
						sm.BaseAsset, sm.QuoteAsset, name, err),
				}
			}
		}
	}

	// A broken websocket feed means any cached book served for this venue is
	// suspect. Skip the pair rather than price an opportunity off degraded data;
	// REST fallback for that symbol stays available on the next scan.
	if reporter, ok := s.adapter.(wsHealthReporter); ok {
		for _, name := range []string{sm.Exchange, pm.Exchange} {
			if !reporter.WSConnectionHealthy(name) {
				return evalResult{
					err: fmt.Errorf("%s/%s skipped: websocket feed for %s unhealthy",
						sm.BaseAsset, sm.QuoteAsset, name),
				}
			}
		}
	}

	positionReason := s.pairPositionReason(sm, pm, exposures, positionStateKnown)
	sb, pb, fr, exchangeMetrics, err := s.fetchMarketData(ctx, sm, pm)
	if err != nil {
		// An exchange that starts failing mid-scan is a whole-venue problem, not
		// a per-pair data issue: skip it rather than failing the entire scan.
		if errors.Is(err, exchange.ErrExchangeUnavailable) {
			return evalResult{
				err:             fmt.Errorf("%s/%s (%s vs %s): %w", sm.BaseAsset, sm.QuoteAsset, sm.Exchange, pm.Exchange, err),
				exchangeMetrics: exchangeMetrics,
			}
		}
		return evalResult{
			err:             fmt.Errorf("%s/%s (%s vs %s): %w", sm.BaseAsset, sm.QuoteAsset, sm.Exchange, pm.Exchange, err),
			fatalErr:        true,
			exchangeMetrics: exchangeMetrics,
		}
	}

	rawSB := sb
	quoteUpper := strings.ToUpper(sm.QuoteAsset)
	isIDRQuote := quoteUpper == "IDR" || quoteUpper == "BIDR"
	isSynthetic := s.cfg.EnableSyntheticIDR && isIDRQuote
	var fxRate float64
	fxLive := false
	if isIDRQuote {
		// IDR/BIDR prices must be converted with a fresh live USDT/IDR rate.
		// Never substitute the configured example rate or a cached rate for
		// live spread calculations.
		fxRate, fxLive = s.getUSDTIDRRate(ctx)
		if !fxLive {
			log.Printf("warning: skipping %s/%s - live USDT/IDR ticker unavailable", sm.BaseAsset, sm.QuoteAsset)
			return evalResult{
				err: fmt.Errorf("%s/%s (%s vs %s): USDT/IDR live ticker unavailable - IDR pair skipped",
					sm.BaseAsset, sm.QuoteAsset, sm.Exchange, pm.Exchange),
				exchangeMetrics: exchangeMetrics,
			}
		}
		if err := validateFXRateSanity(fxRate, s.cfg.SyntheticUSDTIDRRate, s.cfg.FXRateSanityPct); err != nil {
			return evalResult{
				err: fmt.Errorf("%s/%s (%s vs %s): %w", sm.BaseAsset, sm.QuoteAsset, sm.Exchange, pm.Exchange, err),
				exchangeMetrics: exchangeMetrics,
			}
		}
		if isSynthetic {
			sb = normalizeOrderBookCurrency(rawSB, fxRate)
		}
	} else {
		// Direct USDT/USDC pairs do not depend on the IDR FX endpoint.
		fxRate = s.lastKnownFXRate()
	}
	requiredNotional := s.requiredNotionalUsdt()
	op, err := s.calculate(sm, pm, sb, pb, fr, started, positionReason, isSynthetic, fxRate, requiredNotional)
	if err != nil {
		if strings.Contains(err.Error(), "empty order book") {
			s.recordEmptyBook(pairKey, started)
		}
		return evalResult{
			exchangeMetrics: exchangeMetrics,
			err:             err,
		}
	}
	if !fxLive && isIDRQuote {
		// Defense in depth: unreachable in normal flow (the skip above fires
		// first), but kept as a hard gate so a fallback-rate opportunity can
		// never be marked trading-eligible.
		op.ExclusionReasons = append(op.ExclusionReasons, "FX_RATE_FALLBACK_ACTIVE")
	}
	s.clearEmptyBook(pairKey)

	// Lapis 1 & 2 Filter Evaluation & Redis Directives
	publishErr := s.applyHybridMakerTakerRules(ctx, &op, sm, pm, rawSB, pb, fr, fxRate, isIDRQuote, started)

	return evalResult{
		op:              op,
		exchangeMetrics: exchangeMetrics,
		err:             publishErr,
		fatalErr:        publishErr != nil,
	}
}

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
		targetLimitBuyIDR = targetUSDT * fxRate
	}

	globalBestBidUSDT := pb.Bids[0].Price
	globalBestBidIDR := globalBestBidUSDT * fxRate
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
	minimumEntrySpreadPct := (s.cfg.SpotMakerFeePct + s.cfg.PerpTakerFeePct) - fundingSubsidyPct

	var cancelIfGlobalBidDropsBelowUSDT float64
	denom := fxRate * (1.0 - (minimumEntrySpreadPct / 100.0))
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
			_ = rc.PublishEmergencyCancel(ctx, sm.Exchange, sm.Symbol, "CANCEL_ALL_NEGATIVE_FUNDING", lastFundingRate)
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
		posExists, errPos := rc.HasActivePosition(ctx, sm.Exchange, sm.Symbol)
		switch {
		case errPos == nil && posExists:
			hasActive = true
			op.ExclusionReasons = append(op.ExclusionReasons, "ACTIVE_POSITION_EXISTS_IN_EXECUTOR")
		case errPos != nil:
			op.ExclusionReasons = append(op.ExclusionReasons, "POSITION_STATE_UNKNOWN")
		}
	}

	op.TradingEligible = len(op.ExclusionReasons) == 0

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
		TimestampMS:       pb.Timestamp.UnixMilli(),
	}

	if rc != nil {
		if err := rc.UpdateLivePrice(ctx, sm.Exchange, sm.Symbol, pricingSnapshot); err != nil {
			op.ExclusionReasons = append(op.ExclusionReasons, "LIVE_PRICE_PUBLISH_FAILED")
		}

		if op.TradingEligible && !hasActive && s.cfg.EnableExecutorSignals {
			maxPosUSDT := s.cfg.CapitalTotal * s.cfg.AllocationPerPair
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
				},
			}
			if err := rc.PublishSignal(ctx, payload); err != nil {
				// A signal that never reached Redis would be silently lost:
				// surface it as a fatal error so the scan is not "complete".
				return fmt.Errorf("publish signal %s: %w", sm.Symbol, err)
			}
		}
	}
	return nil
}

func (s *Service) evaluateActivePairs(ctx context.Context, pairs [][2]domain.Market, exposures []domain.OpenExposure, positionStateKnown bool, started time.Time) []evalResult {
	workerCount := 15
	if len(pairs) < workerCount {
		workerCount = len(pairs)
	}
	if workerCount < 1 {
		workerCount = 1
	}

	jobs := make(chan [2]domain.Market, len(pairs))
	results := make(chan evalResult, len(pairs))

	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pair := range jobs {
				if ctx.Err() != nil {
					return
				}
				results <- s.evaluatePair(ctx, pair, exposures, positionStateKnown, started)
			}
		}()
	}

	for _, pair := range pairs {
		jobs <- pair
	}
	close(jobs)
	wg.Wait()
	close(results)

	evalResults := make([]evalResult, 0, len(pairs))
	for res := range results {
		evalResults = append(evalResults, res)
	}
	return evalResults
}

func applyEvaluationResults(results []evalResult, result *domain.ScanResult) bool {
	complete := true
	exchangeMetrics := make(map[string]domain.ExchangeScanMetrics)
	for _, res := range results {
		for _, metric := range res.exchangeMetrics {
			aggregate := exchangeMetrics[metric.Exchange]
			aggregate.Exchange = metric.Exchange
			aggregate.Requests += metric.Requests
			aggregate.Errors += metric.Errors
			aggregate.DurationMicros += metric.DurationMicros
			exchangeMetrics[metric.Exchange] = aggregate
		}
		if res.err != nil {
			result.FailedPairs++
			if res.fatalErr {
				complete = false
			}
			result.Errors = append(result.Errors, res.err.Error())
			continue
		}
		result.Opportunities = append(result.Opportunities, res.op)
		if res.op.TradingEligible {
			result.EligiblePairs++
		} else {
			result.ExcludedPairs++
		}
	}
	result.ExchangeMetrics = make([]domain.ExchangeScanMetrics, 0, len(exchangeMetrics))
	for _, metric := range exchangeMetrics {
		result.ExchangeMetrics = append(result.ExchangeMetrics, metric)
	}
	sort.Slice(result.ExchangeMetrics, func(i, j int) bool {
		return result.ExchangeMetrics[i].Exchange < result.ExchangeMetrics[j].Exchange
	})
	return complete
}
