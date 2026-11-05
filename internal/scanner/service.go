package scanner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/shopspring/decimal"
)

type emptyBookRecord struct {
	consecutiveCount int
	frozenUntil      time.Time
}

// spikeRecord tracks consecutive scans in which a pair produced an anomalous
// spread (a candidate spike). A spike that persists across MinSpikeScans
// consecutive scans is treated as a real market dislocation rather than noise.
type spikeRecord struct {
	consecutiveCount int
}

type evalResult struct {
	op              domain.Opportunity
	err             error
	fatalErr        bool
	exchangeMetrics []domain.ExchangeScanMetrics
}

type Service struct {
	cfg           config.Config
	adapter       exchange.ReadOnlyAdapter
	fundingWorker *exchange.FundingWorker
	redisClient   *redishandler.Client
	mu             sync.Mutex
	emptyTrackers  map[string]emptyBookRecord
	spikeTrackers  map[string]*spikeRecord
	lastLiveFXRate float64
}

func New(cfg config.Config, adapter exchange.ReadOnlyAdapter) *Service {
	return &Service{
		cfg:           cfg,
		adapter:       adapter,
		emptyTrackers: make(map[string]emptyBookRecord),
		spikeTrackers: make(map[string]*spikeRecord),
	}
}

// SetFundingWorker sets the background funding rate worker.
func (s *Service) SetFundingWorker(w *exchange.FundingWorker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fundingWorker = w
}

// SetRedisClient sets the Redis client for pub/sub and caching.
func (s *Service) SetRedisClient(r *redishandler.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redisClient = r
}

func (s *Service) Scan(ctx context.Context) (result domain.ScanResult, complete bool, scanErr error) {
	started := time.Now().UTC()
	result = domain.ScanResult{ScanID: id(), Mode: s.cfg.Mode, StartedAt: started, Opportunities: []domain.Opportunity{}}
	defer func() {
		result.CompletedAt = time.Now().UTC()
		result.DurationMS = result.CompletedAt.Sub(started).Milliseconds()
		result.Complete = complete && scanErr == nil
		if scanErr != nil {
			result.Errors = append(result.Errors, scanErr.Error())
		}
	}()

	marketDiscoveryStarted := time.Now()
	spot, err := s.adapter.SpotMarkets(ctx)
	if err != nil {
		result.MarketDiscoveryDurationMS = time.Since(marketDiscoveryStarted).Milliseconds()
		return result, false, fmt.Errorf("spot market discovery: %w", err)
	}
	perp, err := s.adapter.PerpetualMarkets(ctx)
	if err != nil {
		result.MarketDiscoveryDurationMS = time.Since(marketDiscoveryStarted).Milliseconds()
		return result, false, fmt.Errorf("perpetual market discovery: %w", err)
	}
	result.TotalMarkets = len(spot) + len(perp)
	pairs := exchange.MatchMarketsWithSynthetic(spot, perp, s.cfg.QuoteAssets, s.cfg.EnableSyntheticIDR)
	if len(s.cfg.ExcludedSymbols) > 0 {
		pairs = s.filterExcludedSymbols(pairs)
	}
	result.MatchedPairs = len(pairs)
	result.MarketDiscoveryDurationMS = time.Since(marketDiscoveryStarted).Milliseconds()

	activePairs, frozenCount := s.filterActivePairs(pairs, started)
	result.ActivePairs = len(activePairs)
	result.FrozenPairs = frozenCount

	complete = true
	exposureCheckStarted := time.Now()
	exposures, positionStateKnown, err := s.loadExposures(ctx)
	result.ExposureCheckDurationMS = time.Since(exposureCheckStarted).Milliseconds()
	if err != nil {
		complete = false
		result.Errors = append(result.Errors, "open exposure check: "+err.Error())
	}

	pairEvaluationStarted := time.Now()
	evalResults := s.evaluateActivePairs(ctx, activePairs, exposures, positionStateKnown, started)
	result.PairEvaluationDurationMS = time.Since(pairEvaluationStarted).Milliseconds()
	if !applyEvaluationResults(evalResults, &result) {
		complete = false
	}

	sort.Slice(result.Opportunities, func(i, j int) bool { return result.Opportunities[i].NetROIPct > result.Opportunities[j].NetROIPct })
	return result, complete, nil
}

func (s *Service) filterActivePairs(pairs [][2]domain.Market, now time.Time) ([][2]domain.Market, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var activePairs [][2]domain.Market
	frozenCount := 0
	for _, pair := range pairs {
		pairKey := pair[0].Symbol + ":" + pair[1].Symbol
		rec, exists := s.emptyTrackers[pairKey]
		if exists && rec.frozenUntil.After(now) {
			frozenCount++
			continue
		}
		activePairs = append(activePairs, pair)
	}
	return activePairs, frozenCount
}

func (s *Service) filterExcludedSymbols(pairs [][2]domain.Market) [][2]domain.Market {
	excluded := make(map[string]bool, len(s.cfg.ExcludedSymbols))
	for _, sym := range s.cfg.ExcludedSymbols {
		clean := strings.ToUpper(strings.TrimSpace(sym))
		if clean != "" {
			excluded[clean] = true
		}
	}
	if len(excluded) == 0 {
		return pairs
	}
	filtered := make([][2]domain.Market, 0, len(pairs))
	for _, pair := range pairs {
		base := strings.ToUpper(pair[0].BaseAsset)
		sym0 := strings.ToUpper(pair[0].Symbol)
		sym1 := strings.ToUpper(pair[1].Symbol)
		pairKey := fmt.Sprintf("%s/%s", base, strings.ToUpper(pair[1].QuoteAsset))
		if excluded[base] || excluded[sym0] || excluded[sym1] || excluded[pairKey] {
			continue
		}
		filtered = append(filtered, pair)
	}
	return filtered
}

// availabilityReporter is implemented by adapters that track exchange uptime.
// When either venue of a pair is under maintenance the pair is skipped for this
// scan without failing the whole scan, since only that venue is at fault.
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

func (s *Service) exchangeUnavailable(reporter availabilityReporter, exchangeName string) bool {
	return !reporter.ExchangeAvailable(exchangeName)
}

func (s *Service) evaluatePair(ctx context.Context, pair [2]domain.Market, exposures []domain.OpenExposure, positionStateKnown bool, started time.Time) evalResult {
	sm, pm := pair[0], pair[1]
	pairKey := sm.Symbol + ":" + pm.Symbol

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
	if isSynthetic || isIDRQuote {
		fxRate, fxLive = s.getUSDTIDRRate(ctx)
		if isSynthetic {
			sb = normalizeOrderBookCurrency(rawSB, fxRate)
		}
	} else {
		fxRate, fxLive = s.getUSDTIDRRate(ctx)
	}
	if !fxLive && isIDRQuote {
		// IDR pairs must never trade on a stale, synthetic, or hardcoded rate:
		// the USDT/IDR rate is too dynamic for spread math to survive any
		// fallback. Skip the pair entirely until the venue's live ticker
		// (Indodax, the CEX that trades IDR) responds again.
		log.Printf("warning: skipping %s/%s - USDT/IDR live ticker unavailable, no dynamic FX rate",
			sm.BaseAsset, sm.QuoteAsset)
		return evalResult{
			err:             fmt.Errorf("%s/%s (%s vs %s): USDT/IDR live ticker unavailable - IDR pair not tradable without dynamic FX rate",
				sm.BaseAsset, sm.QuoteAsset, sm.Exchange, pm.Exchange),
			exchangeMetrics: exchangeMetrics,
		}
	}
	if !fxLive {
		// Non-IDR pairs do not depend on the rate for spread math, but zero
		// would corrupt metadata; keep the previous scan's behavior of not
		// gating on FX here while refusing to fabricate a number.
		fxRate = s.lastKnownFXRate()
	}
	if err := validateFXRateSanity(fxRate, s.cfg.SyntheticUSDTIDRRate, s.cfg.FXRateSanityPct); err != nil {
		return evalResult{
			err:             fmt.Errorf("%s/%s (%s vs %s): %w", sm.BaseAsset, sm.QuoteAsset, sm.Exchange, pm.Exchange, err),
			exchangeMetrics: exchangeMetrics,
		}
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
		TimestampMS:       time.Now().UnixMilli(),
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

func (s *Service) recordEmptyBook(pairKey string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.emptyTrackers[pairKey]
	rec.consecutiveCount++
	// Circuit breaker: after 3 consecutive scans with empty order books, freeze pair
	if rec.consecutiveCount >= 3 {
		freezeDuration := 15 * time.Minute
		if s.cfg.EmptyBookFreezeMinutes > 0 {
			freezeDuration = time.Duration(s.cfg.EmptyBookFreezeMinutes) * time.Minute
		} else if s.cfg.EmptyBookFreezeHours > 0 {
			freezeDuration = time.Duration(s.cfg.EmptyBookFreezeHours) * time.Hour
		}
		rec.frozenUntil = now.Add(freezeDuration)
	}
	s.emptyTrackers[pairKey] = rec
}

func (s *Service) clearEmptyBook(pairKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.emptyTrackers, pairKey)
}

// FrozenPairInfo describes a pair that has been temporarily frozen.
type FrozenPairInfo struct {
	PairKey     string    `json:"pair_key"`
	FrozenUntil time.Time `json:"frozen_until"`
}

// FrozenCount returns the number of currently frozen pairs.
func (s *Service) FrozenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	count := 0
	for _, rec := range s.emptyTrackers {
		if rec.frozenUntil.After(now) {
			count++
		}
	}
	return count
}

// FrozenPairs returns all currently frozen pairs and their expiry timestamps.
func (s *Service) FrozenPairs() []FrozenPairInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var out []FrozenPairInfo
	for key, rec := range s.emptyTrackers {
		if rec.frozenUntil.After(now) {
			out = append(out, FrozenPairInfo{
				PairKey:     key,
				FrozenUntil: rec.frozenUntil,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FrozenUntil.Before(out[j].FrozenUntil)
	})
	return out
}

func (s *Service) loadExposures(ctx context.Context) ([]domain.OpenExposure, bool, error) {
	if !s.cfg.CheckOpenPositions {
		return nil, true, nil
	}
	exposures, err := s.adapter.OpenExposures(ctx)
	if err != nil {
		return nil, false, err
	}
	return exposures, true, nil
}

func (s *Service) pairPositionReason(sm, pm domain.Market, exposures []domain.OpenExposure, stateKnown bool) string {
	if !s.cfg.CheckOpenPositions {
		return ""
	}
	if !stateKnown {
		return "POSITION_STATUS_UNKNOWN"
	}
	return matchingExposureReason(exposures, sm, pm, s.cfg.ExcludeOpenOrders)
}

func (s *Service) fetchMarketData(ctx context.Context, sm, pm domain.Market) (domain.OrderBook, domain.OrderBook, domain.FundingRate, []domain.ExchangeScanMetrics, error) {
	metrics := make(map[string]domain.ExchangeScanMetrics, 2)
	measure := func(exchangeName string, started time.Time, err error) {
		metric := metrics[exchangeName]
		metric.Exchange = exchangeName
		metric.Requests++
		if err != nil {
			metric.Errors++
		}
		metric.DurationMicros += time.Since(started).Microseconds()
		metrics[exchangeName] = metric
	}
	requestStarted := time.Now()
	sb, e1 := s.adapter.SpotOrderBook(ctx, sm.Symbol)
	measure(sm.Exchange, requestStarted, e1)
	requestStarted = time.Now()
	pb, e2 := s.adapter.PerpetualOrderBook(ctx, pm.Symbol)
	measure(pm.Exchange, requestStarted, e2)
	requestStarted = time.Now()
	var fr domain.FundingRate
	var e3 error
	s.mu.Lock()
	fw := s.fundingWorker
	s.mu.Unlock()
	if fw != nil {
		if info, ok := fw.GetFundingInfo(pm.Symbol); ok {
			fr = domain.FundingRate{
				Rate:            info.LastFundingRate,
				IntervalHours:   8,
				Timestamp:       info.LastUpdated,
				NextFundingTime: info.NextFundingTime,
			}
		} else {
			fr, e3 = s.adapter.FundingRate(ctx, pm.Symbol)
		}
	} else {
		fr, e3 = s.adapter.FundingRate(ctx, pm.Symbol)
	}
	measure(pm.Exchange, requestStarted, e3)
	exchangeMetrics := make([]domain.ExchangeScanMetrics, 0, len(metrics))
	for _, metric := range metrics {
		exchangeMetrics = append(exchangeMetrics, metric)
	}
	if e1 != nil || e2 != nil || e3 != nil {
		// Join rather than stringify so callers can still detect a venue-wide
		// outage with errors.Is(err, exchange.ErrExchangeUnavailable).
		errs := make([]error, 0, 3)
		if e1 != nil {
			errs = append(errs, fmt.Errorf("spot_book: %w", e1))
		}
		if e2 != nil {
			errs = append(errs, fmt.Errorf("perp_book: %w", e2))
		}
		if e3 != nil {
			errs = append(errs, fmt.Errorf("funding: %w", e3))
		}
		return domain.OrderBook{}, domain.OrderBook{}, domain.FundingRate{}, exchangeMetrics, errors.Join(errs...)
	}
	return sb, pb, fr, exchangeMetrics, nil
}

// getUSDTIDRRate returns the rate and whether it came from a live ticker.
// There is deliberately no fallback: a hardcoded or synthetic USDT/IDR rate
// can silently diverge from the real market and destroy arbitrage profit, so
// callers must skip IDR pairs whenever the live ticker is unavailable.
func (s *Service) getUSDTIDRRate(ctx context.Context) (float64, bool) {
	if rp, ok := s.adapter.(exchange.RateProvider); ok {
		rate, err := rp.USDTIDRRate(ctx)
		if err == nil && rate > 0 {
			s.mu.Lock()
			s.lastLiveFXRate = rate
			s.mu.Unlock()
			return rate, true
		}
	}
	return 0.0, false
}

// lastKnownFXRate returns the most recent live rate observed this process, or
// 0 when none was ever seen. Used only for USDT-quote metadata where the rate
// is informational; it is never used to price IDR books.
func (s *Service) lastKnownFXRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLiveFXRate
}

func normalizeOrderBookCurrency(b domain.OrderBook, fxRate float64) domain.OrderBook {
	if fxRate <= 0 || fxRate == 1.0 {
		return b
	}
	out := domain.OrderBook{
		Exchange:  b.Exchange,
		Symbol:    b.Symbol,
		Timestamp: b.Timestamp,
		Bids:      make([]domain.PriceLevel, len(b.Bids)),
		Asks:      make([]domain.PriceLevel, len(b.Asks)),
	}
	for i, l := range b.Bids {
		out.Bids[i] = domain.PriceLevel{
			Price:    l.Price / fxRate,
			Quantity: l.Quantity,
		}
	}
	for i, l := range b.Asks {
		out.Asks[i] = domain.PriceLevel{
			Price:    l.Price / fxRate,
			Quantity: l.Quantity,
		}
	}
	return out
}

func availableBookDepth(levels []domain.PriceLevel) decimal.Decimal {
	sum := decimal.Zero
	zero := decimal.Zero
	for _, l := range levels {
		p := decimal.NewFromFloat(l.Price)
		q := decimal.NewFromFloat(l.Quantity)
		if p.GreaterThan(zero) && q.GreaterThan(zero) {
			sum = sum.Add(q)
		}
	}
	return sum
}

func (s *Service) calculate(sm, pm domain.Market, sb, pb domain.OrderBook, fr domain.FundingRate, now time.Time, positionReason string, isSynthetic bool, fxRate float64, requiredNotional decimal.Decimal) (domain.Opportunity, error) {
	if err := validateOrderBooks(sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	if err := validateOrderBookAge(sb, pb, now, s.cfg.MaxOrderBookAgeSeconds); err != nil {
		return domain.Opportunity{}, err
	}
	if err := validateCrossExchangeTimestampAlignment(sb, pb, time.Duration(s.cfg.MaxCrossExchangeSkewSeconds)*time.Second); err != nil {
		return domain.Opportunity{}, err
	}
	if err := validateOrderBookIntegrity(sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	if err := s.validateLiquidityVelocity(sb, pb, requiredNotional); err != nil {
		return domain.Opportunity{}, err
	}
	if err := s.validateOrderBookImbalance(sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	if err := s.validateLastTradeAge(now, sm, pm, sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	spotAsk := decimal.NewFromFloat(sb.Asks[0].Price)
	spotBid := decimal.NewFromFloat(sb.Bids[0].Price)
	perpBid := decimal.NewFromFloat(pb.Bids[0].Price)
	perpAsk := decimal.NewFromFloat(pb.Asks[0].Price)

	targetBudget := decimal.NewFromFloat(s.cfg.CapitalTotal).Mul(decimal.NewFromFloat(s.cfg.AllocationPerPair))
	perpLevFloat := s.cfg.PerpLeverage
	if perpLevFloat <= 0 {
		perpLevFloat = 1
	}
	perpLev := decimal.NewFromFloat(perpLevFloat)

	isMakerTaker := s.cfg.ExecutionMode == "MAKER_TAKER"

	var spotEntryRef decimal.Decimal
	if isMakerTaker {
		tickSize := sm.TickSize
		if tickSize <= 0 {
			tickSize = s.cfg.LocalPriceTick
		}
		if tickSize <= 0 {
			tickSize = 1.0
		}
		tickDec := decimal.NewFromFloat(tickSize)
		if isSynthetic && fxRate > 0 {
			tickDec = tickDec.Div(decimal.NewFromFloat(fxRate))
		}
		spotEntryRef = spotBid.Add(tickDec)
		if spotEntryRef.GreaterThanOrEqual(spotAsk) && spotAsk.GreaterThan(spotBid) {
			spotEntryRef = spotBid
		}
	} else {
		spotEntryRef = spotAsk
	}

	denom := spotEntryRef.Add(perpBid.Div(perpLev))
	if denom.IsZero() {
		return domain.Opportunity{}, fmt.Errorf("invalid denominator for quantity calculation")
	}
	targetQty := targetBudget.Div(denom)
	qty := targetQty

	if s.cfg.AdaptiveSizingEnabled() {
		maxExecutable := qty
		if isMakerTaker {
			// Maker-Taker: Limit Buy on Spot at spotBid, Market Short on Perp at perpEntry
			// Needs Perp Bids depth for short entry, Spot Bids depth for spot exit, Perp Asks depth for perp exit
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(sb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Asks))
		} else {
			// Taker-Taker: Market Buy on Spot, Market Short on Perp
			// Needs Spot Asks and Perp Bids for entry; Spot Bids and Perp Asks for exit
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(sb.Asks))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(sb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Asks))
		}
		if maxExecutable.GreaterThan(decimal.Zero) && maxExecutable.LessThan(qty) {
			qty = maxExecutable
		}
	}

	minCap := s.cfg.MinAllocatedCapital
	if minCap <= 0 {
		minCap = 10.0
	}
	capitalEstimate := spotEntryRef.Mul(qty).Add(perpBid.Mul(qty).Div(perpLev))
	if capitalEstimate.LessThan(decimal.NewFromFloat(minCap)) {
		return domain.Opportunity{}, fmt.Errorf("insufficient depth for minimum capital: available %.2f, min %.2f", capitalEstimate.InexactFloat64(), minCap)
	}

	prices, err := calculateExecutionPrices(sb, pb, qty, isMakerTaker, spotEntryRef)
	if err != nil {
		return domain.Opportunity{}, err
	}

	hundred := decimal.NewFromInt(100)
	spotTakerFeeRate := decimal.NewFromFloat(s.cfg.SpotTakerFeePct).Div(hundred)
	perpTakerFeeRate := decimal.NewFromFloat(s.cfg.PerpTakerFeePct).Div(hundred)
	// Fee-asset surcharge: model non-quote-asset fee effects (BNB discount,
	// fee billed in another asset) as a flat extra rate on notional so the
	// net profit is not overstated. 0 = disabled.
	var feeAssetSurchargeRate decimal.Decimal
	if s.cfg.FeeAssetSurchargePct > 0 {
		feeAssetSurchargeRate = decimal.NewFromFloat(s.cfg.FeeAssetSurchargePct).Div(hundred)
	}

	var spread decimal.Decimal
	var slippage decimal.Decimal
	var spotFees decimal.Decimal

	if isMakerTaker {
		// Maker-Taker: Limit Buy on Spot at spotEntryRef, Market Short on Perp at perpEntry
		spread = perpBid.Sub(spotEntryRef).Mul(qty)
		// Spot entry slippage is 0 because limit order execution is locked at spotEntryRef
		slippage = spotBid.Sub(prices.spotExit).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)

		spotMakerFeeRate := decimal.NewFromFloat(s.cfg.SpotMakerFeePct).Div(hundred)
		spotEntryFee := prices.spotEntry.Mul(qty).Mul(spotMakerFeeRate)
		spotExitFee := prices.spotExit.Mul(qty).Mul(spotTakerFeeRate)
		spotFees = spotEntryFee.Add(spotExitFee)
	} else {
		// Taker-Taker (default)
		spread = perpBid.Sub(spotAsk).Mul(qty)
		slippage = prices.spotEntry.Sub(spotAsk).
			Add(spotBid.Sub(prices.spotExit)).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)
		spotFees = prices.spotEntry.Add(prices.spotExit).Mul(qty).Mul(spotTakerFeeRate)
	}

	perpFees := prices.perpEntry.Add(prices.perpExit).Mul(qty).Mul(perpTakerFeeRate)
	if !feeAssetSurchargeRate.IsZero() {
		// Surcharge applies to the round-trip notional of both legs.
		surcharge := prices.spotEntry.Add(prices.spotExit).Add(prices.perpEntry).Add(prices.perpExit).Mul(qty).Mul(feeAssetSurchargeRate)
		spotFees = spotFees.Add(surcharge)
	}
	fees := spotFees.Add(perpFees)

	settlements := calculateSettlements(s.cfg.EstimatedHoldingHours, fr.IntervalHours, fr.NextFundingTime, now)
	frRate := decimal.NewFromFloat(fr.Rate)
	settlementsDec := decimal.NewFromInt(int64(settlements))
	funding := prices.perpEntry.Mul(qty).Mul(frRate).Mul(settlementsDec) // positive rate: short receives

	net := spread.Add(funding).Sub(fees).Sub(slippage)
	capital := prices.spotEntry.Mul(qty).Add(prices.perpEntry.Mul(qty).Div(perpLev))
	if capital.LessThanOrEqual(decimal.Zero) {
		return domain.Opportunity{}, fmt.Errorf("invalid allocated capital")
	}

	capacityPct := 100.0
	if targetBudget.GreaterThan(decimal.Zero) {
		pct := capital.Div(targetBudget).Mul(hundred).InexactFloat64()
		if pct < 100.0 {
			capacityPct = pct
		}
	}

	roi := net.Div(capital).Mul(hundred)
	netFloat := net.InexactFloat64()
	roiFloat := roi.InexactFloat64()

	status := "PROFITABLE_IN_RANGE"
	switch {
	case net.LessThanOrEqual(decimal.Zero):
		status = "NOT_PROFITABLE"
	case roiFloat < s.cfg.MinNetROIPct:
		status = "BELOW_MIN_TARGET"
	case roiFloat > s.cfg.MaxNetROIPct:
		status = "ABOVE_MAX_TARGET"
	}

	minAcceptablePerpPrice := calculateMinAcceptablePerpPrice(minAcceptablePerpPriceParams{
		spotEntry:         prices.spotEntry,
		perpExit:          prices.perpExit,
		qty:               qty,
		spotFees:          spotFees,
		perpTakerFeeRate:  perpTakerFeeRate,
		frRate:            frRate,
		settlementsDec:    settlementsDec,
		slippage:          slippage,
		minNetProfitQuote: s.cfg.MinNetProfitQuote,
	})

	execMode := s.cfg.ExecutionMode
	if execMode == "" {
		execMode = "TAKER_TAKER"
	}

	ttlSeconds := s.cfg.MaxOrderTTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = 30
	}

	targetQuote := pm.QuoteAsset
	if !isSynthetic {
		targetQuote = sm.QuoteAsset
	}

	op := domain.Opportunity{
		OpportunityKey:         fmt.Sprintf("%s/%s:%s:%s", sm.BaseAsset, targetQuote, sm.Symbol, pm.Symbol),
		BaseAsset:              sm.BaseAsset,
		QuoteAsset:             targetQuote,
		SpotSymbol:             sm.Symbol,
		PerpSymbol:             pm.Symbol,
		ExecutionMode:          execMode,
		SpotEntryPrice:         prices.spotEntry.InexactFloat64(),
		SpotExitPrice:          prices.spotExit.InexactFloat64(),
		PerpEntryPrice:         prices.perpEntry.InexactFloat64(),
		PerpExitPrice:          prices.perpExit.InexactFloat64(),
		Quantity:               qty.InexactFloat64(),
		SpreadPnL:              spread.InexactFloat64(),
		FundingPnL:             funding.InexactFloat64(),
		TradingFees:            fees.InexactFloat64(),
		Slippage:               slippage.InexactFloat64(),
		NetProfit:              netFloat,
		AllocatedCapital:       capital.InexactFloat64(),
		NetROIPct:              roiFloat,
		ProfitabilityStatus:    status,
		LastScannedAt:          now,
		SpotTargetLimitPrice:   prices.spotEntry.InexactFloat64(),
		MinAcceptablePerpPrice: minAcceptablePerpPrice.InexactFloat64(),
		SpotQueueDepth:         sb.Bids[0].Quantity,
		MaxOrderTTLSeconds:     ttlSeconds,
		IsSynthetic:            isSynthetic,
		SyntheticFXRate:        fxRate,
		OriginalSpotQuote:      sm.QuoteAsset,
		CapacityPct:            capacityPct,
	}
	s.evaluateOpportunity(&op, positionReason, perpBid.InexactFloat64(), spotEntryRef.InexactFloat64(), fr.Rate)
	return op, nil
}

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
