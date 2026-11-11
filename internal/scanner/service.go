package scanner

import (
	"context"
	"log"
	"errors"
	"fmt"
	"math"
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

// OpportunityPersister is deliberately local-write-only in production: the
// implementation journals a scan snapshot before returning and delivers it
// asynchronously, so Supabase latency cannot delay Redis signal publication.
type OpportunityPersister interface {
	SaveOpportunities(context.Context, string, []domain.Opportunity) error
}

type Service struct {
	cfg            config.Config
	adapter        exchange.ReadOnlyAdapter
	fundingWorker  *exchange.FundingWorker
	redisClient    *redishandler.Client
	supabaseClient OpportunityPersister
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

// SetSupabaseClient enables best-effort durable scanner opportunity history.
// Redis remains authoritative for real-time signals and executor coordination.
func (s *Service) SetSupabaseClient(client OpportunityPersister) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.supabaseClient = client
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
		s.mu.Lock()
		persistor := s.supabaseClient
		s.mu.Unlock()
		if persistor != nil && len(result.Opportunities) > 0 {
			persistCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			if err := persistor.SaveOpportunities(persistCtx, result.ScanID, result.Opportunities); err != nil {
				// Persistence is fail-open: it must not suppress Redis signals or scan results.
				log.Printf("CRITICAL: scanner durable outbox enqueue failed for scan %s; audit history may have a gap: %v", result.ScanID, err)
			}
			cancel()
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
	// Match direct quote assets and, when enabled, synthesize IDR/BIDR spot
	// markets against USDT perpetuals. IDR order books are normalized using
	// the live USDT/IDR ticker inside evaluatePair before spread calculation.
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
func (s *Service) exchangeUnavailable(reporter availabilityReporter, exchangeName string) bool {
	return !reporter.ExchangeAvailable(exchangeName)
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
		if err == nil && rate > 0 && !math.IsNaN(rate) && !math.IsInf(rate, 0) {
			s.mu.Lock()
			s.lastLiveFXRate = rate
			s.mu.Unlock()
			return rate, true
		}
	}
	return 0.0, false
}

func containsIDRSpotPair(pairs [][2]domain.Market) bool {
	for _, pair := range pairs {
		quote := strings.ToUpper(strings.TrimSpace(pair[0].QuoteAsset))
		if quote == "IDR" || quote == "BIDR" {
			return true
		}
	}
	return false
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

type opportunityCalculationInput struct {
	spotMarket       domain.Market
	perpMarket       domain.Market
	spotBook         domain.OrderBook
	perpBook         domain.OrderBook
	funding          domain.FundingRate
	now              time.Time
	positionReason   string
	isSynthetic      bool
	fxRate           float64
	requiredNotional decimal.Decimal
	dynamicProfiles  []*redishandler.DynamicFeeProfile
}

type opportunitySizing struct {
	targetBudget decimal.Decimal
	perpLeverage decimal.Decimal
	spotEntryRef decimal.Decimal
	quantity     decimal.Decimal
	makerTaker   bool
}

type opportunityFees struct {
	spotMakerPct  float64
	spotTakerPct  float64
	perpTakerPct  float64
	source        string
	verified      bool
	fetchedAtMS   int64
	spotTakerRate decimal.Decimal
	perpTakerRate decimal.Decimal
	surchargeRate decimal.Decimal
}

type opportunityProfit struct {
	spread                 decimal.Decimal
	slippage               decimal.Decimal
	fees                   decimal.Decimal
	funding                decimal.Decimal
	net                    decimal.Decimal
	capital                decimal.Decimal
	capacityPct            float64
	roi                    decimal.Decimal
	status                 string
	minAcceptablePerpPrice decimal.Decimal
}

func (s *Service) validateCalculationInput(input opportunityCalculationInput) error {
	if err := validateOrderBooks(input.spotBook, input.perpBook); err != nil {
		return err
	}
	if err := validateOrderBookAge(
		input.spotBook, input.perpBook, input.now, s.cfg.MaxOrderBookAgeSeconds,
	); err != nil {
		return err
	}
	if err := validateCrossExchangeTimestampAlignment(
		input.spotBook, input.perpBook,
		time.Duration(s.cfg.MaxCrossExchangeSkewSeconds)*time.Second,
	); err != nil {
		return err
	}
	if err := validateOrderBookIntegrity(input.spotBook, input.perpBook); err != nil {
		return err
	}
	if err := s.validateLiquidityVelocity(
		input.spotBook, input.perpBook, input.requiredNotional,
	); err != nil {
		return err
	}
	if err := s.validateOrderBookImbalance(input.spotBook, input.perpBook); err != nil {
		return err
	}
	return s.validateLastTradeAge(
		input.now, input.spotMarket, input.perpMarket,
		input.spotBook, input.perpBook,
	)
}

func (s *Service) opportunitySpotEntryReference(
	input opportunityCalculationInput,
	sm Market,
	sb, pb OrderBook,
	spotBid, spotAsk decimal.Decimal,
	makerTaker bool,
) decimal.Decimal {
	if !makerTaker {
		return spotAsk
	}
	tickSize := sm.TickSize
	if tickSize <= 0 {
		tickSize = s.cfg.LocalPriceTick
	}
	if tickSize <= 0 {
		tickSize = 1.0
	}
	tick := decimal.NewFromFloat(tickSize)
	if input.isSynthetic && input.fxRate > 0 {
		tick = tick.Div(decimal.NewFromFloat(input.fxRate))
	}
	entry := spotBid.Add(tick)
	if entry.GreaterThanOrEqual(spotAsk) && spotAsk.GreaterThan(spotBid) {
		return spotBid
	}
	return entry
}

func (s *Service) opportunityExecutableDepth(
	spotBook, perpBook OrderBook, makerTaker bool,
) decimal.Decimal {
	depth := decimal.Zero
	if makerTaker {
		depth = availableBookDepth(perpBook.Bids)
		depth = decimal.Min(depth, availableBookDepth(spotBook.Bids))
		depth = decimal.Min(depth, availableBookDepth(perpBook.Asks))
		return depth
	}
	depth = availableBookDepth(spotBook.Asks)
	depth = decimal.Min(depth, availableBookDepth(perpBook.Bids))
	depth = decimal.Min(depth, availableBookDepth(spotBook.Bids))
	return decimal.Min(depth, availableBookDepth(perpBook.Asks))
}

func (s *Service) calculateOpportunitySizing(
	input opportunityCalculationInput,
) (opportunitySizing, error) {
	sm, sb, pb := input.spotMarket, input.spotBook, input.perpBook
	targetBudget := decimal.NewFromFloat(s.cfg.CapitalTotal).
		Mul(decimal.NewFromFloat(s.cfg.AllocationPerPair))
	perpLevFloat := s.cfg.PerpLeverage
	if perpLevFloat <= 0 {
		perpLevFloat = 1
	}
	perpLev := decimal.NewFromFloat(perpLevFloat)
	makerTaker := s.cfg.ExecutionMode == "MAKER_TAKER"
	spotAsk := decimal.NewFromFloat(sb.Asks[0].Price)
	spotBid := decimal.NewFromFloat(sb.Bids[0].Price)
	perpBid := decimal.NewFromFloat(pb.Bids[0].Price)
	spotEntryRef := s.opportunitySpotEntryReference(
		input, sm, sb, spotBid, spotAsk, makerTaker,
	)
	denom := spotEntryRef.Add(perpBid.Div(perpLev))
	if denom.IsZero() {
		return opportunitySizing{}, fmt.Errorf(
			"invalid denominator for quantity calculation",
		)
	}
	qty := targetBudget.Div(denom)
	if s.cfg.AdaptiveSizingEnabled() {
		maxExecutable := s.opportunityExecutableDepth(sb, pb, makerTaker)
		if maxExecutable.GreaterThan(decimal.Zero) && maxExecutable.LessThan(qty) {
			qty = maxExecutable
		}
	}
	minCap := s.cfg.MinAllocatedCapital
	if minCap <= 0 {
		minCap = 10.0
	}
	capitalEstimate := spotEntryRef.Mul(qty).Add(
		perpBid.Mul(qty).Div(perpLev),
	)
	if capitalEstimate.LessThan(decimal.NewFromFloat(minCap)) {
		return opportunitySizing{}, fmt.Errorf(
			"insufficient depth for minimum capital: available %.2f, min %.2f",
			capitalEstimate.InexactFloat64(), minCap,
		)
	}
	return opportunitySizing{
		targetBudget: targetBudget,
		perpLeverage: perpLev,
		spotEntryRef: spotEntryRef,
		quantity:     qty,
		makerTaker:   makerTaker,
	}, nil
}

func (s *Service) resolveOpportunityFees(
	input opportunityCalculationInput,
) opportunityFees {
	sm, pm := input.spotMarket, input.perpMarket
	spotMakerPct := s.cfg.SpotMakerFeePct
	spotTakerPct := s.cfg.SpotTakerFeePct
	perpTakerPct := s.cfg.PerpTakerFeePct
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(sm.Exchange)]; ok {
		if profile.SpotMakerFeePct != nil {
			spotMakerPct = *profile.SpotMakerFeePct
		}
		if profile.SpotTakerFeePct != nil {
			spotTakerPct = *profile.SpotTakerFeePct
		}
	}
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(pm.Exchange)]; ok &&
		profile.PerpTakerFeePct != nil {
		perpTakerPct = *profile.PerpTakerFeePct
	}

	feeSource := "CONFIG"
	feeVerified := false
	var fetchedAtMS int64
	spotDynamic := len(input.dynamicProfiles) > 0 &&
		input.dynamicProfiles[0] != nil
	perpDynamic := len(input.dynamicProfiles) > 1 &&
		input.dynamicProfiles[1] != nil
	if spotDynamic {
		spotMakerPct = input.dynamicProfiles[0].MakerFeePct
		spotTakerPct = input.dynamicProfiles[0].TakerFeePct
		fetchedAtMS = input.dynamicProfiles[0].FetchedAtMS
	}
	if perpDynamic {
		perpTakerPct = input.dynamicProfiles[1].TakerFeePct
		if input.dynamicProfiles[1].FetchedAtMS > fetchedAtMS {
			fetchedAtMS = input.dynamicProfiles[1].FetchedAtMS
		}
	}
	if spotDynamic && perpDynamic {
		feeSource, feeVerified = "EXCHANGE_API", true
	} else if spotDynamic || perpDynamic {
		feeSource = "PARTIAL_EXCHANGE_API"
	}
	hundred := decimal.NewFromInt(100)
	fees := opportunityFees{
		spotMakerPct: spotMakerPct,
		spotTakerPct: spotTakerPct,
		perpTakerPct: perpTakerPct,
		source:       feeSource,
		verified:     feeVerified,
		fetchedAtMS:  fetchedAtMS,
		spotTakerRate: decimal.NewFromFloat(spotTakerPct).Div(hundred),
		perpTakerRate: decimal.NewFromFloat(perpTakerPct).Div(hundred),
	}
	if s.cfg.FeeAssetSurchargePct > 0 {
		fees.surchargeRate = decimal.NewFromFloat(
			s.cfg.FeeAssetSurchargePct,
		).Div(hundred)
	}
	return fees
}

func (s *Service) calculateOpportunityProfit(
	input opportunityCalculationInput, sizing opportunitySizing,
	prices executionPrices, fees opportunityFees,
) (opportunityProfit, error) {
	sb, pb, fr := input.spotBook, input.perpBook, input.funding
	qty := sizing.quantity
	spotAsk := decimal.NewFromFloat(sb.Asks[0].Price)
	spotBid := decimal.NewFromFloat(sb.Bids[0].Price)
	perpBid := decimal.NewFromFloat(pb.Bids[0].Price)
	perpAsk := decimal.NewFromFloat(pb.Asks[0].Price)
	var spread, slippage, spotFees decimal.Decimal
	if sizing.makerTaker {
		spread = perpBid.Sub(sizing.spotEntryRef).Mul(qty)
		slippage = spotBid.Sub(prices.spotExit).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)
		spotMakerRate := decimal.NewFromFloat(fees.spotMakerPct).Div(
			decimal.NewFromInt(100),
		)
		spotEntryFee := prices.spotEntry.Mul(qty).Mul(spotMakerRate)
		spotExitFee := prices.spotExit.Mul(qty).Mul(fees.spotTakerRate)
		spotFees = spotEntryFee.Add(spotExitFee)
	} else {
		spread = perpBid.Sub(spotAsk).Mul(qty)
		slippage = prices.spotEntry.Sub(spotAsk).
			Add(spotBid.Sub(prices.spotExit)).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)
		spotFees = prices.spotEntry.Add(prices.spotExit).
			Mul(qty).Mul(fees.spotTakerRate)
	}
	perpFees := prices.perpEntry.Add(prices.perpExit).
		Mul(qty).Mul(fees.perpTakerRate)
	if !fees.surchargeRate.IsZero() {
		surcharge := prices.spotEntry.Add(prices.spotExit).
			Add(prices.perpEntry).Add(prices.perpExit).
			Mul(qty).Mul(fees.surchargeRate)
		spotFees = spotFees.Add(surcharge)
	}
	totalFees := spotFees.Add(perpFees)
	settlements := calculateSettlements(
		s.cfg.EstimatedHoldingHours, fr.IntervalHours, fr.NextFundingTime, input.now,
	)
	frRate := decimal.NewFromFloat(fr.Rate)
	settlementsDec := decimal.NewFromInt(int64(settlements))
	funding := prices.perpEntry.Mul(qty).Mul(frRate).Mul(settlementsDec)
	net := spread.Add(funding).Sub(totalFees).Sub(slippage)
	capital := prices.spotEntry.Mul(qty).Add(
		prices.perpEntry.Mul(qty).Div(sizing.perpLeverage),
	)
	if capital.LessThanOrEqual(decimal.Zero) {
		return opportunityProfit{}, fmt.Errorf("invalid allocated capital")
	}
	capacityPct := 100.0
	if sizing.targetBudget.GreaterThan(decimal.Zero) {
		pct := capital.Div(sizing.targetBudget).Mul(
			decimal.NewFromInt(100),
		).InexactFloat64()
		if pct < 100.0 {
			capacityPct = pct
		}
	}
	roi := net.Div(capital).Mul(decimal.NewFromInt(100))
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
	minAcceptablePerpPrice := calculateMinAcceptablePerpPrice(
		minAcceptablePerpPriceParams{
			spotEntry:         prices.spotEntry,
			perpExit:          prices.perpExit,
			qty:               qty,
			spotFees:          spotFees,
			perpTakerFeeRate:  fees.perpTakerRate,
			frRate:            frRate,
			settlementsDec:    settlementsDec,
			slippage:          slippage,
			minNetProfitQuote: s.cfg.MinNetProfitQuote,
		},
	)
	return opportunityProfit{
		spread: spread, slippage: slippage, fees: totalFees, funding: funding,
		net: net, capital: capital, capacityPct: capacityPct, roi: roi,
		status: status, minAcceptablePerpPrice: minAcceptablePerpPrice,
	}, nil
}

func (s *Service) buildOpportunity(
	input opportunityCalculationInput, sizing opportunitySizing,
	prices executionPrices, fees opportunityFees, profit opportunityProfit,
) domain.Opportunity {
	sm, pm, sb := input.spotMarket, input.perpMarket,
		input.spotBook
	execMode := s.cfg.ExecutionMode
	if execMode == "" {
		execMode = "TAKER_TAKER"
	}
	ttlSeconds := s.cfg.MaxOrderTTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = 30
	}
	targetQuote := pm.QuoteAsset
	if !input.isSynthetic {
		targetQuote = sm.QuoteAsset
	}
	spotEntryFeePct := fees.spotTakerPct
	if sizing.makerTaker {
		spotEntryFeePct = fees.spotMakerPct
	}
	return domain.Opportunity{
		OpportunityKey:         fmt.Sprintf("%s/%s:%s:%s", sm.BaseAsset, targetQuote, sm.Symbol, pm.Symbol),
		SpotEntryFeePct:        spotEntryFeePct,
		SpotExitFeePct:         fees.spotTakerPct,
		PerpEntryFeePct:        fees.perpTakerPct,
		PerpExitFeePct:         fees.perpTakerPct,
		FeeSource:              fees.source,
		FeeVerified:            fees.verified,
		FeeFetchedAtMS:         fees.fetchedAtMS,
		FeeAssetSurchargePct:   s.cfg.FeeAssetSurchargePct,
		LocalExchange:          sm.Exchange,
		GlobalExchange:         pm.Exchange,
		BaseAsset:              sm.BaseAsset,
		QuoteAsset:             targetQuote,
		SpotSymbol:             sm.Symbol,
		PerpSymbol:              pm.Symbol,
		ExecutionMode:          execMode,
		SpotEntryPrice:         prices.spotEntry.InexactFloat64(),
		SpotExitPrice:          prices.spotExit.InexactFloat64(),
		PerpEntryPrice:         prices.perpEntry.InexactFloat64(),
		PerpExitPrice:          prices.perpExit.InexactFloat64(),
		Quantity:               sizing.quantity.InexactFloat64(),
		SpreadPnL:              profit.spread.InexactFloat64(),
		FundingPnL:             profit.funding.InexactFloat64(),
		TradingFees:            profit.fees.InexactFloat64(),
		Slippage:               profit.slippage.InexactFloat64(),
		NetProfit:              profit.net.InexactFloat64(),
		AllocatedCapital:       profit.capital.InexactFloat64(),
		NetROIPct:              profit.roi.InexactFloat64(),
		ProfitabilityStatus:    profit.status,
		LastScannedAt:          input.now,
		SpotTargetLimitPrice:   prices.spotEntry.InexactFloat64(),
		MinAcceptablePerpPrice: profit.minAcceptablePerpPrice.InexactFloat64(),
		SpotQueueDepth:         sb.Bids[0].Quantity,
		MaxOrderTTLSeconds:     ttlSeconds,
		IsSynthetic:            input.isSynthetic,
		SyntheticFXRate:        input.fxRate,
		OriginalSpotQuote:      sm.QuoteAsset,
		CapacityPct:            profit.capacityPct,
	}
}

func (s *Service) calculate(
	input opportunityCalculationInput,
) (domain.Opportunity, error) {
	if err := s.validateCalculationInput(input); err != nil {
		return domain.Opportunity{}, err
	}
	sizing, err := s.calculateOpportunitySizing(input)
	if err != nil {
		return domain.Opportunity{}, err
	}
	prices, err := calculateExecutionPrices(
		input.spotBook, input.perpBook, sizing.quantity,
		sizing.makerTaker, sizing.spotEntryRef,
	)
	if err != nil {
		return domain.Opportunity{}, err
	}
	fees := s.resolveOpportunityFees(input)
	profit, err := s.calculateOpportunityProfit(input, sizing, prices, fees)
	if err != nil {
		return domain.Opportunity{}, err
	}
	op := s.buildOpportunity(input, sizing, prices, fees, profit)
	s.evaluateOpportunity(
		&op, input.positionReason,
		decimal.NewFromFloat(input.perpBook.Bids[0].Price).InexactFloat64(),
		sizing.spotEntryRef.InexactFloat64(), input.funding.Rate,
	)
	return op, nil
}
