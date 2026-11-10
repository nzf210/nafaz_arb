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
	spotMakerFeePct := s.cfg.SpotMakerFeePct
	spotTakerFeePct := s.cfg.SpotTakerFeePct
	perpTakerFeePct := s.cfg.PerpTakerFeePct
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(sm.Exchange)]; ok {
		if profile.SpotMakerFeePct != nil {
			spotMakerFeePct = *profile.SpotMakerFeePct
		}
		if profile.SpotTakerFeePct != nil {
			spotTakerFeePct = *profile.SpotTakerFeePct
		}
	}
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(pm.Exchange)]; ok && profile.PerpTakerFeePct != nil {
		perpTakerFeePct = *profile.PerpTakerFeePct
	}
	spotTakerFeeRate := decimal.NewFromFloat(spotTakerFeePct).Div(hundred)
	perpTakerFeeRate := decimal.NewFromFloat(perpTakerFeePct).Div(hundred)
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

		spotMakerFeeRate := decimal.NewFromFloat(spotMakerFeePct).Div(hundred)
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
		SpotEntryFeePct:        func() float64 { if isMakerTaker { return spotMakerFeePct }; return spotTakerFeePct }(),
		SpotExitFeePct:         spotTakerFeePct,
		PerpEntryFeePct:        perpTakerFeePct,
		PerpExitFeePct:         perpTakerFeePct,
		FeeSource:              "CONFIG",
		FeeVerified:            false,
		LocalExchange:          sm.Exchange,
		GlobalExchange:         pm.Exchange,
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
