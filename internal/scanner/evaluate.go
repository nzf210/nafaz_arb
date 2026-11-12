package scanner

import (
	"context"
	"errors"
	"math"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
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
	// Evaluate freshness against the time after both order books were received,
	// not the beginning of the full scan (which can be seconds earlier).
	evaluationTime := time.Now().UTC()
	var dynamicSpotFee, dynamicPerpFee *redishandler.DynamicFeeProfile
	s.mu.Lock()
	feeRedis := s.redisClient
	s.mu.Unlock()
	if feeRedis != nil {
		dynamicSpotFee, _ = feeRedis.GetDynamicFeeProfile(ctx, sm.Exchange, "spot", sm.Symbol)
		dynamicPerpFee, _ = feeRedis.GetDynamicFeeProfile(ctx, pm.Exchange, "perp", pm.Symbol)
	}
	op, err := s.calculate(opportunityCalculationInput{
		spotMarket:       sm,
		perpMarket:       pm,
		spotBook:         sb,
		perpBook:         pb,
		funding:          fr,
		now:              evaluationTime,
		positionReason:   positionReason,
		isSynthetic:      isSynthetic,
		fxRate:           fxRate,
		requiredNotional: requiredNotional,
		dynamicProfiles:  []*redishandler.DynamicFeeProfile{dynamicSpotFee, dynamicPerpFee},
	})
	if err != nil {
		if strings.Contains(err.Error(), "empty order book") {
			s.recordEmptyBook(pairKey, started)
		}
		return evalResult{
			exchangeMetrics: exchangeMetrics,
			err:             err,
		}
	}
	// Preserve the exact venue pairing used for this evaluation so alerts and history
	// never infer an exchange from a venue-specific symbol format.
	op.LocalExchange = sm.Exchange
	op.GlobalExchange = pm.Exchange

	// Executor active-position state is resolved inside applyHybridMakerTakerRules
	// (which owns the Redis client); there is no prior knowledge here.
	if !fxLive && isIDRQuote {
		// Defense in depth: unreachable in normal flow (the skip above fires
		// first), but kept as a hard gate so a fallback-rate opportunity can
		// never be marked trading-eligible.
		op.ExclusionReasons = append(op.ExclusionReasons, "FX_RATE_FALLBACK_ACTIVE")
	}
	s.clearEmptyBook(pairKey)

	// Lapis 1 & 2 Filter Evaluation & Redis Directives
	publishErr := s.applyHybridMakerTakerRules(ctx, &op, sm, pm, rawSB, pb, fr, fxRate, isIDRQuote, evaluationTime)

	return evalResult{
		op:              op,
		exchangeMetrics: exchangeMetrics,
		err:             publishErr,
		fatalErr:        publishErr != nil,
	}
}

func validAmountSpec(spec *domain.AmountSpec) bool {
	if spec == nil || spec.Step <= 0 || spec.Min < 0 || spec.BasePerUnit <= 0 {
		return false
	}
	if math.IsNaN(spec.Step) || math.IsInf(spec.Step, 0) ||
		math.IsNaN(spec.Min) || math.IsInf(spec.Min, 0) ||
		math.IsNaN(spec.BasePerUnit) || math.IsInf(spec.BasePerUnit, 0) {
		return false
	}
	return spec.Unit == "BASE" || spec.Unit == "CONTRACT"
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
