package scanner

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
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
	cfg            config.Config
	adapter        exchange.ReadOnlyAdapter
	fundingWorker  *exchange.FundingWorker
	redisClient    *redishandler.Client
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
	// Match direct quote assets and, when enabled, synthesize IDR/BIDR spot
	// markets against USDT perpetuals. IDR order books are normalized using
	// the live USDT/IDR ticker inside evaluatePair before spread calculation.
	pairs := exchange.MatchMarketsForExchanges(spot, perp, s.cfg.QuoteAssets, s.cfg.SpotExchanges, s.cfg.PerpetualExchanges, s.cfg.EnableSyntheticIDR)
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
