package scanner

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func (s *Service) filterActivePairs(pairs [][2]domain.Market, now time.Time) ([][2]domain.Market, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var activePairs [][2]domain.Market
	frozenCount := 0
	for _, pair := range pairs {
		pairKey := strings.ToLower(pair[0].Exchange) + ":" + pair[0].Symbol + "->" + strings.ToLower(pair[1].Exchange) + ":" + pair[1].Symbol
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
