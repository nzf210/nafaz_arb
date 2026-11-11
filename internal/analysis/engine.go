package analysis

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// Engine runs independent analysis strategies over the same normalized snapshot.
type Engine struct {
	mu         sync.RWMutex
	strategies []Strategy
}

func NewEngine(strategies ...Strategy) (*Engine, error) {
	engine := &Engine{}
	for _, strategy := range strategies {
		if err := engine.Register(strategy); err != nil {
			return nil, err
		}
	}
	return engine, nil
}

// Register adds one strategy. Names are stable identifiers and must be unique.
func (e *Engine) Register(strategy Strategy) error {
	if strategy == nil || strings.TrimSpace(strategy.Name()) == "" {
		return fmt.Errorf("analysis strategy and name are required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, current := range e.strategies {
		if strings.EqualFold(current.Name(), strategy.Name()) {
			return fmt.Errorf("analysis strategy %q already registered", strategy.Name())
		}
	}
	e.strategies = append(e.strategies, strategy)
	return nil
}

// Analyze isolates strategy failures: a broken strategy does not discard valid
// results from other strategies. Callers must inspect errors before acting.
func (e *Engine) Analyze(ctx context.Context, snapshot Snapshot) ([]Signal, []error) {
	e.mu.RLock()
	strategies := append([]Strategy(nil), e.strategies...)
	e.mu.RUnlock()

	signals := make([]Signal, 0)
	var failures []error
	for _, strategy := range strategies {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			break
		}
		results, err := strategy.Analyze(ctx, snapshot)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", strategy.Name(), err))
			continue
		}
		for _, signal := range results {
			if err := validateSignal(strategy.Name(), signal); err != nil {
				failures = append(failures, fmt.Errorf("%s returned invalid signal: %w", strategy.Name(), err))
				continue
			}
			if signal.Strategy == "" {
				signal.Strategy = strategy.Name()
			}
			signals = append(signals, signal)
		}
	}
	sort.SliceStable(signals, func(i, j int) bool {
		return signals[i].Confidence > signals[j].Confidence
	})
	return signals, failures
}

func validateSignal(strategy string, signal Signal) error {
	if strings.TrimSpace(signal.Instrument) == "" || strings.TrimSpace(signal.Venue) == "" {
		return fmt.Errorf("instrument and venue are required")
	}
	if signal.Side != SideLong && signal.Side != SideShort && signal.Side != SideFlat {
		return fmt.Errorf("unsupported side %q", signal.Side)
	}
	if math.IsNaN(signal.Confidence) || math.IsInf(signal.Confidence, 0) ||
		signal.Confidence < 0 || signal.Confidence > 1 {
		return fmt.Errorf("confidence must be finite and within [0,1]")
	}
	if signal.Strategy != "" && !strings.EqualFold(signal.Strategy, strategy) {
		return fmt.Errorf("strategy identity mismatch: got %q", signal.Strategy)
	}
	return nil
}
