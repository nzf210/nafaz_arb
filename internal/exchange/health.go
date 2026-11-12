package exchange

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
)

// ErrExchangeUnavailable is returned when an exchange is believed to be in
// maintenance. Callers can check for it with errors.Is to skip that venue's
// pairs instead of treating the failure as a per-pair data problem.
var ErrExchangeUnavailable = errors.New("exchange unavailable (maintenance detected)")

// HealthPolicy describes when an exchange is considered unavailable.
type HealthPolicy struct {
	// Window is how far back successes and failures are counted.
	Window time.Duration
	// ErrorRateThreshold is the fraction of failed requests (0..1) that marks
	// the exchange unavailable.
	ErrorRateThreshold float64
	// MinRequests avoids tripping the breaker on a single unlucky call.
	MinRequests int
}

// Enabled reports whether the policy is configured well enough to run.
func (p HealthPolicy) Enabled() bool {
	return p.Window > 0 && p.ErrorRateThreshold > 0
}

// outcome is one recorded request against an exchange.
type outcome struct {
	at     time.Time
	failed bool
}

// availability tracks a rolling window of request outcomes per exchange.
// A single counter pair would let a long window hide a fresh outage, so every
// outcome carries its own timestamp and old ones are evicted as it fills.
type availability struct {
	mu       sync.Mutex
	policy   HealthPolicy
	now      func() time.Time
	outcomes map[string][]outcome
}

func newAvailability(policy HealthPolicy, now func() time.Time) *availability {
	if now == nil {
		now = time.Now
	}
	return &availability{
		policy:   policy,
		now:      now,
		outcomes: make(map[string][]outcome),
	}
}

// record registers the result of one request against an exchange.
func (a *availability) record(exchange string, failed bool) {
	if !a.policy.Enabled() {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.now()
	cutoff := now.Add(-a.policy.Window)
	kept := a.outcomes[exchange][:0]
	for _, o := range a.outcomes[exchange] {
		if o.at.After(cutoff) {
			kept = append(kept, o)
		}
	}
	a.outcomes[exchange] = append(kept, outcome{at: now, failed: failed})
}

// unavailable reports whether the exchange has crossed the error threshold.
func (a *availability) unavailable(exchange string) bool {
	if !a.policy.Enabled() {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	cutoff := a.now().Add(-a.policy.Window)
	total, failed := 0, 0
	for _, o := range a.outcomes[exchange] {
		if !o.at.After(cutoff) {
			continue
		}
		total++
		if o.failed {
			failed++
		}
	}
	if total < a.policy.MinRequests {
		return false
	}
	return float64(failed)/float64(total) >= a.policy.ErrorRateThreshold
}

// reset clears one exchange's history, e.g. after a successful recovery.
func (a *availability) reset(exchange string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.outcomes, exchange)
}

// classifyHTTPError marks the maintenance signals a venue uses to mean
// "this endpoint is temporarily down" rather than "your request was wrong".
func classifyHTTPError(status int) bool {
	switch status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// AccountStatus reports whether a venue currently accepts deposits and
// withdrawals. A suspended wallet means an eventual exit leg (selling spot,
// transferring funds) cannot settle even if quoting still works, so the
// scanner should skip pairs on that venue.
type AccountStatus = contracts.AccountStatus

type AccountStatusReporter = contracts.AccountStatusProvider

// ErrWalletSuspended wraps errors so callers can tell a suspended wallet from
// a status-check transport failure. Transport failures must fail open; a real
// suspension should fail closed.
var ErrWalletSuspended = errors.New("wallet suspended (deposit or withdrawal disabled)")
