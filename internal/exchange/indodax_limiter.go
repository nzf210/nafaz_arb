package exchange

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	indodaxDefaultRatePerSec     = 5.0
	indodaxDefaultBurst          = 6
	indodaxMaxConcurrent         = 3
	indodaxSlotWait              = 5 * time.Second
	indodaxTickWait              = 50 * time.Millisecond
	indodaxDefault429Cooldown    = 60 * time.Second
	indodaxDefault403Cooldown    = 5 * time.Minute
	indodaxMaxCooldown           = 24 * time.Hour
	mexcSpotMaxConcurrent        = 4
	mexcSpotDefaultRatePerSec    = 10.0
	mexcSpotDefaultBurst         = 10

	tokocryptoMaxConcurrent     = 6
	tokocryptoDefaultRatePerSec = 15.0
	tokocryptoDefaultBurst      = 15
)

// venueLimiter enforces concurrency, token-bucket pacing, and HTTP 403/429
// Retry-After cooldowns for public REST endpoints so 15 scanner workers
// do not trigger exchange or Cloudflare WAF bans.
type venueLimiter struct {
	name          string
	slots         chan struct{}
	ratePerSec    float64
	capacity      int
	mu            sync.Mutex
	tokens        float64
	lastRefill    time.Time
	blockedUntil  time.Time
	blockReason   string
	now           func() time.Time
}

func newVenueLimiter(name string, maxConcurrent int, ratePerSec float64, burst int) *venueLimiter {
	if maxConcurrent <= 0 {
		maxConcurrent = 2
	}
	if ratePerSec <= 0 {
		ratePerSec = 5.0
	}
	if burst <= 0 {
		burst = 5
	}
	now := time.Now
	return &venueLimiter{
		name:       name,
		slots:      make(chan struct{}, maxConcurrent),
		ratePerSec: ratePerSec,
		capacity:   burst,
		tokens:     float64(burst),
		lastRefill: now(),
		now:        now,
	}
}

func (l *venueLimiter) checkBlocked() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Before(l.blockedUntil) {
		return fmt.Errorf("%w: %s cooldown active until %s (%s)",
			ErrExchangeUnavailable, l.name, l.blockedUntil.UTC().Format(time.RFC3339), l.blockReason)
	}
	return nil
}

func (l *venueLimiter) recordHTTPStatus(status int, retryAfter string) {
	if status != http.StatusForbidden && status != http.StatusTooManyRequests {
		return
	}
	cooldown := parseRetryAfterDuration(retryAfter, l.now())
	if cooldown <= 0 {
		if status == http.StatusForbidden {
			cooldown = indodaxDefault403Cooldown
		} else {
			cooldown = indodaxDefault429Cooldown
		}
	}
	if cooldown > indodaxMaxCooldown {
		cooldown = indodaxMaxCooldown
	}
	until := l.now().Add(cooldown)
	l.mu.Lock()
	if until.After(l.blockedUntil) {
		l.blockedUntil = until
		l.blockReason = fmt.Sprintf("HTTP %d", status)
	}
	l.mu.Unlock()
}

func parseRetryAfterDuration(raw string, now time.Time) time.Duration {
	val := strings.TrimSpace(raw)
	if val == "" {
		return 0
	}
	if secs, err := strconv.Atoi(val); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(val); err == nil {
		d := t.Sub(now)
		if d > 0 {
			return d
		}
	}
	return 0
}

func (l *venueLimiter) acquire(ctx context.Context) error {
	if err := l.checkBlocked(); err != nil {
		return err
	}
	select {
	case l.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(indodaxSlotWait):
		return fmt.Errorf("%s: limiter slot unavailable", l.name)
	}

	for {
		if err := l.checkBlocked(); err != nil {
			l.release()
			return err
		}
		if err := ctx.Err(); err != nil {
			l.release()
			return err
		}
		l.mu.Lock()
		now := l.now()
		elapsed := now.Sub(l.lastRefill).Seconds()
		if elapsed > 0 {
			l.tokens = math.Min(float64(l.capacity), l.tokens+elapsed*l.ratePerSec)
			l.lastRefill = now
		}
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - l.tokens) / l.ratePerSec * float64(time.Second))
		l.mu.Unlock()
		if wait > indodaxTickWait {
			wait = indodaxTickWait
		}
		if wait <= 0 {
			wait = time.Millisecond
		}
		select {
		case <-ctx.Done():
			l.release()
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (l *venueLimiter) release() { <-l.slots }

func (l *LiveAdapter) getIndodaxLimiter() *venueLimiter {
	l.indodaxLimiterOnce.Do(func() {
		l.indodaxLimiter = newVenueLimiter("indodax", indodaxMaxConcurrent, indodaxDefaultRatePerSec, indodaxDefaultBurst)
	})
	return l.indodaxLimiter
}

func (l *LiveAdapter) getMEXCSpotLimiter() *venueLimiter {
	l.mexcSpotLimiterOnce.Do(func() {
		l.mexcSpotLimiter = newVenueLimiter("mexc_spot", mexcSpotMaxConcurrent, mexcSpotDefaultRatePerSec, mexcSpotDefaultBurst)
	})
	return l.mexcSpotLimiter
}

func (l *LiveAdapter) indodaxDo(ctx context.Context, req *http.Request) (*http.Response, error) {
	lim := l.getIndodaxLimiter()
	if err := lim.acquire(ctx); err != nil {
		return nil, err
	}
	resp, err := l.client.Do(req)
	lim.release()
	if err != nil {
		return nil, err
	}
	lim.recordHTTPStatus(resp.StatusCode, resp.Header.Get("Retry-After"))
	return resp, nil
}

func (l *LiveAdapter) mexcSpotDo(ctx context.Context, req *http.Request) (*http.Response, error) {
	lim := l.getMEXCSpotLimiter()
	if err := lim.acquire(ctx); err != nil {
		return nil, err
	}
	resp, err := l.client.Do(req)
	lim.release()
	if err != nil {
		return nil, err
	}
	lim.recordHTTPStatus(resp.StatusCode, resp.Header.Get("Retry-After"))
	return resp, nil
}

func (l *LiveAdapter) getTokocryptoLimiter() *venueLimiter {
	l.tokocryptoLimiterOnce.Do(func() {
		l.tokocryptoLimiter = newVenueLimiter("tokocrypto", tokocryptoMaxConcurrent, tokocryptoDefaultRatePerSec, tokocryptoDefaultBurst)
	})
	return l.tokocryptoLimiter
}

func (l *LiveAdapter) tokocryptoDo(ctx context.Context, req *http.Request) (*http.Response, error) {
	lim := l.getTokocryptoLimiter()
	if err := lim.acquire(ctx); err != nil {
		return nil, err
	}
	resp, err := l.client.Do(req)
	lim.release()
	if err != nil {
		return nil, err
	}
	lim.recordHTTPStatus(resp.StatusCode, resp.Header.Get("Retry-After"))
	return resp, nil
}
