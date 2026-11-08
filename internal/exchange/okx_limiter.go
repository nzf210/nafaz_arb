package exchange

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OKX public REST limits (per IP, default pricing tier): 20 req/2s for market
// data endpoints, with /market/books at 40 req/2s and /public/instruments at
// 10 req/2s. The defaults below sit well under those ceilings on purpose.
const (
	okxErrorRateLimitCode = "50011"

	okxDefaultRatePer2s = 15.0 // global gate for public REST

	okxInstrumentsRatePer2s = 5.0  // official ceiling 10/2s
	okxBooksRatePer2s       = 15.0 // official ceiling 40/2s
	okxFundingRatePer2s     = 10.0 // official ceiling 20/2s

	okxMaxRetries     = 3
	okxBaseBackoff    = 500 * time.Millisecond
	okxRequestTimeout = 15 * time.Second
	okxSlotWait       = 2 * time.Second // max wait for an in-flight slot
	okxTickWait       = 50 * time.Millisecond

	okxMaxConcurrent = 3
)

// OKXLimiterConfig tunes the shared OKX limiter. Zero fields fall back to the
// conservative defaults above.
type OKXLimiterConfig struct {
	DefaultRatePer2s     float64
	BooksRatePer2s       float64
	InstrumentsRatePer2s float64
	FundingRatePer2s     float64
	MaxConcurrent        int
}

// endpointClass maps a request path (with query) onto a limiter bucket. Query
// strings carry per-symbol parameters, so they must be stripped or the limiter
// would allocate one bucket per symbol and never actually throttle.
func endpointClass(path string) string {
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	switch {
	case strings.HasPrefix(path, "/api/v5/market/books"):
		return "/api/v5/market/books"
	case strings.HasPrefix(path, "/api/v5/public/instruments"):
		return "/api/v5/public/instruments"
	case strings.HasPrefix(path, "/api/v5/public/funding-rate"):
		return "/api/v5/public/funding-rate"
	}
	return path
}

func okxRateFor(class string, cfg OKXLimiterConfig) float64 {
	switch class {
	case "/api/v5/market/books":
		return orDefault(cfg.BooksRatePer2s, okxBooksRatePer2s)
	case "/api/v5/public/instruments":
		return orDefault(cfg.InstrumentsRatePer2s, okxInstrumentsRatePer2s)
	case "/api/v5/public/funding-rate":
		return orDefault(cfg.FundingRatePer2s, okxFundingRatePer2s)
	}
	return orDefault(cfg.DefaultRatePer2s, okxDefaultRatePer2s)
}

func orDefault(v, fallback float64) float64 {
	if v > 0 {
		return v
	}
	return fallback
}

// OKXEndpointMetrics is the observability payload for one OKX endpoint class.
type OKXEndpointMetrics struct {
	Endpoint     string  `json:"endpoint"`
	Total        int64   `json:"total"`
	Success      int64   `json:"success"`
	Failed       int64   `json:"failed"`
	RateLimited  int64   `json:"rate_limited"`
	Retries      int64   `json:"retries"`
	Timeouts     int64   `json:"timeouts"`
	AvgLatencyMS float64 `json:"avg_latency_ms"`
}

// OKXMetricsSnapshot is the full limiter observability payload.
type OKXMetricsSnapshot struct {
	Endpoints map[string]OKXEndpointMetrics `json:"endpoints"`
	TopErrors []OKXEndpointMetrics          `json:"top_errors"`
}

// okxMetrics counts per-endpoint outcomes since process start.
type okxMetrics struct {
	mu           sync.Mutex
	total        map[string]int64
	success      map[string]int64
	failed       map[string]int64
	rateLimited  map[string]int64
	retries      map[string]int64
	timeouts     map[string]int64
	latencySumMS map[string]int64
}

func newOKXMetrics() *okxMetrics {
	return &okxMetrics{
		total:        map[string]int64{},
		success:      map[string]int64{},
		failed:       map[string]int64{},
		rateLimited:  map[string]int64{},
		retries:      map[string]int64{},
		timeouts:     map[string]int64{},
		latencySumMS: map[string]int64{},
	}
}

func (m *okxMetrics) record(endpoint, outcome string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total[endpoint]++
	m.latencySumMS[endpoint] += latency.Milliseconds()
	switch outcome {
	case "success":
		m.success[endpoint]++
	case "failed":
		m.failed[endpoint]++
	case "rate_limited":
		m.rateLimited[endpoint]++
	case "timeout":
		m.timeouts[endpoint]++
		m.failed[endpoint]++
	}
}

func (m *okxMetrics) recordRetry(endpoint string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries[endpoint]++
}

func (m *okxMetrics) snapshot() OKXMetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]OKXEndpointMetrics, len(m.total))
	for ep, total := range m.total {
		var avg float64
		if total > 0 {
			avg = float64(m.latencySumMS[ep]) / float64(total)
		}
		out[ep] = OKXEndpointMetrics{
			Endpoint:     ep,
			Total:        total,
			Success:      m.success[ep],
			Failed:       m.failed[ep],
			RateLimited:  m.rateLimited[ep],
			Retries:      m.retries[ep],
			Timeouts:     m.timeouts[ep],
			AvgLatencyMS: avg,
		}
	}
	top := make([]OKXEndpointMetrics, 0, len(out))
	for _, em := range out {
		if em.Failed > 0 {
			top = append(top, em)
		}
	}
	for i := 1; i < len(top); i++ {
		for j := i; j > 0 && top[j].Failed > top[j-1].Failed; j-- {
			top[j], top[j-1] = top[j-1], top[j]
		}
	}
	return OKXMetricsSnapshot{Endpoints: out, TopErrors: top}
}

// okxEndpointState is a token bucket for one endpoint class.
type okxEndpointState struct {
	mu         sync.Mutex
	interval   time.Duration
	capacity   int
	tokens     float64
	lastRefill time.Time
}

// okxLimiter serializes OKX public REST traffic: a per-endpoint-class token
// bucket plus a hard cap on in-flight requests. The concurrency cap is what
// stops the 15-worker scanner pool from bursting past OKX's limits. Only OKX
// requests go through it; other venues keep their own paths untouched.
type okxLimiter struct {
	slots   chan struct{} // buffered to maxConcurrent
	mu      sync.Mutex
	states  map[string]*okxEndpointState
	metrics *okxMetrics
	cfg     OKXLimiterConfig
}

func newOKXLimiter(cfg OKXLimiterConfig) *okxLimiter {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = okxMaxConcurrent
	}
	return &okxLimiter{
		slots:   make(chan struct{}, cfg.MaxConcurrent),
		states:  map[string]*okxEndpointState{},
		metrics: newOKXMetrics(),
		cfg:     cfg,
	}
}

func (l *okxLimiter) stateFor(class string) *okxEndpointState {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.states[class]
	if !ok {
		per2s := okxRateFor(class, l.cfg)
		capacity := int(math.Max(1, math.Floor(per2s)))
		st = &okxEndpointState{
			interval:   time.Duration(float64(2*time.Second) / per2s),
			capacity:   capacity,
			tokens:     float64(capacity),
			lastRefill: time.Now(),
		}
		l.states[class] = st
	}
	return st
}

// acquire takes an in-flight slot and one rate token for the class. It returns
// false when the context ends or a slot does not free within okxSlotWait, so a
// wedged request can never deadlock a scan.
func (l *okxLimiter) acquire(ctx context.Context, class string) bool {
	select {
	case l.slots <- struct{}{}:
	case <-ctx.Done():
		return false
	case <-time.After(okxSlotWait):
		return false
	}

	st := l.stateFor(class)
	for {
		if ctx.Err() != nil {
			l.release()
			return false
		}
		st.mu.Lock()
		now := time.Now()
		st.tokens = math.Min(float64(st.capacity), st.tokens+now.Sub(st.lastRefill).Seconds()/st.interval.Seconds())
		st.lastRefill = now
		if st.tokens >= 1 {
			st.tokens--
			st.mu.Unlock()
			return true
		}
		wait := time.Duration((1 - st.tokens) * float64(st.interval))
		st.mu.Unlock()
		if wait > okxTickWait {
			wait = okxTickWait
		}
		select {
		case <-ctx.Done():
			l.release()
			return false
		case <-time.After(wait):
		}
	}
}

func (l *okxLimiter) release() { <-l.slots }

// newOKXRequest builds a GET request against the OKX base URL for a limiter
// path, with the scanner's User-Agent.
func (l *LiveAdapter) newOKXRequest(ctx context.Context, path string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.okxBaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
	return req, nil
}

// okxDo performs one OKX round trip under the shared limiter, with a per-request
// timeout and retry on 429/50011 using exponential backoff plus jitter. The
// request is rebuilt for every attempt, so a retry can never replay a consumed
// body. Errors that cannot improve on retry are returned immediately.
func (l *LiveAdapter) okxDo(ctx context.Context, path string, build func() (*http.Request, error)) (*http.Response, error) {
	class := endpointClass(path)

	l.okxLimiterOnce.Do(func() {
		l.okxLimiter = newOKXLimiter(l.okxLimiterCfg)
	})

	var lastErr error
	for attempt := 0; attempt <= okxMaxRetries; attempt++ {
		if attempt > 0 {
			l.okxLimiter.metrics.recordRetry(class)
		}
		if !l.okxLimiter.acquire(ctx, class) {
			if lastErr == nil {
				lastErr = fmt.Errorf("okx %s: limiter slot unavailable", class)
			}
			return nil, lastErr
		}

		started := time.Now()
		req, err := build()
		if err != nil {
			l.okxLimiter.release()
			return nil, err
		}
		rctx, cancel := context.WithTimeout(ctx, okxRequestTimeout)
		resp, err := l.client.Do(req.WithContext(rctx))
		latency := time.Since(started)
		l.okxLimiter.release()

		if err != nil {
			cancel()
			outcome := "failed"
			if isTimeout(err) {
				outcome = "timeout"
			}
			l.okxLimiter.metrics.record(class, outcome, latency)
			lastErr = err
			if ctx.Err() != nil || !retryableTransportErr(err) {
				return nil, err
			}
			if !l.sleepBackoff(ctx, attempt, 0) {
				return nil, lastErr
			}
			continue
		}

		// Buffer the body BEFORE cancel: cancelling the request context kills
		// the response body's underlying connection, so reading after cancel
		// fails with "read on closed response body". The returned response
		// therefore carries a fully buffered body, safe to outlive rctx.
		limited, retryAfter, bodyErr := inspectOKXResponse(resp)
		cancel()
		if bodyErr != nil {
			l.okxLimiter.metrics.record(class, "failed", latency)
			lastErr = bodyErr
			if ctx.Err() != nil || !retryableTransportErr(bodyErr) {
				return nil, bodyErr
			}
			if !l.sleepBackoff(ctx, attempt, 0) {
				return nil, lastErr
			}
			continue
		}

		if limited {
			l.okxLimiter.metrics.record(class, "rate_limited", latency)
			lastErr = &okxRateLimitError{retryAfter: retryAfter}
			if !l.sleepBackoff(ctx, attempt, retryAfter) {
				return nil, lastErr
			}
			continue
		}

		if resp.StatusCode == http.StatusOK {
			l.okxLimiter.metrics.record(class, "success", latency)
		} else {
			l.okxLimiter.metrics.record(class, "failed", latency)
		}
		return resp, nil
	}
	return nil, lastErr
}

// inspectOKXResponse classifies a response as rate-limited. It must run while
// the request context is still alive: it drains the body on 429 and otherwise
// buffers the full body (OKX instruments responses are ~600KB; a truncated
// buffer corrupts the caller's JSON decode) so the returned response stays
// readable after the caller's context is cancelled. Returns a body error when
// the payload could not be read.
func inspectOKXResponse(resp *http.Response) (bool, time.Duration, error) {
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
	if resp.StatusCode == http.StatusTooManyRequests {
		closeBody(resp)
		return true, retryAfter, nil
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return false, 0, nil
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return false, 0, err
	}
	resp.Body = io.NopCloser(strings.NewReader(string(body)))
	if strings.Contains(string(body), `"`+okxErrorRateLimitCode+`"`) {
		return true, retryAfter, nil
	}
	return false, 0, nil
}

// sleepBackoff waits before the next attempt and reports whether to continue.
func (l *LiveAdapter) sleepBackoff(ctx context.Context, attempt int, retryAfter time.Duration) bool {
	if attempt >= okxMaxRetries {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(backoffWithJitter(attempt, retryAfter)):
		return true
	}
}

func backoffWithJitter(attempt int, retryAfter time.Duration) time.Duration {
	base := okxBaseBackoff * time.Duration(1<<attempt) // 500ms, 1s, 2s
	if retryAfter > base {
		base = retryAfter
	}
	return base + time.Duration(rand.Int63n(int64(base/4)+1))
}

func isTimeout(err error) bool {
	for e := err; e != nil; {
		if t, ok := e.(interface{ Timeout() bool }); ok {
			return t.Timeout()
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}

func retryableTransportErr(err error) bool {
	if err == nil {
		return false
	}
	if isTimeout(err) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "unexpected")
}

func parseRetryAfter(h string) time.Duration {
	s := strings.TrimSpace(h)
	if s == "" {
		return 0
	}
	if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(s); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func closeBody(resp *http.Response) {
	// Drain fully: a truncated drain can leave the connection unusable for
	// keep-alive reuse and misreports body size to the transport.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// okxRateLimitError reports a 429 / 50011 so callers can retry with backoff.
type okxRateLimitError struct {
	retryAfter time.Duration
}

func (e *okxRateLimitError) Error() string {
	return fmt.Sprintf("okx rate limited (429/50011), retry after %s", e.retryAfter)
}
