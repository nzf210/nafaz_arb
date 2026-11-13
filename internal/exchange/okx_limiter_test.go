package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newLimiterTestAdapter points a LiveAdapter at a test server and installs a
// small limiter config so tests stay fast.
func newLimiterTestAdapter(serverURL string, cfg OKXLimiterConfig) *LiveAdapter {
	a := NewLiveAdapter()
	a.SetBaseURLs("", serverURL, "", "", "")
	// Tests serve synthetic order books; seed authoritative fixture metadata
	// rather than relying on the removed production fallback contract size.
	a.contractSizes["BTC-USDT-SWAP"] = 1
	for i := 0; i < 100; i++ {
		a.contractSizes[fmt.Sprintf("BTC%d-USDT-SWAP", i)] = 1
	}
	a.SetOKXLimiterConfig(cfg)
	return a
}

func okxBooksBody() string {
	return `{"code":"0","msg":"","data":[{"bids":[["60000","1","0","1"]],"asks":[["60050","2","0","1"]],"ts":"1690000000000"}]}`
}

// TestOKXBurstIsThrottled serves a burst of OKX book requests through one
// adapter and asserts the limiter never exceeds its configured in-flight cap.
func TestOKXBurstIsThrottled(t *testing.T) {
	var inFlight, maxSeen int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			prev := atomic.LoadInt32(&maxSeen)
			if cur <= prev || atomic.CompareAndSwapInt32(&maxSeen, prev, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okxBooksBody()))
	}))
	defer server.Close()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{
		BooksRatePer2s: 1000, // rate is not the limiter under test here
		MaxConcurrent:  3,
	})

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := adapter.PerpetualOrderBook(context.Background(), fmt.Sprintf("BTC%d-USDT-SWAP", i)); err != nil {
				t.Errorf("book %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxSeen); got > 3 {
		t.Fatalf("in-flight requests peaked at %d, want <= 3", got)
	}
}

// TestOKXError50011RetriesThenSucceeds proves an OKX 200-with-code-50011 body
// is treated as rate limiting and retried to success.
func TestOKXError50011RetriesThenSucceeds(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		w.Header().Set("Content-Type", "application/json")
		if n < 3 {
			_, _ = w.Write([]byte(`{"code":"50011","msg":"Too Many Requests","data":[]}`))
			return
		}
		_, _ = w.Write([]byte(okxBooksBody()))
	}))
	defer server.Close()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{BooksRatePer2s: 1000, MaxConcurrent: 3})
	book, err := adapter.PerpetualOrderBook(context.Background(), "BTC-USDT-SWAP")
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if len(book.Bids) == 0 {
		t.Fatal("expected decoded bids after retry")
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("attempts = %d, want 3 (2 rate-limited + 1 success)", got)
	}
}

// TestOKXHTTP429Retries proves a 429 status is retried to success.
func TestOKXHTTP429Retries(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("slow down"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okxBooksBody()))
	}))
	defer server.Close()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{BooksRatePer2s: 1000, MaxConcurrent: 3})
	if _, err := adapter.PerpetualOrderBook(context.Background(), "BTC-USDT-SWAP"); err != nil {
		t.Fatalf("expected success after 429 retry, got %v", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

// TestOKXRetriesExhausted proves a permanent 429 returns an error after the
// retry budget is spent rather than looping forever.
func TestOKXRetriesExhausted(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("nope"))
	}))
	defer server.Close()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{BooksRatePer2s: 1000, MaxConcurrent: 3})
	_, err := adapter.PerpetualOrderBook(context.Background(), "BTC-USDT-SWAP")
	if err == nil {
		t.Fatal("expected error when retries are exhausted")
	}
	if got := atomic.LoadInt32(&attempts); got != okxMaxRetries+1 {
		t.Fatalf("attempts = %d, want %d", got, okxMaxRetries+1)
	}
	if !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("error should mention rate limit, got %v", err)
	}
}

// TestOKXTimeoutIsCounted proves a hanging server yields a timeout error and
// is reflected in the endpoint metrics.
func TestOKXTimeoutIsCounted(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hang until the test lets go
	}))
	defer func() { close(release); server.Close() }()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{BooksRatePer2s: 1000, MaxConcurrent: 3})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := adapter.PerpetualOrderBook(ctx, "BTC-USDT-SWAP")
	if err == nil {
		t.Fatal("expected timeout error")
	}

	snap := adapter.OKXMetrics()
	em, ok := snap.Endpoints["/api/v5/market/books"]
	if !ok {
		t.Fatalf("expected books endpoint in metrics, got %+v", snap.Endpoints)
	}
	if em.Total == 0 {
		t.Fatalf("expected recorded attempts, got %+v", em)
	}
	if em.Timeouts == 0 && em.Failed == 0 {
		t.Fatalf("expected timeout or failure counted, got %+v", em)
	}
}

// TestNonOKXRequestsNotThrottled proves the limiter only guards OKX: a burst
// of Indodax requests all complete concurrently even with an OKX cap of 1.
func TestNonOKXRequestsNotThrottled(t *testing.T) {
	var inFlight, maxSeen int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/depth/") {
			http.NotFound(w, r)
			return
		}
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			prev := atomic.LoadInt32(&maxSeen)
			if cur <= prev || atomic.CompareAndSwapInt32(&maxSeen, prev, cur) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"buy":[["1","1"]],"sell":[["2","1"]]}`))
	}))
	defer server.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs(server.URL, "", "", "", "")
	adapter.SetOKXLimiterConfig(OKXLimiterConfig{BooksRatePer2s: 1, MaxConcurrent: 1})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := adapter.SpotOrderBook(context.Background(), fmt.Sprintf("btc%didr", i)); err != nil {
				t.Errorf("indodax book %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxSeen); got < 2 {
		t.Fatalf("indodax requests were serialized (peak %d); non-OKX traffic must bypass the OKX limiter", got)
	}
}

// TestManyWorkersShareOneLimiter proves concurrent workers hitting OKX through
// one adapter are all admitted and none deadlocks.
func TestManyWorkersShareOneLimiter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v5/public/funding-rate") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":"0","msg":"","data":[{"fundingRate":"0.0001","fundingTime":"1690028800000","nextFundingTime":"1690057600000","prevFundingTime":"1690000000000","ts":"1690000000000"}]}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okxBooksBody()))
	}))
	defer server.Close()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{BooksRatePer2s: 200, FundingRatePer2s: 200, MaxConcurrent: 3})

	done := make(chan error, 15)
	for i := 0; i < 15; i++ {
		go func(i int) {
			sym := fmt.Sprintf("BTC%d-USDT-SWAP", i)
			if _, err := adapter.PerpetualOrderBook(context.Background(), sym); err != nil {
				done <- err
				return
			}
			_, err := adapter.FundingRate(context.Background(), sym)
			done <- err
		}(i)
	}
	for i := 0; i < 15; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("worker error: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("workers deadlocked: limiter starved a slot")
		}
	}
}

// TestRetryAfterIsHonoured proves a Retry-After header delays the retry
// rather than hammering immediately.
func TestRetryAfterIsHonoured(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okxBooksBody()))
	}))
	defer server.Close()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{BooksRatePer2s: 1000, MaxConcurrent: 3})
	started := time.Now()
	if _, err := adapter.PerpetualOrderBook(context.Background(), "BTC-USDT-SWAP"); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if elapsed := time.Since(started); elapsed < time.Second {
		t.Fatalf("retry happened after %v; Retry-After: 1 must delay it", elapsed)
	}
}

// TestEndpointClassStripsQuery guards the bucket key: per-symbol query strings
// must not create per-symbol buckets.
func TestEndpointClassStripsQuery(t *testing.T) {
	got := endpointClass("/api/v5/market/books?instId=BTC-USDT-SWAP&sz=20")
	if got != "/api/v5/market/books" {
		t.Fatalf("endpointClass = %q, want /api/v5/market/books", got)
	}
}

// TestOKXPermanentErrorNotRetried proves an OKX business error (bad instId,
// code 51001) is surfaced without burning retries.
func TestOKXPermanentErrorNotRetried(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":"51001","msg":"Instrument ID does not exist","data":[]}`))
	}))
	defer server.Close()

	adapter := newLimiterTestAdapter(server.URL, OKXLimiterConfig{BooksRatePer2s: 1000, MaxConcurrent: 3})
	_, err := adapter.PerpetualOrderBook(context.Background(), "NOTREAL-USDT-SWAP")
	if err == nil {
		t.Fatal("expected error for unknown instrument")
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want 1 (permanent error must not retry)", got)
	}
}

// TestLimiterAcquireRespectsContext proves acquire never blocks past ctx.
func TestLimiterAcquireRespectsContext(t *testing.T) {
	l := newOKXLimiter(OKXLimiterConfig{MaxConcurrent: 1})
	// Occupy the single slot.
	if !l.acquire(context.Background(), "/api/v5/market/books") {
		t.Fatal("first acquire should succeed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if l.acquire(ctx, "/api/v5/market/books") {
		t.Fatal("second acquire must fail once ctx expires with the slot held")
	}
	l.release()
}

// Regression: instruments payload is ~600KB; a capped body buffer truncated it
// and the caller's JSON decode then failed with "unexpected EOF", degrading
// every scan even though the HTTP round trip itself succeeded.
func TestInspectOKXResponseLargeBodyPreserved(t *testing.T) {
	payload := `{"code":"0","data":[` + strings.Repeat(`{"instId":"BTC-USDT-SWAP"},`, 30000) + `{"instId":"ETH-USDT-SWAP"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(payload))
	}))
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	limited, _, err := inspectOKXResponse(resp)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if limited {
		t.Fatal("large non-limited body must not be classified as rate limited")
	}

	var decoded struct {
		Code string `json:"code"`
		Data []struct {
			InstID string `json:"instId"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode after inspect: %v", err)
	}
	if decoded.Code != "0" || len(decoded.Data) != 30001 {
		t.Fatalf("truncated decode: code=%q data=%d", decoded.Code, len(decoded.Data))
	}
	resp.Body.Close()
}

// Regression: the request context was cancelled before the body was buffered,
// killing the connection under the response body. Large payloads (~600KB
// instruments list) then failed with "read on closed response body" or decoded
// as truncated JSON, degrading every scan.
func TestOKXDoBodyReadableAfterContextCancel(t *testing.T) {
	payload := `{"code":"0","data":[` + strings.Repeat(`{"instId":"BTC-USDT-SWAP"},`, 30000) + `{"instId":"ETH-USDT-SWAP"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(payload))
	}))
	defer server.Close()

	l := newLimiterTestAdapter(server.URL, OKXLimiterConfig{DefaultRatePer2s: 100, MaxConcurrent: 3})

	resp, err := l.okxDo(context.Background(), "/api/v5/public/instruments", func() (*http.Request, error) {
		return l.newOKXRequest(context.Background(), "/api/v5/public/instruments")
	})
	if err != nil {
		t.Fatalf("okxDo: %v", err)
	}
	defer resp.Body.Close()

	var decoded struct {
		Code string `json:"code"`
		Data []struct {
			InstID string `json:"instId"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode after okxDo returned: %v", err)
	}
	if decoded.Code != "0" || len(decoded.Data) != 30001 {
		t.Fatalf("truncated payload: code=%q data=%d", decoded.Code, len(decoded.Data))
	}
}
