package exchange

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestAvailabilityTripsAfterErrorThreshold(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	a := newAvailability(HealthPolicy{Window: 5 * time.Minute, ErrorRateThreshold: 0.5, MinRequests: 4}, func() time.Time {
		return now
	})

	for i := 0; i < 3; i++ {
		a.record("okx", true)
	}
	if a.unavailable("okx") {
		t.Fatalf("exchange marked unavailable below MinRequests")
	}

	a.record("okx", true) // 4/4 failures
	if !a.unavailable("okx") {
		t.Fatalf("expected okx unavailable after 4 of 4 failures")
	}

	if a.unavailable("indodax") {
		t.Fatalf("unrelated exchange must stay available")
	}
}

func TestAvailabilityIgnoresOldOutcomes(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	a := newAvailability(HealthPolicy{Window: time.Minute, ErrorRateThreshold: 0.5, MinRequests: 2}, func() time.Time {
		return now
	})

	a.record("mexc", true)
	a.record("mexc", true)
	if !a.unavailable("mexc") {
		t.Fatalf("expected mexc unavailable")
	}

	// Slide past the window: the failures must age out.
	now = now.Add(2 * time.Minute)
	if a.unavailable("mexc") {
		t.Fatalf("stale failures must not keep the exchange unavailable")
	}
}

func TestAvailabilityDisabledPolicyAlwaysAvailable(t *testing.T) {
	a := newAvailability(HealthPolicy{}, nil)
	for i := 0; i < 50; i++ {
		a.record("okx", true)
	}
	if a.unavailable("okx") {
		t.Fatalf("a zero-value policy must disable detection")
	}
}

func TestAvailabilityResetClearsHistory(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	a := newAvailability(HealthPolicy{Window: 5 * time.Minute, ErrorRateThreshold: 0.5, MinRequests: 2}, func() time.Time {
		return now
	})
	a.record("indodax", true)
	a.record("indodax", true)
	a.reset("indodax")
	if a.unavailable("indodax") {
		t.Fatalf("reset must clear the recorded outcomes")
	}
}

func TestClassifyHTTPError(t *testing.T) {
	for _, code := range []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		if !classifyHTTPError(code) {
			t.Fatalf("expected %d to be classified as maintenance", code)
		}
	}
	for _, code := range []int{http.StatusOK, http.StatusNotFound, http.StatusTooManyRequests} {
		if classifyHTTPError(code) {
			t.Fatalf("expected %d not to be classified as maintenance", code)
		}
	}
}

func TestTrackedBookReturnsUnavailableWhenTripped(t *testing.T) {
	adapter := NewLiveAdapter()
	adapter.SetHealthPolicy(HealthPolicy{Window: 5 * time.Minute, ErrorRateThreshold: 0.5, MinRequests: 2})

	calls := 0
	failing := func() (domain.OrderBook, error) {
		calls++
		return domain.OrderBook{}, errors.New("boom")
	}

	adapter.trackedBook("okx", failing)
	adapter.trackedBook("okx", failing)
	if adapter.ExchangeAvailable("okx") {
		t.Fatalf("expected okx to be marked unavailable")
	}

	calls = 0
	_, err := adapter.trackedBook("okx", failing)
	if !errors.Is(err, ErrExchangeUnavailable) {
		t.Fatalf("expected ErrExchangeUnavailable, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("unavailable exchange must not be hit again, calls=%d", calls)
	}
}

func TestLiveAdapterFlagsServiceUnavailableAsMaintenance(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	adapter := NewLiveAdapter()
	adapter.SetBaseURLs(srv.URL, srv.URL, srv.URL, srv.URL, srv.URL)

	_, err := adapter.PerpetualOrderBook(t.Context(), "okx:BTC-USDT-SWAP")
	if !errors.Is(err, ErrExchangeUnavailable) {
		t.Fatalf("expected maintenance error for 503, got %v", err)
	}
}

func TestLiveAdapterPolicyDisabledNeverSkips(t *testing.T) {
	adapter := NewLiveAdapter()
	for _, ex := range []string{"okx", "mexc", "indodax", "tokocrypto"} {
		if !adapter.ExchangeAvailable(ex) {
			t.Fatalf("default adapter must report %s available", ex)
		}
	}
	adapter.SetHealthPolicy(HealthPolicy{})
	if !adapter.ExchangeAvailable("okx") {
		t.Fatalf("zero policy must disable detection")
	}
}
