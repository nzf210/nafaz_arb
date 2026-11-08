package supabase

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func sampleOpportunity() domain.Opportunity {
	return domain.Opportunity{
		LocalExchange: "indodax", GlobalExchange: "okx",
		SpotSymbol: "btc_idr", PerpSymbol: "BTC/USDT:USDT",
		BaseAsset: "BTC", QuoteAsset: "USDT", OriginalSpotQuote: "IDR",
		TradingEligible: true, ProfitabilityStatus: "PROFITABLE_IN_RANGE",
		SpotEntryPrice: 1000000000, PerpEntryPrice: 65000,
		NetProfit: 1.25, NetROIPct: 0.3, Quantity: 0.001,
		CapacityPct: 90, LastScannedAt: time.Date(2026, 11, 8, 1, 0, 0, 0, time.UTC),
	}
}

func TestBuildRowsUsesMigrationColumnsAndStablePairKey(t *testing.T) {
	rows := buildRows("scan-123", []domain.Opportunity{sampleOpportunity()})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	row := rows[0]
	if row.PairKey != "indodax:btc_idr" || row.IdempotencyKey != "scan-123:indodax:btc_idr" || row.EligibilityStatus != "ELIGIBLE" {
		t.Fatalf("unexpected key/status: %#v", row)
	}
	if row.QuoteAsset != "IDR" || row.LocalExchange != "indodax" || row.GlobalExchange != "okx" {
		t.Fatalf("unexpected venue/currency mapping: %#v", row)
	}
	if row.ScanID != "scan-123" || row.ObservedAt.IsZero() {
		t.Fatalf("missing scan id or timestamp: %#v", row)
	}
}

func TestSaveOpportunitiesUpsertsIdempotentlyAndRetriesTransientFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/rest/v1/scanner_opportunities" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("on_conflict") != "idempotency_key" {
			t.Errorf("missing idempotency conflict target: %s", r.URL.RawQuery)
		}
		if r.Header.Get("apikey") != "service-role-test" || r.Header.Get("Authorization") != "Bearer service-role-test" {
			t.Errorf("service-role headers not set")
		}
		if !strings.Contains(r.Header.Get("Prefer"), "resolution=merge-duplicates") {
			t.Errorf("missing upsert preference: %q", r.Header.Get("Prefer"))
		}
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if len(rows) != 1 || rows[0]["scan_id"] != "scan-123" {
			t.Errorf("unexpected payload: %#v", rows)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("temporary outage"))
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := &Client{baseURL: server.URL, serviceRoleKey: "service-role-test", httpClient: server.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.SaveOpportunities(ctx, "scan-123", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatalf("SaveOpportunities returned error: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("got %d calls, want retry count 2", got)
	}
}

func TestSaveOpportunitiesDoesNotRetryPermanentSchemaError(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("column does not exist"))
	}))
	defer server.Close()
	client := &Client{baseURL: server.URL, serviceRoleKey: "test", httpClient: server.Client()}
	err := client.SaveOpportunities(context.Background(), "scan-123", []domain.Opportunity{sampleOpportunity()})
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Fatalf("expected permanent HTTP 400 error, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("permanent error retried %d times", calls.Load())
	}
}

func TestNewFromEnvRequiresBothCredentialsAndHonorsDisable(t *testing.T) {
	t.Setenv("SUPABASE_ENABLED", "false")
	t.Setenv("SUPABASE_URL", "https://project.supabase.co")
	t.Setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
	client, err := NewFromEnv()
	if err != nil || client != nil {
		t.Fatalf("disabled integration should return nil,nil; got %#v, %v", client, err)
	}

	t.Setenv("SUPABASE_ENABLED", "true")
	t.Setenv("SUPABASE_URL", "https://project.supabase.co")
	t.Setenv("SUPABASE_SERVICE_ROLE_KEY", "")
	if _, err := NewFromEnv(); err == nil {
		t.Fatal("expected missing service-role key error")
	}
	t.Setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
	client, err = NewFromEnv()
	if err != nil || client == nil {
		t.Fatalf("expected configured client, got %#v, %v", client, err)
	}
}
