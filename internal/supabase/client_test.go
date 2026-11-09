package supabase

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestBuildRowsUsesSchemaColumnsAndStablePairKey(t *testing.T) {
	rows := buildRows("scan-123", []domain.Opportunity{sampleOpportunity()})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	row := rows[0]
	if row.PairKey != "indodax:btc_idr->okx:btc/usdt:usdt" || row.IdempotencyKey != "scan-123:indodax:btc_idr->okx:btc/usdt:usdt" || row.EligibilityStatus != "ELIGIBLE" {
		t.Fatalf("unexpected key/status: %#v", row)
	}
	if row.QuoteAsset != "IDR" || row.LocalExchange != "indodax" || row.GlobalExchange != "okx" {
		t.Fatalf("unexpected venue/currency mapping: %#v", row)
	}
	if row.ScanID != "scan-123" || row.ObservedAt.IsZero() {
		t.Fatalf("missing scan id or timestamp: %#v", row)
	}
}

func TestBuildRowsKeepsDifferentPerpetualRoutesDistinct(t *testing.T) {
	first := sampleOpportunity()
	second := sampleOpportunity()
	second.GlobalExchange = "binance"
	second.PerpSymbol = "BTC/USDT"

	rows := buildRows("scan-123", []domain.Opportunity{first, second})
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].IdempotencyKey == rows[1].IdempotencyKey || rows[0].PairKey == rows[1].PairKey {
		t.Fatalf("different global venues collided: %#v", rows)
	}
}

func TestBuildRowsSkipsIncompleteVenueIdentity(t *testing.T) {
	op := sampleOpportunity()
	op.LocalExchange = ""
	if rows := buildRows("scan-123", []domain.Opportunity{op}); len(rows) != 0 {
		t.Fatalf("got %d rows for invalid venue identity, want 0", len(rows))
	}
}

func TestSaveOpportunitiesUpsertsIdempotentlyAndRetriesTransientFailure(t *testing.T) {
	var calls atomic.Int32
	var firstKey string
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
		key, _ := rows[0]["idempotency_key"].(string)
		if firstKey == "" {
			firstKey = key
		} else if key != firstKey {
			t.Errorf("retry changed idempotency key: first=%q retry=%q", firstKey, key)
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
	if firstKey != "scan-123:indodax:btc_idr->okx:btc/usdt:usdt" {
		t.Fatalf("idempotency key=%q", firstKey)
	}
}

func TestSaveOpportunitiesBatchesLargeScans(t *testing.T) {
	var calls atomic.Int32
	var rowCounts []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			t.Errorf("decode batch: %v", err)
		}
		rowCounts = append(rowCounts, len(rows))
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	opportunities := make([]domain.Opportunity, 0, maxBatchSize+1)
	for i := 0; i < maxBatchSize+1; i++ {
		op := sampleOpportunity()
		op.SpotSymbol = fmt.Sprintf("asset_%03d_idr", i)
		opportunities = append(opportunities, op)
	}
	client := &Client{baseURL: server.URL, serviceRoleKey: "test", httpClient: server.Client()}
	if err := client.SaveOpportunities(context.Background(), "scan-batch", opportunities); err != nil {
		t.Fatalf("SaveOpportunities: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("got %d requests, want 2 batches", calls.Load())
	}
	if len(rowCounts) != 2 || rowCounts[0] != maxBatchSize || rowCounts[1] != 1 {
		t.Fatalf("unexpected batch sizes: %v", rowCounts)
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

func TestNewFromEnvRequiresBothCredentials(t *testing.T) {
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("SUPABASE_KEY", "")
	client, err := NewFromEnv()
	if err != nil || client != nil {
		t.Fatalf("missing credentials should disable integration; got %#v, %v", client, err)
	}

	t.Setenv("SUPABASE_URL", "https://project.supabase.co")
	if _, err := NewFromEnv(); err == nil {
		t.Fatal("expected missing service-role key error")
	}
	t.Setenv("SUPABASE_KEY", "service-role-test")
	client, err = NewFromEnv()
	if err != nil || client == nil {
		t.Fatalf("expected configured client, got %#v, %v", client, err)
	}
	client.Close()
}

func TestNewFromEnvRejectsNonTLSRemoteURL(t *testing.T) {
	t.Setenv("SUPABASE_URL", "http://example.com")
	t.Setenv("SUPABASE_KEY", "service-role-test")
	if _, err := NewFromEnv(); err == nil {
		t.Fatal("expected non-TLS remote URL to be rejected")
	}
}

func TestNewFromEnvAllowsLoopbackHTTPForLocalTests(t *testing.T) {
	t.Setenv("SUPABASE_URL", "http://127.0.0.1:54321")
	t.Setenv("SUPABASE_KEY", "service-role-test")
	client, err := NewFromEnv()
	if err != nil || client == nil {
		t.Fatalf("expected loopback client, got %#v, %v", client, err)
	}
	client.Close()
}

func TestLatestOpportunityScansReturnsTenNewestDistinctScans(t *testing.T) {
	rows := make([]map[string]any, 0, 12)
	for i := 11; i >= 0; i-- {
		op := sampleOpportunity()
		op.LastScannedAt = time.Date(2026, 11, 9, 0, i, 0, 0, time.UTC)
		op.NetROIPct = float64(i)
		rows = append(rows, map[string]any{
			"scan_id": fmt.Sprintf("scan-%02d", i+1),
			"observed_at": op.LastScannedAt,
			"payload": op,
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/rest/v1/scanner_opportunities" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("select") != "scan_id,observed_at,payload" ||
			r.URL.Query().Get("order") != "observed_at.desc,scan_id.desc" ||
			r.URL.Query().Get("limit") != "1000" ||
			r.URL.Query().Get("offset") != "0" {
			t.Errorf("unexpected history query: %s", r.URL.RawQuery)
		}
		if r.Header.Get("apikey") != "service-role-test" {
			t.Error("missing service-role API key")
		}
		_ = json.NewEncoder(w).Encode(rows)
	}))
	defer server.Close()

	client := &Client{baseURL: server.URL, serviceRoleKey: "service-role-test", httpClient: server.Client()}
	scans, err := client.LatestOpportunityScans(context.Background(), 10)
	if err != nil {
		t.Fatalf("LatestOpportunityScans: %v", err)
	}
	if len(scans) != 10 {
		t.Fatalf("got %d scans, want 10", len(scans))
	}
	if scans[0].ScanID != "scan-12" || scans[9].ScanID != "scan-03" {
		t.Fatalf("unexpected scan window: first=%s last=%s", scans[0].ScanID, scans[9].ScanID)
	}
	if len(scans[0].Opportunities) != 1 || scans[0].Opportunities[0].NetROIPct != 11 {
		t.Fatalf("latest scan payload not reconstructed: %+v", scans[0])
	}
}

func TestLatestOpportunityScansRejectsUnavailableReader(t *testing.T) {
	var client *Client
	if _, err := client.LatestOpportunityScans(context.Background(), 10); err == nil {
		t.Fatal("expected missing Supabase reader error")
	}
}
