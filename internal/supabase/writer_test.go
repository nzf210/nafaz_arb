package supabase

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func sampleScan() domain.ScanResult {
	observed := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	return domain.ScanResult{
		ScanID:      "scan-2026-11-10-001",
		Mode:        "LIVE",
		Complete:    true,
		DurationMS:  120,
		CompletedAt: observed,
		Opportunities: []domain.Opportunity{
			{
				OpportunityKey:      "BTC/USDT:indodax:BTCUSDT:okx:BTC-USDT-SWAP",
				LocalExchange:       "indodax",
				GlobalExchange:      "okx",
				BaseAsset:           "BTC",
				QuoteAsset:          "USDT",
				SpotSymbol:          "BTCUSDT",
				PerpSymbol:          "BTC-USDT-SWAP",
				ExecutionMode:       "MAKER_TAKER",
				SpotEntryPrice:      60000,
				PerpEntryPrice:      60050,
				EntrySpreadPct:      0.12,
				CurrentFundingRate:  0.0003,
				NetProfit:           1.25,
				NetROIPct:           0.25,
				Quantity:            0.01,
				CapacityPct:         80,
				ProfitabilityStatus: "PROFITABLE_IN_RANGE",
				TradingEligible:     true,
				LastScannedAt:       observed,
			},
			{
				OpportunityKey:      "ETH/USDT:tokocrypto:ETHUSDT:bybit:ETHUSDT",
				LocalExchange:       "tokocrypto",
				GlobalExchange:      "bybit",
				BaseAsset:           "ETH",
				QuoteAsset:          "USDT",
				SpotSymbol:          "ETHUSDT",
				PerpSymbol:          "ETHUSDT",
				ProfitabilityStatus: "ENTRY_SPREAD_BELOW_MINIMUM",
				ExclusionReasons:    []string{"ENTRY_SPREAD_BELOW_MINIMUM"},
				TradingEligible:     false,
				LastScannedAt:       observed,
			},
		},
	}
}

func closeWriter(t *testing.T, writer *Writer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func TestNewFromEnvDisabledAndRejectsPartialCredentials(t *testing.T) {
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("SUPABASE_KEY", "")
	writer, err := NewFromEnv()
	if err != nil || writer != nil {
		t.Fatalf("expected disabled writer, got writer=%v err=%v", writer, err)
	}

	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
	if _, err := NewFromEnv(); err == nil {
		t.Fatal("expected partial Supabase environment to be rejected")
	}
}

func TestEnqueueScanUpsertsEligibleAndExcludedCandidates(t *testing.T) {
	var requestCount atomic.Int32
	var received []opportunityRow
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method=%s, want POST", r.Method)
		}
		if r.URL.Path != "/rest/v1/scanner_opportunities" || r.URL.Query().Get("on_conflict") != "id" {
			t.Errorf("unexpected endpoint: %s", r.URL.String())
		}
		if got := r.Header.Get("apikey"); got != "service-role-test-key" {
			t.Errorf("apikey header=%q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer service-role-test-key" {
			t.Errorf("authorization header=%q", got)
		}
		if got := r.Header.Get("Prefer"); got != "resolution=merge-duplicates,return=minimal" {
			t.Errorf("Prefer header=%q", got)
		}
		var rows []opportunityRow
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		received = append(received, rows...)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	writer, err := NewWriter(server.URL, "service-role-test-key", server.Client(), 4)
	if err != nil {
		t.Fatal(err)
	}
	scan := sampleScan()
	writer.EnqueueScan(scan)
	writer.EnqueueScan(scan)
	closeWriter(t, writer)

	if requestCount.Load() != 2 {
		t.Fatalf("requests=%d, want 2", requestCount.Load())
	}
	if len(received) != 4 {
		t.Fatalf("received rows=%d, want 4", len(received))
	}
	if received[0].ID != received[2].ID || received[1].ID != received[3].ID {
		t.Fatalf("replayed scan generated different row IDs: %q %q / %q %q", received[0].ID, received[1].ID, received[2].ID, received[3].ID)
	}
	if received[0].LocalExchange != "indodax" || received[0].GlobalExchange != "okx" {
		t.Fatalf("venue mapping mismatch: %+v", received[0])
	}
	if received[1].EligibilityStatus != "ENTRY_SPREAD_BELOW_MINIMUM" || received[1].Reason == "" {
		t.Fatalf("excluded candidate was not retained with its reason: %+v", received[1])
	}
	if received[0].SpotPrice != 60000 || received[0].PerpPrice != 60050 || received[0].ExecutableQty != 0.01 {
		t.Fatalf("numeric mapping mismatch: %+v", received[0])
	}
}

func TestWriterRetriesTransientServerFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	writer, err := NewWriter(server.URL, "service-role-test-key", server.Client(), 2)
	if err != nil {
		t.Fatal(err)
	}
	writer.EnqueueScan(sampleScan())
	closeWriter(t, writer)

	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts=%d, want 2 after one transient failure", got)
	}
}

func TestWriterDoesNotRetryPermanentClientErrors(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	writer, err := NewWriter(server.URL, "service-role-test-key", server.Client(), 2)
	if err != nil {
		t.Fatal(err)
	}
	writer.EnqueueScan(sampleScan())
	closeWriter(t, writer)

	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d, want 1 for permanent 401", got)
	}
}

func TestStableUUIDIsScopedToScanAndPair(t *testing.T) {
	first := stableUUID("scan-1", "BTC/USDT")
	if first != stableUUID("scan-1", "BTC/USDT") {
		t.Fatal("same scan and pair must generate a stable ID")
	}
	if first == stableUUID("scan-2", "BTC/USDT") || first == stableUUID("scan-1", "ETH/USDT") {
		t.Fatal("different scan or pair must generate a different ID")
	}
	if len(first) != 36 {
		t.Fatalf("UUID length=%d, want 36: %s", len(first), first)
	}
}
