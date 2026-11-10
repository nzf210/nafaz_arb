package persistence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func sampleScan() domain.ScanResult {
	at := time.Date(2026, 11, 10, 1, 0, 0, 0, time.UTC)
	return domain.ScanResult{
		ScanID: "scan-123",
		StartedAt: at.Add(-time.Second),
		CompletedAt: at,
		Opportunities: []domain.Opportunity{
			{
				OpportunityKey: "BTC/USDT:indodax:BTCUSDT:okx:BTC-USDT-SWAP",
				LocalExchange: "indodax", GlobalExchange: "okx",
				BaseAsset: "BTC", QuoteAsset: "USDT",
				SpotSymbol: "BTC/USDT", PerpSymbol: "BTC/USDT:USDT",
				ExecutionMode: "MAKER_TAKER", SpotEntryPrice: 60000,
				PerpEntryPrice: 60100, EntrySpreadPct: 0.16,
				CurrentFundingRate: 0.0001, NetProfit: 2.5,
				NetROIPct: 0.5, Quantity: 0.01, CapacityPct: 80,
				ProfitabilityStatus: "PROFITABLE_IN_RANGE", TradingEligible: true,
				LastScannedAt: at, OriginalSpotQuote: "USDT", LocalPriceTick: 0.01,
			},
		},
	}
}

func TestBuildScanBatchIsDeterministicAndSchemaCompatible(t *testing.T) {
	scan := sampleScan()
	first := buildScanBatch(scan)
	second := buildScanBatch(scan)
	if len(first.opportunities) != 1 || len(first.marketPairs) != 1 {
		t.Fatalf("unexpected rows: opportunities=%d market_pairs=%d", len(first.opportunities), len(first.marketPairs))
	}
	if first.opportunities[0]["id"] != second.opportunities[0]["id"] {
		t.Fatal("same scan and pair must generate the same primary key")
	}
	row := first.opportunities[0]
	for _, key := range []string{"scan_id", "pair_key", "local_exchange", "global_exchange", "eligibility_status", "observed_at", "payload"} {
		if _, ok := row[key]; !ok {
			t.Fatalf("scanner_opportunities row missing schema column %q", key)
		}
	}
	pair := first.marketPairs[0]
	if pair["pair_key"] != scan.Opportunities[0].OpportunityKey || pair["local_exchange"] != "indodax" || pair["global_exchange"] != "okx" {
		t.Fatalf("unexpected market_pairs mapping: %#v", pair)
	}
}

func TestBuildScanBatchSkipsMissingVenueIdentity(t *testing.T) {
	scan := sampleScan()
	scan.Opportunities[0].LocalExchange = ""
	batch := buildScanBatch(scan)
	if len(batch.opportunities) != 0 || len(batch.marketPairs) != 0 {
		t.Fatalf("must not persist incomplete venue mapping: %#v", batch)
	}
}

func TestSupabaseWriterRetriesAndUpsertsScannerData(t *testing.T) {
	var marketCalls atomic.Int32
	var opportunityCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") != "service-role-test" || r.Header.Get("Authorization") != "Bearer service-role-test" {
			t.Errorf("missing service-role headers")
		}
		if !strings.Contains(r.Header.Get("Prefer"), "resolution=merge-duplicates") {
			t.Errorf("missing idempotent upsert preference")
		}
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			t.Errorf("decode request: %v", err)
		}
		switch {
		case strings.Contains(r.URL.Path, "/market_pairs"):
			if r.URL.Query().Get("on_conflict") != "pair_key" {
				t.Errorf("market_pairs conflict target=%q", r.URL.Query().Get("on_conflict"))
			}
			if marketCalls.Add(1) == 1 {
				http.Error(w, "temporary", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case strings.Contains(r.URL.Path, "/scanner_opportunities"):
			if r.URL.Query().Get("on_conflict") != "id" {
				t.Errorf("scanner conflict target=%q", r.URL.Query().Get("on_conflict"))
			}
			if len(rows) != 1 || rows[0]["scan_id"] != "scan-123" {
				t.Errorf("unexpected opportunity payload: %#v", rows)
			}
			opportunityCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	writer := NewSupabaseWriter(config.SupabaseConfig{Enabled: true, URL: server.URL, Key: "service-role-test"})
	writer.Start()
	writer.SubmitScan(sampleScan())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if marketCalls.Load() != 2 {
		t.Fatalf("market_pairs calls=%d, want one retry", marketCalls.Load())
	}
	if opportunityCalls.Load() != 1 {
		t.Fatalf("scanner_opportunities calls=%d, want 1", opportunityCalls.Load())
	}
}

func TestDisabledWriterIsNoop(t *testing.T) {
	writer := NewSupabaseWriter(config.SupabaseConfig{})
	writer.Start()
	writer.SubmitScan(sampleScan())
	if writer.Enabled() {
		t.Fatal("writer should be disabled without explicit credentials")
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
