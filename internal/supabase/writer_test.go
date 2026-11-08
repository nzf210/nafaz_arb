package supabase

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestNewDisabledWhenUnconfigured(t *testing.T) {
	if w := New(Config{}); w != nil {
		t.Fatalf("expected nil writer without url/key, got %#v", w)
	}
	if w := New(Config{URL: "https://x.test"}); w != nil {
		t.Fatalf("expected nil writer without key")
	}
	w := New(Config{URL: "https://x.test/", Key: "k"})
	if w == nil {
		t.Fatal("expected writer with url and key")
	}
	w.Close()
}

func TestNilWriterCallsAreSafe(t *testing.T) {
	var w *Writer
	w.RecordScan(domain.ScanResult{ScanID: "s"})
	w.Close()
}

func TestDeterministicIDShape(t *testing.T) {
	a := deterministicID("scan-1", "BTC/USDT:spot:perp")
	b := deterministicID("scan-1", "BTC/USDT:spot:perp")
	c := deterministicID("scan-2", "BTC/USDT:spot:perp")
	if a != b {
		t.Fatalf("id must be stable: %s != %s", a, b)
	}
	if a == c {
		t.Fatalf("different scans must produce different ids")
	}
	if len(a) != 36 || a[8] != '-' || a[13] != '-' || a[18] != '-' || a[23] != '-' {
		t.Fatalf("not uuid-shaped: %s", a)
	}
}

func TestRecordScanPostsEveryOpportunity(t *testing.T) {
	var mu sync.Mutex
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var rows []map[string]any
		if err := json.Unmarshal(body, &rows); err != nil {
			t.Errorf("bad body: %v", err)
		}
		mu.Lock()
		got = append(got, rows...)
		mu.Unlock()
		if r.URL.Query().Get("on_conflict") != "id" {
			t.Errorf("expected on_conflict=id, got %q", r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	w := New(Config{URL: srv.URL, Key: "k"})
	result := domain.ScanResult{
		ScanID:      "scan-1",
		CompletedAt: time.Now().UTC(),
		Opportunities: []domain.Opportunity{
			{
				OpportunityKey: "BTC/IDR:btc_idr:BTC-USDT-SWAP",
				LocalExchange:  "indodax",
				GlobalExchange: "okx",
				BaseAsset:      "BTC",
				QuoteAsset:     "IDR",
				SpotSymbol:     "btc_idr",
				PerpSymbol:     "BTC-USDT-SWAP",
				TradingEligible: true,
			},
			{
				OpportunityKey:  "ETH/IDR:eth_idr:ETH-USDT-SWAP",
				ExclusionReasons: []string{"MIN_NET_PROFIT_NOT_MET"},
				TradingEligible:  false,
			},
		},
	}
	w.RecordScan(result)
	w.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(got), got)
	}
	statuses := map[string]bool{}
	for _, row := range got {
		statuses[row["eligibility_status"].(string)] = true
	}
	if !statuses["ELIGIBLE"] || !statuses["EXCLUDED"] {
		t.Fatalf("expected both eligibility states, got %v", statuses)
	}
	for _, row := range got {
		if row["scan_id"] != "scan-1" {
			t.Fatalf("scan_id not carried: %v", row["scan_id"])
		}
		if _, ok := row["observed_at"].(string); !ok {
			t.Fatalf("observed_at missing: %v", row)
		}
	}
}

func TestDuplicateRowsInOneBatchAreDeduped(t *testing.T) {
	var mu sync.Mutex
	var batches [][]map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var rows []map[string]any
		_ = json.Unmarshal(body, &rows)
		mu.Lock()
		batches = append(batches, rows)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	w := New(Config{URL: srv.URL, Key: "k"})
	op := domain.Opportunity{OpportunityKey: "k", TradingEligible: true}
	result := domain.ScanResult{ScanID: "s", CompletedAt: time.Now()}
	result.Opportunities = []domain.Opportunity{op, op}
	w.RecordScan(result)
	w.Close()

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 1 {
		t.Fatalf("duplicate rows must collapse to one upsert, got %d", total)
	}
}
