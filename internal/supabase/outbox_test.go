package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDurableOutboxSurvivesRestartAndAcknowledges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scanner", "outbox.jsonl")
	first, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Enqueue("scan-1", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatal(err)
	}
	if err := first.Enqueue("scan-1", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatalf("same pending scan should be idempotent: %v", err)
	}
	if count, err := first.PendingCount(); err != nil || count != 1 {
		t.Fatalf("pending count=%d err=%v, want 1", count, err)
	}

	restarted, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	due, err := restarted.Due(10)
	if err != nil || len(due) != 1 {
		t.Fatalf("recovered due records=%d err=%v, want 1", len(due), err)
	}
	if due[0].ScanID != "scan-1" || len(due[0].Rows) != 1 {
		t.Fatalf("unexpected recovered record: %+v", due[0])
	}
	if err := restarted.Ack([]string{due[0].ID}); err != nil {
		t.Fatal(err)
	}
	if count, err := restarted.PendingCount(); err != nil || count != 0 {
		t.Fatalf("pending after ack=%d err=%v, want 0", count, err)
	}
}

// PostgREST rejects a duplicate conflict key inside one upsert statement
// (PGRST102 "All object keys must match"; ON CONFLICT DO UPDATE cannot affect
// the same row twice). A scan whose collected rows collapse onto the same
// idempotency_key must therefore fail locally, not poison the outbox: a record
// that can never be accepted would block every later scan behind it.
func TestEnqueueRejectsRowsCollidingOnIdempotencyKey(t *testing.T) {
	outbox, err := NewDurableOutbox(filepath.Join(t.TempDir(), "outbox.jsonl"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	// Two opportunities for the same venue route in one scan produce the same
	// idempotency_key, which PostgREST rejects as a whole statement.
	duplicated := sampleOpportunity()
	err = outbox.Enqueue("scan-collide", []domain.Opportunity{duplicated, duplicated})
	if !errors.Is(err, ErrDuplicateRows) {
		t.Fatalf("expected ErrDuplicateRows, got %v", err)
	}
	if count, _ := outbox.PendingCount(); count != 0 {
		t.Fatalf("undeliverable record was journaled; pending=%d", count)
	}
	if err := outbox.Enqueue("scan-collide", []domain.Opportunity{duplicated}); err != nil {
		t.Fatalf("distinct scan must still enqueue after a rejected one: %v", err)
	}
	if count, _ := outbox.PendingCount(); count != 1 {
		t.Fatalf("pending=%d, want 1", count)
	}
}

// An identical replay of the same scan stays idempotent while pending, so the
// duplicate guard must not turn a harmless retry into a failure.
func TestEnqueueKeepsIdenticalScanReplayIdempotent(t *testing.T) {
	outbox, err := NewDurableOutbox(filepath.Join(t.TempDir(), "outbox.jsonl"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	op := sampleOpportunity()
	if err := outbox.Enqueue("scan-a", []domain.Opportunity{op}); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue("scan-a", []domain.Opportunity{op}); err != nil {
		t.Fatalf("identical replay must stay idempotent: %v", err)
	}
	if count, _ := outbox.PendingCount(); count != 1 {
		t.Fatalf("pending=%d, want 1", count)
	}
}

func TestPruneUndeliverableDropsOnlyPermanentlyRejectedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	outbox, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	poisoned, healthy := sampleOpportunity(), sampleOpportunity()
	healthy.SpotSymbol = "eth_idr"
	if err := outbox.Enqueue("scan-poisoned", []domain.Opportunity{poisoned}); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue("scan-healthy", []domain.Opportunity{healthy}); err != nil {
		t.Fatal(err)
	}
	records, err := outbox.Due(10)
	if err != nil || len(records) != 2 {
		t.Fatalf("due=%d err=%v", len(records), err)
	}
	for _, record := range records {
		// Three attempts each: one below the floor, one permanent schema error.
		for attempt := 0; attempt < 3; attempt++ {
			cause := errors.New("Supabase scanner_opportunities returned HTTP 503: temporarily unavailable")
			if record.ScanID == "scan-poisoned" {
				cause = errors.New(`Supabase scanner_opportunities returned HTTP 400: {"code":"PGRST102","message":"All object keys must match"}`)
			}
			if err := outbox.Retry([]string{record.ID}, cause); err != nil {
				t.Fatal(err)
			}
		}
	}

	// The guard must not fire on transient failures, however often they repeat.
	report, err := outbox.PruneUndeliverable(`PGRST102|All object keys must match`, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Dropped != 1 || report.Kept != 1 {
		t.Fatalf("dry run dropped=%d kept=%d, want 1/1", report.Dropped, report.Kept)
	}
	if count, _ := outbox.PendingCount(); count != 2 {
		t.Fatalf("dry run modified the journal: pending=%d", count)
	}

	report, err = outbox.PruneUndeliverable(`PGRST102|All object keys must match`, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Dropped != 1 {
		t.Fatalf("dropped=%d, want 1", report.Dropped)
	}
	if count, _ := outbox.PendingCount(); count != 1 {
		t.Fatalf("pending after prune=%d, want 1", count)
	}
	if _, err := os.Stat(path + ".bak"); err != nil {
		t.Fatalf("prune did not back up the original journal: %v", err)
	}
	reopened, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := reopened.Inspect()
	if err != nil || len(entries) != 1 || entries[0].ScanID != "scan-healthy" {
		t.Fatalf("surviving record lost on reload: entries=%v err=%v", entries, err)
	}
}

// A record whose retry metadata is unreadable must still be salvageable,
// otherwise recovery is impossible for exactly the records operators need to
// inspect. Every other field is still trusted.
func TestPruneUndeliverableSalvagesUnreadableRetryMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	row := `{"scan_id":"scan-salvage","pair_key":"indodax:btc_idr->okx:btc/usdt:usdt",` +
		`"idempotency_key":"scan-salvage:indodax:btc_idr->okx:btc/usdt:usdt",` +
		`"local_exchange":"indodax","global_exchange":"okx","local_symbol":"btc_idr",` +
		`"global_symbol":"BTC/USDT:USDT","quote_asset":"IDR","eligibility_status":"ELIGIBLE",` +
		`"spot_price":1,"perp_price":1,"spread_pct":0,"funding_rate":0,` +
		`"estimated_net_profit":1,"estimated_net_roi_pct":0.3,"executable_qty":1,` +
		`"capacity_pct":90,"observed_at":"2026-11-08T01:00:00Z","payload":{}}`
	line := `{"id":"irrelevant","scan_id":"scan-salvage","rows":[` + row + `],` +
		`"created_at":"2026-11-08T01:00:00Z","attempts":4,"next_attempt_at":"not-a-timestamp"}`
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatalf("salvage reader refused the journal: %v", err)
	}
	entries, err := reopened.Inspect()
	if err != nil || len(entries) != 1 || entries[0].ScanID != "scan-salvage" {
		t.Fatalf("salvaged entry lost: entries=%v err=%v", entries, err)
	}
	// Salvaged records carry no retry metadata, so they are never pruned on
	// error text alone; an operator re-runs the scan instead.
	report, err := reopened.PruneUndeliverable(`.*`, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.Dropped != 0 {
		t.Fatalf("salvaged record dropped without a recorded error: %+v", report)
	}
}

func TestDurableOutboxRejectsCapacityWithoutDroppingExistingRows(t *testing.T) {
	outbox, err := NewDurableOutbox(filepath.Join(t.TempDir(), "outbox.jsonl"), 32)
	if err != nil {
		t.Fatal(err)
	}
	err = outbox.Enqueue("scan-capacity", []domain.Opportunity{sampleOpportunity()})
	if !errors.Is(err, ErrOutboxCapacity) {
		t.Fatalf("expected ErrOutboxCapacity, got %v", err)
	}
	if count, countErr := outbox.PendingCount(); countErr != nil || count != 0 {
		t.Fatalf("pending count=%d err=%v, want 0", count, countErr)
	}
}

func TestDurableOutboxTruncatesOnlyTornUncommittedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	outbox, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue("scan-good", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString("{torn-json")
	_ = file.Close()

	restarted, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	due, err := restarted.Due(10)
	if err != nil || len(due) != 1 || due[0].ScanID != "scan-good" {
		t.Fatalf("valid record lost while recovering torn tail: due=%d err=%v", len(due), err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "torn-json") {
		t.Fatal("torn tail was not truncated")
	}
}

func TestDurableOutboxRepairsValidUnterminatedFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	outbox, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue("scan-complete-json", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimSuffix(string(data), "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Due(10); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Enqueue("scan-next", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatal(err)
	}
	records, err := restarted.Due(10)
	if err != nil || len(records) != 2 {
		t.Fatalf("valid unterminated row merged with next append: records=%d err=%v", len(records), err)
	}
}

func TestDurableOutboxFailsClosedOnCommittedCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	if err := os.WriteFile(path, []byte("{invalid}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDurableOutbox(path, 1<<20); err == nil {
		t.Fatal("expected committed corruption to fail startup validation")
	}
}

func TestDurableOutboxRetryPersistsBackoffAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	outbox, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue("scan-retry", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatal(err)
	}
	due, err := outbox.Due(1)
	if err != nil || len(due) != 1 {
		t.Fatalf("Due: records=%d err=%v", len(due), err)
	}
	if err := outbox.Retry([]string{due[0].ID}, errors.New("HTTP 503")); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewDurableOutbox(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := restarted.PendingCount()
	if err != nil || pending != 1 {
		t.Fatalf("pending=%d err=%v, want retained record", pending, err)
	}
	due, err = restarted.Due(10)
	if err != nil || len(due) != 0 {
		t.Fatalf("retry backoff not persisted: due=%d err=%v", len(due), err)
	}
	restarted.mu.Lock()
	dueRecords, readErr := restarted.readRecordsLocked()
	restarted.mu.Unlock()
	if readErr != nil || len(dueRecords) != 1 ||
		dueRecords[0].Attempts != 1 || !strings.Contains(dueRecords[0].LastError, "503") {
		t.Fatalf("retry metadata not persisted: records=%+v err=%v", dueRecords, readErr)
	}
}

func newTestDurableStore(t *testing.T, handler http.Handler) (*DurableOutbox, *DurableStore) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	outbox, err := NewDurableOutbox(filepath.Join(t.TempDir(), "outbox.jsonl"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{baseURL: server.URL, serviceRoleKey: "test", httpClient: server.Client()}
	store, err := NewDurableStore(client, outbox, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return outbox, store
}

func TestScannerReconciliationWorkerAcknowledgesSuccessfulUpsert(t *testing.T) {
	var calls int
	outbox, store := newTestDurableStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/rest/v1/scanner_opportunities" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("on_conflict") != "idempotency_key" {
			t.Errorf("unexpected conflict target: %s", r.URL.RawQuery)
		}
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			t.Errorf("decode upsert: %v", err)
		}
		if len(rows) != 1 || rows[0]["scan_id"] != "scan-worker" {
			t.Errorf("unexpected persisted rows: %+v", rows)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := store.SaveOpportunities(context.Background(), "scan-worker", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatal(err)
	}
	if count, _ := outbox.PendingCount(); count != 1 {
		t.Fatalf("local enqueue count=%d, want 1", count)
	}
	if err := store.reconcileDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("remote calls=%d, want 1", calls)
	}
	if count, _ := outbox.PendingCount(); count != 0 {
		t.Fatalf("pending after reconciliation=%d, want 0", count)
	}
}

func TestScannerReconciliationWorkerRetainsRowsOnRemoteFailure(t *testing.T) {
	outbox, store := newTestDurableStore(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("temporarily unavailable"))
	}))
	if err := store.SaveOpportunities(context.Background(), "scan-outage", []domain.Opportunity{sampleOpportunity()}); err != nil {
		t.Fatal(err)
	}
	if err := store.reconcileDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count, _ := outbox.PendingCount(); count != 1 {
		t.Fatalf("failed remote delivery dropped row; pending=%d", count)
	}
	if due, err := outbox.Due(10); err != nil || len(due) != 0 {
		t.Fatalf("failed row should be in retry backoff: due=%d err=%v", len(due), err)
	}
}
