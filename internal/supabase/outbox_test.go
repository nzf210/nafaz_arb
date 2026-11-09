package supabase

import (
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"context"
	"encoding/json"
	"errors"
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
	dueRecords, readErr := restarted.readLocked()
	restarted.mu.Unlock()
	if readErr != nil || len(dueRecords) != 1 ||
		dueRecords[0].Attempts != 1 || !strings.Contains(dueRecords[0].LastError, "503") {
		t.Fatalf("retry metadata not persisted: records=%+v err=%v", dueRecords, readErr)
	}
}

func TestScannerReconciliationWorkerAcknowledgesSuccessfulUpsert(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer server.Close()

	outbox, err := NewDurableOutbox(filepath.Join(t.TempDir(), "outbox.jsonl"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{baseURL: server.URL, serviceRoleKey: "test", httpClient: server.Client()}
	store, err := NewDurableStore(client, outbox, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("temporarily unavailable"))
	}))
	defer server.Close()
	outbox, err := NewDurableOutbox(filepath.Join(t.TempDir(), "outbox.jsonl"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{baseURL: server.URL, serviceRoleKey: "test", httpClient: server.Client()}
	store, err := NewDurableStore(client, outbox, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
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
