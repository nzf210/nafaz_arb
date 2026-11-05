package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/history"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/nafaz/spot-perp-scanner/internal/scanner"
	redis "github.com/redis/go-redis/v9"
)

func TestOperationalStatusAfterSuccessfulScan(t *testing.T) {
	cfg := config.Config{
		Mode:                     "MOCK",
		ScanIntervalSeconds:      30,
		CapitalTotal:             1000,
		AllocationPerPair:        0.5,
		MinNetROIPct:             0.1,
		MaxNetROIPct:             10,
		MaxSlippagePct:           5,
		QuoteAssets:              []string{"USDT"},
		MaxMissedSuccessfulScans: 2,
	}
	store, err := history.New(t.TempDir() + "/history.json")
	if err != nil {
		t.Fatal(err)
	}
	server := New(scanner.New(cfg, exchange.NewMockAdapter()), store, cfg)

	if _, err := server.TriggerScan(context.Background()); err != nil {
		t.Fatalf("TriggerScan: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/scanner/status", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status code=%d, want %d", rec.Code, http.StatusOK)
	}

	var status OperationalStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Status != "healthy" {
		t.Fatalf("status=%q, want healthy (last error: %s)", status.Status, status.LastError)
	}
	if status.Stale || status.LastSuccessfulAt == nil || status.SuccessfulScanAgeSeconds == nil {
		t.Fatalf("successful scan should be fresh: %+v", status)
	}
	if status.LatestScanComplete == nil || !*status.LatestScanComplete {
		t.Fatalf("latest scan should be complete: %+v", status)
	}
	if status.LatestScanDurationMS < 0 {
		t.Fatalf("scan duration must be non-negative: %d", status.LatestScanDurationMS)
	}
}

type failedMarketAdapter struct {
	exchange.MockAdapter
}

func (failedMarketAdapter) SpotMarkets(context.Context) ([]domain.Market, error) {
	return nil, errors.New("market API unavailable")
}

func TestOperationalStatusReportsScanFailureAndStaleness(t *testing.T) {
	cfg := config.Config{
		Mode:                "LIVE",
		ScanIntervalSeconds: 30,
		CapitalTotal:        1000,
		AllocationPerPair:   0.5,
	}
	store, err := history.New(t.TempDir() + "/history.json")
	if err != nil {
		t.Fatal(err)
	}
	server := New(scanner.New(cfg, &failedMarketAdapter{}), store, cfg)

	if _, err := server.TriggerScan(context.Background()); err == nil {
		t.Fatal("expected scan failure")
	}
	status := server.OperationalStatus()
	if status.Status != "degraded" || status.LastError == "" {
		t.Fatalf("failed scan should report degraded status and error: %+v", status)
	}
	if !status.Stale || status.LastSuccessfulAt != nil {
		t.Fatalf("failed scan must not count as a fresh successful scan: %+v", status)
	}
}

func TestExecutorStatusEndpoint(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to run miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	rc := redishandler.NewWithClient(rdb)

	srv := New(nil, nil, config.Config{Mode: "MOCK"})

	// Disabled: no redis client wired.
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/executor/status", nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode disabled response: %v", err)
	}
	if body["status"] != "DISABLED" {
		t.Errorf("status=%v, want DISABLED", body["status"])
	}

	srv.SetExecutorClient(rc)

	// Offline: executor never published.
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/executor/status", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode offline response: %v", err)
	}
	if body["status"] != "OFFLINE" {
		t.Errorf("status=%v, want OFFLINE", body["status"])
	}

	// Online: executor snapshot is passed through verbatim.
	snapshot := `{"status":"RUNNING","failed_pairs":1,"failures":{"indodax:btc_idr":{"reason":"HEDGE_FAILED"}}}`
	if err := rdb.Set(context.Background(), redishandler.KeyExecutorStatus, snapshot, 30*time.Second).Err(); err != nil {
		t.Fatalf("seed executor status: %v", err)
	}
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/executor/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d, want 200", rec.Code)
	}
	body = map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode online response: %v", err)
	}
	if body["status"] != "RUNNING" {
		t.Errorf("status=%v, want RUNNING", body["status"])
	}
	if body["failed_pairs"] != float64(1) {
		t.Errorf("failed_pairs=%v, want 1", body["failed_pairs"])
	}
}

// TestRunScanRejectsOverlap verifies concurrent scans cannot overlap: the
// second call is rejected with "scan already in progress" instead of running
// a second scanner pass that could double-publish executor signals.
func TestRunScanRejectsOverlap(t *testing.T) {
	cfg := config.Config{Mode: "MOCK", QuoteAssets: []string{"USDT"}, CapitalTotal: 1000, AllocationPerPair: 0.5}
	svc := scanner.New(cfg, exchange.NewMockAdapter())
	store, err := history.New(t.TempDir() + "/history.json")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(svc, store, cfg)

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.runScan(context.Background())
	}()
	if _, err := srv.runScan(context.Background()); err != nil {
		if !strings.Contains(err.Error(), "scan already in progress") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	<-done
}
