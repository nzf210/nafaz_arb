package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/history"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/nafaz/spot-perp-scanner/internal/scanner"
)

const METHOD = "application/json"

type Server struct {
	scanner       *scanner.Service
	history       *history.Store
	cfg           config.Config
	mu            sync.RWMutex
	latest        domain.ScanResult
	scanHooks     []func(domain.ScanResult)
	lastAttemptAt time.Time
	lastOutcomeAt time.Time
	lastSuccessAt time.Time
	lastScanError string
	activeScans   int
	scanInFlight  atomic.Bool
	executor      *redishandler.Client
}

type OperationalStatus struct {
	Status                   string     `json:"status"`
	Mode                     string     `json:"mode"`
	ScanIntervalSeconds      int        `json:"scan_interval_seconds"`
	ReadOnly                 bool       `json:"read_only"`
	ScanInProgress           bool       `json:"scan_in_progress"`
	LastAttemptAt            *time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessfulAt         *time.Time `json:"last_successful_at,omitempty"`
	LatestScanDurationMS     int64      `json:"latest_scan_duration_ms,omitempty"`
	LatestScanComplete       *bool      `json:"latest_scan_complete,omitempty"`
	SuccessfulScanAgeSeconds *int64     `json:"successful_scan_age_seconds,omitempty"`
	StaleAfterSeconds        int64      `json:"stale_after_seconds"`
	Stale                    bool       `json:"stale"`
	LastError                string     `json:"last_error,omitempty"`
}

func New(s *scanner.Service, h *history.Store, c config.Config) *Server {
	return &Server{scanner: s, history: h, cfg: c}
}

// SetExecutorClient wires the Redis client used to read executor status.
func (s *Server) SetExecutorClient(c *redishandler.Client) {
	s.executor = c
}

// RegisterScanHook adds a callback invoked whenever a scan completes.
func (s *Server) RegisterScanHook(fn func(domain.ScanResult)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scanHooks = append(s.scanHooks, fn)
}

// TriggerScan runs an on-demand scan.
func (s *Server) TriggerScan(ctx context.Context) (domain.ScanResult, error) {
	return s.runScan(ctx)
}

// LatestScan returns the most recent scan result.
func (s *Server) LatestScan() domain.ScanResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latest
}

// FinishScanForTest exposes finishScan for tests.
func (s *Server) FinishScanForTest(r domain.ScanResult, complete bool, scanErr error) {
	s.finishScan(r, complete, scanErr)
}

// Store returns the history store.
func (s *Server) Store() *history.Store {
	return s.history
}

// Scanner returns the underlying scanner service.
func (s *Server) Scanner() *scanner.Service {
	return s.scanner
}

// Config returns active configuration.
func (s *Server) Config() config.Config {
	return s.cfg
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/api/v1/scanner/status", s.status)
	mux.HandleFunc("/api/v1/scanner/scan", s.scan)
	mux.HandleFunc("/api/v1/scanner/latest", s.latestScan)
	mux.HandleFunc("/api/v1/scanner/opportunities/history", s.opportunityHistory)
	mux.HandleFunc("/api/v1/scanner/opportunities/metrics", s.opportunityMetrics)
	mux.HandleFunc("/api/v1/executor/status", s.executorStatus)
	return mux
}
func (s *Server) RunPeriodic(ctx context.Context) {
	if _, err := s.runScan(ctx); err != nil {
		log.Printf("initial scheduled scan failed: %v", err)
	}
	t := time.NewTicker(time.Duration(s.cfg.ScanIntervalSeconds) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.runScan(ctx); err != nil {
				log.Printf("scheduled scan failed: %v", err)
			}
		}
	}
}
func (s *Server) runScan(ctx context.Context) (domain.ScanResult, error) {
	// One scan at a time: a periodic tick overlapping a manual trigger could
	// publish duplicate executor signals from slightly different snapshots.
	if !s.scanInFlight.CompareAndSwap(false, true) {
		s.mu.RLock()
		latest := s.latest
		s.mu.RUnlock()
		return latest, fmt.Errorf("scan already in progress")
	}
	defer s.scanInFlight.Store(false)

	s.mu.Lock()
	s.lastAttemptAt = time.Now().UTC()
	s.activeScans++
	s.mu.Unlock()

	r, complete, err := s.scanner.Scan(ctx)
	if err != nil {
		s.finishScan(r, false, err)
		return r, err
	}
	if e := s.history.Observe(r.CompletedAt, r.Opportunities, complete, s.cfg.MaxMissedSuccessfulScans); e != nil {
		r.Complete = false
		r.Errors = append(r.Errors, "persist opportunity history: "+e.Error())
		s.finishScan(r, false, e)
		return r, e
	}
	s.finishScan(r, complete, nil)
	s.mu.Lock()
	hooks := make([]func(domain.ScanResult), len(s.scanHooks))
	copy(hooks, s.scanHooks)
	s.mu.Unlock()

	for _, h := range hooks {
		h(r)
	}
	return r, nil
}
func (s *Server) finishScan(r domain.ScanResult, complete bool, scanErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latest.ScanID == "" || !r.StartedAt.Before(s.latest.StartedAt) {
		s.latest = r
	}
	if complete && (s.lastSuccessAt.IsZero() || r.CompletedAt.After(s.lastSuccessAt)) {
		s.lastSuccessAt = r.CompletedAt
	}
	if s.activeScans > 0 {
		s.activeScans--
	}
	if s.lastOutcomeAt.IsZero() || !r.StartedAt.Before(s.lastOutcomeAt) {
		s.lastOutcomeAt = r.StartedAt
		switch {
		case scanErr != nil:
			s.lastScanError = scanErr.Error()
		case !complete:
			s.lastScanError = strings.Join(r.Errors, "; ")
			if s.lastScanError == "" {
				s.lastScanError = "scan incomplete"
			}
		default:
			s.lastScanError = ""
		}
	}
}

// OperationalStatus reports the latest scan outcome and data freshness.
func (s *Server) OperationalStatus() OperationalStatus {
	now := time.Now().UTC()
	s.mu.RLock()
	defer s.mu.RUnlock()

	staleAfter := int64(s.cfg.ScanIntervalSeconds * 2)
	if staleAfter <= 0 {
		staleAfter = 60
	}
	status := OperationalStatus{
		Status:              "starting",
		Mode:                s.cfg.Mode,
		ScanIntervalSeconds: s.cfg.ScanIntervalSeconds,
		ReadOnly:            true,
		ScanInProgress:      s.activeScans > 0,
		StaleAfterSeconds:   staleAfter,
		Stale:               s.lastSuccessAt.IsZero(),
		LastError:           s.lastScanError,
	}
	if !s.lastAttemptAt.IsZero() {
		attempt := s.lastAttemptAt
		status.LastAttemptAt = &attempt
	}
	if s.latest.ScanID != "" {
		complete := s.latest.Complete
		status.LatestScanComplete = &complete
		status.LatestScanDurationMS = s.latest.DurationMS
	}
	if !s.lastSuccessAt.IsZero() {
		success := s.lastSuccessAt
		status.LastSuccessfulAt = &success
		age := int64(now.Sub(success).Seconds())
		if age < 0 {
			age = 0
		}
		status.SuccessfulScanAgeSeconds = &age
		status.Stale = age > staleAfter
	}
	if !s.lastAttemptAt.IsZero() {
		status.Status = "degraded"
	}
	if !s.lastSuccessAt.IsZero() && !status.Stale && s.lastScanError == "" {
		status.Status = "healthy"
	}
	if s.activeScans > 0 && s.lastSuccessAt.IsZero() {
		status.Status = "scanning"
	}
	return status
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, METHOD, 405)
		return
	}
	write(w, map[string]string{"status": "ok"})
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, METHOD, 405)
		return
	}
	write(w, s.OperationalStatus())
}

// executorStatus exposes the executor's self-reported state, including the
// terminal FAILED pairs that would otherwise only be visible in logs.
func (s *Server) executorStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, METHOD, 405)
		return
	}
	if s.executor == nil {
		write(w, map[string]any{"status": "DISABLED", "detail": "redis integration is disabled"})
		return
	}
	raw, found, err := s.executor.GetExecutorStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !found {
		write(w, map[string]any{"status": "OFFLINE", "detail": "executor has not published a status snapshot"})
		return
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		http.Error(w, "invalid executor status payload", 500)
		return
	}
	write(w, snapshot)
}
func (s *Server) scan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, METHOD, 405)
		return
	}
	result, err := s.runScan(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	write(w, result)
}
func (s *Server) latestScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, METHOD, 405)
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	write(w, s.latest)
}
func (s *Server) opportunityHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, METHOD, 405)
		return
	}
	from, to := timeFilter(r)
	items := s.history.List(from, to, r.URL.Query().Get("status"))
	write(w, items)
}
func (s *Server) opportunityMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, METHOD, 405)
		return
	}
	from, to := timeFilter(r)
	write(w, s.history.GetMetrics(from, to, time.Now().UTC()))
}
func timeFilter(r *http.Request) (time.Time, time.Time) {
	parse := func(k string) time.Time {
		v := r.URL.Query().Get(k)
		if v == "" {
			return time.Time{}
		}
		t, _ := time.Parse(time.RFC3339, v)
		return t
	}
	return parse("from"), parse("to")
}
func write(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
