package supabase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	defaultScannerOutboxMaxBytes = int64(128 << 20)
	defaultReconcileInterval     = 2 * time.Second
	maxOutboxBatchRecords        = 5
)

var ErrOutboxCapacity = errors.New("scanner durable outbox capacity reached")

// ErrDuplicateRows rejects a batch whose rows collide on the upsert conflict
// target. PostgREST fails such a statement outright, so journaling it would
// leave a record that can never be acknowledged and would block every later
// scan behind it in the outbox.
var ErrDuplicateRows = errors.New("scanner outbox rows collide on idempotency_key")

type outboxRecord struct {
	ID            string           `json:"id"`
	ScanID        string           `json:"scan_id"`
	Rows          []opportunityRow `json:"rows"`
	CreatedAt     time.Time        `json:"created_at"`
	Attempts      int              `json:"attempts"`
	NextAttemptAt time.Time        `json:"next_attempt_at"`
	LastError     string           `json:"last_error,omitempty"`
}

// DurableOutbox is a single-process, append-only JSONL journal. It uses fsync
// on enqueue and atomic rewrite on acknowledgement/retry. Mount its directory
// on persistent local storage; do not share it between scanner replicas.
type DurableOutbox struct {
	path     string
	maxBytes int64
	mu       sync.Mutex
	records  map[string]outboxRecord
	order    []string
}

func NewDurableOutbox(path string, maxBytes int64) (*DurableOutbox, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("scanner outbox path is required")
	}
	if maxBytes <= 0 {
		maxBytes = defaultScannerOutboxMaxBytes
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create scanner outbox directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open scanner outbox: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure scanner outbox permissions: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("sync scanner outbox: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close scanner outbox: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat scanner outbox at startup: %w", err)
	}
	if info.Size() > maxBytes {
		log.Printf("warning: scanner outbox exceeds capacity (%d > %d bytes); opening in drain-only mode until acknowledged", info.Size(), maxBytes)
	}
	outbox := &DurableOutbox{
		path: path, maxBytes: maxBytes,
		records: make(map[string]outboxRecord),
	}
	loaded, validateErr := outbox.readRecordsLocked()
	if validateErr != nil {
		return nil, fmt.Errorf("validate scanner outbox: %w", validateErr)
	}
	for _, record := range loaded {
		if _, exists := outbox.records[record.ID]; !exists {
			outbox.order = append(outbox.order, record.ID)
		}
		outbox.records[record.ID] = record
	}
	return outbox, nil
}

func (o *DurableOutbox) Enqueue(scanID string, opportunities []domain.Opportunity) error {
	if o == nil || len(opportunities) == 0 {
		return nil
	}
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return fmt.Errorf("scan_id is required for scanner outbox")
	}
	rows := buildRows(scanID, opportunities)
	if len(rows) == 0 {
		return nil
	}
	if err := rejectDuplicateRows(rows); err != nil {
		return err
	}
	digest, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("encode scanner outbox rows: %w", err)
	}
	sum := sha256.Sum256(append([]byte(scanID+":"), digest...))
	record := outboxRecord{
		ID:        hex.EncodeToString(sum[:]),
		ScanID:    scanID,
		Rows:      rows,
		CreatedAt: time.Now().UTC(),
	}
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode scanner outbox record: %w", err)
	}
	line = append(line, '\n')

	o.mu.Lock()
	defer o.mu.Unlock()
	if _, exists := o.records[record.ID]; exists {
		return nil // same scan replay is idempotent while pending
	}
	info, err := os.Stat(o.path)
	if err != nil {
		return fmt.Errorf("stat scanner outbox: %w", err)
	}
	if info.Size()+int64(len(line)) > o.maxBytes {
		return fmt.Errorf("%w: max_bytes=%d", ErrOutboxCapacity, o.maxBytes)
	}
	file, err := os.OpenFile(o.path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("append scanner outbox: %w", err)
	}
	if _, err := file.Write(line); err != nil {
		_ = file.Close()
		return fmt.Errorf("write scanner outbox: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync scanner outbox: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close scanner outbox: %w", err)
	}
	o.records[record.ID] = record
	o.order = append(o.order, record.ID)
	return nil
}

// rejectDuplicateRows mirrors the PostgREST upsert constraint: rows delivered
// in one statement must be unique on the on_conflict target.
func rejectDuplicateRows(rows []opportunityRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, exists := seen[row.IdempotencyKey]; exists {
			return fmt.Errorf("%w: %s", ErrDuplicateRows, row.IdempotencyKey)
		}
		seen[row.IdempotencyKey] = struct{}{}
	}
	return nil
}

func (o *DurableOutbox) Due(limit int) ([]outboxRecord, error) {
	if o == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = maxOutboxBatchRecords
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now().UTC()
	due := make([]outboxRecord, 0, limit)
	for _, id := range o.order {
		record := o.records[id]
		if !record.NextAttemptAt.IsZero() && record.NextAttemptAt.After(now) {
			continue
		}
		due = append(due, record)
		if len(due) >= limit {
			break
		}
	}
	return due, nil
}

func (o *DurableOutbox) Ack(ids []string) error {
	return o.mutate(ids, func(record *outboxRecord) bool { return false })
}

func (o *DurableOutbox) Retry(ids []string, cause error) error {
	message := "delivery failed"
	if cause != nil {
		message = cause.Error()
	}
	return o.mutate(ids, func(record *outboxRecord) bool {
		record.Attempts++
		delay := time.Second * time.Duration(1<<min(record.Attempts, 8))
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		record.NextAttemptAt = time.Now().UTC().Add(delay)
		record.LastError = message
		return true
	})
}

func (o *DurableOutbox) mutate(ids []string, update func(*outboxRecord) bool) error {
	if o == nil || len(ids) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	kept := make([]outboxRecord, 0, len(o.records))
	nextRecords := make(map[string]outboxRecord, len(o.records))
	nextOrder := make([]string, 0, len(o.order))
	for _, id := range o.order {
		record := o.records[id]
		if _, ok := wanted[id]; ok && !update(&record) {
			continue
		}
		kept = append(kept, record)
		nextRecords[id] = record
		nextOrder = append(nextOrder, id)
	}
	if err := o.rewriteLocked(kept); err != nil {
		return err
	}
	o.records = nextRecords
	o.order = nextOrder
	return nil
}

// OutboxEntry is a read-only view of one pending journal record for operators.
type OutboxEntry struct {
	ScanID        string
	Rows          int
	Attempts      int
	LastError     string
	NextAttemptAt time.Time
}

// Inspect reports the pending journal in replay order without modifying it.
func (o *DurableOutbox) Inspect() ([]OutboxEntry, error) {
	if o == nil {
		return nil, fmt.Errorf("scanner outbox is not open")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	records, err := o.readLocked()
	if err != nil {
		return nil, err
	}
	entries := make([]OutboxEntry, 0, len(records))
	for _, record := range records {
		entries = append(entries, OutboxEntry{
			ScanID:        record.ScanID,
			Rows:          len(record.Rows),
			Attempts:      record.Attempts,
			LastError:     record.LastError,
			NextAttemptAt: record.NextAttemptAt,
		})
	}
	return entries, nil
}

func (o *DurableOutbox) PendingCount() (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.records), nil
}

// readLocked returns the in-memory journal, which is authoritative while the
// process holds the file: Retry/Ack update records without rewriting disk, so
// re-reading the file here would report pre-retry state and could let a prune
// discard a record whose next attempt is still pending.
func (o *DurableOutbox) readLocked() ([]outboxRecord, error) {
	records := make([]outboxRecord, 0, len(o.order))
	for _, id := range o.order {
		records = append(records, o.records[id])
	}
	return records, nil
}

// recordID is the journal identity of one scan snapshot. It is recomputed when
// a record is re-read during salvage because nothing else in the line is
// trusted; Enqueue derives it the same way at commit time.
func recordID(scanID string, rows []opportunityRow) string {
	digest, err := json.Marshal(rows)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte(scanID+":"), digest...))
	return hex.EncodeToString(sum[:])
}

// PruneReport describes what PruneUndeliverable found and removed.
type PruneReport struct {
	Total        int
	Kept         int
	Dropped      int
	DroppedRows  int
	Truncated    bool
	SampleScanID string
	Reasons      map[string]int
}

// salvageReader strips the fields only the delivery path needs, letting a
// recovery tool parse a journal whose retry metadata is malformed. The
// production path uses it as soon as any single record fails to decode:
// failing closed there turns one bad line into a scanner that will not start.
type salvageReader struct {
	ScanID string          `json:"scan_id"`
	Rows   json.RawMessage `json:"rows"`
}

const (
	// defaultPruneAttemptFloor keeps a record that has only been retried a
	// couple of times: ordinary transient outages must not be thrown away.
	defaultPruneAttemptFloor = 3
	// maxPruneDropsPerRun bounds collateral damage when the match pattern is
	// broader than intended. A human can raise it deliberately.
	maxPruneDropsPerRun = 1000
)

// PruneUndeliverable removes records whose LastError matches pattern, after
// first moving the untouched original file to <path>.bak. It exists for the
// PGRST102 case: the scanner fixes its row builder at runtime, while a record
// already journaled by the old build stays permanently undeliverable and, since
// Due() replays the oldest record first, blocks every later scan behind it.
//
// The pattern must not match transient transport text; require e.g.
// "PGRST102|All object keys must match" rather than "400".
func (o *DurableOutbox) PruneUndeliverable(pattern string, minAttempts int, dryRun bool) (PruneReport, error) {
	report := PruneReport{Reasons: map[string]int{}}
	if o == nil {
		return report, fmt.Errorf("scanner outbox is not open")
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return report, fmt.Errorf("compile prune pattern: %w", err)
	}
	if minAttempts <= 0 {
		minAttempts = defaultPruneAttemptFloor
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// Replay order is the in-memory journal: Retry() updates records without
	// rewriting the file, so the on-disk lines can be stale.
	records, err := o.readLocked()
	if err != nil {
		return report, err
	}
	report.Total = len(records)

	kept := make([]outboxRecord, 0, len(records))
	var droppedIDs []string
	for _, record := range records {
		if len(droppedIDs) < maxPruneDropsPerRun &&
			record.Attempts >= minAttempts && compiled.MatchString(record.LastError) {
			droppedIDs = append(droppedIDs, record.ID)
			report.Dropped++
			report.DroppedRows += len(record.Rows)
			report.Reasons[record.LastError]++
			if report.SampleScanID == "" {
				report.SampleScanID = record.ScanID
			}
			continue
		}
		kept = append(kept, record)
	}
	report.Kept = len(kept)
	report.Truncated = report.Dropped == maxPruneDropsPerRun

	if report.Dropped == 0 || dryRun {
		return report, nil
	}
	if err := copyFile(o.path, o.path+".bak"); err != nil {
		return report, fmt.Errorf("back up scanner outbox before prune: %w", err)
	}
	if err := o.rewriteLocked(kept); err != nil {
		return report, err
	}
	nextRecords := make(map[string]outboxRecord, len(kept))
	nextOrder := make([]string, 0, len(kept))
	for _, record := range kept {
		nextRecords[record.ID] = record
		nextOrder = append(nextOrder, record.ID)
	}
	o.records = nextRecords
	o.order = nextOrder
	return report, nil
}

func copyFile(source, destination string) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.WriteFile(destination, data, 0o600); err != nil {
		return err
	}
	file, err := os.Open(destination)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (o *DurableOutbox) readRecordsLocked() ([]outboxRecord, error) {
	data, err := os.ReadFile(o.path)
	if err != nil {
		return nil, fmt.Errorf("read scanner outbox: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	lines := strings.Split(string(data), "\n")
	records := make([]outboxRecord, 0, len(lines))
	for index, line := range lines {
		if line == "" {
			continue
		}
		var record outboxRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			// A torn final append without a newline is safe to truncate. A
			// malformed committed line is corruption and must fail closed.
			if index == len(lines)-1 && !strings.HasSuffix(string(data), "\n") {
				if truncateErr := os.Truncate(o.path, int64(len(data)-len(line))); truncateErr != nil {
					return nil, fmt.Errorf("truncate incomplete scanner outbox tail: %w", truncateErr)
				}
				file, openErr := os.OpenFile(o.path, os.O_WRONLY, 0o600)
				if openErr != nil {
					return nil, fmt.Errorf("open scanner outbox after tail recovery: %w", openErr)
				}
				syncErr := file.Sync()
				closeErr := file.Close()
				if syncErr != nil {
					return nil, fmt.Errorf("sync scanner outbox tail recovery: %w", syncErr)
				}
				if closeErr != nil {
					return nil, fmt.Errorf("close scanner outbox after tail recovery: %w", closeErr)
				}
				break
			}
			// Recovery mode: keep everything the delivery path can still consume
			// rather than refusing to start over one unreadable retry field.
			var salvage salvageReader
			if salvageErr := json.Unmarshal([]byte(line), &salvage); salvageErr != nil ||
				salvage.ScanID == "" || len(salvage.Rows) == 0 {
				return nil, fmt.Errorf("scanner outbox corrupt at record %d: %w", index+1, err)
			}
			var rows []opportunityRow
			if rowsErr := json.Unmarshal(salvage.Rows, &rows); rowsErr != nil {
				return nil, fmt.Errorf("scanner outbox corrupt at record %d: %w", index+1, err)
			}
			record = outboxRecord{ID: recordID(salvage.ScanID, rows), ScanID: salvage.ScanID, Rows: rows}
		}
		if record.ID == "" || record.ScanID == "" || len(record.Rows) == 0 {
			return nil, fmt.Errorf("scanner outbox record %d is missing required fields", index+1)
		}
		records = append(records, record)
		if index == len(lines)-1 && !strings.HasSuffix(string(data), "\n") {
			// The JSON payload reached disk but the process stopped before its
			// delimiter. Preserve it and terminate the record before future appends.
			file, openErr := os.OpenFile(o.path, os.O_APPEND|os.O_WRONLY, 0o600)
			if openErr != nil {
				return nil, fmt.Errorf("open scanner outbox to finish final delimiter: %w", openErr)
			}
			_, writeErr := file.Write([]byte("\n"))
			syncErr := file.Sync()
			closeErr := file.Close()
			if writeErr != nil {
				return nil, fmt.Errorf("finish scanner outbox final delimiter: %w", writeErr)
			}
			if syncErr != nil {
				return nil, fmt.Errorf("sync scanner outbox final delimiter: %w", syncErr)
			}
			if closeErr != nil {
				return nil, fmt.Errorf("close scanner outbox after delimiter repair: %w", closeErr)
			}
		}
	}
	return records, nil
}

func (o *DurableOutbox) rewriteLocked(records []outboxRecord) error {
	temp, err := os.CreateTemp(filepath.Dir(o.path), ".scanner-outbox-*.tmp")
	if err != nil {
		return fmt.Errorf("create scanner outbox compaction file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	for _, record := range records {
		line, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			_ = temp.Close()
			return fmt.Errorf("encode scanner outbox during compaction: %w", marshalErr)
		}
		if _, err := temp.Write(append(line, '\n')); err != nil {
			_ = temp.Close()
			return fmt.Errorf("write scanner outbox compaction: %w", err)
		}
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync scanner outbox compaction: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close scanner outbox compaction: %w", err)
	}
	if err := os.Rename(tempPath, o.path); err != nil {
		return fmt.Errorf("replace scanner outbox after compaction: %w", err)
	}
	dir, err := os.Open(filepath.Dir(o.path))
	if err != nil {
		return fmt.Errorf("open scanner outbox directory for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync scanner outbox directory: %w", err)
	}
	return nil
}

func (o *DurableOutbox) Path() string { return o.path }

// DurableStore separates the scanner's durable local enqueue from remote
// PostgREST delivery. The reconciliation worker only replays idempotent
// opportunity upserts; it never reads exchange accounts or changes exposure.
type DurableStore struct {
	client   *Client
	outbox   *DurableOutbox
	interval time.Duration
}

func NewDurableStore(client *Client, outbox *DurableOutbox, interval time.Duration) (*DurableStore, error) {
	if client == nil || outbox == nil {
		return nil, fmt.Errorf("Supabase client and scanner outbox are required")
	}
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	return &DurableStore{client: client, outbox: outbox, interval: interval}, nil
}

// SaveOpportunities only performs a durable local write. It never waits for
// Supabase/network delivery, preserving the scanner's fail-open signal path.
func (s *DurableStore) SaveOpportunities(_ context.Context, scanID string, opportunities []domain.Opportunity) error {
	return s.outbox.Enqueue(scanID, opportunities)
}

// Run is the background delivery reconciliation worker. A successful
// idempotent PostgREST upsert is the acknowledgement boundary; every failure
// leaves the local record queued with a bounded retry delay.
func (s *DurableStore) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if err := s.reconcileDue(ctx); err != nil && ctx.Err() == nil {
			log.Printf("warning: scanner Supabase reconciliation worker: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *DurableStore) reconcileDue(ctx context.Context) error {
	records, err := s.outbox.Due(maxOutboxBatchRecords)
	if err != nil || len(records) == 0 {
		return err
	}
	ids := make([]string, 0, len(records))
	rows := make([]opportunityRow, 0)
	for _, record := range records {
		ids = append(ids, record.ID)
		rows = append(rows, record.Rows...)
	}
	requestCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	deliveryErr := s.client.saveRows(requestCtx, rows)
	cancel()
	if deliveryErr != nil {
		if retryErr := s.outbox.Retry(ids, deliveryErr); retryErr != nil {
			return fmt.Errorf("persist scanner outbox retry schedule: %w", retryErr)
		}
		log.Printf("warning: scanner outbox delivery deferred records=%d rows=%d: %v", len(records), len(rows), deliveryErr)
		return nil
	}
	if err := s.outbox.Ack(ids); err != nil {
		return fmt.Errorf("ack delivered scanner outbox records: %w", err)
	}
	return nil
}
