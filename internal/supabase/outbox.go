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
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	defaultScannerOutboxMaxBytes = int64(128 << 20)
	defaultReconcileInterval     = 2 * time.Second
	maxOutboxBatchRecords        = 25
)

var ErrOutboxCapacity = errors.New("scanner durable outbox capacity reached")

type outboxRecord struct {
	ID            string            `json:"id"`
	ScanID        string            `json:"scan_id"`
	Rows          []opportunityRow  `json:"rows"`
	CreatedAt     time.Time         `json:"created_at"`
	Attempts      int               `json:"attempts"`
	NextAttemptAt time.Time         `json:"next_attempt_at"`
	LastError     string            `json:"last_error,omitempty"`
}

// DurableOutbox is a single-process, append-only JSONL journal. It uses fsync
// on enqueue and atomic rewrite on acknowledgement/retry. Mount its directory
// on persistent local storage; do not share it between scanner replicas.
type DurableOutbox struct {
	path     string
	maxBytes int64
	mu       sync.Mutex
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
	return &DurableOutbox{path: path, maxBytes: maxBytes}, nil
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
	records, err := o.readLocked()
	if err != nil {
		return err
	}
	for _, existing := range records {
		if existing.ID == record.ID {
			return nil // same scan replay is idempotent while pending
		}
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
	records, err := o.readLocked()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	due := make([]outboxRecord, 0, limit)
	for _, record := range records {
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
	records, err := o.readLocked()
	if err != nil {
		return err
	}
	kept := make([]outboxRecord, 0, len(records))
	for _, record := range records {
		if _, ok := wanted[record.ID]; !ok {
			kept = append(kept, record)
			continue
		}
		if update(&record) {
			kept = append(kept, record)
		}
	}
	return o.rewriteLocked(kept)
}

func (o *DurableOutbox) PendingCount() (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	records, err := o.readLocked()
	return len(records), err
}

func (o *DurableOutbox) readLocked() ([]outboxRecord, error) {
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
			return nil, fmt.Errorf("scanner outbox corrupt at record %d: %w", index+1, err)
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
	if err != nil {
		return err
	}
	for _, record := range records {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		err := s.client.saveRows(requestCtx, record.Rows)
		cancel()
		if err != nil {
			if retryErr := s.outbox.Retry([]string{record.ID}, err); retryErr != nil {
				return fmt.Errorf("persist retry schedule for scan %s: %w", record.ScanID, retryErr)
			}
			log.Printf("warning: scanner outbox delivery deferred scan=%s attempts=%d: %v", record.ScanID, record.Attempts+1, err)
			continue
		}
		if err := s.outbox.Ack([]string{record.ID}); err != nil {
			return fmt.Errorf("ack delivered scanner scan %s: %w", record.ScanID, err)
		}
	}
	return nil
}
