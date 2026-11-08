// Package supabase is a best-effort writer for the scanner's share of the
// nafaz core schema. Writes are queued on a bounded channel and flushed by a
// single goroutine, so an unreachable database can never block or slow a scan.
package supabase

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	queueSize    = 1024
	batchSize    = 50
	requestLimit = 8 * time.Second
)

// Config mirrors the executor's SUPABASE_URL/SUPABASE_KEY pair. An empty URL or
// key disables the writer, and every call becomes a no-op.
type Config struct {
	URL string
	Key string
}

type row struct {
	table      string
	onConflict string
	payload    map[string]any
}

// Writer queues rows and upserts them in batches. A nil *Writer is valid and
// inert, so callers never branch on configuration.
type Writer struct {
	url    string
	key    string
	client *http.Client
	queue  chan row
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	drops  int
}

// New returns nil when Supabase is not configured.
func New(cfg Config) *Writer {
	url := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	key := strings.TrimSpace(cfg.Key)
	if url == "" || key == "" {
		return nil
	}
	w := &Writer{
		url:    url,
		key:    key,
		client: &http.Client{Timeout: requestLimit},
		queue:  make(chan row, queueSize),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

// Close drains what it can and stops the flusher.
func (w *Writer) Close() {
	if w == nil {
		return
	}
	w.once.Do(func() { close(w.queue) })
	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
	}
	w.mu.Lock()
	if w.drops > 0 {
		log.Printf("supabase writer dropped %d writes under backpressure", w.drops)
	}
	w.mu.Unlock()
}

func (w *Writer) emit(r row) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case w.queue <- r:
	default:
		w.drops++
	}
}

func (w *Writer) run() {
	defer close(w.done)
	batch := make([]row, 0, batchSize)
	for r := range w.queue {
		batch = append(batch, r)
		if len(batch) >= batchSize {
			w.flush(batch)
			batch = batch[:0]
		}
	}
	w.flush(batch)
}

func (w *Writer) flush(rows []row) {
	if len(rows) == 0 {
		return
	}
	// PostgREST rejects a batch whose objects have different key sets, and one
	// upsert cannot touch the same row twice, so group and dedupe first.
	type group struct{ table, onConflict string }
	index := map[group]int{}
	var groups []group
	var buckets [][]map[string]any
	var keys []map[string]bool
	for _, r := range rows {
		g := group{r.table, r.onConflict}
		i, ok := index[g]
		if !ok {
			i = len(groups)
			index[g] = i
			groups = append(groups, g)
			buckets = append(buckets, nil)
			keys = append(keys, map[string]bool{})
		}
		key := fmt.Sprint(r.payload["id"], r.payload["event_key"])
		if keys[i][key] {
			// Newest write for the row wins: drop the stale duplicate.
			for j := range buckets[i] {
				if fmt.Sprint(buckets[i][j]["id"], buckets[i][j]["event_key"]) == key {
					buckets[i][j] = r.payload
					break
				}
			}
			continue
		}
		keys[i][key] = true
		buckets[i] = append(buckets[i], r.payload)
	}

	for i, g := range groups {
		body, err := json.Marshal(buckets[i])
		if err != nil {
			log.Printf("supabase encode %s: %v", g.table, err)
			continue
		}
		url := fmt.Sprintf("%s/rest/v1/%s", w.url, g.table)
		if g.onConflict != "" {
			url += "?on_conflict=" + g.onConflict
		}
		var lastErr error
		for attempt := 1; attempt <= 3; attempt++ {
			if lastErr = w.post(url, body); lastErr == nil {
				break
			}
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		if lastErr != nil {
			log.Printf("supabase write failed table=%s rows=%d: %v", g.table, len(buckets[i]), lastErr)
		}
	}
}

func (w *Writer) post(url string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("apikey", w.key)
	req.Header.Set("Authorization", "Bearer "+w.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "resolution=merge-duplicates,return=minimal")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// RecordScan persists every opportunity of one scan, including the excluded
// ones, so eligibility can be audited after the fact.
func (w *Writer) RecordScan(result domain.ScanResult) {
	if w == nil {
		return
	}
	for _, op := range result.Opportunities {
		eligibility := "EXCLUDED"
		if op.TradingEligible {
			eligibility = "ELIGIBLE"
		}
		observed := op.LastScannedAt
		if observed.IsZero() {
			observed = result.CompletedAt
		}
		raw, err := json.Marshal(op)
		if err != nil {
			continue
		}
		w.emit(row{
			table:      "scanner_opportunities",
			onConflict: "id",
			payload: map[string]any{
				"id":                    deterministicID(result.ScanID, op.OpportunityKey),
				"scan_id":               result.ScanID,
				"pair_key":              op.OpportunityKey,
				"local_exchange":        op.LocalExchange,
				"global_exchange":       op.GlobalExchange,
				"local_symbol":          op.SpotSymbol,
				"global_symbol":         op.PerpSymbol,
				"quote_asset":           op.QuoteAsset,
				"execution_mode":        op.ExecutionMode,
				"eligibility_status":    eligibility,
				"reason":                strings.Join(op.ExclusionReasons, ","),
				"spot_price":            op.SpotEntryPrice,
				"perp_price":            op.PerpEntryPrice,
				"spread_pct":            op.EntrySpreadPct,
				"funding_rate":          op.CurrentFundingRate,
				"estimated_net_profit":  op.NetProfit,
				"estimated_net_roi_pct": op.NetROIPct,
				"executable_qty":        op.Quantity,
				"capacity_pct":          op.CapacityPct,
				"observed_at":           observed.UTC().Format(time.RFC3339Nano),
				"payload":               json.RawMessage(raw),
			},
		})
	}
}

// deterministicID builds a UUID-shaped id from the scan identity so a retried
// batch upserts instead of duplicating a snapshot.
func deterministicID(scanID, pairKey string) string {
	sum := md5.Sum([]byte(scanID + "|" + pairKey))
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}
