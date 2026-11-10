package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	defaultQueueSize = 64
	maxBatchRetries  = 3
)

type scanBatch struct {
	opportunities []map[string]any
	marketPairs   []map[string]any
}

// SupabaseWriter persists scanner snapshots asynchronously. Redis remains the
// low-latency signal and coordination channel; a database outage never blocks a scan.
type SupabaseWriter struct {
	enabled bool
	url     string
	key     string
	client  *http.Client
	queue   chan scanBatch
	started atomic.Bool
	closed  atomic.Bool
	dropped atomic.Uint64
	wg      sync.WaitGroup
}

func NewSupabaseWriter(cfg config.SupabaseConfig) *SupabaseWriter {
	w := &SupabaseWriter{
		enabled: cfg.Enabled && strings.TrimSpace(cfg.URL) != "" && strings.TrimSpace(cfg.Key) != "",
		url:     strings.TrimRight(strings.TrimSpace(cfg.URL), "/"),
		key:     strings.TrimSpace(cfg.Key),
		client:  &http.Client{Timeout: 8 * time.Second},
		queue:   make(chan scanBatch, defaultQueueSize),
	}
	return w
}

func (w *SupabaseWriter) Enabled() bool {
	return w != nil && w.enabled
}

func (w *SupabaseWriter) DroppedBatches() uint64 {
	if w == nil {
		return 0
	}
	return w.dropped.Load()
}

func (w *SupabaseWriter) Start() {
	if !w.Enabled() || !w.started.CompareAndSwap(false, true) {
		return
	}
	w.wg.Add(1)
	go w.run()
	log.Printf("Supabase scanner persistence enabled")
}

// SubmitScan is intentionally non-blocking. The local JSON history and Redis
// signal path remain authoritative for their existing responsibilities.
func (w *SupabaseWriter) SubmitScan(result domain.ScanResult) {
	if !w.Enabled() || w.closed.Load() || len(result.Opportunities) == 0 {
		return
	}
	batch := buildScanBatch(result)
	if len(batch.opportunities) == 0 {
		return
	}
	select {
	case w.queue <- batch:
	default:
		w.dropped.Add(1)
		log.Printf("Supabase scanner queue full; dropped scan snapshot scan_id=%s", result.ScanID)
	}
}

func (w *SupabaseWriter) Close(ctx context.Context) error {
	if !w.Enabled() || !w.started.Load() || !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(w.queue)
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *SupabaseWriter) run() {
	defer w.wg.Done()
	for batch := range w.queue {
		w.flush(batch)
	}
}

func (w *SupabaseWriter) flush(batch scanBatch) {
	if len(batch.marketPairs) > 0 {
		if err := w.post(context.Background(), "market_pairs", "pair_key", batch.marketPairs); err != nil {
			log.Printf("Supabase market_pairs write failed: %v", err)
		}
	}
	if len(batch.opportunities) > 0 {
		if err := w.post(context.Background(), "scanner_opportunities", "id", batch.opportunities); err != nil {
			log.Printf("Supabase scanner_opportunities write failed: %v", err)
		}
	}
}

func (w *SupabaseWriter) post(parent context.Context, table, conflict string, rows []map[string]any) error {
	body, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", table, err)
	}
	endpoint := fmt.Sprintf("%s/rest/v1/%s?on_conflict=%s", w.url, table, conflict)
	var lastErr error
	for attempt := 1; attempt <= maxBatchRetries; attempt++ {
		ctx, cancel := context.WithTimeout(parent, 8*time.Second)
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if reqErr != nil {
			cancel()
			return fmt.Errorf("build %s request: %w", table, reqErr)
		}
		req.Header.Set("apikey", w.key)
		req.Header.Set("Authorization", "Bearer "+w.key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "identity")
		req.Header.Set("Prefer", "resolution=merge-duplicates,return=minimal")
		resp, doErr := w.client.Do(req)
		if doErr == nil {
			responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				cancel()
				return nil
			}
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
			retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
			cancel()
			if !retryable {
				return fmt.Errorf("%s rejected: %w", table, lastErr)
			}
		} else {
			lastErr = doErr
			cancel()
		}
		if attempt < maxBatchRetries {
			time.Sleep(time.Duration(attempt*attempt) * 150 * time.Millisecond)
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", table, maxBatchRetries, lastErr)
}

func buildScanBatch(result domain.ScanResult) scanBatch {
	scanID := strings.TrimSpace(result.ScanID)
	if scanID == "" {
		stamp := result.StartedAt
		if stamp.IsZero() {
			stamp = result.CompletedAt
		}
		scanID = stamp.UTC().Format(time.RFC3339Nano)
	}
	observedAt := result.CompletedAt
	if observedAt.IsZero() {
		observedAt = result.StartedAt
	}
	observedAt = observedAt.UTC()

	opRows := make(map[string]map[string]any, len(result.Opportunities))
	pairRows := make(map[string]map[string]any, len(result.Opportunities))
	for _, op := range result.Opportunities {
		if strings.TrimSpace(op.OpportunityKey) == "" {
			continue
		}
		localExchange := strings.ToLower(strings.TrimSpace(op.LocalExchange))
		globalExchange := strings.ToLower(strings.TrimSpace(op.GlobalExchange))
		if localExchange == "" || globalExchange == "" {
			continue
		}
		at := op.LastScannedAt
		if at.IsZero() {
			at = observedAt
		}
		status := strings.TrimSpace(op.ProfitabilityStatus)
		if status == "" {
			if op.TradingEligible {
				status = "ELIGIBLE"
			} else {
				status = "EXCLUDED"
			}
		}
		payload, err := json.Marshal(op)
		if err != nil {
			log.Printf("skip malformed Supabase opportunity %s: %v", op.OpportunityKey, err)
			continue
		}
		id := stableUUID(scanID + ":" + op.OpportunityKey)
		reason := strings.Join(op.ExclusionReasons, "; ")
		opRows[id] = map[string]any{
			"id":                    id,
			"scan_id":               scanID,
			"pair_key":              op.OpportunityKey,
			"local_exchange":        localExchange,
			"global_exchange":       globalExchange,
			"local_symbol":          op.SpotSymbol,
			"global_symbol":         op.PerpSymbol,
			"quote_asset":           op.QuoteAsset,
			"execution_mode":        op.ExecutionMode,
			"eligibility_status":    status,
			"reason":                reason,
			"spot_price":            op.SpotEntryPrice,
			"perp_price":            op.PerpEntryPrice,
			"spread_pct":            op.EntrySpreadPct,
			"funding_rate":          op.CurrentFundingRate,
			"estimated_net_profit":  op.NetProfit,
			"estimated_net_roi_pct": op.NetROIPct,
			"executable_qty":        op.Quantity,
			"capacity_pct":          op.CapacityPct,
			"observed_at":           at.UTC().Format(time.RFC3339Nano),
			"payload":               json.RawMessage(payload),
		}
		localQuote := strings.ToUpper(strings.TrimSpace(op.OriginalSpotQuote))
		if localQuote == "" {
			localQuote = strings.ToUpper(strings.TrimSpace(op.QuoteAsset))
		}
		pairRows[op.OpportunityKey] = map[string]any{
			"pair_key":          op.OpportunityKey,
			"local_exchange":    localExchange,
			"global_exchange":   globalExchange,
			"local_symbol":      op.SpotSymbol,
			"global_symbol":     op.PerpSymbol,
			"base_asset":        op.BaseAsset,
			"local_quote_asset": localQuote,
			"global_quote_asset": strings.ToUpper(strings.TrimSpace(op.QuoteAsset)),
			"price_tick":        op.LocalPriceTick,
			"is_enabled":        true,
			"metadata": map[string]any{
				"last_scan_id": scanID,
				"execution_mode": op.ExecutionMode,
				"is_synthetic": op.IsSynthetic,
			},
		}
	}
	batch := scanBatch{
		opportunities: make([]map[string]any, 0, len(opRows)),
		marketPairs:   make([]map[string]any, 0, len(pairRows)),
	}
	for _, row := range opRows {
		batch.opportunities = append(batch.opportunities, row)
	}
	for _, row := range pairRows {
		batch.marketPairs = append(batch.marketPairs, row)
	}
	return batch
}

func stableUUID(value string) string {
	sum := sha256.Sum256([]byte(value))
	raw := sum[:16]
	raw[6] = (raw[6] & 0x0f) | 0x50
	raw[8] = (raw[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(raw)
	return hexValue[0:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" + hexValue[16:20] + "-" + hexValue[20:32]
}
