package supabase

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	defaultQueueSize = 64
	maxBatchRows     = 50
	maxAttempts      = 3
	requestTimeout   = 8 * time.Second
)

var urlNamespace = []byte{
	0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1,
	0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
}

type opportunityRow struct {
	ID                 string          `json:"id"`
	ScanID             string          `json:"scan_id"`
	PairKey            string          `json:"pair_key"`
	LocalExchange      string          `json:"local_exchange"`
	GlobalExchange     string          `json:"global_exchange"`
	LocalSymbol        string          `json:"local_symbol"`
	GlobalSymbol       string          `json:"global_symbol"`
	QuoteAsset         string          `json:"quote_asset"`
	ExecutionMode      string          `json:"execution_mode,omitempty"`
	EligibilityStatus  string          `json:"eligibility_status"`
	Reason             string          `json:"reason,omitempty"`
	SpotPrice          float64         `json:"spot_price"`
	PerpPrice          float64         `json:"perp_price"`
	SpreadPct          float64         `json:"spread_pct"`
	FundingRate        float64         `json:"funding_rate"`
	EstimatedNetProfit float64         `json:"estimated_net_profit"`
	EstimatedNetROIPct float64         `json:"estimated_net_roi_pct"`
	ExecutableQty      float64         `json:"executable_qty"`
	CapacityPct        float64         `json:"capacity_pct"`
	ObservedAt         time.Time       `json:"observed_at"`
	Payload            json.RawMessage `json:"payload"`
}

// Writer sends scanner observations to scanner_opportunities asynchronously.
// Database availability never gates market scanning or executor decisions.
type Writer struct {
	baseURL    string
	serviceKey string
	client     *http.Client
	queue      chan []opportunityRow
	stop       chan struct{}
	done       chan struct{}
	mu         sync.RWMutex
	closed     bool
	dropped    atomic.Uint64
}

func NewFromEnv() (*Writer, error) {
	baseURL := strings.TrimSpace(os.Getenv("SUPABASE_URL"))
	serviceKey := strings.TrimSpace(os.Getenv("SUPABASE_KEY"))
	if baseURL == "" && serviceKey == "" {
		return nil, nil
	}
	if baseURL == "" || serviceKey == "" {
		return nil, errors.New("both SUPABASE_URL and SUPABASE_KEY are required")
	}
	return NewWriter(baseURL, serviceKey, nil, defaultQueueSize)
}

func NewWriter(baseURL, serviceKey string, client *http.Client, queueSize int) (*Writer, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, errors.New("SUPABASE_URL must be a valid http(s) URL")
	}
	if strings.TrimSpace(serviceKey) == "" {
		return nil, errors.New("SUPABASE_KEY is required")
	}
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	if queueSize < 1 {
		queueSize = defaultQueueSize
	}
	w := &Writer{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		serviceKey: strings.TrimSpace(serviceKey),
		client: client,
		queue: make(chan []opportunityRow, queueSize),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go w.run()
	return w, nil
}

// EnqueueScan stores eligible and excluded candidates. A stable ID from scan_id
// and pair_key makes replayed observations safe for PostgREST upsert.
func (w *Writer) EnqueueScan(scan domain.ScanResult) {
	rows, err := rowsFromScan(scan)
	if err != nil {
		log.Printf("warning: Supabase opportunity payload encoding failed: %v", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return
	}
	select {
	case w.queue <- rows:
	default:
		dropped := w.dropped.Add(1)
		log.Printf("warning: Supabase opportunity queue full; dropped scan batch (total dropped batches=%d)", dropped)
	}
}

func (w *Writer) DroppedBatches() uint64 { return w.dropped.Load() }

func (w *Writer) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.stop)
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) run() {
	defer close(w.done)
	for {
		select {
		case rows := <-w.queue:
			w.flush(rows)
		case <-w.stop:
			for {
				select {
				case rows := <-w.queue:
					w.flush(rows)
				default:
					return
				}
			}
		}
	}
}

func (w *Writer) flush(rows []opportunityRow) {
	for start := 0; start < len(rows); start += maxBatchRows {
		end := start + maxBatchRows
		if end > len(rows) {
			end = len(rows)
		}
		if err := w.postBatch(rows[start:end]); err != nil {
			log.Printf("warning: Supabase scanner_opportunities write failed: %v", err)
		}
	}
}

func (w *Writer) postBatch(rows []opportunityRow) error {
	body, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}
	endpoint := w.baseURL + "/rest/v1/scanner_opportunities?on_conflict=id"
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		req, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if requestErr != nil {
			cancel()
			return fmt.Errorf("build request: %w", requestErr)
		}
		req.Header.Set("apikey", w.serviceKey)
		req.Header.Set("Authorization", "Bearer "+w.serviceKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "resolution=merge-duplicates,return=minimal")
		resp, requestErr := w.client.Do(req)
		if requestErr != nil {
			lastErr = requestErr
			cancel()
		} else {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
			_ = resp.Body.Close()
			status := resp.StatusCode
			cancel()
			if status >= http.StatusOK && status < http.StatusMultipleChoices {
				return nil
			}
			lastErr = fmt.Errorf("HTTP %d", status)
			if status < http.StatusInternalServerError && status != http.StatusTooManyRequests {
				return lastErr
			}
		}
		if attempt < maxAttempts {
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

func rowsFromScan(scan domain.ScanResult) ([]opportunityRow, error) {
	scanID := strings.TrimSpace(scan.ScanID)
	if scanID == "" {
		stamp := scan.StartedAt
		if stamp.IsZero() {
			stamp = scan.CompletedAt
		}
		if stamp.IsZero() {
			return nil, nil
		}
		scanID = stamp.UTC().Format(time.RFC3339Nano)
	}
	rows := make([]opportunityRow, 0, len(scan.Opportunities))
	for _, opportunity := range scan.Opportunities {
		pairKey := strings.TrimSpace(opportunity.OpportunityKey)
		if pairKey == "" {
			pairKey = strings.TrimSpace(opportunity.SpotSymbol + ":" + opportunity.PerpSymbol)
		}
		if pairKey == ":" || pairKey == "" {
			continue
		}
		status := strings.TrimSpace(opportunity.ProfitabilityStatus)
		if status == "" {
			if opportunity.TradingEligible {
				status = "ELIGIBLE"
			} else {
				status = "EXCLUDED"
			}
		}
		reason := strings.Join(opportunity.ExclusionReasons, "; ")
		if reason == "" && !opportunity.TradingEligible {
			reason = status
		}
		observedAt := opportunity.LastScannedAt
		if observedAt.IsZero() {
			observedAt = scan.CompletedAt
		}
		if observedAt.IsZero() {
			observedAt = scan.StartedAt
		}
		if observedAt.IsZero() {
			observedAt = time.Now().UTC()
		}
		payload, err := json.Marshal(map[string]any{
			"scan_id": scanID,
			"mode": scan.Mode,
			"complete": scan.Complete,
			"duration_ms": scan.DurationMS,
			"opportunity": opportunity,
		})
		if err != nil {
			return nil, fmt.Errorf("encode opportunity %q: %w", pairKey, err)
		}
		localExchange := strings.ToLower(strings.TrimSpace(opportunity.LocalExchange))
		if localExchange == "" {
			localExchange = exchangeFromSymbol(opportunity.SpotSymbol)
		}
		globalExchange := strings.ToLower(strings.TrimSpace(opportunity.GlobalExchange))
		if globalExchange == "" {
			globalExchange = exchangeFromSymbol(opportunity.PerpSymbol)
		}
		rows = append(rows, opportunityRow{
			ID: stableUUID(scanID, pairKey),
			ScanID: scanID,
			PairKey: pairKey,
			LocalExchange: localExchange,
			GlobalExchange: globalExchange,
			LocalSymbol: opportunity.SpotSymbol,
			GlobalSymbol: opportunity.PerpSymbol,
			QuoteAsset: opportunity.QuoteAsset,
			ExecutionMode: opportunity.ExecutionMode,
			EligibilityStatus: status,
			Reason: reason,
			SpotPrice: opportunity.SpotEntryPrice,
			PerpPrice: opportunity.PerpEntryPrice,
			SpreadPct: opportunity.EntrySpreadPct,
			FundingRate: opportunity.CurrentFundingRate,
			EstimatedNetProfit: opportunity.NetProfit,
			EstimatedNetROIPct: opportunity.NetROIPct,
			ExecutableQty: opportunity.Quantity,
			CapacityPct: opportunity.CapacityPct,
			ObservedAt: observedAt.UTC(),
			Payload: payload,
		})
	}
	return rows, nil
}

func exchangeFromSymbol(symbol string) string {
	prefix, _, found := strings.Cut(strings.TrimSpace(symbol), ":")
	if !found {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(prefix))
}

func stableUUID(scanID, pairKey string) string {
	h := sha1.New()
	_, _ = h.Write(urlNamespace)
	_, _ = h.Write([]byte(scanID + "\x00" + pairKey))
	value := h.Sum(nil)[:16]
	value[6] = (value[6] & 0x0f) | 0x50
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}
