package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"os"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// Client writes durable scanner history through the Supabase PostgREST API.
// Redis remains the low-latency signal, cache and coordination layer.
type Client struct {
	baseURL        string
	serviceRoleKey string
	httpClient     *http.Client
}

// NewFromEnv returns nil when Supabase credentials are not configured. The
// existing SUPABASE_KEY variable is shared with the executor and must contain
// the server-only service_role key, never an anon/publishable key.
func NewFromEnv() (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("SUPABASE_URL")), "/")
	key := strings.TrimSpace(os.Getenv("SUPABASE_KEY"))
	if baseURL == "" && key == "" {
		return nil, nil
	}
	if baseURL == "" || key == "" {
		return nil, fmt.Errorf("SUPABASE_URL and SUPABASE_KEY must both be set")
	}

	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("SUPABASE_URL must be an absolute HTTPS URL")
	}
	if parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopbackHost(parsed.Hostname())) {
		return nil, fmt.Errorf("SUPABASE_URL must use HTTPS (HTTP is allowed only for loopback tests)")
	}

	return &Client{
		baseURL:        baseURL,
		serviceRoleKey: key,
		httpClient:     &http.Client{Timeout: 15 * time.Second},
	}, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type opportunityRow struct {
	ScanID             string              `json:"scan_id"`
	PairKey            string              `json:"pair_key"`
	IdempotencyKey     string              `json:"idempotency_key"`
	LocalExchange      string              `json:"local_exchange"`
	GlobalExchange     string              `json:"global_exchange"`
	LocalSymbol        string              `json:"local_symbol"`
	GlobalSymbol       string              `json:"global_symbol"`
	QuoteAsset         string              `json:"quote_asset"`
	ExecutionMode      string              `json:"execution_mode,omitempty"`
	EligibilityStatus  string              `json:"eligibility_status"`
	Reason             string              `json:"reason,omitempty"`
	SpotPrice          float64             `json:"spot_price"`
	PerpPrice          float64             `json:"perp_price"`
	SpreadPct          float64             `json:"spread_pct"`
	FundingRate        float64             `json:"funding_rate"`
	EstimatedNetProfit float64             `json:"estimated_net_profit"`
	EstimatedNetROIPct float64             `json:"estimated_net_roi_pct"`
	ExecutableQty      float64             `json:"executable_qty"`
	CapacityPct        float64             `json:"capacity_pct"`
	ObservedAt         time.Time           `json:"observed_at"`
	Payload            domain.Opportunity `json:"payload"`
}

func buildRows(scanID string, opportunities []domain.Opportunity) []opportunityRow {
	rows := make([]opportunityRow, 0, len(opportunities))
	for _, op := range opportunities {
		localExchange := strings.ToLower(strings.TrimSpace(op.LocalExchange))
		globalExchange := strings.ToLower(strings.TrimSpace(op.GlobalExchange))
		localSymbol := strings.TrimSpace(op.SpotSymbol)
		globalSymbol := strings.TrimSpace(op.PerpSymbol)
		if localExchange == "" || globalExchange == "" || localSymbol == "" || globalSymbol == "" {
			continue
		}

		pairKey := localExchange + ":" + strings.ToLower(localSymbol) + "->" + globalExchange + ":" + strings.ToLower(globalSymbol)
		status := strings.TrimSpace(op.ProfitabilityStatus)
		if op.TradingEligible {
			status = "ELIGIBLE"
		} else if status == "" {
			status = "EXCLUDED"
		}
		quote := strings.TrimSpace(op.OriginalSpotQuote)
		if quote == "" {
			quote = strings.TrimSpace(op.QuoteAsset)
		}
		observedAt := op.LastScannedAt.UTC()
		if op.LastScannedAt.IsZero() {
			observedAt = time.Now().UTC()
		}

		rows = append(rows, opportunityRow{
			ScanID:             scanID,
			PairKey:            pairKey,
			IdempotencyKey:     scanID + ":" + pairKey,
			LocalExchange:      op.LocalExchange,
			GlobalExchange:     op.GlobalExchange,
			LocalSymbol:        localSymbol,
			GlobalSymbol:       globalSymbol,
			QuoteAsset:         strings.ToUpper(quote),
			ExecutionMode:      op.ExecutionMode,
			EligibilityStatus:  status,
			Reason:             strings.Join(op.ExclusionReasons, "; "),
			SpotPrice:          op.SpotEntryPrice,
			PerpPrice:          op.PerpEntryPrice,
			SpreadPct:          op.EntrySpreadPct,
			FundingRate:        op.CurrentFundingRate,
			EstimatedNetProfit: op.NetProfit,
			EstimatedNetROIPct: op.NetROIPct,
			ExecutableQty:      op.Quantity,
			CapacityPct:        op.CapacityPct,
			ObservedAt:         observedAt,
			Payload:            op,
		})
	}
	return rows
}

// SaveOpportunities upserts by a deterministic idempotency_key derived from
// scanID and pairKey. Transient HTTP/network failures are retried, while
// permanent schema/authentication errors are returned immediately.
func (c *Client) SaveOpportunities(ctx context.Context, scanID string, opportunities []domain.Opportunity) error {
	if c == nil || len(opportunities) == 0 {
		return nil
	}
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return fmt.Errorf("scan_id is required for Supabase persistence")
	}
	if c.httpClient == nil {
		return fmt.Errorf("Supabase HTTP client is not initialized")
	}

	return c.saveRows(ctx, buildRows(scanID, opportunities))
}

// saveRows is shared by direct callers and the durable outbox reconciliation
// worker. The worker replays the exact serialized row payload that was journaled.
func (c *Client) saveRows(ctx context.Context, rows []opportunityRow) error {
	if c == nil || len(rows) == 0 {
		return nil
	}
	if c.httpClient == nil {
		return fmt.Errorf("Supabase HTTP client is not initialized")
	}
	batchCount := (len(rows) + maxBatchSize - 1) / maxBatchSize
	for start := 0; start < len(rows); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := c.saveBatch(ctx, rows[start:end]); err != nil {
			batchNumber := start/maxBatchSize + 1
			return fmt.Errorf("persist scanner opportunity batch %d/%d: %w", batchNumber, batchCount, err)
		}
	}
	return nil
}

const maxBatchSize = 100

func (c *Client) saveBatch(ctx context.Context, rows []opportunityRow) error {
	body, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("encode scanner opportunities: %w", err)
	}

	endpoint := c.baseURL + "/rest/v1/scanner_opportunities?on_conflict=idempotency_key"
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if reqErr != nil {
			return reqErr
		}
		req.Header.Set("apikey", c.serviceRoleKey)
		req.Header.Set("Authorization", "Bearer "+c.serviceRoleKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "resolution=merge-duplicates,return=minimal")

		resp, doErr := c.httpClient.Do(req)
		if doErr != nil {
			lastErr = fmt.Errorf("Supabase scanner_opportunities request: %w", doErr)
			if ctx.Err() != nil {
				return ctx.Err()
			}
		} else {
			responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf(
				"Supabase scanner_opportunities returned HTTP %d: %s",
				resp.StatusCode,
				strings.TrimSpace(string(responseBody)),
			)
			retryable := resp.StatusCode == http.StatusRequestTimeout ||
				resp.StatusCode == http.StatusTooManyRequests ||
				resp.StatusCode >= http.StatusInternalServerError
			if !retryable {
				return lastErr
			}
		}

		if attempt < 2 {
			timer := time.NewTimer(time.Duration(100*(1<<attempt)) * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}


// readScannerHistory performs one GET against scanner_opportunities with a
// bounded retry: transient network stalls previously surfaced as Telegram
// history failures even though writes kept flowing through the outbox.
func (c *Client) readScannerHistory(ctx context.Context, endpoint string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("create scanner history request: %w", err)
		}
		req.Header.Set("apikey", c.serviceRoleKey)
		req.Header.Set("Authorization", "Bearer "+c.serviceRoleKey)
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("read scanner opportunity history: %w", err)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		} else {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if readErr != nil {
					return nil, fmt.Errorf("read scanner opportunity history body: %w", readErr)
				}
				return body, nil
			}
			lastErr = fmt.Errorf("read scanner opportunity history returned HTTP %d", resp.StatusCode)
			retryable := resp.StatusCode == http.StatusRequestTimeout ||
				resp.StatusCode == http.StatusTooManyRequests ||
				resp.StatusCode >= http.StatusInternalServerError
			if !retryable {
				return nil, lastErr
			}
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(100*(1<<attempt)) * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, lastErr
}

// LatestOpportunityScans returns opportunities grouped by the newest distinct
// scan IDs. It pages through PostgREST so the project's max-rows setting does
// not silently reduce the requested scan history to a single page.
func (c *Client) LatestOpportunityScans(ctx context.Context, scanLimit int) ([]domain.ScanResult, error) {
	if c == nil || c.httpClient == nil {
		return nil, fmt.Errorf("Supabase opportunity history is not configured")
	}
	if scanLimit <= 0 {
		scanLimit = 10
	}
	const pageSize = 1000
	const maxPages = 100
	type storedRow struct {
		ScanID     string             `json:"scan_id"`
		ObservedAt time.Time          `json:"observed_at"`
		Payload    domain.Opportunity `json:"payload"`
	}
	byScan := make(map[string]*domain.ScanResult, scanLimit+1)
	order := make([]string, 0, scanLimit+1)
	windowComplete := false
	for page := 0; page < maxPages && !windowComplete; page++ {
		query := url.Values{}
		query.Set("select", "scan_id,observed_at,payload")
		query.Set("scan_id", "not.is.null")
		query.Set("order", "observed_at.desc,scan_id.desc")
		query.Set("limit", fmt.Sprint(pageSize))
		query.Set("offset", fmt.Sprint(page*pageSize))
		endpoint := c.baseURL + "/rest/v1/scanner_opportunities?" + query.Encode()
		body, err := c.readScannerHistory(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		var rows []storedRow
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("decode scanner opportunity history: %w", err)
		}
		for _, row := range rows {
			if strings.TrimSpace(row.ScanID) == "" {
				continue
			}
			result, exists := byScan[row.ScanID]
			if !exists {
				if len(order) == scanLimit {
					// The first row of the next scan proves the requested
					// window is complete; all rows for the prior scan were read.
					windowComplete = true
					break
				}
				result = &domain.ScanResult{
					ScanID: row.ScanID, StartedAt: row.ObservedAt,
					CompletedAt: row.ObservedAt, Opportunities: []domain.Opportunity{},
				}
				byScan[row.ScanID] = result
				order = append(order, row.ScanID)
			}
			if row.ObservedAt.After(result.CompletedAt) {
				result.CompletedAt = row.ObservedAt
			}
			if row.Payload.LastScannedAt.IsZero() {
				row.Payload.LastScannedAt = row.ObservedAt
			}
			result.Opportunities = append(result.Opportunities, row.Payload)
		}
		if len(rows) < pageSize {
			break
		}
	}
	out := make([]domain.ScanResult, 0, len(order))
	for _, scanID := range order {
		result := *byScan[scanID]
		sort.Slice(result.Opportunities, func(i, j int) bool {
			return result.Opportunities[i].NetROIPct > result.Opportunities[j].NetROIPct
		})
		result.MatchedPairs = len(result.Opportunities)
		result.EligiblePairs = 0
		result.ExcludedPairs = 0
		for _, op := range result.Opportunities {
			if op.TradingEligible {
				result.EligiblePairs++
			} else {
				result.ExcludedPairs++
			}
		}
		out = append(out, result)
	}
	return out, nil
}

// Close releases idle connections when the scanner shuts down.
func (c *Client) Close() {
	if c != nil && c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}
