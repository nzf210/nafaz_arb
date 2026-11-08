package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// Client writes durable scanner history through the Supabase PostgREST API.
// Redis remains the low-latency signal, cache and coordination layer.
type Client struct {
	baseURL string
	serviceRoleKey string
	httpClient *http.Client
}

func NewFromEnv() (*Client, error) {
	enabled := strings.TrimSpace(os.Getenv("SUPABASE_ENABLED"))
	if strings.EqualFold(enabled, "false") || enabled == "0" {
		return nil, nil
	}
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("SUPABASE_URL")), "/")
	key := strings.TrimSpace(os.Getenv("SUPABASE_SERVICE_ROLE_KEY"))
	if baseURL == "" && key == "" {
		return nil, nil
	}
	if baseURL == "" || key == "" {
		return nil, fmt.Errorf("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must both be set")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, fmt.Errorf("SUPABASE_URL must be an absolute http(s) URL")
	}
	return &Client{
		baseURL: baseURL,
		serviceRoleKey: key,
		httpClient: &http.Client{Timeout: 3 * time.Second},
	}, nil
}

type opportunityRow struct {
	ScanID string `json:"scan_id"`
	PairKey string `json:"pair_key"`
	LocalExchange string `json:"local_exchange"`
	GlobalExchange string `json:"global_exchange"`
	LocalSymbol string `json:"local_symbol"`
	GlobalSymbol string `json:"global_symbol"`
	QuoteAsset string `json:"quote_asset"`
	ExecutionMode string `json:"execution_mode,omitempty"`
	EligibilityStatus string `json:"eligibility_status"`
	Reason string `json:"reason,omitempty"`
	SpotPrice float64 `json:"spot_price"`
	PerpPrice float64 `json:"perp_price"`
	SpreadPct float64 `json:"spread_pct"`
	FundingRate float64 `json:"funding_rate"`
	EstimatedNetProfit float64 `json:"estimated_net_profit"`
	EstimatedNetROIPct float64 `json:"estimated_net_roi_pct"`
	ExecutableQty float64 `json:"executable_qty"`
	CapacityPct float64 `json:"capacity_pct"`
	ObservedAt time.Time `json:"observed_at"`
	Payload domain.Opportunity `json:"payload"`
}

func buildRows(scanID string, opportunities []domain.Opportunity) []opportunityRow {
	rows := make([]opportunityRow, 0, len(opportunities))
	for _, op := range opportunities {
		pairKey := strings.ToLower(strings.TrimSpace(op.LocalExchange)) + ":" + strings.ToLower(strings.TrimSpace(op.SpotSymbol))
		if pairKey == ":" || strings.TrimSpace(op.LocalExchange) == "" || strings.TrimSpace(op.GlobalExchange) == "" {
			continue
		}
		status := strings.TrimSpace(op.ProfitabilityStatus)
		if op.TradingEligible {
			status = "ELIGIBLE"
		} else if status == "" {
			status = "EXCLUDED"
		}
		quote := op.OriginalSpotQuote
		if quote == "" {
			quote = op.QuoteAsset
		}
		rows = append(rows, opportunityRow{
			ScanID: scanID, PairKey: pairKey,
			LocalExchange: op.LocalExchange, GlobalExchange: op.GlobalExchange,
			LocalSymbol: op.SpotSymbol, GlobalSymbol: op.PerpSymbol,
			QuoteAsset: quote, ExecutionMode: op.ExecutionMode,
			EligibilityStatus: status, Reason: strings.Join(op.ExclusionReasons, "; "),
			SpotPrice: op.SpotEntryPrice, PerpPrice: op.PerpEntryPrice,
			SpreadPct: op.EntrySpreadPct, FundingRate: op.CurrentFundingRate,
			EstimatedNetProfit: op.NetProfit, EstimatedNetROIPct: op.NetROIPct,
			ExecutableQty: op.Quantity, CapacityPct: op.CapacityPct,
			ObservedAt: op.LastScannedAt.UTC(), Payload: op,
		})
	}
	return rows
}

// SaveOpportunities performs an idempotent upsert keyed by (scan_id, pair_key).
// It retries transient transport/server failures but never affects scan eligibility
// or the Redis signal path.
func (c *Client) SaveOpportunities(ctx context.Context, scanID string, opportunities []domain.Opportunity) error {
	if c == nil || len(opportunities) == 0 {
		return nil
	}
	if strings.TrimSpace(scanID) == "" {
		return fmt.Errorf("scan_id is required for Supabase persistence")
	}
	rows := buildRows(scanID, opportunities)
	if len(rows) == 0 {
		return nil
	}
	body, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("encode scanner opportunities: %w", err)
	}
	endpoint := c.baseURL + "/rest/v1/scanner_opportunities?on_conflict=scan_id%2Cpair_key"
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
		if doErr == nil {
			responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("Supabase scanner_opportunities returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
			if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
				return lastErr
			}
		} else {
			lastErr = fmt.Errorf("Supabase scanner_opportunities request: %w", doErr)
		}
		if attempt < 2 {
			delay := time.Duration(100*(1<<attempt)) * time.Millisecond
			timer := time.NewTimer(delay)
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
