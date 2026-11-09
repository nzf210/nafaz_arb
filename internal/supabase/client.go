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
		httpClient:     &http.Client{Timeout: 3 * time.Second},
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

		pairKey := localExchange + ":" + strings.ToLower(localSymbol)
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
	if strings.TrimSpace(scanID) == "" {
		return fmt.Errorf("scan_id is required for Supabase persistence")
	}
	if c.httpClient == nil {
		return fmt.Errorf("Supabase HTTP client is not initialized")
	}

	rows := buildRows(scanID, opportunities)
	if len(rows) == 0 {
		return nil
	}
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

// Close releases idle connections when the scanner shuts down.
func (c *Client) Close() {
	if c != nil && c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}
