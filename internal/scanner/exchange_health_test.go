package scanner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
)

// unavailableAdapter pretends one exchange is under maintenance and counts how
// often the scanner still asks it for data.
type unavailableAdapter struct {
	exchange.MockAdapter
	down      map[string]bool
	spotCalls int
}

func (u *unavailableAdapter) ExchangeAvailable(name string) bool {
	return !u.down[name]
}

func (u *unavailableAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	u.spotCalls++
	return u.MockAdapter.SpotOrderBook(ctx, symbol)
}

func TestScanSkipsPairsOnUnavailableExchange(t *testing.T) {
	cfg := config.Config{
		Mode:              "MOCK",
		QuoteAssets:       []string{"USDT"},
		CapitalTotal:      1000,
		AllocationPerPair: 0.5,
		PerpLeverage:      1,
	}
	adapter := &unavailableAdapter{
		MockAdapter: *exchange.NewMockAdapter(),
		down:        map[string]bool{"indodax": true},
	}
	s := New(cfg, adapter)

	result, _, _ := s.Scan(context.Background())
	if adapter.spotCalls != 0 {
		t.Fatalf("must not fetch order books for a downed exchange, got %d calls", adapter.spotCalls)
	}

	skipped := 0
	for _, e := range result.Errors {
		if strings.Contains(e, "unavailable (maintenance)") {
			skipped++
		}
	}
	if skipped == 0 {
		t.Fatalf("expected maintenance skip messages, got errors: %v", result.Errors)
	}
}

// maintenanceAdapter fails every perp book with ErrExchangeUnavailable, as the
// live adapter does once the error-rate breaker trips mid-scan.
type maintenanceAdapter struct {
	exchange.MockAdapter
}

func (m *maintenanceAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	return domain.OrderBook{}, errors.Join(exchange.ErrExchangeUnavailable, errors.New("okx depth status 503"))
}

func TestScanDoesNotFailCompletelyWhenExchangeGoesDownMidScan(t *testing.T) {
	cfg := config.Config{
		Mode:              "MOCK",
		QuoteAssets:       []string{"USDT"},
		CapitalTotal:      1000,
		AllocationPerPair: 0.5,
		PerpLeverage:      1,
	}
	s := New(cfg, &maintenanceAdapter{MockAdapter: *exchange.NewMockAdapter()})

	result, complete, _ := s.Scan(context.Background())
	if !complete {
		t.Fatalf("a single venue outage must not mark the whole scan incomplete")
	}
	if len(result.Errors) == 0 {
		t.Fatalf("expected the maintenance failures to be reported")
	}
}

// healthyAdapter keeps the exported interface assertion honest.
type healthyAdapter struct {
	exchange.MockAdapter
}

func (h *healthyAdapter) ExchangeAvailable(string) bool { return true }

func TestAvailabilityReporterIsOptional(t *testing.T) {
	if _, ok := interface{}(&healthyAdapter{}).(availabilityReporter); !ok {
		t.Fatalf("healthyAdapter must satisfy availabilityReporter")
	}
	// The plain mock adapter does not report availability; the scanner must
	// simply skip the check instead of failing.
	var adapter exchange.ReadOnlyAdapter = exchange.NewMockAdapter()
	if _, ok := adapter.(availabilityReporter); ok {
		t.Fatalf("mock adapter unexpectedly implements availabilityReporter")
	}
}

// suspendedWalletAdapter reports one venue with a suspended wallet (P2-05).
type suspendedWalletAdapter struct {
	exchange.MockAdapter
}

func (m *suspendedWalletAdapter) AccountAvailable(exchangeName string) (exchange.AccountStatus, error) {
	if exchangeName == "indodax" {
		return exchange.AccountStatus{DepositEnabled: true, WithdrawalEnabled: false}, exchange.ErrWalletSuspended
	}
	return exchange.AccountStatus{DepositEnabled: true, WithdrawalEnabled: true}, nil
}

func TestWalletSuspendedSkipsPairsOnThatVenue(t *testing.T) {
	cfg := config.Config{
		Mode:              "MOCK",
		QuoteAssets:       []string{"USDT"},
		CapitalTotal:      1000,
		AllocationPerPair: 0.5,
		PerpLeverage:      1,
	}
	adapter := &suspendedWalletAdapter{MockAdapter: *exchange.NewMockAdapter()}
	s := New(cfg, adapter)

	result, complete, err := s.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}
	_ = complete
	// Pairs involving the suspended venue must be skipped with a reason.
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e, "ErrWalletSuspended") || strings.Contains(e, "wallet suspended") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a wallet-suspended skip message, errors: %v", result.Errors)
	}
}

func TestAccountStatusReporterIsOptional(t *testing.T) {
	var adapter exchange.ReadOnlyAdapter = exchange.NewMockAdapter()
	if _, ok := adapter.(exchange.AccountStatusReporter); ok {
		t.Fatalf("mock adapter unexpectedly implements AccountStatusReporter")
	}
	// LiveAdapter must implement it.
	var live exchange.ReadOnlyAdapter = exchange.NewLiveAdapter()
	if _, ok := live.(exchange.AccountStatusReporter); !ok {
		t.Fatalf("LiveAdapter must implement AccountStatusReporter")
	}
}
