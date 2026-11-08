package scanner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/shopspring/decimal"
)

func (s *Service) fetchMarketData(ctx context.Context, sm, pm domain.Market) (domain.OrderBook, domain.OrderBook, domain.FundingRate, []domain.ExchangeScanMetrics, error) {
	metrics := make(map[string]domain.ExchangeScanMetrics, 2)
	measure := func(exchangeName string, started time.Time, err error) {
		metric := metrics[exchangeName]
		metric.Exchange = exchangeName
		metric.Requests++
		if err != nil {
			metric.Errors++
		}
		metric.DurationMicros += time.Since(started).Microseconds()
		metrics[exchangeName] = metric
	}
	requestStarted := time.Now()
	sb, e1 := s.adapter.SpotOrderBook(ctx, sm.Symbol)
	measure(sm.Exchange, requestStarted, e1)
	requestStarted = time.Now()
	pb, e2 := s.adapter.PerpetualOrderBook(ctx, pm.Symbol)
	measure(pm.Exchange, requestStarted, e2)
	requestStarted = time.Now()
	var fr domain.FundingRate
	var e3 error
	s.mu.Lock()
	fw := s.fundingWorker
	s.mu.Unlock()
	if fw != nil {
		if info, ok := fw.GetFundingInfo(pm.Symbol); ok {
			fr = domain.FundingRate{
				Rate:            info.LastFundingRate,
				IntervalHours:   8,
				Timestamp:       info.LastUpdated,
				NextFundingTime: info.NextFundingTime,
			}
		} else {
			fr, e3 = s.adapter.FundingRate(ctx, pm.Symbol)
		}
	} else {
		fr, e3 = s.adapter.FundingRate(ctx, pm.Symbol)
	}
	measure(pm.Exchange, requestStarted, e3)
	exchangeMetrics := make([]domain.ExchangeScanMetrics, 0, len(metrics))
	for _, metric := range metrics {
		exchangeMetrics = append(exchangeMetrics, metric)
	}
	if e1 != nil || e2 != nil || e3 != nil {
		// Join rather than stringify so callers can still detect a venue-wide
		// outage with errors.Is(err, exchange.ErrExchangeUnavailable).
		errs := make([]error, 0, 3)
		if e1 != nil {
			errs = append(errs, fmt.Errorf("spot_book: %w", e1))
		}
		if e2 != nil {
			errs = append(errs, fmt.Errorf("perp_book: %w", e2))
		}
		if e3 != nil {
			errs = append(errs, fmt.Errorf("funding: %w", e3))
		}
		return domain.OrderBook{}, domain.OrderBook{}, domain.FundingRate{}, exchangeMetrics, errors.Join(errs...)
	}
	return sb, pb, fr, exchangeMetrics, nil
}

// getUSDTIDRRate returns the rate and whether it came from a live ticker.
// There is deliberately no fallback: a hardcoded or synthetic USDT/IDR rate
// can silently diverge from the real market and destroy arbitrage profit, so
// callers must skip IDR pairs whenever the live ticker is unavailable.
func (s *Service) getUSDTIDRRate(ctx context.Context) (float64, bool) {
	if rp, ok := s.adapter.(exchange.RateProvider); ok {
		rate, err := rp.USDTIDRRate(ctx)
		if err == nil && rate > 0 && !math.IsNaN(rate) && !math.IsInf(rate, 0) {
			s.mu.Lock()
			s.lastLiveFXRate = rate
			s.mu.Unlock()
			return rate, true
		}
	}
	return 0.0, false
}

func containsIDRSpotPair(pairs [][2]domain.Market) bool {
	for _, pair := range pairs {
		quote := strings.ToUpper(strings.TrimSpace(pair[0].QuoteAsset))
		if quote == "IDR" || quote == "BIDR" {
			return true
		}
	}
	return false
}

// lastKnownFXRate returns the most recent live rate observed this process, or
// 0 when none was ever seen. Used only for USDT-quote metadata where the rate
// is informational; it is never used to price IDR books.
func (s *Service) lastKnownFXRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastLiveFXRate
}

func normalizeOrderBookCurrency(b domain.OrderBook, fxRate float64) domain.OrderBook {
	if fxRate <= 0 || fxRate == 1.0 {
		return b
	}
	out := domain.OrderBook{
		Exchange:  b.Exchange,
		Symbol:    b.Symbol,
		Timestamp: b.Timestamp,
		Bids:      make([]domain.PriceLevel, len(b.Bids)),
		Asks:      make([]domain.PriceLevel, len(b.Asks)),
	}
	for i, l := range b.Bids {
		out.Bids[i] = domain.PriceLevel{
			Price:    l.Price / fxRate,
			Quantity: l.Quantity,
		}
	}
	for i, l := range b.Asks {
		out.Asks[i] = domain.PriceLevel{
			Price:    l.Price / fxRate,
			Quantity: l.Quantity,
		}
	}
	return out
}

func availableBookDepth(levels []domain.PriceLevel) decimal.Decimal {
	sum := decimal.Zero
	zero := decimal.Zero
	for _, l := range levels {
		p := decimal.NewFromFloat(l.Price)
		q := decimal.NewFromFloat(l.Quantity)
		if p.GreaterThan(zero) && q.GreaterThan(zero) {
			sum = sum.Add(q)
		}
	}
	return sum
}
