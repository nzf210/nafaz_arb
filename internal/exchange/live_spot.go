package exchange

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// USDTIDRRate retrieves the current live USDT/IDR exchange rate from Indodax ticker with caching.
func (l *LiveAdapter) USDTIDRRate(ctx context.Context) (float64, error) {
	l.mu.RLock()
	if l.lastUSDTIDRRate > 0 && time.Since(l.lastUSDTIDRTime) < 60*time.Second {
		rate := l.lastUSDTIDRRate
		l.mu.RUnlock()
		return rate, nil
	}
	client := l.indodaxClient
	l.mu.RUnlock()
	if client == nil {
		return 0, fmt.Errorf("indodax client is not configured")
	}
	rate, err := client.USDTIDRRate(ctx)
	if err != nil {
		return 0, err
	}
	l.mu.Lock()
	l.lastUSDTIDRRate = rate
	l.lastUSDTIDRTime = time.Now().UTC()
	l.mu.Unlock()
	return rate, nil
}

// SpotMarkets discovers active spot pairs on Tokocrypto and Indodax (and MEXC Spot if target is mexc/all).
func (l *LiveAdapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	var allMarkets []domain.Market
	var errs []string

	tokoMarkets, err := l.fetchTokocryptoSpotMarkets(ctx)
	if err == nil {
		allMarkets = append(allMarkets, tokoMarkets...)
		l.SubscribeTokocryptoBooks(tokoMarkets)
	} else {
		errs = append(errs, fmt.Sprintf("toko: %v", err))
	}

	indodaxMarkets, err := l.fetchIndodaxSpotMarkets(ctx)
	if err == nil {
		allMarkets = append(allMarkets, indodaxMarkets...)
	} else {
		errs = append(errs, fmt.Sprintf("indodax: %v", err))
	}


	if len(allMarkets) == 0 {
		return nil, fmt.Errorf("failed to discover spot markets (%s)", strings.Join(errs, "; "))
	}

	return allMarkets, nil
}

func (l *LiveAdapter) fetchTokocryptoSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	l.mu.RLock()
	if len(l.cachedTokocryptoSpot) > 0 && time.Since(l.lastTokocryptoFetch) < time.Hour {
		res := l.cachedTokocryptoSpot
		l.mu.RUnlock()
		return res, nil
	}
	client := l.tokocryptoClient
	l.mu.RUnlock()
	if client == nil { return l.getFallbackTokocryptoSpot(fmt.Errorf("tokocrypto client is not configured")) }
	markets, err := client.Markets(ctx)
	if err != nil { return l.getFallbackTokocryptoSpot(err) }
	if len(markets) > 0 {
		l.mu.Lock()
		l.cachedTokocryptoSpot = markets
		l.lastTokocryptoFetch = time.Now().UTC()
		l.mu.Unlock()
	}
	return markets, nil
}

func (l *LiveAdapter) getFallbackTokocryptoSpot(err error) ([]domain.Market, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedTokocryptoSpot) > 0 {
		return l.cachedTokocryptoSpot, nil
	}
	return nil, err
}

func (l *LiveAdapter) fetchIndodaxSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	l.mu.RLock()
	if len(l.cachedIndodaxSpot) > 0 && time.Since(l.lastIndodaxSpotFetch) < time.Hour {
		res := l.cachedIndodaxSpot
		l.mu.RUnlock()
		return res, nil
	}
	client := l.indodaxClient
	l.mu.RUnlock()
	if client == nil { return l.getFallbackIndodaxSpot(fmt.Errorf("indodax client is not configured")) }
	markets, err := client.Markets(ctx)
	if err != nil { return l.getFallbackIndodaxSpot(err) }
	if len(markets) > 0 {
		l.mu.Lock()
		l.cachedIndodaxSpot = markets
		l.lastIndodaxSpotFetch = time.Now().UTC()
		l.mu.Unlock()
	}
	return markets, nil
}

func (l *LiveAdapter) getFallbackIndodaxSpot(err error) ([]domain.Market, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedIndodaxSpot) > 0 {
		return l.cachedIndodaxSpot, nil
	}
	return nil, err
}


// SubscribeTokocryptoBooks dynamically tracks active MBX spot symbols.
func (l *LiveAdapter) SubscribeTokocryptoBooks(markets []domain.Market) {
	l.mu.RLock(); supervisor := l.tokocryptoSupervisor; l.mu.RUnlock()
	if supervisor == nil { return }
	symbols := make([]string,0,len(markets))
	for _, market := range markets { if strings.EqualFold(market.Exchange,"tokocrypto") && market.Active { symbols=append(symbols,strings.TrimPrefix(market.Symbol,"toko:")) } }
	supervisor.SetSymbols(sortedUniqueSymbols(symbols))
}
