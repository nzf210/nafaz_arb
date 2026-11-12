package exchange

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/indodax"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/mexc"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/okx"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/tokocrypto"
)

const (
	MEXC_SPOT        = "mexc_spot:"
	USER_AGENT       = "User-Agent"
	USER_AGENT_VALUE = "Mozilla/5.0 (compatible; NafazScanner/1.0)"
)

type LiveAdapter struct {
	client            *http.Client
	indodaxClient     *indodax.Client
	tokocryptoClient  *tokocrypto.Client
	mexcClient        *mexc.Client
	okxClient         *okx.Client
	indodaxBaseURL    string
	tokocryptoBaseURL string
	okxBaseURL        string
	mexcBaseURL       string
	mexcSpotBaseURL   string

	mu                 sync.RWMutex
	contractSizes      map[string]float64
	lastUSDTIDRRate    float64
	lastUSDTIDRTime    time.Time
	targetPerpExchange string

	cachedIndodaxSpot    []domain.Market
	lastIndodaxSpotFetch time.Time
	cachedTokocryptoSpot []domain.Market
	lastTokocryptoFetch  time.Time
	cachedOKXPerp        []domain.Market
	lastOKXPerpFetch     time.Time

	health *availability
	// walletStatuses caches per-venue deposit/withdrawal state (P2-05).
	walletStatuses map[string]walletStatusEntry

	// wsCache is populated only when EnableWebSocketFeed is on; nil otherwise so
	// the REST path stays byte-for-byte unchanged by default.
	wsCache       *WSBookCache
	wsMaxAge      time.Duration
	wsClient      *WSClient
	wsStopCleanup func()

	// okxLimiter is the process-wide gate for OKX public REST.
	okxLimiter     *okxLimiter
	okxLimiterOnce sync.Once
	okxLimiterCfg  OKXLimiterConfig
	okxBooks       *OKXBookManager // compatibility for existing in-package callers/tests
	okxVenueBooks  *okx.BookManager
	okxWSMaxAge    time.Duration
	okxBookCalls   map[string]*okxBookCall
	tokocryptoBooks *tokocrypto.BookManager
	mexcPerpBooks *mexc.PerpetualBookManager
	venueWSMaxAge time.Duration
	tokocryptoSupervisor *venueBookSupervisor
	mexcPerpSupervisor *venueBookSupervisor
}

func NewLiveAdapter() *LiveAdapter {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	tr := &http.Transport{
		// Dial the resolved IP but verify the certificate against the real
		// hostname: without ServerName a TLS handshake against an IP address
		// fails verification, which is why InsecureSkipVerify was a trap here.
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				host = addr
			}
			resolvedIP := resolveHost(ctx, host)
			rawConn, err := dialer.DialContext(ctx, network, net.JoinHostPort(resolvedIP, port))
			if err != nil {
				return nil, err
			}
			tlsConn := tls.Client(rawConn, &tls.Config{ServerName: host})
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				rawConn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return dialer.DialContext(ctx, network, addr)
			}
			resolvedIP := resolveHost(ctx, host)
			return dialer.DialContext(ctx, network, net.JoinHostPort(resolvedIP, port))
		},
	}
	httpClient := &http.Client{Transport: tr, Timeout: 15 * time.Second}
	adapter := &LiveAdapter{
		health:            newAvailability(HealthPolicy{}, nil),
		walletStatuses:    make(map[string]walletStatusEntry),
		client:            httpClient,
		indodaxBaseURL:    "https://indodax.com",
		tokocryptoBaseURL: "https://www.tokocrypto.com",
		okxBaseURL:        "https://www.okx.com",
		mexcBaseURL:       "https://contract.mexc.com",
		mexcSpotBaseURL:   "https://api.mexc.com",
		contractSizes:     make(map[string]float64),
		okxBookCalls:      make(map[string]*okxBookCall),
	}
	adapter.indodaxClient = indodax.New(httpClient, adapter.indodaxBaseURL)
	adapter.tokocryptoClient = tokocrypto.New(httpClient, adapter.tokocryptoBaseURL)
	adapter.mexcClient = mexc.New(httpClient, adapter.mexcBaseURL, adapter.mexcSpotBaseURL)
	adapter.okxClient = okx.New(httpClient, adapter.okxBaseURL)
	return adapter
}

// SetOKXLimiterConfig installs shared-limiter settings. Call before the first
// OKX request; settings after the limiter has been created have no effect.
func (l *LiveAdapter) SetOKXLimiterConfig(cfg OKXLimiterConfig) {
	l.okxLimiterCfg = cfg
	l.mu.RLock(); client := l.okxClient; l.mu.RUnlock()
	if client != nil { client.SetLimiterConfig(okx.OKXLimiterConfig{DefaultRatePer2s:cfg.DefaultRatePer2s,BooksRatePer2s:cfg.BooksRatePer2s,InstrumentsRatePer2s:cfg.InstrumentsRatePer2s,FundingRatePer2s:cfg.FundingRatePer2s,MaxConcurrent:cfg.MaxConcurrent}) }
}

// OKXMetrics returns the OKX rate-limiter counters for observability. It
// returns an empty snapshot when no OKX request has been made yet, so callers
// never see a nil map.
func (l *LiveAdapter) OKXMetrics() okx.OKXMetricsSnapshot {
	l.mu.RLock(); client := l.okxClient; l.mu.RUnlock()
	if client == nil { return okx.OKXMetricsSnapshot{Endpoints:map[string]okx.OKXEndpointMetrics{}} }
	return client.Metrics()
}


// SetBaseURLs allows overriding API base URLs for testing.
func (l *LiveAdapter) SetBaseURLs(urls ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(urls) > 0 && urls[0] != "" {
		l.indodaxBaseURL = urls[0]
		l.indodaxClient = indodax.New(l.client, l.indodaxBaseURL)
	}
	if len(urls) > 1 && urls[1] != "" {
		l.okxBaseURL = urls[1]
		l.okxClient = okx.New(l.client, l.okxBaseURL)
		if l.okxLimiterCfg != (OKXLimiterConfig{}) { l.okxClient.SetLimiterConfig(okx.OKXLimiterConfig{DefaultRatePer2s:l.okxLimiterCfg.DefaultRatePer2s,BooksRatePer2s:l.okxLimiterCfg.BooksRatePer2s,InstrumentsRatePer2s:l.okxLimiterCfg.InstrumentsRatePer2s,FundingRatePer2s:l.okxLimiterCfg.FundingRatePer2s,MaxConcurrent:l.okxLimiterCfg.MaxConcurrent}) }
	}
	if len(urls) > 2 && urls[2] != "" {
		l.mexcBaseURL = urls[2]
		l.mexcClient = mexc.New(l.client, l.mexcBaseURL, l.mexcSpotBaseURL)
	}
	if len(urls) > 3 && urls[3] != "" {
		l.mexcSpotBaseURL = urls[3]
		l.mexcClient = mexc.New(l.client, l.mexcBaseURL, l.mexcSpotBaseURL)
	}
	if len(urls) > 4 && urls[4] != "" {
		l.tokocryptoBaseURL = urls[4]
		l.tokocryptoClient = tokocrypto.New(l.client, l.tokocryptoBaseURL)
	}
}

// SetHealthPolicy configures exchange maintenance detection. A zero window or
// threshold disables it.
func (l *LiveAdapter) SetHealthPolicy(policy HealthPolicy) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.health == nil {
		l.health = newAvailability(policy, nil)
		return
	}
	l.health = newAvailability(policy, l.health.now)
}

// ExchangeAvailable reports whether the exchange is currently believed to be up.
// Adapters without maintenance detection always report true.
func (l *LiveAdapter) ExchangeAvailable(exchange string) bool {
	l.mu.RLock()
	health := l.health
	l.mu.RUnlock()
	if health == nil {
		return true
	}
	return !health.unavailable(exchange)
}

func (l *LiveAdapter) recordOutcome(exchange string, err error) {
	l.mu.RLock()
	health := l.health
	l.mu.RUnlock()
	if health != nil {
		health.record(exchange, err != nil)
	}
}

// walletStatuses caches the most recent wallet state per venue so the scanner
// hot loop does not hit the status endpoint every scan. A stale-but-known
// result is preferred over hammering the venue; entries expire after
// walletStatusTTL.
type walletStatusEntry struct {
	status  AccountStatus
	fetched time.Time
}

const walletStatusTTL = 5 * time.Minute

// accountAvailable checks the venue's wallet status. A check that fails to
// complete (network error, unsupported venue) fails open with Healthy=true so
// a broken status endpoint cannot halt trading; only a confirmed suspension
// reports unhealthy.
func (l *LiveAdapter) accountAvailable(exchange string) AccountStatus {
	l.mu.RLock()
	entry, ok := l.walletStatuses[exchange]
	l.mu.RUnlock()
	if ok && time.Since(entry.fetched) < walletStatusTTL {
		return entry.status
	}

	status := fetchWalletStatus(exchange)
	l.mu.Lock()
	l.walletStatuses[exchange] = walletStatusEntry{status: status, fetched: time.Now().UTC()}
	l.mu.Unlock()
	return status
}

// AccountAvailable implements accountStatusReporter.
func (l *LiveAdapter) AccountAvailable(exchange string) (AccountStatus, error) {
	status := l.accountAvailable(exchange)
	if !status.Healthy() {
		return status, ErrWalletSuspended
	}
	return status, nil
}

// fetchWalletStatus derives wallet health from venue public endpoints.
// Venues without a known status endpoint fail open.
func fetchWalletStatus(exchange string) AccountStatus {
	// Public wallet-status endpoints are venue-specific and authenticated for
	// most venues; without credentials this check is advisory only. All
	// venues report healthy here until a concrete per-venue implementation
	// lands — the wiring and TTL cache are in place so adding one is a
	// single function.
	return AccountStatus{DepositEnabled: true, WithdrawalEnabled: true}
}

// EnableWebSocketFeed turns on the realtime book feed. Books pushed over
// websocket are served from cache while they are younger than maxAge; anything
// older (or any symbol the feed does not cover) falls back to REST.
//
// Default off: the feeds are best-effort and unverified against every venue's
// payload quirks, so an operator opts in per deployment.
func (l *LiveAdapter) EnableWebSocketFeed(ctx context.Context, maxAge time.Duration) {
	l.mu.Lock()
	if l.wsCache == nil {
		l.wsCache = NewWSBookCache()
	}
	l.wsMaxAge = maxAge
	if l.tokocryptoBooks == nil { l.tokocryptoBooks = tokocrypto.NewBookManager(100) }
	if l.mexcPerpBooks == nil { l.mexcPerpBooks = mexc.NewPerpetualBookManager(100) }
	l.venueWSMaxAge = maxAge
	if l.tokocryptoSupervisor == nil { l.tokocryptoSupervisor = l.newTokocryptoSupervisor(); l.tokocryptoSupervisor.Start(ctx) }
	if l.mexcPerpSupervisor == nil { l.mexcPerpSupervisor = l.newMEXCPerpetualSupervisor(); l.mexcPerpSupervisor.Start(ctx) }
	stopCleanup := l.wsCache.StartStaleBookCleanup(maxAge, 5*time.Minute)
	l.wsStopCleanup = stopCleanup
	l.mu.Unlock()

	client := NewWSClient(l.wsCache, nil,
		indodaxDepthStream(l.indodaxBaseURL),
		mexcSpotDepthStream(l.mexcSpotBaseURL),
	)
	l.mu.Lock()
	l.wsClient = client
	l.mu.Unlock()
	client.Start(ctx)
}

// EnableOKXBookWebSocket starts the authoritative OKX books-channel feed.
func (l *LiveAdapter) EnableOKXBookWebSocket(ctx context.Context, wsURL string, staleAfter, reconnect, maxBackoff time.Duration) {
	l.mu.Lock()
	if l.okxVenueBooks != nil { l.mu.Unlock(); return }
	if strings.TrimSpace(wsURL) == "" { wsURL = "wss://ws.okx.com:8443/ws/v5/public" }
	if staleAfter <= 0 { staleAfter = 5 * time.Second }
	manager := okx.NewBookManager(wsURL, nil, func(symbol string) float64 {
		l.mu.RLock(); defer l.mu.RUnlock()
		if v:=l.contractSizes[symbol]; v>0 { return v }
		if v:=l.contractSizes["okx:"+symbol]; v>0 { return v }
		return 0
	}, reconnect, maxBackoff)
	l.okxWSMaxAge = staleAfter
	l.okxVenueBooks = manager
	l.mu.Unlock()
	manager.SetHTTPClient(wsHTTPClient())
	manager.Start(ctx)
}

func (l *LiveAdapter) SubscribeOKXBooks(markets []domain.Market) {
	l.mu.RLock()
	manager := l.okxVenueBooks
	legacyManager := l.okxBooks
	l.mu.RUnlock()
	if manager == nil && legacyManager == nil { return }
	symbols := make([]string, 0, len(markets))
	for _, market := range markets {
		if strings.EqualFold(market.Exchange, "okx") && market.Active {
			symbols = append(symbols, market.Symbol)
		}
	}
	if manager != nil { manager.SetInstruments(symbols) }
	if legacyManager != nil { legacyManager.SetInstruments(symbols) }
}

func (l *LiveAdapter) OKXWebSocketConnected() bool {
	l.mu.RLock(); manager, legacy := l.okxVenueBooks, l.okxBooks; l.mu.RUnlock()
	return (manager != nil && manager.IsConnected()) || (legacy != nil && legacy.IsConnected())
}

// WebSocketStats reports (hits, misses) for the cached realtime books.
func (l *LiveAdapter) WebSocketStats() (hits, misses int64) {
	l.mu.RLock()
	cache := l.wsCache
	l.mu.RUnlock()
	if cache == nil {
		return 0, 0
	}
	return cache.Stats()
}

// WSConnectionHealthy reports whether the websocket feed for an exchange is
// currently believed to be delivering data. When the feed is off (cache nil)
// the check passes vacuously so REST-only deployments are never blocked.
func (l *LiveAdapter) WSConnectionHealthy(exchange string) bool {
	l.mu.RLock()
	cache, maxAge, okxManager := l.wsCache, l.wsMaxAge, l.okxVenueBooks
	l.mu.RUnlock()
	if strings.EqualFold(exchange, "okx") && okxManager != nil {
		return okxManager.IsConnected()
	}
	if cache == nil { return true }
	return cache.IsExchangeConnectionHealthy(exchange, maxAge*3)
}

// LastBookUpdate reports when the websocket feed last delivered a book for an
// exchange|symbol. The zero time means the feed never delivered one; REST-only
// deployments (nil cache) also return zero, which the scanner treats as "no
// activity data" rather than "inactive".
func (l *LiveAdapter) LastBookUpdate(exchange, symbol string) time.Time {
	l.mu.RLock()
	cache, okxManager := l.wsCache, l.okxVenueBooks
	l.mu.RUnlock()
	if strings.EqualFold(exchange, "okx") && okxManager != nil {
		if book, ok := okxManager.Get(symbol, 0); ok { return book.ReceivedAt }
	}
	if cache == nil { return time.Time{} }
	return cache.LastBookUpdate(exchange, symbol)
}

// SetTargetPerpExchange filters which perpetual exchange(s) to scan (e.g. "okx", "mexc", or "" for all).
func (l *LiveAdapter) SetTargetPerpExchange(ex string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.targetPerpExchange = strings.ToLower(strings.TrimSpace(ex))
}

// Close stops the websocket feed and associated cleanup goroutines. This should
// be called when the scanner is shutting down to prevent resource leaks.
func (l *LiveAdapter) Close() {
	if l.wsStopCleanup != nil {
		l.wsStopCleanup()
		l.wsStopCleanup = nil
	}
	if l.wsClient != nil {
		l.wsClient.Stop()
		l.wsClient = nil
	}
	if l.okxBooks != nil { l.okxBooks.Stop(); l.okxBooks = nil }
	if l.okxVenueBooks != nil { l.okxVenueBooks.Stop(); l.okxVenueBooks = nil }
	if l.tokocryptoSupervisor != nil { l.tokocryptoSupervisor.Stop(); l.tokocryptoSupervisor = nil }
	if l.mexcPerpSupervisor != nil { l.mexcPerpSupervisor.Stop(); l.mexcPerpSupervisor = nil }
}
