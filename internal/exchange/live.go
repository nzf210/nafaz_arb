package exchange

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

type dohResponse struct {
	Answer []struct {
		Data string `json:"data"`
		Type int    `json:"type"`
	} `json:"Answer"`
}

const (
	MEXC_SPOT        = "mexc_spot:"
	USER_AGENT       = "User-Agent"
	USER_AGENT_VALUE = "Mozilla/5.0 (compatible; NafazScanner/1.0)"
)

var (
	dnsCache   = make(map[string]string)
	dnsCacheMu sync.RWMutex
)

func needsDoH(host string) bool {
	h := strings.ToLower(host)
	return strings.Contains(h, "okx") || strings.Contains(h, "binance") || strings.Contains(h, "mexc")
}

func getCachedHost(host string) (string, bool) {
	dnsCacheMu.RLock()
	ip, ok := dnsCache[host]
	dnsCacheMu.RUnlock()
	return ip, ok && ip != ""
}

func cacheHost(host, ip string) string {
	dnsCacheMu.Lock()
	dnsCache[host] = ip
	dnsCacheMu.Unlock()
	return ip
}

func resolveStandard(ctx context.Context, host string) string {
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err == nil && len(ips) > 0 {
		for _, ipCandidate := range ips {
			if !strings.Contains(ipCandidate, ":") {
				return cacheHost(host, ipCandidate)
			}
		}
		return ips[0]
	}
	return host
}

func resolveViaDoH(ctx context.Context, host string) string {
	dohURL := fmt.Sprintf("https://1.1.1.1/dns-query?name=%s&type=A", host)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dohURL, nil)
	if err != nil {
		return ""
	}

	req.Header.Set("accept", "application/dns-json")
	dohClient := &http.Client{Timeout: 3 * time.Second}
	resp, err := dohClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	var r dohResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return ""
	}
	for _, a := range r.Answer {
		if a.Type == 1 && a.Data != "" && !strings.HasPrefix(a.Data, "36.86.63.") {
			return cacheHost(host, a.Data)
		}
	}
	return ""
}

func resolveFallback(ctx context.Context, host string) string {
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err == nil && len(ips) > 0 {
		for _, ipCandidate := range ips {
			if !strings.HasPrefix(ipCandidate, "36.86.63.") && !strings.Contains(ipCandidate, ":") {
				return cacheHost(host, ipCandidate)
			}
		}
	}
	return host
}

func resolveHost(ctx context.Context, host string) string {
	if ip, ok := getCachedHost(host); ok {
		return ip
	}

	// Domestic Indonesian exchanges (Indodax, Tokocrypto) are legal and not blocked; use standard resolver directly
	if !needsDoH(host) {
		return resolveStandard(ctx, host)
	}

	// For blocked foreign exchanges (OKX, Binance, MEXC): Always query Cloudflare DNS-over-HTTPS (DoH) first
	if ip := resolveViaDoH(ctx, host); ip != "" {
		return ip
	}

	// Fallback to standard lookup if DoH fails
	return resolveFallback(ctx, host)
}

type LiveAdapter struct {
	client            *http.Client
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
	return &LiveAdapter{
		health:            newAvailability(HealthPolicy{}, nil),
		walletStatuses:    make(map[string]walletStatusEntry),
		client:            &http.Client{Transport: tr, Timeout: 15 * time.Second},
		indodaxBaseURL:    "https://indodax.com",
		tokocryptoBaseURL: "https://www.tokocrypto.com",
		okxBaseURL:        "https://www.okx.com",
		mexcBaseURL:       "https://contract.mexc.com",
		mexcSpotBaseURL:   "https://api.mexc.com",
		contractSizes:     make(map[string]float64),
	}
}

// SetBaseURLs allows overriding API base URLs for testing.
func (l *LiveAdapter) SetBaseURLs(urls ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(urls) > 0 && urls[0] != "" {
		l.indodaxBaseURL = urls[0]
	}
	if len(urls) > 1 && urls[1] != "" {
		l.okxBaseURL = urls[1]
	}
	if len(urls) > 2 && urls[2] != "" {
		l.mexcBaseURL = urls[2]
	}
	if len(urls) > 3 && urls[3] != "" {
		l.mexcSpotBaseURL = urls[3]
	}
	if len(urls) > 4 && urls[4] != "" {
		l.tokocryptoBaseURL = urls[4]
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
	stopCleanup := l.wsCache.StartStaleBookCleanup(maxAge, 5*time.Minute)
	l.wsStopCleanup = stopCleanup
	l.mu.Unlock()

	client := NewWSClient(l.wsCache, nil,
		indodaxDepthStream(l.indodaxBaseURL),
		okxBookStream(l.okxBaseURL),
		mexcSpotDepthStream(l.mexcSpotBaseURL),
	)
	l.mu.Lock()
	l.wsClient = client
	l.mu.Unlock()
	client.Start(ctx)
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
	cache, maxAge := l.wsCache, l.wsMaxAge
	l.mu.RUnlock()
	if cache == nil {
		return true
	}
	return cache.IsExchangeConnectionHealthy(exchange, maxAge*3)
}

// LastBookUpdate reports when the websocket feed last delivered a book for an
// exchange|symbol. The zero time means the feed never delivered one; REST-only
// deployments (nil cache) also return zero, which the scanner treats as "no
// activity data" rather than "inactive".
func (l *LiveAdapter) LastBookUpdate(exchange, symbol string) time.Time {
	l.mu.RLock()
	cache := l.wsCache
	l.mu.RUnlock()
	if cache == nil {
		return time.Time{}
	}
	return cache.LastBookUpdate(exchange, symbol)
}

// SetTargetPerpExchange filters which perpetual exchange(s) to scan (e.g. "okx", "mexc", or "" for all).
func (l *LiveAdapter) SetTargetPerpExchange(ex string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.targetPerpExchange = strings.ToLower(strings.TrimSpace(ex))
}

// USDTIDRRate retrieves the current live USDT/IDR exchange rate from Indodax ticker with caching.
func (l *LiveAdapter) USDTIDRRate(ctx context.Context) (float64, error) {
	l.mu.RLock()
	if l.lastUSDTIDRRate > 0 && time.Since(l.lastUSDTIDRTime) < 60*time.Second {
		rate := l.lastUSDTIDRRate
		l.mu.RUnlock()
		return rate, nil
	}
	l.mu.RUnlock()

	endpoint := l.indodaxBaseURL + "/api/ticker/usdtidr"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("indodax usdtidr ticker req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("indodax usdtidr ticker request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("indodax usdtidr ticker status: %d", resp.StatusCode)
	}

	var raw struct {
		Ticker struct {
			Buy  string `json:"buy"`
			Sell string `json:"sell"`
			Last string `json:"last"`
		} `json:"ticker"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return 0, fmt.Errorf("decode indodax usdtidr: %w", err)
	}

	last, err := strconv.ParseFloat(raw.Ticker.Last, 64)
	if err != nil || last <= 0 {
		buy, _ := strconv.ParseFloat(raw.Ticker.Buy, 64)
		sell, _ := strconv.ParseFloat(raw.Ticker.Sell, 64)
		if buy > 0 && sell > 0 {
			last = (buy + sell) / 2
		}
	}
	if last <= 0 {
		return 0, fmt.Errorf("invalid indodax usdtidr rate: %s", raw.Ticker.Last)
	}

	l.mu.Lock()
	l.lastUSDTIDRRate = last
	l.lastUSDTIDRTime = time.Now().UTC()
	l.mu.Unlock()

	return last, nil
}

// SpotMarkets discovers active spot pairs on Tokocrypto and Indodax (and MEXC Spot if target is mexc/all).
func (l *LiveAdapter) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	var allMarkets []domain.Market
	var errs []string

	tokoMarkets, err := l.fetchTokocryptoSpotMarkets(ctx)
	if err == nil {
		allMarkets = append(allMarkets, tokoMarkets...)
	} else {
		errs = append(errs, fmt.Sprintf("toko: %v", err))
	}

	indodaxMarkets, err := l.fetchIndodaxSpotMarkets(ctx)
	if err == nil {
		allMarkets = append(allMarkets, indodaxMarkets...)
	} else {
		errs = append(errs, fmt.Sprintf("indodax: %v", err))
	}

	l.mu.RLock()
	includeMEXCSpot := l.targetPerpExchange == "" || l.targetPerpExchange == "mexc" || l.targetPerpExchange == "all"
	l.mu.RUnlock()

	if includeMEXCSpot {
		mexcMarkets, err := l.fetchMEXCSpotMarkets(ctx)
		if err == nil {
			allMarkets = append(allMarkets, mexcMarkets...)
		} else {
			errs = append(errs, fmt.Sprintf("mexc_spot: %v", err))
		}
	}

	if len(allMarkets) == 0 {
		return nil, fmt.Errorf("failed to discover spot markets (%s)", strings.Join(errs, "; "))
	}

	return allMarkets, nil
}

func (l *LiveAdapter) fetchTokocryptoSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	l.mu.RLock()
	if len(l.cachedTokocryptoSpot) > 0 && time.Since(l.lastTokocryptoFetch) < 1*time.Hour {
		res := l.cachedTokocryptoSpot
		l.mu.RUnlock()
		return res, nil
	}
	l.mu.RUnlock()

	endpoint := l.tokocryptoBaseURL + "/open/v1/common/symbols"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return l.getFallbackTokocryptoSpot(err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return l.getFallbackTokocryptoSpot(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return l.getFallbackTokocryptoSpot(fmt.Errorf("tokocrypto symbols status: %d", resp.StatusCode))
	}

	var raw struct {
		Code int `json:"code"`
		Data struct {
			List []struct {
				Symbol            string `json:"symbol"`
				BaseAsset         string `json:"baseAsset"`
				QuoteAsset        string `json:"quoteAsset"`
				SpotTradingEnable int    `json:"spotTradingEnable"`
			} `json:"list"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return l.getFallbackTokocryptoSpot(fmt.Errorf("decode tokocrypto symbols: %w", err))
	}

	var markets []domain.Market
	for _, s := range raw.Data.List {
		if s.SpotTradingEnable != 1 || s.Symbol == "" || s.BaseAsset == "" || s.QuoteAsset == "" {
			continue
		}
		markets = append(markets, domain.Market{
			Exchange:   "tokocrypto",
			Symbol:     "toko:" + s.Symbol,
			BaseAsset:  strings.ToUpper(s.BaseAsset),
			QuoteAsset: strings.ToUpper(s.QuoteAsset),
			MarketType: "SPOT",
			Active:     true,
		})
	}

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
	if len(l.cachedIndodaxSpot) > 0 && time.Since(l.lastIndodaxSpotFetch) < 1*time.Hour {
		res := l.cachedIndodaxSpot
		l.mu.RUnlock()
		return res, nil
	}
	l.mu.RUnlock()

	endpoint := l.indodaxBaseURL + "/api/pairs"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return l.getFallbackIndodaxSpot(err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return l.getFallbackIndodaxSpot(fmt.Errorf("indodax pairs request: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return l.getFallbackIndodaxSpot(fmt.Errorf("indodax pairs status: %d", resp.StatusCode))
	}

	var indodaxPairs []struct {
		ID             string `json:"id"`
		Symbol         string `json:"symbol"`
		BaseCurrency   string `json:"base_currency"`   // quote currency (e.g. usdt, idr)
		TradedCurrency string `json:"traded_currency"` // base currency (e.g. btc, eth)
	}

	if err := json.NewDecoder(resp.Body).Decode(&indodaxPairs); err != nil {
		return l.getFallbackIndodaxSpot(fmt.Errorf("decode indodax pairs: %w", err))
	}

	var markets []domain.Market
	for _, p := range indodaxPairs {
		if p.ID == "" || p.TradedCurrency == "" || p.BaseCurrency == "" {
			continue
		}
		markets = append(markets, domain.Market{
			Exchange:   "indodax",
			Symbol:     p.ID,
			BaseAsset:  strings.ToUpper(p.TradedCurrency),
			QuoteAsset: strings.ToUpper(p.BaseCurrency),
			MarketType: "SPOT",
			Active:     true,
		})
	}

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

func (l *LiveAdapter) fetchMEXCSpotMarkets(ctx context.Context) ([]domain.Market, error) {
	endpoint := l.mexcSpotBaseURL + "/api/v3/defaultSymbols"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("mexc spot symbols req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mexc spot symbols request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mexc spot symbols status: %d", resp.StatusCode)
	}

	var raw struct {
		Code int      `json:"code"`
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode mexc spot symbols: %w", err)
	}

	var markets []domain.Market
	for _, sym := range raw.Data {
		var base, quote string
		switch {
		case strings.HasSuffix(sym, "USDT"):
			quote = "USDT"
			base = strings.TrimSuffix(sym, "USDT")
		case strings.HasSuffix(sym, "USDC"):
			quote = "USDC"
			base = strings.TrimSuffix(sym, "USDC")
		default:
			continue
		}
		if base == "" {
			continue
		}
		markets = append(markets, domain.Market{
			Exchange:   "mexc",
			Symbol:     MEXC_SPOT + sym,
			BaseAsset:  strings.ToUpper(base),
			QuoteAsset: strings.ToUpper(quote),
			MarketType: "SPOT",
			Active:     true,
		})
	}
	return markets, nil
}

// PerpetualMarkets discovers active perpetual SWAP instruments on OKX and MEXC, respecting targetPerpExchange filter if set.
func (l *LiveAdapter) PerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	var allMarkets []domain.Market

	l.mu.RLock()
	targetEx := l.targetPerpExchange
	l.mu.RUnlock()

	includeOKX := targetEx == "" || targetEx == "okx" || targetEx == "all"
	includeMEXC := targetEx == "" || targetEx == "mexc" || targetEx == "all"

	var errs []string
	if includeOKX {
		okxMarkets, err := l.fetchOKXPerpetualMarkets(ctx)
		if err == nil {
			allMarkets = append(allMarkets, okxMarkets...)
		} else {
			errs = append(errs, fmt.Sprintf("okx: %v", err))
		}
	}

	if includeMEXC {
		mexcMarkets, err := l.fetchMEXCPerpetualMarkets(ctx)
		if err == nil {
			allMarkets = append(allMarkets, mexcMarkets...)
		} else {
			errs = append(errs, fmt.Sprintf("mexc: %v", err))
		}
	}

	if len(allMarkets) == 0 {
		return nil, fmt.Errorf("failed to discover perpetual markets (target: %s, errors: %s)", targetEx, strings.Join(errs, "; "))
	}

	return allMarkets, nil
}

type okxInstrumentData struct {
	InstID    string `json:"instId"`
	InstType  string `json:"instType"`
	CtVal     string `json:"ctVal"`
	CtValCcy  string `json:"ctValCcy"`
	SettleCcy string `json:"settleCcy"`
	State     string `json:"state"`
	TickSz    string `json:"tickSz"`
	LotSz     string `json:"lotSz"`
	MinSz     string `json:"minSz"`
}

func extractOKXBaseAsset(instID, ctValCcy string) string {
	if ctValCcy != "" {
		return ctValCcy
	}
	parts := strings.Split(instID, "-")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func (l *LiveAdapter) getCachedOKXPerp() ([]domain.Market, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedOKXPerp) > 0 && time.Since(l.lastOKXPerpFetch) < 1*time.Hour {
		return l.cachedOKXPerp, true
	}
	return nil, false
}

func (l *LiveAdapter) processOKXInstrument(inst okxInstrumentData) (domain.Market, bool) {
	if inst.State != "live" || inst.InstID == "" {
		return domain.Market{}, false
	}

	baseAsset := extractOKXBaseAsset(inst.InstID, inst.CtValCcy)

	ctVal, _ := strconv.ParseFloat(inst.CtVal, 64)
	if ctVal <= 0 {
		ctVal = 1
	}
	l.contractSizes[inst.InstID] = ctVal
	l.contractSizes["okx:"+inst.InstID] = ctVal

	tickSz, _ := strconv.ParseFloat(inst.TickSz, 64)
	lotSz, _ := strconv.ParseFloat(inst.LotSz, 64)
	minSz, _ := strconv.ParseFloat(inst.MinSz, 64)

	return domain.Market{
		Exchange:     "okx",
		Symbol:       inst.InstID,
		BaseAsset:    strings.ToUpper(baseAsset),
		QuoteAsset:   strings.ToUpper(inst.SettleCcy),
		MarketType:   "PERPETUAL",
		Active:       true,
		ContractSize: ctVal,
		TickSize:     tickSz,
		LotSize:      lotSz,
		MinOrder:     minSz,
	}, true
}

func (l *LiveAdapter) fetchOKXPerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	if cached, ok := l.getCachedOKXPerp(); ok {
		return cached, nil
	}

	endpoint := l.okxBaseURL + "/api/v5/public/instruments?instType=SWAP"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return l.getFallbackOKXPerp(err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return l.getFallbackOKXPerp(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return l.getFallbackOKXPerp(fmt.Errorf("okx instruments status: %d", resp.StatusCode))
	}

	var okxResp struct {
		Code string              `json:"code"`
		Msg  string              `json:"msg"`
		Data []okxInstrumentData `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&okxResp); err != nil {
		return l.getFallbackOKXPerp(err)
	}
	if okxResp.Code != "0" {
		return l.getFallbackOKXPerp(fmt.Errorf("okx error: %s", okxResp.Msg))
	}

	var markets []domain.Market
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, inst := range okxResp.Data {
		if m, ok := l.processOKXInstrument(inst); ok {
			markets = append(markets, m)
		}
	}

	if len(markets) > 0 {
		l.cachedOKXPerp = markets
		l.lastOKXPerpFetch = time.Now().UTC()
	}

	return markets, nil
}

func (l *LiveAdapter) getFallbackOKXPerp(err error) ([]domain.Market, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.cachedOKXPerp) > 0 {
		return l.cachedOKXPerp, nil
	}
	return nil, err
}

func (l *LiveAdapter) fetchMEXCPerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	endpoint := l.mexcBaseURL + "/api/v1/contract/detail"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mexc contract detail status: %d", resp.StatusCode)
	}

	var mexcResp struct {
		Success bool `json:"success"`
		Code    int  `json:"code"`
		Data    []struct {
			Symbol       string  `json:"symbol"`
			BaseCoin     string  `json:"baseCoin"`
			QuoteCoin    string  `json:"quoteCoin"`
			ContractSize float64 `json:"contractSize"`
			PriceUnit    float64 `json:"priceUnit"`
			VolUnit      float64 `json:"volUnit"`
			MinVol       float64 `json:"minVol"`
			State        int     `json:"state"` // 0: enabled
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&mexcResp); err != nil {
		return nil, err
	}
	if !mexcResp.Success {
		return nil, fmt.Errorf("mexc contract detail error code: %d", mexcResp.Code)
	}

	var markets []domain.Market
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, c := range mexcResp.Data {
		if c.State != 0 || c.Symbol == "" {
			continue
		}

		ctVal := c.ContractSize
		if ctVal <= 0 {
			ctVal = 1
		}
		l.contractSizes[c.Symbol] = ctVal
		l.contractSizes["mexc:"+c.Symbol] = ctVal

		markets = append(markets, domain.Market{
			Exchange:     "mexc",
			Symbol:       "mexc:" + c.Symbol,
			BaseAsset:    strings.ToUpper(c.BaseCoin),
			QuoteAsset:   strings.ToUpper(c.QuoteCoin),
			MarketType:   "PERPETUAL",
			Active:       true,
			ContractSize: ctVal,
			TickSize:     c.PriceUnit,
			LotSize:      c.VolUnit,
			MinOrder:     c.MinVol,
		})
	}
	return markets, nil
}

// SpotOrderBook fetches live order book depth from Tokocrypto, Indodax, or MEXC Spot.
// trackedBook runs an order-book fetch and records its outcome for maintenance
// detection. HTTP error codes are logged so an operator can tell a venue outage
// (503/502/504) from a broken symbol.
func (l *LiveAdapter) trackedBook(exchangeName string, fetch func() (domain.OrderBook, error)) (domain.OrderBook, error) {
	return l.trackedBookForSymbol(exchangeName, "", fetch)
}

// trackedBookForSymbol is trackedBook plus a websocket cache lookup. A fresh
// realtime book short-circuits the REST fetch (and counts as a WS hit); a miss
// falls through to REST, so enabling the feed can never reduce coverage.
func (l *LiveAdapter) trackedBookForSymbol(exchangeName, symbol string, fetch func() (domain.OrderBook, error)) (domain.OrderBook, error) {
	if !l.ExchangeAvailable(exchangeName) {
		return domain.OrderBook{}, fmt.Errorf("%w: %s", ErrExchangeUnavailable, exchangeName)
	}
	if symbol != "" {
		l.mu.RLock()
		cache, maxAge := l.wsCache, l.wsMaxAge
		l.mu.RUnlock()
		if cache != nil {
			book, ok, isDuplicate, isOutOfOrder, isGap, _ := cache.Get(exchangeName, symbol, maxAge)
			if !ok {
				// Cache miss (including stale books) - fall through to REST fetch
			} else if isDuplicate {
				// Accept duplicate book (same sequence ID) - return cached book
				return book, nil
			} else if isOutOfOrder || isGap {
				// Reject out-of-order or gap book - fall through to REST fetch
			} else {
				// Valid cached book - return it
				return book, nil
			}
		}
	}
	book, err := fetch()
	l.recordOutcome(exchangeName, err)
	return book, err
}

func (l *LiveAdapter) SpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if strings.HasPrefix(symbol, "toko:") {
		return l.trackedBookForSymbol("tokocrypto", symbol, func() (domain.OrderBook, error) {
			return l.fetchTokocryptoSpotOrderBook(ctx, symbol)
		})
	}
	if strings.HasPrefix(symbol, MEXC_SPOT) || strings.HasPrefix(symbol, "mexc:") {
		return l.trackedBookForSymbol("mexc", symbol, func() (domain.OrderBook, error) {
			return l.fetchMEXCSpotOrderBook(ctx, symbol)
		})
	}
	return l.trackedBookForSymbol("indodax", symbol, func() (domain.OrderBook, error) {
		return l.fetchIndodaxSpotOrderBook(ctx, symbol)
	})
}

func (l *LiveAdapter) fetchTokocryptoSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSym := strings.TrimPrefix(symbol, "toko:")
	endpoint := fmt.Sprintf("%s/open/v1/market/depth?symbol=%s&limit=20", l.tokocryptoBaseURL, url.QueryEscape(rawSym))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("tokocrypto depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("tokocrypto depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: tokocrypto depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("tokocrypto depth status: %d", resp.StatusCode)
	}

	var raw struct {
		Code int `json:"code"`
		Data struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode tokocrypto depth: %w", err)
	}

	bids := parseStringLevels(raw.Data.Bids, 1.0)
	asks := parseStringLevels(raw.Data.Asks, 1.0)

	return domain.OrderBook{
		Exchange:  "tokocrypto",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(),
	}, nil
}

func (l *LiveAdapter) fetchIndodaxSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	pair := strings.ToLower(symbol)
	endpoint := fmt.Sprintf("%s/api/depth/%s", l.indodaxBaseURL, url.PathEscape(pair))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("indodax depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("indodax depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: indodax depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("indodax depth status: %d", resp.StatusCode)
	}

	var rawBook struct {
		Buy  [][]any `json:"buy"`  // bids
		Sell [][]any `json:"sell"` // asks
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawBook); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode indodax depth: %w", err)
	}

	bids := parseLevels(rawBook.Buy, 1.0)
	asks := parseLevels(rawBook.Sell, 1.0)

	return domain.OrderBook{
		Exchange:  "indodax",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(),
	}, nil
}

func (l *LiveAdapter) fetchMEXCSpotOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSym := strings.TrimPrefix(symbol, MEXC_SPOT)
	rawSym = strings.TrimPrefix(rawSym, "mexc:")
	endpoint := fmt.Sprintf("%s/api/v3/depth?symbol=%s&limit=20", l.mexcSpotBaseURL, url.QueryEscape(rawSym))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: mexc spot depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("mexc spot depth status: %d", resp.StatusCode)
	}

	var rawBook struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rawBook); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode mexc spot depth: %w", err)
	}

	bids := parseStringLevels(rawBook.Bids, 1.0)
	asks := parseStringLevels(rawBook.Asks, 1.0)

	return domain.OrderBook{
		Exchange:  "mexc",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(),
	}, nil
}

// PerpetualOrderBook fetches live order book from OKX or MEXC with contract size normalization.
func (l *LiveAdapter) PerpetualOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	if isMEXCSymbol(symbol) {
		return l.trackedBookForSymbol("mexc", symbol, func() (domain.OrderBook, error) {
			return l.fetchMEXCOrderBook(ctx, symbol)
		})
	}
	return l.trackedBookForSymbol("okx", symbol, func() (domain.OrderBook, error) {
		return l.fetchOKXOrderBook(ctx, symbol)
	})
}

func isMEXCSymbol(symbol string) bool {
	return strings.HasPrefix(symbol, "mexc:") || strings.Contains(symbol, "_")
}

func cleanSymbol(symbol string) string {
	s := strings.TrimPrefix(symbol, "mexc:")
	return strings.TrimPrefix(s, "okx:")
}

func (l *LiveAdapter) fetchOKXOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v5/market/books?instId=%s&sz=20", l.okxBaseURL, url.QueryEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("okx depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("okx depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: okx depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("okx depth status: %d", resp.StatusCode)
	}

	var okxResp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
			Ts   string     `json:"ts"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&okxResp); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode okx depth: %w", err)
	}
	if okxResp.Code != "0" || len(okxResp.Data) == 0 {
		return domain.OrderBook{}, fmt.Errorf("okx depth error: %s (%s)", okxResp.Msg, okxResp.Code)
	}

	l.mu.RLock()
	ctVal, ok := l.contractSizes[symbol]
	if !ok {
		ctVal, ok = l.contractSizes[rawSymbol]
	}
	l.mu.RUnlock()
	if !ok || ctVal <= 0 {
		ctVal = 1.0
	}

	bookData := okxResp.Data[0]
	bids := parseStringLevels(bookData.Bids, ctVal)
	asks := parseStringLevels(bookData.Asks, ctVal)

	ts := time.Now().UTC()
	if ms, err := strconv.ParseInt(bookData.Ts, 10, 64); err == nil && ms > 0 {
		ts = time.UnixMilli(ms).UTC()
	}

	return domain.OrderBook{
		Exchange:  "okx",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: ts,
	}, nil
}

func (l *LiveAdapter) fetchMEXCOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v1/contract/depth/%s?limit=20", l.mexcBaseURL, url.PathEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc depth req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("mexc depth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.OrderBook{}, fmt.Errorf("%w: mexc depth status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.OrderBook{}, fmt.Errorf("mexc depth status: %d", resp.StatusCode)
	}

	var mexcResp struct {
		Success bool `json:"success"`
		Code    int  `json:"code"`
		Data    struct {
			Bids      [][]any `json:"bids"`
			Asks      [][]any `json:"asks"`
			Timestamp int64   `json:"timestamp"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&mexcResp); err != nil {
		return domain.OrderBook{}, fmt.Errorf("decode mexc depth: %w", err)
	}
	if !mexcResp.Success {
		return domain.OrderBook{}, fmt.Errorf("mexc depth error code: %d", mexcResp.Code)
	}

	l.mu.RLock()
	ctVal, ok := l.contractSizes[symbol]
	if !ok {
		ctVal, ok = l.contractSizes[rawSymbol]
	}
	l.mu.RUnlock()
	if !ok || ctVal <= 0 {
		ctVal = 1.0
	}

	bids := parseLevels(mexcResp.Data.Bids, ctVal)
	asks := parseLevels(mexcResp.Data.Asks, ctVal)

	ts := time.Now().UTC()
	if mexcResp.Data.Timestamp > 0 {
		ts = time.UnixMilli(mexcResp.Data.Timestamp).UTC()
	}

	return domain.OrderBook{
		Exchange:  "mexc",
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: ts,
	}, nil
}

// FundingRate retrieves current funding rate from OKX or MEXC.
func (l *LiveAdapter) FundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	if isMEXCSymbol(symbol) {
		return l.fetchMEXCFundingRate(ctx, symbol)
	}
	return l.fetchOKXFundingRate(ctx, symbol)
}

func (l *LiveAdapter) fetchOKXFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v5/public/funding-rate?instId=%s", l.okxBaseURL, url.QueryEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("okx funding req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("okx funding request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.FundingRate{}, fmt.Errorf("%w: okx funding status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.FundingRate{}, fmt.Errorf("okx funding status: %d", resp.StatusCode)
	}

	var okxResp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			FundingRate     string `json:"fundingRate"`
			FundingTime     string `json:"fundingTime"`
			PrevFundingTime string `json:"prevFundingTime"`
			Ts              string `json:"ts"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&okxResp); err != nil {
		return domain.FundingRate{}, fmt.Errorf("decode okx funding: %w", err)
	}
	if okxResp.Code != "0" || len(okxResp.Data) == 0 {
		return domain.FundingRate{}, fmt.Errorf("okx funding error: %s (%s)", okxResp.Msg, okxResp.Code)
	}

	item := okxResp.Data[0]
	rate, _ := strconv.ParseFloat(item.FundingRate, 64)

	intervalHours := 8
	ft, _ := strconv.ParseInt(item.FundingTime, 10, 64)
	pft, _ := strconv.ParseInt(item.PrevFundingTime, 10, 64)
	if ft > 0 && pft > 0 && ft > pft {
		diffHours := int((ft - pft) / (1000 * 3600))
		if diffHours > 0 {
			intervalHours = diffHours
		}
	}

	ts := time.Now().UTC()
	if ms, err := strconv.ParseInt(item.Ts, 10, 64); err == nil && ms > 0 {
		ts = time.UnixMilli(ms).UTC()
	}

	var nextFunding time.Time
	if ft > 0 {
		nextFunding = time.UnixMilli(ft).UTC()
	}

	return domain.FundingRate{
		Rate:            rate,
		IntervalHours:   intervalHours,
		Timestamp:       ts,
		NextFundingTime: nextFunding,
	}, nil
}

func (l *LiveAdapter) fetchMEXCFundingRate(ctx context.Context, symbol string) (domain.FundingRate, error) {
	rawSymbol := cleanSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v1/contract/funding_rate/%s", l.mexcBaseURL, url.PathEscape(rawSymbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("mexc funding req: %w", err)
	}
	req.Header.Set(USER_AGENT, USER_AGENT_VALUE)

	resp, err := l.client.Do(req)
	if err != nil {
		return domain.FundingRate{}, fmt.Errorf("mexc funding request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if classifyHTTPError(resp.StatusCode) {
			return domain.FundingRate{}, fmt.Errorf("%w: mexc funding status %d", ErrExchangeUnavailable, resp.StatusCode)
		}
		return domain.FundingRate{}, fmt.Errorf("mexc funding status: %d", resp.StatusCode)
	}

	var mexcResp struct {
		Success bool `json:"success"`
		Code    int  `json:"code"`
		Data    struct {
			FundingRate  float64 `json:"fundingRate"`
			CollectCycle int     `json:"collectCycle"`
			Timestamp    int64   `json:"timestamp"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&mexcResp); err != nil {
		return domain.FundingRate{}, fmt.Errorf("decode mexc funding: %w", err)
	}
	if !mexcResp.Success {
		return domain.FundingRate{}, fmt.Errorf("mexc funding error code: %d", mexcResp.Code)
	}

	interval := mexcResp.Data.CollectCycle
	if interval <= 0 {
		interval = 8
	}

	ts := time.Now().UTC()
	if mexcResp.Data.Timestamp > 0 {
		ts = time.UnixMilli(mexcResp.Data.Timestamp).UTC()
	}

	return domain.FundingRate{
		Rate:            mexcResp.Data.FundingRate,
		IntervalHours:   interval,
		Timestamp:       ts,
		NextFundingTime: nextSettlementTime(ts, interval),
	}, nil
}

func nextSettlementTime(t time.Time, intervalHours int) time.Time {
	if intervalHours <= 0 {
		intervalHours = 8
	}
	utc := t.UTC()
	year, month, day := utc.Date()
	hour := utc.Hour()
	nextSlot := ((hour / intervalHours) + 1) * intervalHours
	if nextSlot >= 24 {
		return time.Date(year, month, day+1, nextSlot-24, 0, 0, 0, time.UTC)
	}
	return time.Date(year, month, day, nextSlot, 0, 0, 0, time.UTC)
}

// OpenExposures returns public scanner open exposure state. Read-only by design.
func (l *LiveAdapter) OpenExposures(ctx context.Context) ([]domain.OpenExposure, error) {
	return []domain.OpenExposure{}, nil
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
}

// parseLevels decodes Indodax's depth payload. Levels arrive as
// [price, quantity] where both may be strings or numbers.
func parseLevels(raw [][]any, multiplier float64) []domain.PriceLevel {
	var levels []domain.PriceLevel
	for _, item := range raw {
		if len(item) < 2 {
			continue
		}
		p := toFloat(item[0])
		q := toFloat(item[1]) * multiplier
		if p > 0 && q > 0 {
			levels = append(levels, domain.PriceLevel{Price: p, Quantity: q})
		}
	}
	return levels
}

func parseStringLevels(raw [][]string, multiplier float64) []domain.PriceLevel {
	var levels []domain.PriceLevel
	for _, item := range raw {
		if len(item) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(item[0], 64)
		q, _ := strconv.ParseFloat(item[1], 64)
		q = q * multiplier
		if p > 0 && q > 0 {
			levels = append(levels, domain.PriceLevel{Price: p, Quantity: q})
		}
	}
	return levels
}

func toFloat(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}
