package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type dohResponse struct {
	Answer []struct {
		Data string `json:"data"`
		Type int    `json:"type"`
	} `json:"Answer"`
}

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
