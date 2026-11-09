package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

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
			l.SubscribeOKXBooks(okxMarkets)
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

func (l *LiveAdapter) fetchOKXPerpetualMarkets(ctx context.Context) ([]domain.Market, error) {
	if cached, ok := l.getCachedOKXPerp(); ok {
		return cached, nil
	}

	endpoint := "/api/v5/public/instruments?instType=SWAP"

	resp, err := l.okxDo(ctx, endpoint, func() (*http.Request, error) {
		return l.newOKXRequest(ctx, endpoint)
	})
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
