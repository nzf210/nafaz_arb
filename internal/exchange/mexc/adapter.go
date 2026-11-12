package mexc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
)

type Adapter struct {
	client *Client
	source contracts.MarketDataAdapter
	enabled bool
	onMarkets func([]domain.Market)
	onOutcome func(error)
	mu sync.RWMutex
	markets []domain.Market
	spotMarkets []domain.Market
	spotFetchedAt time.Time
	contractSizes map[string]float64
	fetchedAt time.Time
}

func NewAdapter(client *Client,source contracts.MarketDataAdapter,enabled bool,onMarkets func([]domain.Market),onOutcome func(error))*Adapter{
	return &Adapter{client:client,source:source,enabled:enabled,onMarkets:onMarkets,onOutcome:onOutcome,contractSizes:make(map[string]float64)}
}
func(a *Adapter)SpotMarkets(ctx context.Context)([]domain.Market,error){
	a.mu.RLock()
	if len(a.spotMarkets)>0&&time.Since(a.spotFetchedAt)<time.Hour{cached:=append([]domain.Market(nil),a.spotMarkets...);a.mu.RUnlock();return cached,nil}
	a.mu.RUnlock()
	if a.client==nil{err:=fmt.Errorf("mexc client is not configured");if a.onOutcome!=nil{a.onOutcome(err)};return nil,err}
	markets,err:=a.client.SpotMarkets(ctx)
	if a.onOutcome!=nil{a.onOutcome(err)}
	if err!=nil{a.mu.RLock();cached:=append([]domain.Market(nil),a.spotMarkets...);a.mu.RUnlock();if len(cached)>0{return cached,nil};return nil,err}
	if len(markets)>0{a.mu.Lock();a.spotMarkets=append([]domain.Market(nil),markets...);a.spotFetchedAt=time.Now().UTC();a.mu.Unlock()}
	return markets,nil
}
func(a *Adapter)PerpetualMarkets(ctx context.Context)([]domain.Market,error){
	if !a.enabled{return nil,nil}
	a.mu.RLock();if len(a.markets)>0&&time.Since(a.fetchedAt)<time.Hour{cached:=append([]domain.Market(nil),a.markets...);a.mu.RUnlock();return cached,nil};a.mu.RUnlock()
	if a.client==nil{err:=fmt.Errorf("mexc client is not configured");if a.onOutcome!=nil{a.onOutcome(err)};return nil,err}
	markets,err:=a.client.PerpetualMarkets(ctx)
	if a.onOutcome!=nil{a.onOutcome(err)}
	if err!=nil{a.mu.RLock();cached:=append([]domain.Market(nil),a.markets...);a.mu.RUnlock();if len(cached)>0{return cached,nil};return nil,err}
	if len(markets)>0{
		a.mu.Lock();a.markets=append([]domain.Market(nil),markets...);a.fetchedAt=time.Now().UTC()
		for _,m:=range markets{if m.AmountSpec!=nil&&m.AmountSpec.BasePerUnit>0{a.contractSizes[m.Symbol]=m.AmountSpec.BasePerUnit}}
		a.mu.Unlock()
		if a.onMarkets!=nil{a.onMarkets(markets)}
	}
	return markets,nil
}
func(a *Adapter)SpotOrderBook(ctx context.Context,symbol string)(domain.OrderBook,error){if a.source!=nil{return a.source.SpotOrderBook(ctx,symbol)};if a.client==nil{return domain.OrderBook{},fmt.Errorf("mexc client is not configured")};return a.client.SpotOrderBook(ctx,symbol)}
func(a *Adapter)PerpetualOrderBook(ctx context.Context,symbol string)(domain.OrderBook,error){
	if a.source!=nil{return a.source.PerpetualOrderBook(ctx,symbol)}
	a.mu.RLock();size:=a.contractSizes[symbol];a.mu.RUnlock()
	if a.client==nil{return domain.OrderBook{},fmt.Errorf("mexc client is not configured")}
	if size<=0{return domain.OrderBook{},fmt.Errorf("mexc contract size is unknown for %s",symbol)}
	return a.client.PerpetualOrderBook(ctx,symbol,size)
}
func(a *Adapter)FundingRate(ctx context.Context,symbol string)(domain.FundingRate,error){if a.source!=nil{return a.source.FundingRate(ctx,symbol)};if a.client==nil{return domain.FundingRate{},fmt.Errorf("mexc client is not configured")};return a.client.FundingRate(ctx,symbol)}
func(a *Adapter)OpenExposures(ctx context.Context)([]domain.OpenExposure,error){if a.source==nil{return nil,fmt.Errorf("executor exposure provider is unavailable")};return a.source.OpenExposures(ctx)}
func(a *Adapter)ExchangeAvailable(name string)bool{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.ExchangeAvailable("mexc")};return true}
func(a *Adapter)WSConnectionHealthy(name string)bool{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.WSConnectionHealthy("mexc")};return true}
func(a *Adapter)LastBookUpdate(name,symbol string)time.Time{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.LastBookUpdate("mexc",symbol)};return time.Time{}}
func(a *Adapter)AccountAvailable(name string)(contracts.AccountStatus,error){if h,ok:=a.source.(contracts.AccountStatusProvider);ok{return h.AccountAvailable("mexc")};return contracts.AccountStatus{DepositEnabled:true,WithdrawalEnabled:true},nil}

var _ contracts.MarketDataAdapter=(*Adapter)(nil)
var _ contracts.AccountStatusProvider=(*Adapter)(nil)
var _ contracts.HealthReporter=(*Adapter)(nil)
