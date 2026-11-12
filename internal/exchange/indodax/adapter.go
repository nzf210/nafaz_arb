package indodax

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
	onOutcome func(error)
	mu sync.RWMutex
	markets []domain.Market
	fetchedAt time.Time
}

func NewAdapter(client *Client, source contracts.MarketDataAdapter, onOutcome func(error)) *Adapter {
	return &Adapter{client:client,source:source,onOutcome:onOutcome}
}

func (a *Adapter) SpotMarkets(ctx context.Context)([]domain.Market,error){
	a.mu.RLock()
	if len(a.markets)>0&&time.Since(a.fetchedAt)<time.Hour{cached:=append([]domain.Market(nil),a.markets...);a.mu.RUnlock();return cached,nil}
	a.mu.RUnlock()
	if a.client==nil{err:=fmt.Errorf("indodax client is not configured");if a.onOutcome!=nil{a.onOutcome(err)};return nil,err}
	markets,err:=a.client.Markets(ctx)
	if a.onOutcome!=nil{a.onOutcome(err)}
	if err!=nil{
		a.mu.RLock();cached:=append([]domain.Market(nil),a.markets...);a.mu.RUnlock()
		if len(cached)>0{return cached,nil}
		return nil,err
	}
	if len(markets)>0{a.mu.Lock();a.markets=append([]domain.Market(nil),markets...);a.fetchedAt=time.Now().UTC();a.mu.Unlock()}
	return markets,nil
}

func (*Adapter) PerpetualMarkets(context.Context)([]domain.Market,error){return nil,nil}
func (a *Adapter) SpotOrderBook(ctx context.Context,symbol string)(domain.OrderBook,error){
	if a.source!=nil{return a.source.SpotOrderBook(ctx,symbol)}
	if a.client==nil{return domain.OrderBook{},fmt.Errorf("indodax client is not configured")}
	return a.client.OrderBook(ctx,symbol)
}
func (*Adapter) PerpetualOrderBook(context.Context,string)(domain.OrderBook,error){return domain.OrderBook{},fmt.Errorf("indodax perpetual markets are unsupported")}
func (*Adapter) FundingRate(context.Context,string)(domain.FundingRate,error){return domain.FundingRate{},fmt.Errorf("indodax funding is unsupported")}
func (a *Adapter) OpenExposures(ctx context.Context)([]domain.OpenExposure,error){
	if a.source==nil{return nil,fmt.Errorf("executor exposure provider is unavailable")}
	return a.source.OpenExposures(ctx)
}
func (a *Adapter) USDTIDRRate(ctx context.Context)(float64,error){
	if provider,ok:=a.source.(contracts.RateProvider);ok{return provider.USDTIDRRate(ctx)}
	if a.client==nil{return 0,fmt.Errorf("indodax client is not configured")}
	return a.client.USDTIDRRate(ctx)
}
func (a *Adapter) ExchangeAvailable(_ string)bool{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.ExchangeAvailable("indodax")};return false}
func (a *Adapter) WSConnectionHealthy(_ string)bool{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.WSConnectionHealthy("indodax")};return false}
func (a *Adapter) LastBookUpdate(_ string,symbol string)time.Time{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.LastBookUpdate("indodax",symbol)};return time.Time{}}
func (a *Adapter) AccountAvailable(_ string)(contracts.AccountStatus,error){
	if h,ok:=a.source.(contracts.AccountStatusProvider);ok{return h.AccountAvailable("indodax")}
	return contracts.AccountStatus{},fmt.Errorf("indodax account status provider is unavailable")
}

var _ contracts.MarketDataAdapter=(*Adapter)(nil)
var _ contracts.RateProvider=(*Adapter)(nil)
var _ contracts.AccountStatusProvider=(*Adapter)(nil)
var _ contracts.HealthReporter=(*Adapter)(nil)
