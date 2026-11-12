package tokocrypto

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
	onMarkets func([]domain.Market)
	mu sync.RWMutex
	markets []domain.Market
	fetchedAt time.Time
}

func NewAdapter(client *Client,source contracts.MarketDataAdapter,onMarkets func([]domain.Market))*Adapter{
	return &Adapter{client:client,source:source,onMarkets:onMarkets}
}
func(a *Adapter)SpotMarkets(ctx context.Context)([]domain.Market,error){
	a.mu.RLock()
	if len(a.markets)>0&&time.Since(a.fetchedAt)<time.Hour{cached:=append([]domain.Market(nil),a.markets...);a.mu.RUnlock();return cached,nil}
	a.mu.RUnlock()
	if a.client==nil{return nil,fmt.Errorf("tokocrypto client is not configured")}
	markets,err:=a.client.Markets(ctx)
	if err!=nil{a.mu.RLock();cached:=append([]domain.Market(nil),a.markets...);a.mu.RUnlock();if len(cached)>0{return cached,nil};return nil,err}
	if len(markets)>0{
		a.mu.Lock();a.markets=append([]domain.Market(nil),markets...);a.fetchedAt=time.Now().UTC();a.mu.Unlock()
		if a.onMarkets!=nil{a.onMarkets(markets)}
	}
	return markets,nil
}
func(*Adapter)PerpetualMarkets(context.Context)([]domain.Market,error){return nil,nil}
func(a *Adapter)SpotOrderBook(ctx context.Context,symbol string)(domain.OrderBook,error){
	if a.source!=nil{return a.source.SpotOrderBook(ctx,symbol)}
	if a.client==nil{return domain.OrderBook{},fmt.Errorf("tokocrypto client is not configured")}
	return a.client.OrderBook(ctx,symbol)
}
func(*Adapter)PerpetualOrderBook(context.Context,string)(domain.OrderBook,error){return domain.OrderBook{},fmt.Errorf("tokocrypto perpetual markets are unsupported")}
func(*Adapter)FundingRate(context.Context,string)(domain.FundingRate,error){return domain.FundingRate{},fmt.Errorf("tokocrypto funding is unsupported")}
func(a *Adapter)OpenExposures(ctx context.Context)([]domain.OpenExposure,error){if a.source==nil{return nil,fmt.Errorf("executor exposure provider is unavailable")};return a.source.OpenExposures(ctx)}
func(a *Adapter)ExchangeAvailable(name string)bool{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.ExchangeAvailable("tokocrypto")};return true}
func(a *Adapter)WSConnectionHealthy(name string)bool{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.WSConnectionHealthy("tokocrypto")};return true}
func(a *Adapter)LastBookUpdate(name,symbol string)time.Time{if h,ok:=a.source.(contracts.HealthReporter);ok{return h.LastBookUpdate("tokocrypto",symbol)};return time.Time{}}
func(a *Adapter)AccountAvailable(name string)(contracts.AccountStatus,error){if h,ok:=a.source.(contracts.AccountStatusProvider);ok{return h.AccountAvailable("tokocrypto")};return contracts.AccountStatus{DepositEnabled:true,WithdrawalEnabled:true},nil}

var _ contracts.MarketDataAdapter=(*Adapter)(nil)
var _ contracts.AccountStatusProvider=(*Adapter)(nil)
var _ contracts.HealthReporter=(*Adapter)(nil)
