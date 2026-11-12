// Package registry composes venue adapters behind venue-neutral contracts.
package registry

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
	"sync"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
)

type Registry struct {
	mu sync.RWMutex
	adapters map[string]contracts.MarketDataAdapter
}

func New() *Registry { return &Registry{adapters:make(map[string]contracts.MarketDataAdapter)} }

func (r *Registry) Register(name string, adapter contracts.MarketDataAdapter) error {
	name=strings.ToLower(strings.TrimSpace(name))
	if name==""||adapter==nil{return fmt.Errorf("exchange name and adapter are required")}
	r.mu.Lock();defer r.mu.Unlock()
	if _,exists:=r.adapters[name];exists{return fmt.Errorf("exchange %q already registered",name)}
	r.adapters[name]=adapter
	return nil
}

func (r *Registry) Replace(name string, adapter contracts.MarketDataAdapter) error {
	name=strings.ToLower(strings.TrimSpace(name))
	if name==""||adapter==nil{return fmt.Errorf("exchange name and adapter are required")}
	r.mu.Lock();r.adapters[name]=adapter;r.mu.Unlock();return nil
}

func (r *Registry) Get(name string)(contracts.MarketDataAdapter,bool){
	r.mu.RLock();a,ok:=r.adapters[strings.ToLower(strings.TrimSpace(name))];r.mu.RUnlock();return a,ok
}

func (r *Registry) Names()[]string{
	r.mu.RLock();out:=make([]string,0,len(r.adapters));for name:=range r.adapters{out=append(out,name)};r.mu.RUnlock();sort.Strings(out);return out
}

// Composite exposes all registered venues through one scanner-facing adapter.
// It aggregates discovery and routes single-market reads by normalized venue
// symbol. It deliberately does not expose order execution.
type Composite struct{ registry *Registry }
func NewComposite(r *Registry)*Composite{return &Composite{registry:r}}
func (c *Composite) adapter(name string)(contracts.MarketDataAdapter,error){
	if c==nil||c.registry==nil{return nil,fmt.Errorf("exchange registry is not configured")}
	a,ok:=c.registry.Get(name);if !ok { a,ok=c.registry.Get("default") };if !ok{return nil,fmt.Errorf("exchange %q is not registered",name)};return a,nil
}
func (c *Composite) aggregate(ctx context.Context,perpetual bool)([]domain.Market,error){
	if c==nil||c.registry==nil{return nil,fmt.Errorf("exchange registry is not configured")}
	var out []domain.Market;var errs []string
	for _,name:=range c.registry.Names(){a,_:=c.registry.Get(name);var markets []domain.Market;var err error
		if perpetual{markets,err=a.PerpetualMarkets(ctx)}else{markets,err=a.SpotMarkets(ctx)}
		if err!=nil{errs=append(errs,name+": "+err.Error());continue};out=append(out,markets...)
	}
	if len(out)==0&&len(errs)>0{return nil,fmt.Errorf("no exchange markets available: %s",strings.Join(errs,"; "))}
	return out,nil
}
func(c *Composite)SpotMarkets(ctx context.Context)([]domain.Market,error){return c.aggregate(ctx,false)}
func(c *Composite)PerpetualMarkets(ctx context.Context)([]domain.Market,error){return c.aggregate(ctx,true)}

func venueForSymbol(symbol string,perpetual bool)string{
	s:=strings.ToLower(symbol)
	switch{
	case strings.HasPrefix(s,"toko:"):return "tokocrypto"
	case strings.HasPrefix(s,"mexc_spot:"):return "mexc"
	case strings.HasPrefix(s,"mexc:")||strings.Contains(s,"_"):return "mexc"
	case strings.HasPrefix(s,"okx:")||strings.HasSuffix(s,"-swap"):return "okx"
	case perpetual:return "okx"
	default:return "indodax"
	}
}
func(c *Composite)SpotOrderBook(ctx context.Context,symbol string)(domain.OrderBook,error){
	name:=venueForSymbol(symbol,false);a,err:=c.adapter(name);if err!=nil{return domain.OrderBook{},err};return a.SpotOrderBook(ctx,symbol)
}
func(c *Composite)PerpetualOrderBook(ctx context.Context,symbol string)(domain.OrderBook,error){
	name:=venueForSymbol(symbol,true);a,err:=c.adapter(name);if err!=nil{return domain.OrderBook{},err};return a.PerpetualOrderBook(ctx,symbol)
}
func(c *Composite)FundingRate(ctx context.Context,symbol string)(domain.FundingRate,error){
	name:=venueForSymbol(symbol,true);a,err:=c.adapter(name);if err!=nil{return domain.FundingRate{},err};return a.FundingRate(ctx,symbol)
}
func(c *Composite)OpenExposures(ctx context.Context)([]domain.OpenExposure,error){
	if c==nil||c.registry==nil{return nil,fmt.Errorf("exchange registry is not configured")}
	var out []domain.OpenExposure;var errs []string
	for _,name:=range c.registry.Names(){a,_:=c.registry.Get(name);items,err:=a.OpenExposures(ctx);if err!=nil{errs=append(errs,name+": "+err.Error());continue};out=append(out,items...)}
	if len(out)==0&&len(errs)>0{return nil,fmt.Errorf("no exposure providers available: %s",strings.Join(errs,"; "))};return out,nil
}
func(c *Composite)USDTIDRRate(ctx context.Context)(float64,error){
	a,err:=c.adapter("indodax");if err!=nil{return 0,err};provider,ok:=a.(contracts.RateProvider);if !ok{return 0,fmt.Errorf("indodax does not provide USDT/IDR conversion")};return provider.USDTIDRRate(ctx)
}


// ExchangeAvailable forwards optional venue health reporting to the selected
// adapter. Missing reporters are treated as healthy, matching scanner policy.
func (c *Composite) ExchangeAvailable(name string) bool {
	a,err:=c.adapter(name);if err!=nil{return false}
	reporter,ok:=a.(interface{ExchangeAvailable(string)bool});if !ok{return true}
	return reporter.ExchangeAvailable(name)
}

// WSConnectionHealthy forwards optional websocket health reporting.
func (c *Composite) WSConnectionHealthy(name string) bool {
	a,err:=c.adapter(name);if err!=nil{return false}
	reporter,ok:=a.(interface{WSConnectionHealthy(string)bool});if !ok{return true}
	return reporter.WSConnectionHealthy(name)
}

// LastBookUpdate forwards optional per-symbol websocket activity timestamps.
func (c *Composite) LastBookUpdate(name,symbol string) time.Time {
	a,err:=c.adapter(name);if err!=nil{return time.Time{}}
	reporter,ok:=a.(interface{LastBookUpdate(string,string)time.Time});if !ok{return time.Time{}}
	return reporter.LastBookUpdate(name,symbol)
}

// AccountAvailable forwards the optional wallet-state guard.
func (c *Composite) AccountAvailable(name string)(exchange.AccountStatus,error) {
	a,err:=c.adapter(name);if err!=nil{return exchange.AccountStatus{},err}
	reporter,ok:=a.(interface{AccountAvailable(string)(exchange.AccountStatus,error)})
	if !ok{return exchange.AccountStatus{DepositEnabled:true,WithdrawalEnabled:true},nil}
	return reporter.AccountAvailable(name)
}
