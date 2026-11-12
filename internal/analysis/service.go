package analysis

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
)

type FeeResolver func(context.Context, string, string) (FeeSchedule, bool, error)

type ServiceConfig struct {
	QuoteAssets []string
	MaxMarkets int
	MaxConcurrency int
	MaxBookAge time.Duration
	MinInterval time.Duration
	EstimatedSlippagePct float64
}

type Result struct {
	StartedAt time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Complete bool `json:"complete"`
	DiscoveredMarkets int `json:"discovered_markets"`
	SelectedMarkets int `json:"selected_markets"`
	FetchedBooks int `json:"fetched_books"`
	Signals []Signal `json:"signals"`
	Errors []string `json:"errors,omitempty"`
}

// Service performs an on-demand analysis scan. It only reads public spot books
// and verified fee snapshots; it never publishes executor signals or orders.
type Service struct {
	adapter contracts.MarketDataAdapter
	engine *Engine
	resolveFee FeeResolver
	cfg ServiceConfig
	mu sync.Mutex
	inFlight bool
	lastStarted time.Time
}

func NewService(adapter contracts.MarketDataAdapter, engine *Engine, resolver FeeResolver, cfg ServiceConfig) (*Service, error) {
	if adapter == nil || engine == nil || resolver == nil {
		return nil, fmt.Errorf("market adapter, analysis engine and verified fee resolver are required")
	}
	if cfg.MaxMarkets <= 0 { cfg.MaxMarkets = 40 }
	if cfg.MaxConcurrency <= 0 { cfg.MaxConcurrency = 4 }
	if cfg.MaxBookAge <= 0 { cfg.MaxBookAge = 3*time.Second }
	if cfg.MinInterval <= 0 { cfg.MinInterval = 15*time.Second }
	return &Service{adapter:adapter,engine:engine,resolveFee:resolver,cfg:cfg},nil
}

func (s *Service) Analyze(ctx context.Context) (Result,error) {
	started:=time.Now().UTC()
	result:=Result{StartedAt:started,Signals:[]Signal{},Complete:true}
	s.mu.Lock()
	if s.inFlight {
		s.mu.Unlock()
		result.Complete=false
		result.CompletedAt=time.Now().UTC()
		return result,fmt.Errorf("analysis scan already in progress")
	}
	if !s.lastStarted.IsZero() && started.Sub(s.lastStarted)<s.cfg.MinInterval {
		wait:=s.cfg.MinInterval-started.Sub(s.lastStarted)
		s.mu.Unlock()
		result.Complete=false
		result.CompletedAt=time.Now().UTC()
		return result,fmt.Errorf("analysis scan cooldown active; retry in %s",wait.Round(time.Second))
	}
	s.inFlight=true
	s.lastStarted=started
	s.mu.Unlock()
	defer func(){s.mu.Lock();s.inFlight=false;s.mu.Unlock()}()
	markets,err:=s.adapter.SpotMarkets(ctx)
	if err!=nil { result.Complete=false;result.Errors=append(result.Errors,err.Error());result.CompletedAt=time.Now().UTC();return result,fmt.Errorf("spot market discovery: %w",err) }
	result.DiscoveredMarkets=len(markets)
	selected:=s.selectCrossVenueMarkets(markets)
	result.SelectedMarkets=len(selected)
	if len(selected)==0 { result.CompletedAt=time.Now().UTC();return result,nil }

	snapshot:=Snapshot{
		At:time.Now().UTC(),Markets:selected,
		OrderBooks:make(map[string]domain.OrderBook,len(selected)),
		Fees:make(map[string]FeeSchedule,len(selected)),
		MaxBookAge:s.cfg.MaxBookAge,
		EstimatedSlippagePct:s.cfg.EstimatedSlippagePct,
	}
	sem:=make(chan struct{},s.cfg.MaxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, market:=range selected {
		market:=market
		wg.Add(1)
		go func(){
			defer wg.Done()
			select {
			case sem<-struct{}{}:
			case <-ctx.Done():
				mu.Lock();result.Errors=append(result.Errors,ctx.Err().Error());mu.Unlock();return
			}
			defer func(){<-sem}()
			book,bookErr:=s.adapter.SpotOrderBook(ctx,market.Symbol)
			if bookErr!=nil {
				mu.Lock();result.Errors=append(result.Errors,fmt.Sprintf("%s:%s book: %v",market.Exchange,market.Symbol,bookErr));mu.Unlock();return
			}
			if !strings.EqualFold(book.Exchange,market.Exchange)||!strings.EqualFold(book.Symbol,market.Symbol) {
				mu.Lock();result.Errors=append(result.Errors,fmt.Sprintf("%s:%s returned mismatched order book identity",market.Exchange,market.Symbol));mu.Unlock();return
			}
			if !validAnalysisBook(book, snapshot.At, s.cfg.MaxBookAge) {
				mu.Lock();result.Errors=append(result.Errors,fmt.Sprintf("%s:%s returned a stale or invalid order book",market.Exchange,market.Symbol));mu.Unlock();return
			}
			fee,ok,feeErr:=s.resolveFee(ctx,market.Exchange,market.BaseAsset+"/"+market.QuoteAsset)
			if feeErr!=nil {
				mu.Lock();result.Errors=append(result.Errors,fmt.Sprintf("%s:%s fee profile: %v",market.Exchange,market.Symbol,feeErr));mu.Unlock();return
			}
			if !ok {
				mu.Lock();result.Errors=append(result.Errors,fmt.Sprintf("%s:%s has no fresh verified exchange fee profile",market.Exchange,market.Symbol));mu.Unlock();return
			}
			if !validPercentage(fee.TakerPct) || !validPercentage(fee.MakerPct) {
				mu.Lock();result.Errors=append(result.Errors,fmt.Sprintf("%s:%s returned invalid fee percentages",market.Exchange,market.Symbol));mu.Unlock();return
			}
			mu.Lock()
			snapshot.OrderBooks[BookKey(market.Exchange,market.Symbol)]=book
			snapshot.Fees[FeeKey(market.Exchange,market.Symbol)]=fee
			result.FetchedBooks++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if err:=ctx.Err();err!=nil { result.Complete=false;result.Errors=append(result.Errors,err.Error()) }
	signals,failures:=s.engine.Analyze(ctx,snapshot)
	result.Signals=signals
	for _,failure:=range failures { result.Errors=append(result.Errors,failure.Error()) }
	result.Complete=result.Complete&&len(result.Errors)==0&&result.FetchedBooks==len(selected)
	result.CompletedAt=time.Now().UTC()
	return result,nil
}

func (s *Service) selectCrossVenueMarkets(markets []domain.Market) []domain.Market {
	quotes:=make(map[string]bool,len(s.cfg.QuoteAssets))
	for _,quote:=range s.cfg.QuoteAssets { quotes[strings.ToUpper(strings.TrimSpace(quote))]=true }
	groups:=make(map[string][]domain.Market)
	for _,market:=range markets {
		if !market.Active||!strings.EqualFold(market.MarketType,"SPOT")||market.Exchange==""||market.Symbol==""||market.BaseAsset==""||market.QuoteAsset=="" { continue }
		if len(quotes)>0&&!quotes[strings.ToUpper(market.QuoteAsset)] { continue }
		key:=strings.ToUpper(market.BaseAsset)+"/"+strings.ToUpper(market.QuoteAsset)
		groups[key]=append(groups[key],market)
	}
	keys:=make([]string,0,len(groups));for key:=range groups{keys=append(keys,key)};sort.Strings(keys)
	selected:=make([]domain.Market,0)
	for _,key:=range keys {
		group:=groups[key]
		venues:=make(map[string]bool)
		for _,market:=range group{venues[strings.ToLower(market.Exchange)]=true}
		if len(venues)<2 { continue }
		sort.Slice(group,func(i,j int)bool{
			if group[i].Exchange==group[j].Exchange{return group[i].Symbol<group[j].Symbol}
			return group[i].Exchange<group[j].Exchange
		})
		for _,market:=range group {
			if s.cfg.MaxMarkets>0&&len(selected)>=s.cfg.MaxMarkets { return selected }
			selected=append(selected,market)
		}
	}
	return selected
}

func validAnalysisBook(book domain.OrderBook, at time.Time, maxAge time.Duration) bool {
	stamp:=book.ReceivedAt
	if stamp.IsZero(){stamp=book.Timestamp}
	if stamp.IsZero()||stamp.After(at.Add(time.Second))||at.Sub(stamp)>maxAge||len(book.Bids)==0||len(book.Asks)==0{return false}
	bid,ask:=book.Bids[0],book.Asks[0]
	return bid.Price>0&&ask.Price>0&&bid.Quantity>0&&ask.Quantity>0&&bid.Price<ask.Price
}
