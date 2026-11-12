package analysis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

type marketAdapterStub struct {
	markets []domain.Market
	books map[string]domain.OrderBook
	err error
}
func (m marketAdapterStub) SpotMarkets(context.Context)([]domain.Market,error){return m.markets,m.err}
func (m marketAdapterStub) PerpetualMarkets(context.Context)([]domain.Market,error){return nil,nil}
func (m marketAdapterStub) SpotOrderBook(_ context.Context,symbol string)(domain.OrderBook,error){
	for _,book:=range m.books{if book.Symbol==symbol{return book,nil}}
	return domain.OrderBook{},errors.New("book not found")
}
func (m marketAdapterStub) PerpetualOrderBook(context.Context,string)(domain.OrderBook,error){return domain.OrderBook{},errors.New("unsupported")}
func (m marketAdapterStub) FundingRate(context.Context,string)(domain.FundingRate,error){return domain.FundingRate{},errors.New("unsupported")}
func (m marketAdapterStub) OpenExposures(context.Context)([]domain.OpenExposure,error){return nil,nil}

func TestAnalysisServiceUsesVerifiedFeesAndBoundedMarketSelection(t *testing.T) {
	now:=time.Now()
	markets:=[]domain.Market{
		{Exchange:"a",Symbol:"A:BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
		{Exchange:"b",Symbol:"B:BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
		{Exchange:"c",Symbol:"ETH/USDT",BaseAsset:"ETH",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
	}
	books:=map[string]domain.OrderBook{
		"a":{Exchange:"a",Symbol:"A:BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:100,Quantity:1}},Asks:[]domain.PriceLevel{{Price:100.1,Quantity:1}}},
		"b":{Exchange:"b",Symbol:"B:BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:102,Quantity:1}},Asks:[]domain.PriceLevel{{Price:102.1,Quantity:1}}},
	}
	adapter:=marketAdapterStub{markets:markets,books:books}
	engine,err:=NewEngine(CrossVenueSpotStrategy{MinNetReturnPct:.1})
	if err!=nil{t.Fatal(err)}
	resolver:=func(_ context.Context,exchange,symbol string)(FeeSchedule,bool,error){
		return FeeSchedule{TakerPct:.1},true,nil
	}
	service,err:=NewService(adapter,engine,resolver,ServiceConfig{QuoteAssets:[]string{"USDT"},MaxMarkets:10,MaxConcurrency:2,MaxBookAge:time.Second,EstimatedSlippagePct:.05})
	if err!=nil{t.Fatal(err)}
	result,err:=service.Analyze(context.Background())
	if err!=nil{t.Fatal(err)}
	if !result.Complete||result.SelectedMarkets!=2||result.FetchedBooks!=2||len(result.Signals)!=1{
		t.Fatalf("unexpected analysis result: %+v",result)
	}
}

func TestAnalysisServiceFailsClosedWhenFeeProfileMissing(t *testing.T) {
	now:=time.Now()
	adapter:=marketAdapterStub{
		markets:[]domain.Market{
			{Exchange:"a",Symbol:"A:BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
			{Exchange:"b",Symbol:"B:BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
		},
		books:map[string]domain.OrderBook{
			"a":{Exchange:"a",Symbol:"A:BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:100,Quantity:1}},Asks:[]domain.PriceLevel{{Price:100.1,Quantity:1}}},
			"b":{Exchange:"b",Symbol:"B:BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:102,Quantity:1}},Asks:[]domain.PriceLevel{{Price:102.1,Quantity:1}}},
		},
	}
	engine,err:=NewEngine(CrossVenueSpotStrategy{})
	if err!=nil{t.Fatal(err)}
	service,err:=NewService(adapter,engine,func(context.Context,string,string)(FeeSchedule,bool,error){return FeeSchedule{},false,nil},ServiceConfig{})
	if err!=nil{t.Fatal(err)}
	result,err:=service.Analyze(context.Background())
	if err!=nil{t.Fatal(err)}
	if result.Complete||len(result.Signals)!=0||len(result.Errors)==0{
		t.Fatalf("missing verified fee profile must fail closed: %+v",result)
	}
}
