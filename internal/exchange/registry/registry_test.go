package registry

import (
	"context"
	"fmt"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

type stub struct{name string}
func(s stub)SpotMarkets(context.Context)([]domain.Market,error){return []domain.Market{{Exchange:s.name,Symbol:s.name+"spot",MarketType:"SPOT",Active:true}},nil}
func(s stub)PerpetualMarkets(context.Context)([]domain.Market,error){return []domain.Market{{Exchange:s.name,Symbol:s.name+"-USDT-SWAP",MarketType:"PERPETUAL",Active:true}},nil}
func(s stub)SpotOrderBook(_ context.Context,symbol string)(domain.OrderBook,error){return domain.OrderBook{Exchange:s.name,Symbol:symbol},nil}
func(s stub)PerpetualOrderBook(_ context.Context,symbol string)(domain.OrderBook,error){return domain.OrderBook{Exchange:s.name,Symbol:symbol},nil}
func(s stub)FundingRate(context.Context,string)(domain.FundingRate,error){return domain.FundingRate{Rate:0.01},nil}
func(s stub)OpenExposures(context.Context)([]domain.OpenExposure,error){return nil,nil}

func TestCompositeAggregatesAndRoutes(t *testing.T){
	r:=New()
	for _,name:=range []string{"indodax","okx","mexc","tokocrypto"}{if err:=r.Register(name,stub{name});err!=nil{t.Fatal(err)}}
	c:=NewComposite(r)
	spot,err:=c.SpotMarkets(context.Background());if err!=nil||len(spot)!=4{t.Fatalf("spot=%d err=%v",len(spot),err)}
	perp,err:=c.PerpetualMarkets(context.Background());if err!=nil||len(perp)!=4{t.Fatalf("perp=%d err=%v",len(perp),err)}
	book,err:=c.SpotOrderBook(context.Background(),"toko:BTCUSDT");if err!=nil||book.Exchange!="tokocrypto"{t.Fatalf("book=%+v err=%v",book,err)}
	book,err=c.PerpetualOrderBook(context.Background(),"mexc:BTC_USDT");if err!=nil||book.Exchange!="mexc"{t.Fatalf("book=%+v err=%v",book,err)}
	funding,err:=c.FundingRate(context.Background(),"BTC-USDT-SWAP");if err!=nil||funding.Rate!=0.01{t.Fatalf("funding=%+v err=%v",funding,err)}
}


type failingStub struct {
	stub
	failMarkets bool
	failExposures bool
}
func (s failingStub) SpotMarkets(ctx context.Context)([]domain.Market,error){
	if s.failMarkets{return nil,fmt.Errorf("venue discovery unavailable")}
	return s.stub.SpotMarkets(ctx)
}
func (s failingStub) OpenExposures(ctx context.Context)([]domain.OpenExposure,error){
	if s.failExposures{return nil,fmt.Errorf("exposure registry unavailable")}
	return s.stub.OpenExposures(ctx)
}

func TestCompositeFailsClosedOnPartialMarketDiscovery(t *testing.T){
	r:=New()
	if err:=r.Register("indodax",stub{"indodax"});err!=nil{t.Fatal(err)}
	if err:=r.Register("tokocrypto",failingStub{stub:stub{"tokocrypto"},failMarkets:true});err!=nil{t.Fatal(err)}
	markets,err:=NewComposite(r).SpotMarkets(context.Background())
	if err==nil||len(markets)!=1{t.Fatalf("partial discovery must return data plus error; markets=%d err=%v",len(markets),err)}
}

func TestCompositeFailsClosedOnPartialExposureState(t *testing.T){
	r:=New()
	if err:=r.Register("indodax",stub{"indodax"});err!=nil{t.Fatal(err)}
	if err:=r.Register("tokocrypto",failingStub{stub:stub{"tokocrypto"},failExposures:true});err!=nil{t.Fatal(err)}
	exposures,err:=NewComposite(r).OpenExposures(context.Background())
	if err==nil||exposures!=nil{t.Fatalf("partial exposure state must be unknown; exposures=%+v err=%v",exposures,err)}
}
