package registry

import (
	"context"
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
