package venueadapter

import (
	"context"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

type source struct{}
func (source) SpotMarkets(context.Context) ([]domain.Market,error) {
	return []domain.Market{
		{Exchange:"indodax",Symbol:"btcusdt",MarketType:"SPOT"},
		{Exchange:"tokocrypto",Symbol:"toko:BTCUSDT",MarketType:"SPOT"},
	},nil
}
func (source) PerpetualMarkets(context.Context) ([]domain.Market,error) {
	return []domain.Market{
		{Exchange:"okx",Symbol:"BTC-USDT-SWAP",MarketType:"PERPETUAL"},
		{Exchange:"mexc",Symbol:"mexc:BTC_USDT",MarketType:"PERPETUAL"},
	},nil
}
func (source) SpotOrderBook(_ context.Context,s string)(domain.OrderBook,error){return domain.OrderBook{Symbol:s},nil}
func (source) PerpetualOrderBook(_ context.Context,s string)(domain.OrderBook,error){return domain.OrderBook{Symbol:s},nil}
func (source) FundingRate(context.Context,string)(domain.FundingRate,error){return domain.FundingRate{},nil}
func (source) OpenExposures(context.Context)([]domain.OpenExposure,error){return nil,nil}

func TestVenueAdapterScopesMarketDiscovery(t *testing.T) {
	a,err:=New("tokocrypto",source{});if err!=nil{t.Fatal(err)}
	spot,err:=a.SpotMarkets(context.Background());if err!=nil{t.Fatal(err)}
	if len(spot)!=1||spot[0].Exchange!="tokocrypto"{t.Fatalf("unexpected spot markets: %+v",spot)}
	perp,err:=a.PerpetualMarkets(context.Background());if err!=nil{t.Fatal(err)}
	if len(perp)!=0{t.Fatalf("Tokocrypto should not inherit another venue's perpetual markets: %+v",perp)}
}

func TestVenueAdapterRejectsCrossVenueSymbols(t *testing.T) {
	a,err:=New("okx",source{});if err!=nil{t.Fatal(err)}
	if _,err:=a.PerpetualOrderBook(context.Background(),"mexc:BTC_USDT");err==nil{t.Fatal("expected cross-venue symbol rejection")}
	if _,err:=a.PerpetualOrderBook(context.Background(),"BTC-USDT-SWAP");err!=nil{t.Fatal(err)}
}
