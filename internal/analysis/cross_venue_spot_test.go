package analysis

import (
	"context"
	"testing"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestCrossVenueSpotStrategyFindsNetPositiveDislocation(t *testing.T) {
	now:=time.Now()
	snapshot:=Snapshot{
		At:now, MaxBookAge:time.Second,
		Markets:[]domain.Market{
			{Exchange:"tokocrypto",Symbol:"BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
			{Exchange:"indodax",Symbol:"BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
		},
		OrderBooks:map[string]domain.OrderBook{
			BookKey("tokocrypto","BTC/USDT"):{Exchange:"tokocrypto",Symbol:"BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:100,Quantity:2}},Asks:[]domain.PriceLevel{{Price:100.1,Quantity:1}}},
			BookKey("indodax","BTC/USDT"):{Exchange:"indodax",Symbol:"BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:102,Quantity:1}},Asks:[]domain.PriceLevel{{Price:102.1,Quantity:2}}},
		},
		Fees:map[string]FeeSchedule{FeeKey("tokocrypto","BTC/USDT"):{TakerPct:.1},FeeKey("indodax","BTC/USDT"):{TakerPct:.1}},
		EstimatedSlippagePct:.05,
	}
	strategy:=CrossVenueSpotStrategy{MinNetReturnPct:.1}
	signals,err:=strategy.Analyze(context.Background(),snapshot)
	if err!=nil{t.Fatal(err)}
	if len(signals)!=1{t.Fatalf("got %d signals, want one profitable direction: %+v",len(signals),signals)}
	if signals[0].Metadata["buy_venue"]!="tokocrypto"||signals[0].Metadata["sell_venue"]!="indodax"{
		t.Fatalf("unexpected route: %+v",signals[0])
	}
	if signals[0].ExpectedReturn<=.1{t.Fatalf("fees/slippage were not applied: %+v",signals[0])}
}

func TestCrossVenueSpotStrategyRejectsStaleOrUnpricedBooks(t *testing.T) {
	now:=time.Now()
	snapshot:=Snapshot{
		At:now,MaxBookAge:time.Second,
		Markets:[]domain.Market{
			{Exchange:"a",Symbol:"BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
			{Exchange:"b",Symbol:"BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
		},
		OrderBooks:map[string]domain.OrderBook{
			BookKey("a","BTC/USDT"):{Exchange:"a",Symbol:"BTC/USDT",Timestamp:now.Add(-time.Minute),Bids:[]domain.PriceLevel{{Price:100,Quantity:1}},Asks:[]domain.PriceLevel{{Price:101,Quantity:1}}},
			BookKey("b","BTC/USDT"):{Exchange:"b",Symbol:"BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:103,Quantity:1}},Asks:[]domain.PriceLevel{{Price:104,Quantity:1}}},
		},
		Fees:map[string]FeeSchedule{FeeKey("a","BTC/USDT"):{TakerPct:0},FeeKey("b","BTC/USDT"):{TakerPct:0}},
	}
	signals,err:= (CrossVenueSpotStrategy{}).Analyze(context.Background(),snapshot)
	if err!=nil{t.Fatal(err)}
	if len(signals)!=0{t.Fatalf("stale book must not produce signals: %+v",signals)}
}

func TestCrossVenueSpotStrategyRequiresFeeAndValidSnapshot(t *testing.T) {
	strategy:=CrossVenueSpotStrategy{}
	if _,err:=strategy.Analyze(context.Background(),Snapshot{});err==nil{t.Fatal("missing snapshot time must fail")}
	now:=time.Now()
	snapshot:=Snapshot{At:now,Markets:[]domain.Market{
		{Exchange:"a",Symbol:"BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
		{Exchange:"b",Symbol:"BTC/USDT",BaseAsset:"BTC",QuoteAsset:"USDT",MarketType:"SPOT",Active:true},
	},OrderBooks:map[string]domain.OrderBook{
		BookKey("a","BTC/USDT"):{Exchange:"a",Symbol:"BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:100,Quantity:1}},Asks:[]domain.PriceLevel{{Price:101,Quantity:1}}},
		BookKey("b","BTC/USDT"):{Exchange:"b",Symbol:"BTC/USDT",Timestamp:now,Bids:[]domain.PriceLevel{{Price:103,Quantity:1}},Asks:[]domain.PriceLevel{{Price:104,Quantity:1}}},
	}}
	signals,err:=strategy.Analyze(context.Background(),snapshot)
	if err!=nil{t.Fatal(err)}
	if len(signals)!=0{t.Fatal("missing venue fee schedules must fail closed")}
}
