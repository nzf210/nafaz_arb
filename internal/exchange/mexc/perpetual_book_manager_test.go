package mexc

import (
	"testing"
	"time"
)

func TestPerpetualBookManagerVersionsAndContractScaling(t *testing.T){
	m:=NewPerpetualBookManager(10)
	err:=m.Bootstrap("BTC_USDT",PerpetualDepthSnapshot{Version:10,Bids:[][]float64{{100,2}},Asks:[][]float64{{101,3}}},0.01)
	if err!=nil{t.Fatal(err)}
	ok,err:=m.Apply(PerpetualDepthUpdate{Symbol:"BTC_USDT",Begin:11,End:12,Bids:[][]float64{{100,4},{99,1}},Asks:[][]float64{{101,0}},ContractSize:0.01,Timestamp:time.Now()})
	if err!=nil||!ok{t.Fatalf("apply: ok=%v err=%v",ok,err)}
	book,ok:=m.Get("BTC_USDT",time.Minute);if !ok{t.Fatal("expected book")}
	if book.SequenceID!=12||book.Bids[0].Quantity!=0.04||len(book.Asks)!=0{t.Fatalf("unexpected normalized book: %+v",book)}
}

func TestPerpetualBookManagerInvalidatesOnVersionGap(t *testing.T){
	m:=NewPerpetualBookManager(10)
	_ = m.Bootstrap("ETH_USDT",PerpetualDepthSnapshot{Version:4,Bids:[][]float64{{10,1}},Asks:[][]float64{{11,1}}},1)
	if _,err:=m.Apply(PerpetualDepthUpdate{Symbol:"ETH_USDT",Begin:6,End:6,Bids:[][]float64{{10,1}},Asks:[][]float64{}});err==nil{t.Fatal("expected gap")}
	if _,ok:=m.Get("ETH_USDT",time.Minute);ok{t.Fatal("gap must invalidate book")}
}
