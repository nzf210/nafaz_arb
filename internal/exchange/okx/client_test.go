package okx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientNormalizesInstruments(t *testing.T) {
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.URL.Path!="/api/v5/public/instruments"{t.Fatalf("path=%s",r.URL.Path)}
		_,_=w.Write([]byte(`{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","ctVal":"0.01","ctValCcy":"BTC","settleCcy":"USDT","state":"live","tickSz":"0.1","lotSz":"1","minSz":"1"}]}`))
	}))
	defer server.Close()
	c:=New(server.Client(),server.URL);c.SetLimiterConfig(OKXLimiterConfig{InstrumentsRatePer2s:100,MaxConcurrent:2})
	markets,err:=c.PerpetualMarkets(context.Background());if err!=nil{t.Fatal(err)}
	if len(markets)!=1||markets[0].AmountSpec==nil||markets[0].AmountSpec.BasePerUnit!=0.01{t.Fatalf("markets=%+v",markets)}
}

func TestClientFundingAndDepthNormalization(t *testing.T) {
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		switch r.URL.Path{
		case "/api/v5/public/funding-rate":
			_,_=w.Write([]byte(`{"code":"0","msg":"","data":[{"fundingRate":"0.0002","fundingTime":"1790000000000","prevFundingTime":"1789971200000","ts":"1789990000000"}]}`))
		case "/api/v5/market/books":
			_,_=w.Write([]byte(`{"code":"0","msg":"","data":[{"bids":[["100","2","0","1"]],"asks":[["101","3","0","1"]],"ts":"1790000000000"}]}`))
		default:http.NotFound(w,r)
		}
	}))
	defer server.Close()
	c:=New(server.Client(),server.URL);c.SetLimiterConfig(OKXLimiterConfig{FundingRatePer2s:100,BooksRatePer2s:100,MaxConcurrent:2})
	funding,err:=c.FundingRate(context.Background(),"okx:BTC-USDT-SWAP");if err!=nil{t.Fatal(err)}
	if funding.Rate!=0.0002||funding.IntervalHours!=8||funding.NextFundingTime.IsZero(){t.Fatalf("funding=%+v",funding)}
	book,err:=c.PerpetualOrderBook(context.Background(),"BTC-USDT-SWAP",0.01);if err!=nil{t.Fatal(err)}
	if len(book.Bids)!=1||book.Bids[0].Quantity!=0.02||book.Source!="exchange_timestamp"{t.Fatalf("book=%+v",book)}
}

func TestBookManagerRequiresSnapshotAndTracksSequence(t *testing.T) {
	m:=NewBookManager("ws://127.0.0.1:1",nil,nil,time.Millisecond,time.Second)
	frame:=[]byte(`{"action":"snapshot","arg":{"instId":"BTC-USDT-SWAP"},"data":[{"seqId":10,"prevSeqId":-1,"ts":"1790000000000","bids":[["100","2","0","1"]],"asks":[["101","3","0","1"]]}]}`)
	if _,skip,err:=m.ApplyFrame(frame,time.Now());err!=nil||skip{t.Fatalf("snapshot skip=%v err=%v",skip,err)}
	book,ok:=m.Get("BTC-USDT-SWAP",time.Second)
	if !ok||book.SequenceID!=10||len(book.Bids)!=1{t.Fatalf("book=%+v ok=%v",book,ok)}
}
