package mexc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPerpetualMarketsNormalizeContractRules(t *testing.T) {
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.URL.Path!="/api/v1/contract/detail"{t.Fatalf("path=%s",r.URL.Path)}
		_,_=w.Write([]byte(`{"success":true,"code":0,"data":[{"symbol":"BTC_USDT","baseCoin":"BTC","quoteCoin":"USDT","contractSize":0.001,"priceUnit":0.1,"volUnit":1,"minVol":1,"state":0},{"symbol":"ETH_USDT","baseCoin":"ETH","quoteCoin":"USDT","contractSize":0.01,"volUnit":1,"minVol":1,"state":1}]}`))
	}))
	defer server.Close()
	client:=New(server.Client(),server.URL,server.URL)
	markets,err:=client.PerpetualMarkets(context.Background())
	if err!=nil{t.Fatal(err)}
	if len(markets)!=1{t.Fatalf("markets=%d",len(markets))}
	m:=markets[0]
	if m.Symbol!="mexc:BTC_USDT"||m.AmountSpec==nil||m.AmountSpec.BasePerUnit!=0.001{t.Fatalf("market normalization: %+v",m)}
}

func TestFundingRateUsesExchangeTimestamp(t *testing.T) {
	const timestamp int64=1790000000000
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.URL.Path!="/api/v1/contract/funding_rate/BTC_USDT"{t.Fatalf("path=%s",r.URL.Path)}
		_,_=w.Write([]byte(`{"success":true,"code":0,"data":{"fundingRate":0.0001,"collectCycle":8,"timestamp":1790000000000}}`))
	}))
	defer server.Close()
	rate,err:=New(server.Client(),server.URL,server.URL).FundingRate(context.Background(),"mexc:BTC_USDT")
	if err!=nil{t.Fatal(err)}
	if rate.Rate!=0.0001||rate.IntervalHours!=8||!rate.Timestamp.Equal(time.UnixMilli(timestamp).UTC()){t.Fatalf("funding normalization: %+v",rate)}
}

func TestPerpetualOrderBookScalesContracts(t *testing.T) {
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		_,_=w.Write([]byte(`{"success":true,"code":0,"data":{"bids":[[100,2]],"asks":[[101,3]],"timestamp":1790000000000}}`))
	}))
	defer server.Close()
	book,err:=New(server.Client(),server.URL,server.URL).PerpetualOrderBook(context.Background(),"mexc:BTC_USDT",0.01)
	if err!=nil{t.Fatal(err)}
	if len(book.Bids)!=1||book.Bids[0].Quantity!=0.02||len(book.Asks)!=1||book.Asks[0].Quantity!=0.03{t.Fatalf("book normalization: %+v",book)}
}

func TestSpotOrderBookKeepsBaseQuantity(t *testing.T) {
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.URL.Path!="/api/v3/depth"||r.URL.Query().Get("symbol")!="BTCUSDT"{t.Fatalf("request=%s",r.URL.String())}
		_,_=w.Write([]byte(`{"bids":[["100","2"]],"asks":[["101","3"]]}`))
	}))
	defer server.Close()
	book,err:=New(server.Client(),server.URL,server.URL).SpotOrderBook(context.Background(),"mexc_spot:BTCUSDT")
	if err!=nil{t.Fatal(err)}
	if len(book.Bids)!=1||book.Bids[0].Quantity!=2{t.Fatalf("spot book: %+v",book)}
}

func TestIsUnavailable(t *testing.T) {
	if !IsUnavailable(&StatusError{StatusCode:http.StatusServiceUnavailable}){t.Fatal("503 should be unavailable")}
	if IsUnavailable(&StatusError{StatusCode:http.StatusBadRequest}){t.Fatal("400 should not be unavailable")}
}
