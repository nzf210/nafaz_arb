package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const defaultBaseURL = "https://www.okx.com"

// Client owns public OKX REST and WebSocket market-data capabilities.
type Client struct {
	httpClient *http.Client
	baseURL string
	limiterCfg OKXLimiterConfig
	limiterOnce sync.Once
	limiter *okxLimiter
}

func New(httpClient *http.Client, baseURL string) *Client {
	if httpClient == nil { httpClient = http.DefaultClient }
	if strings.TrimSpace(baseURL) == "" { baseURL = defaultBaseURL }
	return &Client{httpClient:httpClient, baseURL:strings.TrimRight(baseURL,"/")}
}

// SetLimiterConfig must be called before the first REST request.
func (c *Client) SetLimiterConfig(cfg OKXLimiterConfig) { c.limiterCfg = cfg }

// Metrics reports shared REST limiter counters.
func (c *Client) Metrics() OKXMetricsSnapshot {
	c.limiterOnce.Do(func(){ c.limiter = newOKXLimiter(c.limiterCfg) })
	return c.limiter.metrics.snapshot()
}

type instrument struct {
	InstID string `json:"instId"`
	InstType string `json:"instType"`
	CtVal string `json:"ctVal"`
	CtValCcy string `json:"ctValCcy"`
	SettleCcy string `json:"settleCcy"`
	State string `json:"state"`
	TickSz string `json:"tickSz"`
	LotSz string `json:"lotSz"`
	MinSz string `json:"minSz"`
}

// PerpetualMarkets returns active SWAP instruments with explicit contract rules.
func (c *Client) PerpetualMarkets(ctx context.Context) ([]domain.Market,error) {
	const path="/api/v5/public/instruments?instType=SWAP"
	resp,err:=c.do(ctx,path,func()(*http.Request,error){return c.newRequest(ctx,path)})
	if err!=nil{return nil,fmt.Errorf("okx instruments request: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{return nil,fmt.Errorf("okx instruments status: %d",resp.StatusCode)}
	var payload struct{Code string `json:"code"`; Msg string `json:"msg"`; Data []instrument `json:"data"`}
	if err:=json.NewDecoder(resp.Body).Decode(&payload);err!=nil{return nil,fmt.Errorf("decode okx instruments: %w",err)}
	if payload.Code!="0"{return nil,fmt.Errorf("okx instruments error: %s (%s)",payload.Msg,payload.Code)}
	markets:=make([]domain.Market,0,len(payload.Data))
	for _,item:=range payload.Data {
		if item.State!="live"||item.InstID==""{continue}
		ctVal,_:=strconv.ParseFloat(item.CtVal,64); tick,_:=strconv.ParseFloat(item.TickSz,64); lot,_:=strconv.ParseFloat(item.LotSz,64); min,_:=strconv.ParseFloat(item.MinSz,64)
		base:=item.CtValCcy
		if base=="" { parts:=strings.Split(item.InstID,"-"); if len(parts)>0{base=parts[0]} }
		m:=domain.Market{Exchange:"okx",Symbol:item.InstID,BaseAsset:strings.ToUpper(base),QuoteAsset:strings.ToUpper(item.SettleCcy),MarketType:"PERPETUAL",Active:true,ContractSize:ctVal,TickSize:tick,LotSize:lot,MinOrder:min}
		if ctVal>0&&lot>0&&min>=0&&!math.IsNaN(ctVal)&&!math.IsInf(ctVal,0)&&!math.IsNaN(lot)&&!math.IsInf(lot,0)&&!math.IsNaN(min)&&!math.IsInf(min,0)&&strings.EqualFold(item.CtValCcy,base){
			m.AmountSpec=&domain.AmountSpec{Unit:"CONTRACT",Step:lot,Min:min,BasePerUnit:ctVal}
		}
		markets=append(markets,m)
	}
	return markets,nil
}

// FundingRate returns the current funding rate and the exchange's next settlement time.
func (c *Client) FundingRate(ctx context.Context,symbol string)(domain.FundingRate,error){
	path:="/api/v5/public/funding-rate?instId="+url.QueryEscape(cleanSymbol(symbol))
	resp,err:=c.do(ctx,path,func()(*http.Request,error){return c.newRequest(ctx,path)})
	if err!=nil{return domain.FundingRate{},fmt.Errorf("okx funding request: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{return domain.FundingRate{},fmt.Errorf("okx funding status: %d",resp.StatusCode)}
	var payload struct{Code string `json:"code"`; Msg string `json:"msg"`; Data []struct{FundingRate string `json:"fundingRate"`; FundingTime string `json:"fundingTime"`; PrevFundingTime string `json:"prevFundingTime"`; Ts string `json:"ts"`} `json:"data"`}
	if err:=json.NewDecoder(resp.Body).Decode(&payload);err!=nil{return domain.FundingRate{},fmt.Errorf("decode okx funding: %w",err)}
	if payload.Code!="0"||len(payload.Data)==0{return domain.FundingRate{},fmt.Errorf("okx funding error: %s (%s)",payload.Msg,payload.Code)}
	item:=payload.Data[0]; rate,err:=strconv.ParseFloat(item.FundingRate,64);if err!=nil{return domain.FundingRate{},fmt.Errorf("invalid OKX funding rate: %w",err)}
	interval:=8; ft,_:=strconv.ParseInt(item.FundingTime,10,64); pft,_:=strconv.ParseInt(item.PrevFundingTime,10,64)
	if ft>pft&&pft>0{hours:=int((ft-pft)/(1000*3600));if hours>0{interval=hours}}
	ts:=time.Now().UTC();if ms,e:=strconv.ParseInt(item.Ts,10,64);e==nil&&ms>0{ts=time.UnixMilli(ms).UTC()}
	var next time.Time;if ft>0{next=time.UnixMilli(ft).UTC()}
	return domain.FundingRate{Rate:rate,IntervalHours:interval,Timestamp:ts,NextFundingTime:next},nil
}

// PerpetualOrderBook is a REST recovery/bootstrap path. The live scanner should
// normally consume BookManager snapshots and use this only after a WS miss.
func (c *Client) PerpetualOrderBook(ctx context.Context,symbol string,contractSize float64)(domain.OrderBook,error){
	path:="/api/v5/market/books?instId="+url.QueryEscape(cleanSymbol(symbol))+"&sz=20"
	resp,err:=c.do(ctx,path,func()(*http.Request,error){return c.newRequest(ctx,path)})
	if err!=nil{return domain.OrderBook{},fmt.Errorf("okx depth request: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{return domain.OrderBook{},fmt.Errorf("okx depth status: %d",resp.StatusCode)}
	var payload struct{Code string `json:"code"`;Msg string `json:"msg"`;Data []struct{Bids [][]string `json:"bids"`;Asks [][]string `json:"asks"`;Ts string `json:"ts"`} `json:"data"`}
	if err:=json.NewDecoder(resp.Body).Decode(&payload);err!=nil{return domain.OrderBook{},fmt.Errorf("decode okx depth: %w",err)}
	if payload.Code!="0"||len(payload.Data)==0{return domain.OrderBook{},fmt.Errorf("okx depth error: %s (%s)",payload.Msg,payload.Code)}
	if contractSize<=0||math.IsNaN(contractSize)||math.IsInf(contractSize,0){contractSize=1}
	item:=payload.Data[0];ts:=time.Now().UTC();if ms,e:=strconv.ParseInt(item.Ts,10,64);e==nil&&ms>0{ts=time.UnixMilli(ms).UTC()}
	return domain.OrderBook{Exchange:"okx",Symbol:symbol,Bids:parseLevels(item.Bids,contractSize),Asks:parseLevels(item.Asks,contractSize),Timestamp:ts,ReceivedAt:time.Now().UTC(),Source:"exchange_timestamp"},nil
}

func cleanSymbol(symbol string)string{return strings.TrimPrefix(symbol,"okx:")}
func parseLevels(rows [][]string,multiplier float64)[]domain.PriceLevel{
	out:=make([]domain.PriceLevel,0,len(rows))
	for _,row:=range rows{if len(row)<2{continue};p,e1:=strconv.ParseFloat(row[0],64);q,e2:=strconv.ParseFloat(row[1],64);q*=multiplier;if e1==nil&&e2==nil&&p>0&&q>0{out=append(out,domain.PriceLevel{Price:p,Quantity:q})}}
	return out
}
