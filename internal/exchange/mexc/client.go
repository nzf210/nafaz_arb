package mexc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

const (
	defaultContractURL = "https://contract.mexc.com"
	defaultSpotURL = "https://api.mexc.com"
	userAgent = "Mozilla/5.0 (compatible; NafazScanner/1.0)"
)

type StatusError struct { StatusCode int }
func (e *StatusError) Error() string { return fmt.Sprintf("mexc HTTP status: %d", e.StatusCode) }
func IsUnavailable(err error) bool {
	var statusErr *StatusError
	return errors.As(err, &statusErr) && (statusErr.StatusCode == http.StatusBadGateway || statusErr.StatusCode == http.StatusServiceUnavailable || statusErr.StatusCode == http.StatusGatewayTimeout)
}

// Client owns MEXC public spot/perpetual market-data endpoints and normalization.
type Client struct {
	httpClient *http.Client
	contractURL string
	spotURL string
}

func New(httpClient *http.Client, contractURL, spotURL string) *Client {
	if httpClient == nil { httpClient = http.DefaultClient }
	if strings.TrimSpace(contractURL) == "" { contractURL = defaultContractURL }
	if strings.TrimSpace(spotURL) == "" { spotURL = defaultSpotURL }
	return &Client{httpClient:httpClient, contractURL:strings.TrimRight(contractURL,"/"), spotURL:strings.TrimRight(spotURL,"/")}
}

func (c *Client) get(ctx context.Context, baseURL, path string) (*http.Response,error) {
	req, err := http.NewRequestWithContext(ctx,http.MethodGet,baseURL+path,nil)
	if err != nil { return nil,err }
	req.Header.Set("User-Agent",userAgent)
	return c.httpClient.Do(req)
}

func (c *Client) SpotMarkets(ctx context.Context) ([]domain.Market, error) {
	resp, err := c.get(ctx, c.spotURL, "/api/v3/exchangeInfo")
	if err != nil { return nil, fmt.Errorf("mexc spot exchange info request: %w", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return nil, &StatusError{StatusCode: resp.StatusCode} }
	var raw struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
			Status json.RawMessage `json:"status"`
			BaseAsset string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
			Filters []struct {
				FilterType string `json:"filterType"`
				TickSize string `json:"tickSize"`
				StepSize string `json:"stepSize"`
				MinQty string `json:"minQty"`
				MinNotional string `json:"minNotional"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode mexc spot exchange info: %w", err)
	}
	markets := make([]domain.Market, 0, len(raw.Symbols))
	for _, item := range raw.Symbols {
		status := ""
		if err := json.Unmarshal(item.Status, &status); err != nil {
			var number json.Number
			if json.Unmarshal(item.Status, &number) == nil { status = number.String() }
		}
		if item.Symbol == "" || item.BaseAsset == "" || item.QuoteAsset == "" ||
			!(status == "1" || strings.EqualFold(status, "TRADING") || strings.EqualFold(status, "ENABLED")) {
			continue
		}
		market := domain.Market{
			Exchange: "mexc", Symbol: "mexc_spot:" + strings.ToUpper(item.Symbol),
			BaseAsset: strings.ToUpper(item.BaseAsset), QuoteAsset: strings.ToUpper(item.QuoteAsset),
			MarketType: "SPOT", Active: true,
		}
		for _, filter := range item.Filters {
			switch strings.ToUpper(filter.FilterType) {
			case "PRICE_FILTER":
				market.TickSize, _ = strconv.ParseFloat(filter.TickSize, 64)
			case "LOT_SIZE":
				market.LotSize, _ = strconv.ParseFloat(filter.StepSize, 64)
				market.MinOrder, _ = strconv.ParseFloat(filter.MinQty, 64)
			case "MIN_NOTIONAL", "NOTIONAL":
				if market.MinOrder == 0 { market.MinOrder, _ = strconv.ParseFloat(filter.MinNotional, 64) }
			}
		}
		if validPositive(market.LotSize) && validNonNegative(market.MinOrder) {
			market.AmountSpec = &domain.AmountSpec{Unit:"BASE",Step:market.LotSize,Min:market.MinOrder,BasePerUnit:1}
		}
		markets = append(markets, market)
	}
	return markets, nil
}

func (c *Client) PerpetualMarkets(ctx context.Context) ([]domain.Market,error) {
	resp,err:=c.get(ctx,c.contractURL,"/api/v1/contract/detail")
	if err!=nil { return nil,fmt.Errorf("mexc contract detail request: %w",err) }
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK { return nil,&StatusError{StatusCode:resp.StatusCode} }
	var raw struct {
		Success bool `json:"success"`
		Code int `json:"code"`
		Data []struct {
			Symbol string `json:"symbol"`
			BaseCoin string `json:"baseCoin"`
			QuoteCoin string `json:"quoteCoin"`
			ContractSize float64 `json:"contractSize"`
			PriceUnit float64 `json:"priceUnit"`
			VolUnit float64 `json:"volUnit"`
			MinVol float64 `json:"minVol"`
			State int `json:"state"`
		} `json:"data"`
	}
	if err:=json.NewDecoder(resp.Body).Decode(&raw);err!=nil{return nil,fmt.Errorf("decode mexc contract detail: %w",err)}
	if !raw.Success{return nil,fmt.Errorf("mexc contract detail error code: %d",raw.Code)}
	markets:=make([]domain.Market,0,len(raw.Data))
	for _,item:=range raw.Data {
		if item.State!=0 || item.Symbol=="" {continue}
		market:=domain.Market{Exchange:"mexc",Symbol:"mexc:"+item.Symbol,BaseAsset:strings.ToUpper(item.BaseCoin),QuoteAsset:strings.ToUpper(item.QuoteCoin),MarketType:"PERPETUAL",Active:true,ContractSize:item.ContractSize,TickSize:item.PriceUnit,LotSize:item.VolUnit,MinOrder:item.MinVol}
		if validPositive(item.ContractSize)&&validPositive(item.VolUnit)&&validNonNegative(item.MinVol)&&item.BaseCoin!="" {
			market.AmountSpec=&domain.AmountSpec{Unit:"CONTRACT",Step:item.VolUnit,Min:item.MinVol,BasePerUnit:item.ContractSize}
		}
		markets=append(markets,market)
	}
	return markets,nil
}

func (c *Client) FundingRate(ctx context.Context,symbol string)(domain.FundingRate,error) {
	rawSymbol:=cleanSymbol(symbol)
	resp,err:=c.get(ctx,c.contractURL,"/api/v1/contract/funding_rate/"+url.PathEscape(rawSymbol))
	if err!=nil{return domain.FundingRate{},fmt.Errorf("mexc funding request: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{return domain.FundingRate{},&StatusError{StatusCode:resp.StatusCode}}
	var raw struct {
		Success bool `json:"success"`
		Code int `json:"code"`
		Data struct {
			FundingRate float64 `json:"fundingRate"`
			CollectCycle int `json:"collectCycle"`
			Timestamp int64 `json:"timestamp"`
		} `json:"data"`
	}
	if err:=json.NewDecoder(resp.Body).Decode(&raw);err!=nil{return domain.FundingRate{},fmt.Errorf("decode mexc funding: %w",err)}
	if !raw.Success{return domain.FundingRate{},fmt.Errorf("mexc funding error code: %d",raw.Code)}
	interval:=raw.Data.CollectCycle
	if interval<=0{interval=8}
	ts:=time.Now().UTC()
	if raw.Data.Timestamp>0{ts=time.UnixMilli(raw.Data.Timestamp).UTC()}
	return domain.FundingRate{Rate:raw.Data.FundingRate,IntervalHours:interval,Timestamp:ts,NextFundingTime:nextSettlementTime(ts,interval)},nil
}

func (c *Client) SpotOrderBook(ctx context.Context,symbol string)(domain.OrderBook,error) {
	rawSymbol:=strings.TrimPrefix(strings.TrimPrefix(symbol,"mexc_spot:"),"mexc:")
	path:="/api/v3/depth?symbol="+url.QueryEscape(rawSymbol)+"&limit=20"
	resp,err:=c.get(ctx,c.spotURL,path)
	if err!=nil{return domain.OrderBook{},fmt.Errorf("mexc spot depth request: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{return domain.OrderBook{},&StatusError{StatusCode:resp.StatusCode}}
	var raw struct{ Bids [][]string `json:"bids"`; Asks [][]string `json:"asks"` }
	if err:=json.NewDecoder(resp.Body).Decode(&raw);err!=nil{return domain.OrderBook{},fmt.Errorf("decode mexc spot depth: %w",err)}
	now:=time.Now().UTC()
	return domain.OrderBook{Exchange:"mexc",Symbol:symbol,Bids:parseStringLevels(raw.Bids,1),Asks:parseStringLevels(raw.Asks,1),Timestamp:now,ReceivedAt:now,Source:"receive_time"},nil
}

func (c *Client) PerpetualOrderBook(ctx context.Context,symbol string,contractSize float64)(domain.OrderBook,error) {
	rawSymbol:=cleanSymbol(symbol)
	path:="/api/v1/contract/depth/"+url.PathEscape(rawSymbol)+"?limit=20"
	resp,err:=c.get(ctx,c.contractURL,path)
	if err!=nil{return domain.OrderBook{},fmt.Errorf("mexc depth request: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{return domain.OrderBook{},&StatusError{StatusCode:resp.StatusCode}}
	var raw struct {
		Success bool `json:"success"`
		Code int `json:"code"`
		Data struct { Bids [][]any `json:"bids"`; Asks [][]any `json:"asks"`; Timestamp int64 `json:"timestamp"` } `json:"data"`
	}
	if err:=json.NewDecoder(resp.Body).Decode(&raw);err!=nil{return domain.OrderBook{},fmt.Errorf("decode mexc depth: %w",err)}
	if !raw.Success{return domain.OrderBook{},fmt.Errorf("mexc depth error code: %d",raw.Code)}
	if !validPositive(contractSize){contractSize=1}
	now:=time.Now().UTC()
	if raw.Data.Timestamp>0{now=time.UnixMilli(raw.Data.Timestamp).UTC()}
	return domain.OrderBook{Exchange:"mexc",Symbol:symbol,Bids:parseAnyLevels(raw.Data.Bids,contractSize),Asks:parseAnyLevels(raw.Data.Asks,contractSize),Timestamp:now,ReceivedAt:time.Now().UTC(),Source:"exchange_timestamp"},nil
}

func cleanSymbol(symbol string) string {
	symbol = strings.TrimPrefix(symbol, "mexc:")
	return strings.TrimPrefix(symbol, "mexc_spot:")
}
func validPositive(v float64)bool{return v>0&&!math.IsNaN(v)&&!math.IsInf(v,0)}
func validNonNegative(v float64)bool{return v>=0&&!math.IsNaN(v)&&!math.IsInf(v,0)}
func parseStringLevels(rows [][]string,multiplier float64)[]domain.PriceLevel{
	out:=make([]domain.PriceLevel,0,len(rows))
	for _,row:=range rows{if len(row)<2{continue};p,e1:=strconv.ParseFloat(row[0],64);q,e2:=strconv.ParseFloat(row[1],64);q*=multiplier;if e1==nil&&e2==nil&&validPositive(p)&&validPositive(q){out=append(out,domain.PriceLevel{Price:p,Quantity:q})}}
	return out
}
func parseAnyLevels(rows [][]any,multiplier float64)[]domain.PriceLevel{
	out:=make([]domain.PriceLevel,0,len(rows))
	for _,row:=range rows{if len(row)<2{continue};p,ok1:=number(row[0]);q,ok2:=number(row[1]);q*=multiplier;if ok1&&ok2&&validPositive(p)&&validPositive(q){out=append(out,domain.PriceLevel{Price:p,Quantity:q})}}
	return out
}
func number(v any)(float64,bool){
	switch x:=v.(type){case float64:return x,validNonNegative(x);case string:n,e:=strconv.ParseFloat(x,64);return n,e==nil&&validNonNegative(n);case json.Number:n,e:=x.Float64();return n,e==nil&&validNonNegative(n);default:return 0,false}
}
func nextSettlementTime(t time.Time,interval int)time.Time{
	if interval<=0{interval=8};utc:=t.UTC();y,m,d:=utc.Date();next:=((utc.Hour()/interval)+1)*interval
	if next>=24{return time.Date(y,m,d+1,next-24,0,0,0,time.UTC)}
	return time.Date(y,m,d,next,0,0,0,time.UTC)
}
