package mexc

import (
	"encoding/json"
	"fmt"
	"time"
)

// DecodePerpetualDepthUpdate parses the JSON push.depth event. Subscribe with
// compress=false so frames remain JSON rather than compressed binary payloads.
func DecodePerpetualDepthUpdate(payload []byte,contractSize float64)(PerpetualDepthUpdate,error){
	var raw struct{
		Channel string `json:"channel"`
		Symbol string `json:"symbol"`
		Timestamp int64 `json:"ts"`
		Data struct{
			Begin int64 `json:"begin"`
			End int64 `json:"end"`
			Version int64 `json:"version"`
			Bids [][]json.RawMessage `json:"bids"`
			Asks [][]json.RawMessage `json:"asks"`
		}`json:"data"`
	}
	if err:=json.Unmarshal(payload,&raw);err!=nil{return PerpetualDepthUpdate{},fmt.Errorf("decode mexc perpetual depth: %w",err)}
	if raw.Channel!=""&&raw.Channel!="push.depth"{return PerpetualDepthUpdate{},fmt.Errorf("unexpected mexc channel %q",raw.Channel)}
	if raw.Symbol==""{return PerpetualDepthUpdate{},fmt.Errorf("mexc depth event missing symbol")}
	bids,err:=decodeContractRows(raw.Data.Bids);if err!=nil{return PerpetualDepthUpdate{},fmt.Errorf("mexc bids: %w",err)}
	asks,err:=decodeContractRows(raw.Data.Asks);if err!=nil{return PerpetualDepthUpdate{},fmt.Errorf("mexc asks: %w",err)}
	var at time.Time;if raw.Timestamp>0{at=time.UnixMilli(raw.Timestamp).UTC()}
	return PerpetualDepthUpdate{Symbol:raw.Symbol,Begin:raw.Data.Begin,End:raw.Data.End,Version:raw.Data.Version,Bids:bids,Asks:asks,Timestamp:at,ContractSize:contractSize},nil
}

func decodeContractRows(rows [][]json.RawMessage)([][]float64,error){
	out:=make([][]float64,0,len(rows))
	for _,row:=range rows{
		if len(row)<2{return nil,fmt.Errorf("depth row must contain price and volume")}
		var level [2]float64
		for i:=0;i<2;i++{
			if err:=json.Unmarshal(row[i],&level[i]);err!=nil{return nil,fmt.Errorf("invalid numeric depth value: %w",err)}
		}
		out=append(out,[]float64{level[0],level[1]})
	}
	return out,nil
}
