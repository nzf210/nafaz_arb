package tokocrypto

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// DecodeDepthUpdate parses one MBX diff-depth event into the manager contract.
func DecodeDepthUpdate(payload []byte) (DepthUpdate,error) {
	var raw struct {
		Event string `json:"e"`
		EventTime int64 `json:"E"`
		Symbol string `json:"s"`
		First int64 `json:"U"`
		Final int64 `json:"u"`
		Bids [][]string `json:"b"`
		Asks [][]string `json:"a"`
	}
	if err:=json.Unmarshal(payload,&raw);err!=nil{return DepthUpdate{},fmt.Errorf("decode tokocrypto depth event: %w",err)}
	if raw.Event!=""&&raw.Event!="depthUpdate"{return DepthUpdate{},fmt.Errorf("unexpected tokocrypto event %q",raw.Event)}
	if raw.Symbol==""||raw.First<=0||raw.Final<raw.First{return DepthUpdate{},fmt.Errorf("invalid tokocrypto depth event metadata")}
	var at time.Time
	if raw.EventTime>0{at=time.UnixMilli(raw.EventTime).UTC()}
	return DepthUpdate{Symbol:raw.Symbol,FirstUpdateID:raw.First,FinalUpdateID:raw.Final,Bids:raw.Bids,Asks:raw.Asks,EventTime:at},nil
}

// DecodeDepthSnapshot parses the public REST snapshot response.
func DecodeDepthSnapshot(payload []byte)(DepthSnapshot,error){
	var raw struct{
		LastUpdateID int64 `json:"lastUpdateId"`
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
		Timestamp int64 `json:"timestamp"`
		Data *struct{
			LastUpdateID int64 `json:"lastUpdateId"`
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
		}`json:"data"`
	}
	if err:=json.Unmarshal(payload,&raw);err!=nil{return DepthSnapshot{},fmt.Errorf("decode tokocrypto depth snapshot: %w",err)}
	if raw.Data!=nil{raw.LastUpdateID=raw.Data.LastUpdateID;raw.Bids=raw.Data.Bids;raw.Asks=raw.Data.Asks}
	if raw.LastUpdateID<=0{return DepthSnapshot{},fmt.Errorf("tokocrypto snapshot missing lastUpdateId")}
	var at time.Time;if raw.Timestamp>0{at=time.UnixMilli(raw.Timestamp).UTC()}
	return DepthSnapshot{LastUpdateID:raw.LastUpdateID,Bids:raw.Bids,Asks:raw.Asks,Timestamp:at},nil
}

func parseDepthNumber(raw json.RawMessage)(float64,error){
	var n float64
	if err:=json.Unmarshal(raw,&n);err==nil{return n,nil}
	var s string
	if err:=json.Unmarshal(raw,&s);err!=nil{return 0,err}
	return strconv.ParseFloat(s,64)
}
