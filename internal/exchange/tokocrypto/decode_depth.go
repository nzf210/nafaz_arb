package tokocrypto

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// DecodeDepthUpdate parses one MBX diff-depth event into the manager contract.
func IsDepthUpdate(payload []byte) bool {
	var envelope struct {
		Event string `json:"e"`
		Data struct { Event string `json:"e"` } `json:"data"`
	}
	if json.Unmarshal(payload, &envelope) != nil { return false }
	return envelope.Event == "depthUpdate" || envelope.Data.Event == "depthUpdate"
}

// DecodeDepthUpdate accepts both raw MBX events and combined-stream envelopes.
// NextMe includes pu (the previous final update id), which is the authoritative
// continuity field for that matching engine.
func DecodeDepthUpdate(payload []byte) (DepthUpdate,error) {
	var envelope struct { Data json.RawMessage `json:"data"` }
	if err:=json.Unmarshal(payload,&envelope);err!=nil{return DepthUpdate{},fmt.Errorf("decode tokocrypto depth event: %w",err)}
	eventPayload := payload
	if len(envelope.Data)>0 && string(envelope.Data)!="null" { eventPayload = envelope.Data }
	var raw struct {
		Event string `json:"e"`
		EventTime int64 `json:"E"`
		TradeTime int64 `json:"T"`
		Symbol string `json:"s"`
		First int64 `json:"U"`
		Final int64 `json:"u"`
		Previous int64 `json:"pu"`
		Bids [][]string `json:"b"`
		Asks [][]string `json:"a"`
	}
	if err:=json.Unmarshal(eventPayload,&raw);err!=nil{return DepthUpdate{},fmt.Errorf("decode tokocrypto depth data: %w",err)}
	if raw.Event!="depthUpdate"{return DepthUpdate{},fmt.Errorf("unexpected tokocrypto event %q",raw.Event)}
	if raw.Symbol==""||raw.First<=0||raw.Final<raw.First{return DepthUpdate{},fmt.Errorf("invalid tokocrypto depth event metadata")}
	eventMS := raw.EventTime
	if eventMS <= 0 { eventMS = raw.TradeTime }
	var at time.Time
	if eventMS>0{at=time.UnixMilli(eventMS).UTC()}
	return DepthUpdate{Symbol:raw.Symbol,FirstUpdateID:raw.First,FinalUpdateID:raw.Final,PreviousUpdateID:raw.Previous,Bids:raw.Bids,Asks:raw.Asks,EventTime:at},nil
}

// DecodeDepthSnapshot parses the public REST snapshot response.
func DecodeDepthSnapshot(payload []byte)(DepthSnapshot,error){
	var raw struct{
		LastUpdateID int64 `json:"lastUpdateId"`
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
		Timestamp int64 `json:"timestamp"`
		EventTime int64 `json:"E"`
		TradeTime int64 `json:"T"`
		Data *struct{
			LastUpdateID int64 `json:"lastUpdateId"`
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
		}`json:"data"`
	}
	if err:=json.Unmarshal(payload,&raw);err!=nil{return DepthSnapshot{},fmt.Errorf("decode tokocrypto depth snapshot: %w",err)}
	if raw.Data!=nil{raw.LastUpdateID=raw.Data.LastUpdateID;raw.Bids=raw.Data.Bids;raw.Asks=raw.Data.Asks}
	if raw.LastUpdateID<=0{return DepthSnapshot{},fmt.Errorf("tokocrypto snapshot missing lastUpdateId")}
	timestamp := raw.Timestamp
	if timestamp <= 0 { timestamp = raw.EventTime }
	if timestamp <= 0 { timestamp = raw.TradeTime }
	var at time.Time;if timestamp>0{at=time.UnixMilli(timestamp).UTC()}
	return DepthSnapshot{LastUpdateID:raw.LastUpdateID,Bids:raw.Bids,Asks:raw.Asks,Timestamp:at},nil
}

func parseDepthNumber(raw json.RawMessage)(float64,error){
	var n float64
	if err:=json.Unmarshal(raw,&n);err==nil{return n,nil}
	var s string
	if err:=json.Unmarshal(raw,&s);err!=nil{return 0,err}
	return strconv.ParseFloat(s,64)
}
