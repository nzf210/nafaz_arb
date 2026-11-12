package tokocrypto

import "testing"

func TestDecodeDepthUpdate(t *testing.T){
	event,err:=DecodeDepthUpdate([]byte(`{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":10,"u":12,"b":[["100","2"]],"a":[["101","0"]]}`))
	if err!=nil{t.Fatal(err)}
	if event.Symbol!="BTCUSDT"||event.FirstUpdateID!=10||event.FinalUpdateID!=12||len(event.Bids)!=1||event.EventTime.IsZero(){t.Fatalf("unexpected event: %+v",event)}
}

func TestDecodeDepthSnapshotEnvelope(t *testing.T){
	snapshot,err:=DecodeDepthSnapshot([]byte(`{"code":0,"data":{"lastUpdateId":22,"bids":[["10","1"]],"asks":[["11","2"]]},"timestamp":1700000000000}`))
	if err!=nil{t.Fatal(err)}
	if snapshot.LastUpdateID!=22||len(snapshot.Bids)!=1||len(snapshot.Asks)!=1{t.Fatalf("unexpected snapshot: %+v",snapshot)}
}
