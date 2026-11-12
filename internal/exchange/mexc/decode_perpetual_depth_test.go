package mexc

import "testing"

func TestDecodePerpetualDepthUpdateVersionRange(t *testing.T){
	event,err:=DecodePerpetualDepthUpdate([]byte(`{"channel":"push.depth","symbol":"BTC_USDT","ts":1700000000000,"data":{"begin":20,"end":21,"bids":[[100,2,1]],"asks":[[101,3,1]]}}`),0.01)
	if err!=nil{t.Fatal(err)}
	if event.Symbol!="BTC_USDT"||event.Begin!=20||event.End!=21||event.ContractSize!=0.01||event.Bids[0][1]!=2{t.Fatalf("unexpected event: %+v",event)}
}

func TestDecodePerpetualDepthUpdateRejectsWrongChannel(t *testing.T){
	if _,err:=DecodePerpetualDepthUpdate([]byte(`{"channel":"push.deal","symbol":"BTC_USDT","data":{}}`),1);err==nil{t.Fatal("expected channel error")}
}
