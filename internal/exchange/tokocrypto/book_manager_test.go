package tokocrypto

import (
	"testing"
	"time"
)

func TestBookManagerSnapshotBridgeAndDeltas(t *testing.T) {
	m:=NewBookManager(10)
	if err:=m.Bootstrap("BTCUSDT",DepthSnapshot{LastUpdateID:100,Bids:[][]string{{"10","2"}},Asks:[][]string{{"11","3"}}});err!=nil{t.Fatal(err)}
	if _,ok:=m.Get("BTCUSDT",time.Minute);ok{t.Fatal("snapshot must not be exposed before websocket bridge")}
	if ok,err:=m.Apply(DepthUpdate{Symbol:"BTCUSDT",FirstUpdateID:99,FinalUpdateID:100});err!=nil||ok{t.Fatalf("stale bridge: ok=%v err=%v",ok,err)}
	if ok,err:=m.Apply(DepthUpdate{Symbol:"BTCUSDT",FirstUpdateID:99,FinalUpdateID:101,Bids:[][]string{{"10","4"},{"9","1"}},Asks:[][]string{{"11","0"}},EventTime:time.Now()});err!=nil||!ok{t.Fatalf("bridge: ok=%v err=%v",ok,err)}
	if ok,err:=m.Apply(DepthUpdate{Symbol:"BTCUSDT",FirstUpdateID:102,FinalUpdateID:102,Asks:[][]string{{"12","2"}},EventTime:time.Now()});err!=nil||!ok{t.Fatalf("next delta: ok=%v err=%v",ok,err)}
	book,ok:=m.Get("BTCUSDT",time.Minute);if !ok{t.Fatal("expected synchronized book")}
	if book.SequenceID!=102||book.Bids[0].Quantity!=4||len(book.Asks)!=1||book.Asks[0].Price!=12{t.Fatalf("unexpected book: %+v",book)}
}

func TestBookManagerRequiresSnapshotBridgeAndInvalidatesOnGap(t *testing.T){
	m:=NewBookManager(10)
	_ = m.Bootstrap("ETHUSDT",DepthSnapshot{LastUpdateID:5,Bids:[][]string{{"1","1"}},Asks:[][]string{{"2","1"}}})
	if _,err:=m.Apply(DepthUpdate{Symbol:"ETHUSDT",FirstUpdateID:7,FinalUpdateID:7});err==nil{t.Fatal("expected gap")}
	if _,ok:=m.Get("ETHUSDT",time.Minute);ok{t.Fatal("gap must invalidate book")}
}


func TestBookManagerUsesNextMePreviousUpdateID(t *testing.T) {
	m := NewBookManager(10)
	if err := m.Bootstrap("BTCIDR", DepthSnapshot{LastUpdateID:100, Bids:[][]string{{"10","2"}}, Asks:[][]string{{"11","3"}}}); err != nil { t.Fatal(err) }
	if ok, err := m.Apply(DepthUpdate{Symbol:"BTCIDR", FirstUpdateID:120, FinalUpdateID:130, PreviousUpdateID:100, Bids:[][]string{{"10","4"}}, EventTime:time.Now()}); err != nil || !ok {
		t.Fatalf("NextMe bridge failed: ok=%v err=%v", ok, err)
	}
	if ok, err := m.Apply(DepthUpdate{Symbol:"BTCIDR", FirstUpdateID:150, FinalUpdateID:160, PreviousUpdateID:130, Asks:[][]string{{"11","0"},{"12","1"}}, EventTime:time.Now()}); err != nil || !ok {
		t.Fatalf("NextMe contiguous delta failed: ok=%v err=%v", ok, err)
	}
	if _, err := m.Apply(DepthUpdate{Symbol:"BTCIDR", FirstUpdateID:170, FinalUpdateID:180, PreviousUpdateID:159}); err == nil {
		t.Fatal("expected NextMe previous-update gap")
	}
	if _, ok := m.Get("BTCIDR", time.Minute); ok { t.Fatal("NextMe sequence gap must invalidate book") }
}
