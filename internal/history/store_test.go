package history

import (
	"testing"
	"time"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)
func TestEpisodeCountAndDuration(t *testing.T){
	path:=t.TempDir()+"/history.json";s,err:=New(path);if err!=nil{t.Fatal(err)}
	t0:=time.Date(2026,1,1,0,0,0,0,time.UTC);candidate:=domain.Opportunity{OpportunityKey:"BTC/USDT:spot:perp",BaseAsset:"BTC",QuoteAsset:"USDT",TradingEligible:true}
	for i:=0;i<3;i++{if err:=s.Observe(t0.Add(time.Duration(i)*30*time.Second),[]domain.Opportunity{candidate},true,2);err!=nil{t.Fatal(err)}}
	if err:=s.Observe(t0.Add(90*time.Second),nil,true,2);err!=nil{t.Fatal(err)}
	if err:=s.Observe(t0.Add(120*time.Second),nil,true,2);err!=nil{t.Fatal(err)}
	items:=s.List(time.Time{},time.Time{},"");if len(items)!=1{t.Fatalf("episodes=%d, want 1",len(items))}
	if items[0].LifecycleStatus!="ENDED"{t.Fatalf("status=%s, want ENDED",items[0].LifecycleStatus)}
	if items[0].ObservedDurationSeconds!=60{t.Fatalf("duration=%d, want 60",items[0].ObservedDurationSeconds)}
}
