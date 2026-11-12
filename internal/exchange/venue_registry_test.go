package exchange

import (
	"testing"
	"context"

	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
)

func TestLiveAdapterBuildsDedicatedCEXAdapters(t *testing.T) {
	live:=NewLiveAdapter()
	defer live.Close()
	venues:=live.VenueAdapters()
	for _,name:=range []string{"indodax","tokocrypto","okx","mexc"}{
		adapter,ok:=venues[name]
		if !ok{t.Fatalf("missing %s adapter",name)}
		if _,ok:=adapter.(contracts.MarketDataAdapter);!ok{t.Fatalf("%s does not implement market data contract",name)}
		if _,ok:=adapter.(contracts.HealthReporter);!ok{t.Fatalf("%s does not forward health reporting",name)}
		if _,ok:=adapter.(contracts.AccountStatusProvider);!ok{t.Fatalf("%s does not forward account status",name)}
	}
}

func TestLiveAdapterDoesNotPretendPublicMarketDataMeansNoExposure(t *testing.T) {
	live:=NewLiveAdapter()
	defer live.Close()
	exposures,err:=live.OpenExposures(context.Background())
	if err==nil||exposures!=nil{
		t.Fatalf("missing private exposure provider must fail closed: exposures=%+v err=%v",exposures,err)
	}
}
