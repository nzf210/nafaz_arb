package exchange

import (
	"testing"

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
