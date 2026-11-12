package scanner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	redis "github.com/redis/go-redis/v9"
)

func TestLoadExposuresUsesExecutorPositionAndOrderRegistry(t *testing.T) {
	mr,err:=miniredis.Run()
	if err!=nil{t.Fatal(err)}
	defer mr.Close()
	rdb:=redis.NewClient(&redis.Options{Addr:mr.Addr()})
	defer rdb.Close()
	ctx:=context.Background()

	position,_:=json.Marshal(domain.ActivePosition{
		LocalExchange:"indodax",GlobalExchange:"okx",Symbol:"btc_idr",Qty:.01,
	})
	if err:=rdb.Set(ctx,redishandler.KeyPrefixActivePos+"indodax:btc_idr",position,0).Err();err!=nil{t.Fatal(err)}
	order:=`{"exchange":"okx","market_type":"PERPETUAL","symbol":"BTC-USDT-SWAP","status":"open","remaining_quantity":0.01,"recovery_status":"ADOPTED"}`
	if err:=rdb.Set(ctx,redishandler.KeyPrefixOrderRecord+"order-1",order,0).Err();err!=nil{t.Fatal(err)}
	if err:=rdb.HSet(ctx,redishandler.KeyOrderRecoveryIndex,"order-1","1").Err();err!=nil{t.Fatal(err)}

	cfg:=config.Config{CheckOpenPositions:true,ExcludeOpenOrders:true}
	service:=New(cfg,exchange.NewMockAdapter())
	service.SetRedisClient(redishandler.NewWithClient(rdb))
	exposures,known,err:=service.loadExposures(ctx)
	if err!=nil{t.Fatal(err)}
	if !known||len(exposures)!=2{t.Fatalf("known=%v exposures=%+v",known,exposures)}
	if exposures[0].Kind!="POSITION"||exposures[0].BaseAsset!="BTC"||exposures[0].QuoteAsset!="IDR"{
		t.Fatalf("unexpected position exposure: %+v",exposures[0])
	}
	if exposures[1].Kind!="OPEN_ORDER"||!exposures[1].Managed||exposures[1].Exchange!="okx"{
		t.Fatalf("unexpected open-order exposure: %+v",exposures[1])
	}
}

func TestLoadExposuresFailsClosedOnMalformedExecutorPosition(t *testing.T) {
	mr,err:=miniredis.Run()
	if err!=nil{t.Fatal(err)}
	defer mr.Close()
	rdb:=redis.NewClient(&redis.Options{Addr:mr.Addr()})
	defer rdb.Close()
	ctx:=context.Background()
	if err:=rdb.Set(ctx,redishandler.KeyPrefixActivePos+"broken","{",0).Err();err!=nil{t.Fatal(err)}
	service:=New(config.Config{CheckOpenPositions:true},exchange.NewMockAdapter())
	service.SetRedisClient(redishandler.NewWithClient(rdb))
	if _,known,err:=service.loadExposures(ctx);err==nil||known{
		t.Fatalf("malformed position must be unknown and fail closed: known=%v err=%v",known,err)
	}
}
