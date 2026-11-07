package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/api"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/history"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/nafaz/spot-perp-scanner/internal/scanner"
	"github.com/nafaz/spot-perp-scanner/internal/telegram"
)
func main(){
	path:=flag.String("config","config.json","path to JSON config");flag.Parse()
	cfg,err:=config.Load(*path);if err!=nil{log.Fatal(err)}
	var adapter exchange.ReadOnlyAdapter
	switch cfg.Mode {
	case "LIVE":
		live := exchange.NewLiveAdapter()
		if cfg.GlobalFuturesExchange != "" {
			live.SetTargetPerpExchange(cfg.GlobalFuturesExchange)
		live.SetEnabledSpotExchanges(cfg.EnabledSpotExchanges)
		}
		live.SetHealthPolicy(exchange.HealthPolicy{
			Window:             time.Duration(cfg.ExchangeErrorWindowSeconds) * time.Second,
			ErrorRateThreshold: cfg.ExchangeErrorRateThreshold,
			MinRequests:        cfg.ExchangeErrorMinRequests,
		})
		if cfg.EnableWebSocketFeed {
			live.EnableWebSocketFeed(context.Background(), time.Duration(cfg.WebSocketBookMaxAgeSeconds)*time.Second)
		}
		adapter = live
	case "MOCK":
		adapter = exchange.NewMockAdapter()
	default:
		log.Fatalf("invalid mode %q: must be MOCK or LIVE", cfg.Mode)
	}
	svc:=scanner.New(cfg,adapter)

	ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()

	// Initialize Redis handler if enabled
	var redisClient *redishandler.Client
	if cfg.Redis.Enabled {
		rc, err := redishandler.New(cfg.Redis)
		if err != nil {
			if cfg.Mode == "LIVE" {
				// Without Redis the scanner cannot gate on executor positions,
				// publish signals, or notify on halts: running would silently
				// stream unaudited opportunities, so refuse to start.
				log.Fatalf("redis is enabled but unreachable (%s): %v - refusing to start in LIVE mode", cfg.Redis.Addr, err)
			}
			log.Printf("warning: redis connection failed (%s): %v", cfg.Redis.Addr, err)
		} else {
			redisClient = rc
			svc.SetRedisClient(rc)
			defer rc.Close()
			log.Printf("redis integration connected to %s", cfg.Redis.Addr)
		}
	}

	// Initialize background Funding Rate worker
	fw := exchange.NewFundingWorker(cfg.FundingPollingIntervalSeconds, "")
	if redisClient != nil {
		fw.SetOnNegativeCallback(func(sym string, info exchange.FundingInfo) {
			_ = redisClient.PublishEmergencyCancel(ctx, "", sym, "CANCEL_ALL_NEGATIVE_FUNDING", info.LastFundingRate)
		})
	}
	fw.Start(ctx)
	svc.SetFundingWorker(fw)
	defer fw.Stop()

	store,err:=history.New(cfg.HistoryPath);if err!=nil{log.Fatal(err)}
	server:=api.New(svc,store,cfg)
	if redisClient != nil {
		server.SetExecutorClient(redisClient)
	}
	go server.RunPeriodic(ctx)

	tgBot:=telegram.New(cfg,server)
	if redisClient != nil {
		tgBot.SetRedisClient(redisClient)
	}
	if err:=tgBot.Start(ctx);err!=nil{
		log.Printf("warning: telegram bot: %v",err)
	}

	httpServer:=&http.Server{Addr:cfg.ListenAddr,Handler:server.Handler()}
	go func(){<-ctx.Done();_ = httpServer.Shutdown(context.Background())}()
	log.Printf("read-only scanner (%s) listening at %s",cfg.Mode,cfg.ListenAddr)
	if err:=httpServer.ListenAndServe();err!=nil&&err!=http.ErrServerClosed{log.Fatal(err)}
}
