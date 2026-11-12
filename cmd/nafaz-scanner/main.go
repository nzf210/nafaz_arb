package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/api"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/contracts"
	"github.com/nafaz/spot-perp-scanner/internal/exchange/registry"
	"github.com/nafaz/spot-perp-scanner/internal/history"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/nafaz/spot-perp-scanner/internal/scanner"
	"github.com/nafaz/spot-perp-scanner/internal/supabase"
	"github.com/nafaz/spot-perp-scanner/internal/telegram"
)
func main(){
	path:=flag.String("config","config.json","path to JSON config");flag.Parse()
	cfg,err:=config.Load(*path);if err!=nil{log.Fatal(err)}
	var adapter contracts.MarketDataAdapter
	switch cfg.Mode {
	case "LIVE":
		live := exchange.NewLiveAdapter()
		if cfg.GlobalFuturesExchange != "" {
			live.SetTargetPerpExchange(cfg.GlobalFuturesExchange)
		}
		live.SetHealthPolicy(exchange.HealthPolicy{
			Window:             time.Duration(cfg.ExchangeErrorWindowSeconds) * time.Second,
			ErrorRateThreshold: cfg.ExchangeErrorRateThreshold,
			MinRequests:        cfg.ExchangeErrorMinRequests,
		})
		if cfg.EnableWebSocketFeed {
			live.EnableWebSocketFeed(context.Background(), time.Duration(cfg.WebSocketBookMaxAgeSeconds)*time.Second)
		}
		if cfg.OKXWebSocketEnabled != nil && *cfg.OKXWebSocketEnabled {
			staleAfter, _ := time.ParseDuration(cfg.OKXWSStaleAfter)
			reconnect, _ := time.ParseDuration(cfg.OKXWSReconnectInterval)
			maxBackoff, _ := time.ParseDuration(cfg.OKXWSMaxReconnectBackoff)
			live.EnableOKXBookWebSocket(context.Background(), cfg.OKXWSURL, staleAfter, reconnect, maxBackoff)
		}
		defer live.Close()
		venues := registry.New()
		if err := venues.Register("default", live); err != nil { log.Fatalf("register live venue adapter: %v", err) }
		adapter = registry.NewComposite(venues)
	case "MOCK":
		adapter = exchange.NewMockAdapter()
	default:
		log.Fatalf("invalid mode %q: must be MOCK or LIVE", cfg.Mode)
	}
	svc:=scanner.New(cfg,adapter)
	remoteStore, storeErr := supabase.NewFromEnv()
	var durableStore *supabase.DurableStore
	if storeErr != nil {
		// Supabase is an audit/history sink, never a prerequisite for Redis signals.
		log.Printf("warning: Supabase opportunity persistence disabled: %v", storeErr)
	} else if remoteStore != nil {
		outboxPath := os.Getenv("SUPABASE_SCANNER_OUTBOX_PATH")
		if outboxPath == "" {
			outboxPath = "/app/data/scanner/supabase-outbox.jsonl"
		}
		outbox, outboxErr := supabase.NewDurableOutbox(outboxPath, 0)
		if outboxErr != nil {
			log.Printf("warning: Supabase scanner outbox unavailable; persistence disabled: %v", outboxErr)
			remoteStore.Close()
		} else {
			durableStore, outboxErr = supabase.NewDurableStore(remoteStore, outbox, 0)
			if outboxErr != nil {
				log.Printf("warning: Supabase scanner outbox worker unavailable: %v", outboxErr)
				remoteStore.Close()
			} else {
				svc.SetSupabaseClient(durableStore)
				log.Printf("Supabase scanner durable outbox enabled -> %s", outbox.Path())
			}
		}
	}

	ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
	if durableStore != nil {
		go durableStore.Run(ctx)
		defer remoteStore.Close()
		log.Printf("Supabase scanner reconciliation worker started")
	}

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
	if remoteStore != nil {
		tgBot.SetOpportunityHistoryReader(remoteStore)
		tgBot.SetPositionLedgerReader(remoteStore)
	}
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
