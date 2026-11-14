package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/api"
	"github.com/nafaz/spot-perp-scanner/internal/config"
	"github.com/nafaz/spot-perp-scanner/internal/exchange"
	"github.com/nafaz/spot-perp-scanner/internal/history"
	"github.com/nafaz/spot-perp-scanner/internal/postgres"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/nafaz/spot-perp-scanner/internal/scanner"
	"github.com/nafaz/spot-perp-scanner/internal/supabase"
	"github.com/nafaz/spot-perp-scanner/internal/telegram"
)
func main(){
	path:=flag.String("config","config.json","path to JSON config");flag.Parse()
	if abs, err := filepath.Abs(*path); err == nil {
		*path = abs
	}
	cfg,err:=config.Load(*path);if err!=nil{log.Fatal(err)}
	var adapter exchange.ReadOnlyAdapter
	switch cfg.Mode {
	case "LIVE":
		live := exchange.NewLiveAdapter()
		if dir := filepath.Dir(cfg.HistoryPath); dir != "" && dir != "." {
			live.SetMarketCacheDir(dir)
		} else {
			live.SetMarketCacheDir("./data")
		}
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
		indodaxWSEnabled := strings.TrimSpace(cfg.IndodaxWSToken) != ""
		if cfg.IndodaxWebSocketEnabled != nil {
			indodaxWSEnabled = *cfg.IndodaxWebSocketEnabled
		}
		if indodaxWSEnabled {
			if strings.TrimSpace(cfg.IndodaxWSToken) == "" {
				log.Printf("warning: indodax_ws_enabled=true without indodax_ws_token; Indodax will remain on throttled REST fallback")
			} else {
				staleAfter, _ := time.ParseDuration(cfg.IndodaxWSStaleAfter)
				reconnect, _ := time.ParseDuration(cfg.OKXWSReconnectInterval)
				maxBackoff, _ := time.ParseDuration(cfg.OKXWSMaxReconnectBackoff)
				live.EnableIndodaxBookWebSocket(context.Background(), cfg.IndodaxWSURL, cfg.IndodaxWSToken, staleAfter, reconnect, maxBackoff)
			}
		}
		cacheDir := os.Getenv("MARKET_CACHE_DIR")
		if cacheDir == "" {
			if _, err := os.Stat("cache/indodax_spot_markets.json"); err == nil {
				cacheDir = "cache"
			} else {
				cacheDir = "/app/cache"
			}
		}
		live.SetMarketCacheDir(cacheDir)

		defer live.Close()
		adapter = live
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

	// Initialize local PostgreSQL client if configured
	var pgClient *postgres.Client
	pgDSN := strings.TrimSpace(os.Getenv("POSTGRES_DSN"))
	if pgDSN == "" {
		pgDSN = strings.TrimSpace(cfg.PostgresDSN)
	}
	if pgDSN != "" {
		pc, err := postgres.NewFromDSN(pgDSN)
		if err != nil {
			log.Printf("warning: postgres connection failed: %v", err)
		} else {
			pgClient = pc
			defer pgClient.Close()
			log.Printf("postgres local persistence connected")
		}
	}

	// Synchronize configuration from Database (PostgreSQL / Supabase) / Redis (SSOT)
	if pgClient != nil {
		dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if dbCfg, err := pgClient.LoadStrategyConfig(dbCtx, "default"); err == nil && len(dbCfg) > 0 {
			config.ApplyDynamicConfig(&cfg, dbCfg)
			svc.UpdateConfig(cfg)
			log.Printf("applied dynamic strategy config from PostgreSQL database")
			if redisClient != nil {
				_ = redisClient.SaveConfig(dbCtx, dbCfg)
			}
		} else if err != nil {
			log.Printf("info: could not load strategy config from PostgreSQL: %v", err)
		}
		dbCancel()
	} else if remoteStore != nil {
		dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if dbCfg, err := remoteStore.LoadStrategyConfig(dbCtx, "default"); err == nil && len(dbCfg) > 0 {
			config.ApplyDynamicConfig(&cfg, dbCfg)
			svc.UpdateConfig(cfg)
			log.Printf("applied dynamic strategy config from Supabase database")
			if redisClient != nil {
				_ = redisClient.SaveConfig(dbCtx, dbCfg)
			}
		} else if err != nil {
			log.Printf("info: could not load strategy config from Supabase: %v", err)
		}
		dbCancel()
	} else if redisClient != nil {
		rCtx, rCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if rCfg, err := redisClient.LoadConfig(rCtx); err == nil && len(rCfg) > 0 {
			config.ApplyDynamicConfig(&cfg, rCfg)
			svc.UpdateConfig(cfg)
			log.Printf("applied dynamic strategy config from Redis")
		}
		rCancel()
	}

	store,err:=history.New(cfg.HistoryPath);if err!=nil{log.Fatal(err)}
	server:=api.New(svc,store,cfg)
	if redisClient != nil {
		server.SetExecutorClient(redisClient)
	}
	go server.RunPeriodic(ctx)

	tgBot:=telegram.New(cfg,server)
	tgBot.SetConfigPath(*path)
	if pgClient != nil {
		tgBot.SetPositionLedgerReader(pgClient)
		tgBot.SetStrategyConfigStore(pgClient)
	} else if remoteStore != nil {
		tgBot.SetOpportunityHistoryReader(remoteStore)
		tgBot.SetPositionLedgerReader(remoteStore)
		tgBot.SetStrategyConfigStore(remoteStore)
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
