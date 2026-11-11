# NAFAZ Spot–Perpetual Scanner Boilerplate

Starter Go project for the **read-only** Scanner described in the PRD: discover/match spot and perpetual markets, estimate a buy-spot/short-perpetual opportunity, apply a configurable net-ROI range, and record opportunity counts and observed duration.

> This is a development scaffold, not production trading software. It does not place, cancel, or close orders. Supports both `MOCK` mode and `LIVE` mode (Spot: Tokocrypto, Indodax, MEXC; Perpetual: OKX, MEXC read-only market data). Do not grant trading or withdrawal permissions.

## Requirements

- Go 1.22+
- Uses `github.com/shopspring/decimal` for fixed-point financial arithmetic

## CEX amount rules

The scanner and executor use **base-asset quantity** as the shared quantity contract.
Each market publishes an `amount_spec` describing how its venue accepts order amounts:

- `unit`: `BASE` for spot/base-denominated orders or `CONTRACT` for futures contracts.
- `step`: the venue's order-amount increment, expressed in `unit`.
- `min`: the minimum order amount, expressed in `unit`.
- `base_per_unit`: base-asset quantity represented by one `unit` (1 for `BASE`; contract
  size for `CONTRACT`).

Before opening a position, the scanner requires valid rules for both legs and the
executor rounds the shared base quantity down to a value representable by both markets.
An order below either venue minimum is rejected rather than sent with a guessed amount.
New venues must therefore expose authoritative step, minimum, and contract-size metadata
from their market metadata/API; add parser and regression coverage for spot and/or futures
as applicable. Keep scanner pair IDs separate from the CCXT unified market symbols used
for execution: the executor resolves IDs against loaded venue markets and fails closed
when the match is missing or ambiguous.

## Run

```sh
cp config.example.json config.json
go test ./...
go run ./cmd/nafaz-scanner -config config.json
```

### Live trading behavior and safeguards

Live entry sizing uses the signal budget, exposure cap, and fee reserve; it does not
poll private balance endpoints before submitting an order. The exchange validates
available spot funds when the limit order is sent and validates futures margin when
the hedge is submitted. Rejection alerts include the exchange error. A spot fill can
still occur before a futures-margin rejection, so the executor attempts its guarded
spot rollback and reports any unresolved exposure. Simulated entries continue to use
the paper account's balance snapshot.

INDODAX is the exception: its keys are issued on `indodax.com/trade_api` and are
TAPIv2-only. The legacy `/tapi` endpoint that CCXT still signs rejects them with
`invalid_version_key`, so the executor talks to `https://api.indodax.com/api/v2`
directly through `executor/indodax_v2.py` (`X-APIKEY` + `Sign` = HMAC-SHA256 over the
request query string). Create or rotate INDODAX keys on the Trade API page — keys
generated on the legacy API page will not authenticate.

Both images run as a non-root user. Set `APP_UID` and `APP_GID` in the deployment `.env`
file to the owner IDs of the host `data/` directory (default: 1000/1000) so the
scanner can write its persistent history bind mount without running as root. The executor image runs as a non-root user. Do not bake `config.json`, exchange API keys,
or Supabase service-role credentials into images; mount the runtime config or inject
secrets through the deployment environment. Redis is bound to host loopback in the
provided Compose file because it has no authentication configured.

### Run with Podman (VPS)

Untuk panduan lengkap deployment menggunakan Podman di VPS, lihat panduan di [PODMAN.md](./PODMAN.md).

```sh
mkdir -p data
sudo chown -R "$(id -u):$(id -g)" data
# Set APP_UID/APP_GID in .env only if your host data owner is not 1000:1000.
podman compose up -d --build
# atau:
# podman build -t nafaz-scanner . && podman run -d --name nafaz-scanner -p 8080:8080 -v $(pwd)/config.json:/app/config.json:ro,Z -v $(pwd)/data:/app/data:Z nafaz-scanner
```

Then call:

```sh
curl http://localhost:8080/health
curl -X POST -H "Authorization: Bearer $API_AUTH_TOKEN" http://localhost:8080/api/v1/scanner/scan
curl -H "Authorization: Bearer $API_AUTH_TOKEN" http://localhost:8080/api/v1/scanner/latest
curl -H "Authorization: Bearer $API_AUTH_TOKEN" 'http://localhost:8080/api/v1/scanner/opportunities/metrics'
curl -H "Authorization: Bearer $API_AUTH_TOKEN" 'http://localhost:8080/api/v1/scanner/opportunities/history'
```

The periodic scanner runs at `scan_interval_seconds`. `POST /api/v1/scanner/scan` also triggers a scan immediately. The mock account snapshot contains an untracked BTC perpetual exposure, so BTC/USDT is excluded with `RECONCILIATION_REQUIRED`; ETH is evaluated independently. If the required exposure read fails, candidates are marked `POSITION_STATUS_UNKNOWN` and fail closed. Opportunity history is stored in the configured JSON file using atomic replacement; replace `history.Store` with PostgreSQL (or another durable database) before multi-instance or production deployment.

`GET /api/v1/scanner/status` reports the latest scan duration, last successful scan, data age, stale state, and most recent error. Scan results also include total and per-phase timings plus per-exchange order-book/funding request counts, errors, and accumulated request time (microseconds). A successful result becomes stale after two configured scan intervals without another successful scan. The `/health` endpoint remains a process-liveness check.

## Profit filter

`min_net_roi_pct` and `max_net_roi_pct` are inclusive percentage values: `0.50` means 0.50%, not 0.005. The sample 0.50–2.00 range is illustrative and configurable. Net ROI is calculated over `TOTAL_DEPLOYED_CAPITAL` (spot notional plus perpetual notional divided by configured leverage). The calculator reports the quote currency of the matched pair; it does not convert USDT and USDC.

- `PROFITABLE_IN_RANGE`: positive net profit and ROI within the configured range.
- `NOT_PROFITABLE`: net profit is zero or negative.
- `BELOW_MIN_TARGET`: positive net profit, ROI below the minimum.
- `ABOVE_MAX_TARGET`: positive net profit, ROI above the maximum; outside the configured target, not a loss.

Opportunities are calculated using a **Hybrid Model**:
1. **Spread PnL (Basis Convergence):**
   - **Taker-Taker (`TAKER_TAKER`):** `(PerpBid - SpotAsk) * Qty` captures the entry premium between perpetual and spot.
   - **Maker-Taker (`MAKER_TAKER`):** `(PerpBid - SpotBid) * Qty` captures the basis premium plus local bid-ask spread by placing a Limit Buy at the local Best Bid.
2. **Funding PnL:** Cash flow collected across `EstimatedHoldingHours` based on real exchange `NextFundingTime` schedule.
3. **Execution Slippage:** Order book depth impact beyond best prices for both entry and exit. In Maker-Taker mode, spot entry slippage is 0 because the limit price is locked.
4. **Trading Fees:**
   - **Taker-Taker:** `spot_taker_fee_pct` + `perp_taker_fee_pct`.
   - **Maker-Taker:** `spot_maker_fee_pct` (entry) + `spot_taker_fee_pct` (exit) + `perp_taker_fee_pct` (entry + exit).
5. **Regime Protection:** Pairs with negative basis (`PerpBid < SpotEntryRef`) and non-positive funding rates are automatically excluded (`UNFAVORABLE_REGIME_NEGATIVE_BASIS_AND_FUNDING`).

### Maker-Taker & Executor Guardrails

In `MAKER_TAKER` execution mode, `domain.Opportunity` outputs structured guardrails for automated trading executors:
- `execution_mode`: `"MAKER_TAKER"` or `"TAKER_TAKER"`.
- `spot_target_limit_price`: The exact recommended Limit Buy price on the local spot exchange (Best Bid).
- `min_acceptable_perp_price`: The exact breakeven Perpetual price floor. If global Perp drops below this price, the Executor must immediately cancel or abort the local Limit order to avoid adverse selection (toxic fills).
- `spot_queue_depth`: The orderbook quantity at the best bid to evaluate queue position / fill likelihood.
- `max_order_ttl_seconds`: Time-to-live expiration timeout for resting limit orders.

### Scanner Opportunity Maximization Features

The scanner includes advanced optimization mechanisms to discover and capture maximum viable arbitrage opportunities:
1. **Adaptive Position Sizing (`enable_adaptive_sizing: true`, `min_allocated_capital: 10.0`):**
   - In traditional scanners, if a pair's order book depth is smaller than the target capital allocation (e.g., book only has $150 depth for a $500 target trade), the pair is discarded immediately with an "insufficient depth" error.
   - With Adaptive Sizing, the engine automatically scales the trade quantity down to the maximum executable capacity supported across all order books (provided the resulting capital meets `min_allocated_capital`, default $10).
   - Reports `capacity_pct` indicating book utilization (e.g., 30.0% of target allocation).
2. **Synthetic IDR ↔ USDT matching (`enable_synthetic_idr: true`):**
   - Direct spot/perpetual pairs are matched using the configured `quote_assets` (normally `USDT` and `USDC`).
   - When enabled, IDR/BIDR spot markets are additionally matched with same-base USDT perpetual markets.
   - IDR/BIDR order-book prices are normalized into USDT using the live USDT/IDR ticker. No hardcoded or stale FX rate is accepted for live spread calculations.
   - If the live FX ticker is unavailable or fails sanity validation, only that IDR/BIDR pair is skipped and reported; other USDT/USDC pairs continue scanning.
3. **Tunable Empty Book Circuit Breaker (`empty_book_freeze_minutes: 15`):**
   - Replaces rigid 6-hour lockouts with a configurable freeze window in minutes (default: 15 minutes) when a pair experiences 3 consecutive empty book scans, preventing prolonged omission of pairs during temporary liquidity gaps.

Only `PROFITABLE_IN_RANGE` candidates that pass other filters are marked eligible. Estimates depend on order-book depth, fees, funding assumptions, and scan timing. Funding rates in the adapter contract are decimals (0.0001 = 0.01% per settlement); fee configuration values are percentages (0.30 = 0.30%).

## Important implementation notes

- The mock order books use base-asset quantities. A live OKX adapter must normalize contract quantities using contract metadata before passing books to the calculator.
- The sample funding settlement calculation is a simple holding-hours/interval estimate; production code should account for exchange settlement timestamps and instrument-specific rules.
- Monetary and financial calculations in `scanner.Service` (quantity allocation, VWAP depth matching, slippage, spread, fees, and net ROI) use exact fixed-point arithmetic via `github.com/shopspring/decimal` to eliminate binary floating-point drift. Results are marshaled cleanly to output domain structures.
- The recorded duration is the time the scanner observed an opportunity, not the exact time it existed in the market. Resolution is limited by scan interval.
- API error scans do not count as successful misses and do not end an active opportunity.
- MOCK mode keeps the local API unauthenticated for development. LIVE mode requires `api_auth_token` (or `API_AUTH_TOKEN`) and protects all API routes with Bearer authentication; `/health` remains public for liveness checks. Keep the token in server-side secrets only. Rate limiting and network restrictions are still recommended.

## Telegram Bot Interface

The scanner includes an embedded Telegram Bot interface designed for Telegram Groups and direct chats:
- **Zero external dependencies**: Implemented using Go standard library `net/http`.
- **Group commands**:
  - `/menu` — Buka keyboard tombol interaktif.
  - `/scan` — Jalankan scan pasar on-demand.
  - `/latest` — Lihat hasil scan terakhir & top peluang ROI tertinggi.
  - `/pair <simbol>` — Cek rincian peluang koin tertentu (contoh: `/pair BTC` atau `/pair ETH`).
  - `/active` — Lihat peluang yang sedang berstatus aktif berkelanjutan di histori.
  - `/frozen` — Lihat daftar pasangan pasar yang sedang dibekukan oleh circuit breaker beserta waktu sisa pembekuan.
  - `/status` — Status scanner, mode, latensi bursa, umur data sukses, dan status alert.
  - `/metrics` — Statistik durasi & histori arbitrase.
  - `/mute [durasi]` — Bisukan notifikasi alert sementara (default: `1h`, atau contoh: `/mute 30m`, `/mute 2h`).
  - `/unmute` — Aktifkan kembali notifikasi alert.
  - `/help` — Petunjuk lengkap & bantuan.
- **Automated Alerts**: Proactively notifies the configured group whenever an opportunity matching target criteria (`PROFITABLE_IN_RANGE` & trading eligible) is observed, with contextual interactive buttons for deep-dive inspection.
- **Configurable Alert Rules**: Configure the minimum net ROI, minimum net profit, and per-opportunity cooldown. Defaults preserve the existing behavior: no extra thresholds and a 15-minute cooldown.
- **Access control**: Whitelists authorized group IDs (`allowed_chat_ids` / `chat_id`).

### Configuration

Add your Telegram bot configuration in `config.json` or pass via environment variables:

```json
{
  "telegram": {
    "enabled": true,
    "bot_token": "",
    "chat_id": -100123456789,
    "allowed_chat_ids": [-100123456789],
    "alert_on_new": true,
    "alert_cooldown_minutes": 15,
    "alert_min_net_roi_pct": 0,
    "alert_min_net_profit_quote": 0
  }
}
```

Alert thresholds are inclusive minimums applied only to already trading-eligible opportunities. A zero threshold disables that additional filter. `alert_min_net_profit_quote` uses the pair's quote asset; it does not convert between quote currencies.

Or via environment variables:
- `TELEGRAM_ENABLED=true`
- `TELEGRAM_BOT_TOKEN="your_token"`
- `TELEGRAM_CHAT_ID="-100123456789"`

## Layout

```text
cmd/nafaz-scanner/       HTTP server and scheduled scan loop
internal/config/         JSON configuration and validation
internal/domain/         Market, book, opportunity, and scan models
internal/exchange/       Read-only adapter contract, deterministic mock, and funding rate worker
internal/redishandler/   Redis pipeline publisher and state coordinator (TTL + Pub/Sub)
internal/scanner/        Pair matching, 2-layer filter (Funding + Maker-Taker Spread), ranking
internal/history/        Opportunity episode lifecycle and metrics
internal/api/            Read-only REST API
internal/telegram/       Telegram bot client, group command handler, and alerts
executor/                Python Executor service (CCXT + asyncio + Redis)
tests/                   Python Executor unit & integration test suite
```

## Hybrid Maker-Taker + Positive Funding Rate Arbitrage Architecture

### Core Rules (Strict Constraints)
1. **Local CEX (Indodax, Tokocrypto):** ONLY Spot one-way transactions (`Buy Spot` for Entry, `Sell Spot` for Exit).
2. **Global CEX Futures (Binance, Bybit, OKX, MEXC):** ONLY Short 1x transactions (`Market Sell` when local Limit Buy fills, `Market Buy reduceOnly=True` when local Limit Sell fills). Never open Long Futures.
3. **No coin transfers / withdrawals** between exchanges.

### Scanner Golang (2-Layer Opportunity Filter)
1. **Lapis 1 (Funding Filter):**
   - Polls `premiumIndex` every 30–60s via background `FundingWorker`.
   - Requires `lastFundingRate >= 0.0002` (+0.02%).
   - If `lastFundingRate < 0`, immediately sends `CANCEL_ALL_NEGATIVE_FUNDING` via Redis Pub/Sub channel `arb:emergency_cancel` to cancel pending local Limit Buys.
2. **Lapis 2 (Maker-Taker Entry Spread):**
   - Target Limit Buy: Maker price `LocalBestBid + 1 tick`.
   - Entry Spread (%): `((GlobalBestBidUSDT * UsdtIdrMidRate) - TargetLimitBuyIDR) / (GlobalBestBidUSDT * UsdtIdrMidRate) * 100`.
   - Minimum Entry Spread (%): `(LocalMakerFeePct + GlobalTakerFeePct) - FundingSubsidyPct`.
     - `FundingSubsidyPct = CurrentFundingRatePct * 1.0` if `minutesToNextFunding <= 30` (Sniping Window).
     - `FundingSubsidyPct = CurrentFundingRatePct * 0.5` if `minutesToNextFunding > 30`.
     - Lower bound minimum floor: `0.08%`.
   - Adverse Selection Threshold: `cancel_if_global_bid_drops_below_usdt = TargetLimitBuyIDR / (UsdtIdrMidRate * (1 - MinEntrySpreadPct/100))`.

### Redis Hybrid Scheme (Golang Scanner <-> Python Executor)
- `arb:signal:{local_ex}:{symbol}` (String JSON, TTL 3s): Latest `ExecutorPayload` from Scanner.
- `arb:live_price:{local_ex}:{symbol}` (String JSON, TTL 3s): Real-time pricing snapshot (bids/asks/FX) updated continuously on orderbook changes.
- `arb:active_pos:{local_ex}:{symbol}` (String JSON, written by Python): Active hedged position (`entry_price_idr`, `short_entry_usdt`, `qty`, `accumulated_funding_usdt`). Prevents scanner from emitting duplicate entry signals while keeping live prices updated.
- `arb:pending_entry:{local_ex}:{symbol}` (String JSON, written by Python): Persisted pending entry order. On executor startup, orders still open on the venue are re-adopted (so a crash cannot leave a resting buy unmonitored and unhedged); records whose order is gone are cleared.
- Channel `arb:signal_notify`: Published by Golang with `{local_ex}:{symbol}`. Python consumes and verifies `latency <= 500ms`.
- Channel `arb:emergency_cancel`: Instant cancellation broadcast on negative funding rates.

Scanner/Executor fail-closed behavior: if the executor position check fails, the
pair is excluded (`POSITION_STATE_UNKNOWN`); if a signal cannot be published to
Redis, the scan is marked incomplete; a USDT/IDR ticker failure skips the IDR
pair entirely (there is no fallback: the rate is only ever taken live from the
CEX that trades IDR, never hardcoded or synthetic — a stale rate would destroy
spread accuracy and profit); and the scanner refuses to start in LIVE mode when
Redis is enabled but unreachable. Before an emergency spot rollback after a
hedge error, the
executor checks the actual futures position via `fetch_positions`: an open (or
unverifiable) short blocks the rollback and marks the pair
`HEDGE_STATE_UNKNOWN` for manual inspection.

### Python Executor (4 States)
- **State 1 (Entry Order Pegging):** On fresh signal, places Maker Limit Buy at `recommended_limit_buy_idr`. Monitors Global Bid; cancels order if Global Bid falls below `cancel_if_global_bid_drops_below_usdt`.
- **State 2 (Partial Fill & Instant Short Hedge):** Tracks local filled quantity. Once partial fill delta >= $10, immediately executes `open_global_short_market` (Market Sell 1x) on Global Futures for the exact filled coin quantity and records active position in Redis.
- **State 3 (Dynamic Exit Limit Sell + Funding Decay Pegging):** Never uses static exit limit sell. Re-calculates and cancels/replaces local Limit Sell IDR whenever Global Ask moves > 0.15% OR funding hour passes:
  `effective_exit_spread_pct = base_target_exit_spread_pct - ((accumulated_funding_usdt / position_value_usdt) * 100)`
  `limit_sell_idr = (global_ask_usdt * usdt_idr_rate) * (1 + effective_exit_spread_pct / 100)`
- **State 4 (Exit Execution):** When local Limit Sell fills (partial or full), immediately executes Market Buy (`reduceOnly=True`) on Global Futures to close the Short position. Deletes `arb:active_pos` in Redis when completely exited.


## Reliability & Risk Controls

These knobs are all optional and default to the pre-Fase-3 behaviour, so an
existing deployment keeps working unchanged unless it opts in.

### Scanner (`config.json` / environment)

| Key | Env | Default | Effect |
|---|---|---|---|
| `min_top_of_book_quote_usdt` | `MIN_TOP_OF_BOOK_QUOTE_USDT` | `0` (off) | Rejects pairs whose **best price level** holds less than this notional on the spot ask or perp bid. The threshold is capped at the per-pair allocation budget, so a small account is never asked for liquidity it does not need. A weak proxy for the "24h volume" idea: it costs no extra REST calls and measures exactly what a taker order will walk into. |
| `okx_ws_enabled` | `OKX_WS_ENABLED` | `true` in LIVE, `false` in MOCK | Starts one persistent OKX public WebSocket connection and subscribes to active SWAP instruments on the `books` channel. A connected socket is not considered ready until a valid snapshot is received. |
| `okx_ws_url` | `OKX_WS_URL` | `wss://ws.okx.com/ws/v5/public` | Public endpoint. Override with the regional WebSocket host required by the OKX account region. |
| `okx_ws_stale_after` | `OKX_WS_STALE_AFTER` | `65s` | Maximum age since the last instrument-specific book frame. OKX may send an empty heartbeat after 60 seconds without book changes; connection ping/pong refreshes receive-time freshness separately. Stale or not-yet-snapshotted books use REST. Accepts Go durations such as `65s`. |
| `okx_ws_reconnect_interval` | `OKX_WS_RECONNECT_INTERVAL` | `1s` | Initial reconnect delay after disconnect or subscription failure. |
| `okx_ws_max_reconnect_backoff` | `OKX_WS_MAX_RECONNECT_BACKOFF` | `30s` | Maximum exponential reconnect delay. Must be at least the initial reconnect interval. |
| `enable_websocket_feed` | `ENABLE_WEBSOCKET_FEED` | `false` | Legacy best-effort feed switch for non-OKX streams. It does not control the OKX books manager. |
| `websocket_book_max_age_seconds` | `WEBSOCKET_BOOK_MAX_AGE_SECONDS` | `2` | Maximum age for the legacy non-OKX websocket cache. |
| `exchange_error_window_seconds` | — | `300` | Maintenance-detection window (P2-05). |
| `exchange_error_rate_threshold` | — | `0.5` | Error ratio that marks a venue unavailable for the rest of the window. |
| `exchange_error_min_requests` | — | `5` | Minimum observations before the ratio is trusted. |

**OKX order-book integrity:** the OKX manager subscribes per instrument to
`books`, replaces local state on `snapshot`, and applies `update` price-level
deltas only when `prevSeqId` matches the cached `seqId`. Duplicate updates are
ignored. A sequence gap, malformed update, disconnect, or stale receive time
invalidates the cache; the scanner then uses a per-instrument single-flight REST
recovery request through the shared OKX REST limiter. It never serves a partial
or pre-snapshot WebSocket book. The mock WebSocket tests verify this behavior;
they do not constitute a live OKX/VPS soak test. Indodax and MEXC spot feeds
remain outside this OKX-specific change.

### Executor (`executor` block in `config.json` / environment)\n\nPending maker entry orders default to a 120-second TTL (`max_order_ttl_seconds` / `MAX_ORDER_TTL_SECONDS`). TTL applies to both unfilled and partially filled orders: the executor cancels the remaining quantity, reconciles the venue's final cumulative fill, and hedges that fill before releasing unused reserves. Fill/hedge progress is persisted for restart recovery. The independent adverse-selection guard can still cancel sooner when the global bid crosses the signal's cancellation threshold; this safety guard is intentionally retained.

Unfilled maker quotes can be refreshed after 45 seconds (`maker_refresh_after_seconds` / `MAKER_REFRESH_AFTER_SECONDS`; set `0` to disable), with a 300-second per-pair cooldown (`maker_refresh_cooldown_seconds` / `MAKER_REFRESH_COOLDOWN_SECONDS`). Refresh is deliberately cancel-and-re-signal, not blind cancel/replace: it requires a fresh live snapshot, a higher local best bid below the ask, and the candidate price to preserve the configured/scanner minimum entry spread. It only applies when the order has zero filled and zero hedged quantity. The executor confirms cancellation and reconciles any fill race, then waits for the scanner's next fresh signal to submit a new order. It does not submit a replacement itself, so there is no untracked replacement-order window. TTL and adverse-selection cancellation remain higher-priority guards.

| Key | Env | Default | Effect |
|---|---|---|---|
| `max_entry_slippage_pct` | `MAX_ENTRY_SLIPPAGE_PCT` | `0.003` | Aborts the hedge if the futures ask moved more than this fraction past the expected short price. **Fail-open**: an unreadable ticker logs and proceeds, because refusing to hedge leaves spot naked. `0` disables. |
| `entry_revalidation_min_spread_pct` | `ENTRY_REVALIDATION_MIN_SPREAD_PCT` | `0` (use payload floor) | Before placing the maker buy, re-quotes the global futures bid and rejects the signal when the live entry spread fell below this floor. When `0`, the payload's `min_entry_spread_pct` from the scanner is used instead. **Fail-open** on an unreadable ticker. |
| `max_exposure_per_exchange_usdt` | `MAX_EXPOSURE_PER_EXCHANGE_USDT` | `0` (off) | Caps total committed capital per venue across open positions *and* pending entries. Left at `0` because no per-venue limit has been agreed. |
| `balance_mismatch_fraction` | `BALANCE_MISMATCH_FRACTION` | `0.20` | Simulated-entry balance drift threshold; live entries do not poll balances for this guard. `0` disables mismatch comparison. |
| `max_consecutive_order_failures` | `MAX_CONSECUTIVE_ORDER_FAILURES` | `50` | Halts only the affected venue for new entries after 50 consecutive system/order-transport failures. Business rejections such as insufficient balance, minimum notional, and invalid symbol do not count. `0` disables. Clear via `resume_exchange()` or restart. |

**Kill switch:** publish `{"action": "on"}` to the `arb:kill_switch` Redis channel (or use the Telegram `/killswitch on` / `/killswitch off` commands). ON halts *all* new entries globally until an explicit OFF — it never auto-resets on a UTC rollover. Open positions continue to be managed.
| `estimated_round_trip_fee_pct` | `ESTIMATED_ROUND_TRIP_FEE_PCT` | `0.001` | Shrinks the position so both legs' fees are still covered by the exit. If that drops the size below `min_allocated_capital` the entry is **rejected** rather than silently undersized. |
| `private_stream_enabled` | `PRIVATE_STREAM_ENABLED` | `false` | Reads order/balance updates from a private feed cache before polling REST. Stale entries fall back to REST. This is the cache and fallback path, not a venue auth client. **Status: the cache is not yet populated** — no websocket subscriber writes to it, so enabling this does not currently reduce REST polling. Leave it off until a feed publisher exists; the rate-limit resilience below does not depend on it. |
| `private_stream_max_age_seconds` | `PRIVATE_STREAM_MAX_AGE_SECONDS` | `5.0` | How long a pushed order update is trusted before re-polling. |
| `monitor_interval_seconds` | `MONITOR_INTERVAL_SECONDS` | `0.5` | Monitoring loop cadence. The venue rate-limits **per IP**, so a sub-second tick with several pending orders drains the quota and order-status reads start answering HTTP 429. |
| `max_order_fetch_failures` | `MAX_ORDER_FETCH_FAILURES` | `10` | Consecutive order-status fetch failures tolerated for one pending entry before it is force-reconciled. Without this a venue outage pins the order and its reserved funds open indefinitely. |
| `order_fetch_backoff_max_seconds` | `ORDER_FETCH_BACKOFF_MAX_SECONDS` | `60.0` | Ceiling for the per-order exponential backoff applied to failing status polls. |
| `venue_rate_limit_cooldown_seconds` | `VENUE_RATE_LIMIT_COOLDOWN_SECONDS` | `30.0` | How long a venue answering HTTP 429/503 on order placement is paused for new entries. The cooldown clears itself; open positions stay managed throughout. |

Order-status reads are resilient to venue throttling: a failed fetch counts
toward `max_order_fetch_failures` and spaces the next attempt out with
exponential backoff up to `order_fetch_backoff_max_seconds`, instead of retrying
on every monitoring tick. The maker-order TTL is enforced from the local clock as
well as from a fetched payload, so an order cannot be pinned open just because
its status is unreadable. Reconciliation that cannot confirm a terminal state
retains the pending order and its reserved funds rather than assuming the worst.

Every numeric executor setting is range-checked at startup by
`ExecutorConfig.sanitize()`. A value out of range is replaced by its default and a
warning is printed, and a `0` that disables a safety net (slippage guard, fee
buffer) is called out explicitly so it is a visible choice rather than an accident.

Order placement sends a deterministic `clientOrderId` (`{pair}-{leg}-{seq}`) on
every leg so a retried submit is rejected by the venue instead of double-filling.
The sequence is seeded from wall-clock time at startup, so ids never collide
across process restarts. Binance Cloud venues such as Tokocrypto validate the id
against the broker id registered to the account and reject anything else with
error `3703 Invalid client ID`. That broker id is issued per account, so it is
configuration, not code: set `exchanges.tokocrypto.options.broker_id` in
`config.json` or `TOKOCRYPTO_BROKER_ID` in the environment. The executor then
prepends it at submission time and trims the pair slug (never the
`-{leg}-{seq}` tail) to stay within the venue's 36-character limit. When it is
unset, no id is sent and ccxt mints a venue-native one, which the venue accepts.


## Supabase Database

The initial Supabase PostgreSQL schema and migration workflow are documented in
[`supabase/README.md`](./supabase/README.md). The versioned migration creates
operational tables for exchange accounts, strategy/risk configuration, market pairs,
scanner opportunities, orders/fills, positions/legs, funding payments, closed trades,
balances, risk events, and executor events/status snapshots.

**Runtime persistence:** the Go scanner writes evaluated snapshots to
`scanner_opportunities`; the Python executor writes `positions`, `orders`,
`order_fills`, `closed_trades`, `executor_events`, and `risk_events`.
Scanner opportunity snapshots are first committed to a persistent JSONL outbox
and replayed by a background delivery reconciliation worker; executor events use
a persistent SQLite outbox. Upserts and order/fill writes use deterministic
idempotency keys. Redis remains authoritative for live coordination and active
exposure recovery; the scanner's existing JSON history remains in place for its API.

Set `SUPABASE_URL` and `SUPABASE_KEY` in the server environment to enable both
writers. `SUPABASE_KEY` must be the server-only Supabase `service_role` secret,
never the anon/publishable key. Do not commit it, put it in the image, or expose it
to frontend/Telegram clients. If credentials are absent, persistence is disabled;
a database outage does not block exchange risk management.

Runtime exchange reconciliation: the executor periodically re-runs the
universal order-registry reconciliation and verifies restored/live futures
short exposure against the venue. Spot and perpetual wallet balances are
captured as append-only `balance_snapshots` observations for audit. A failed
balance read is not recorded as zero and blocks new entries on the affected
venue scope. Balance snapshots are observations only: because wallets may
contain funds unrelated to bot positions, they are not treated as proof of
position ownership or used to automatically resize/close a position.
`position_legs`, `funding_payments`, and `executor_status_snapshots` remain
schema-only. Supabase history must never replace authenticated exchange-side
exposure verification.
See [Supabase runtime and deployment notes](./supabase/README.md).


For LIVE mode, configure a cryptographically random `api_auth_token` of at least 32 characters in `config.json` or `API_AUTH_TOKEN` in the server environment. The scanner refuses to load LIVE configuration without it. LIVE startup also verifies restored futures short quantities against the exchange before resuming automatic position management.

The executor remains in paper mode by default (`EXECUTOR_MOCK_MODE=true`). Do not set
`EXECUTOR_MOCK_MODE=false` until the scanner's `mode`, exchange credentials,
quote assets, balance permissions, and venue-specific order/funding/position behavior
have all been validated in paper mode. Scanner LIVE mode and executor LIVE mode are
separate switches and must be configured deliberately.