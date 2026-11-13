<!-- Dokumentasi proyek ini ditulis dalam Bahasa Indonesia. Nama berkas, simbol kode, nama fungsi, nama API, dan istilah teknis baku dipertahankan agar dapat dicari di repositori. -->

# Peta Pemanggilan API Privat — Nafazbot

Acuan: `9cc3dcd11d0999e828309053ba4cffce98a28675` on `feat/unified-dynamic-fee-engine`.
Cabang audit: `audit/architecture-baseline-regression-contract`.

## 1. Klasifikasi

- **PRIVATE**: operasi akun/order/posisi/transaksi/biaya yang memerlukan autentikasi.
- **PUBLIC**: unauthenticated data pasar, ticker, instruments, funding rate, public books.
- **MIXED/VERIFIKASI**: CCXT method can be private or public depending on venue/version; verifikasi generated endpoint mapping.
- **NOT A CEX CALL**: Redis, Supabase, Telegram, dan DNS.

The Python ExchangeClient facade is the principal batas API privat bursa. Saat ini fasad tersebut meneruskan permintaan langsung ke instans CCXT atau Indodax TAPIv2. The Go scanner exchange adapter inspected here makes public market-data requests; it is not the executor API privat gateway.

## 2. Inventaris operasi privat utama

| Operation | Facade method / direct call | Underlying call or endpoint | CEX scope | Caller families | Risk / required policy |
|---|---|---|---|---|---|
| Spot limit buy | `place_local_limit_buy` | CCXT `create_limit_buy_order`; Indodax `POST /api/v2/order` | Active Spot venue (Tokocrypto/Indodax; generic CCXT only if configured) | Entry submission | TINGGI; durable intent first; never blind retry |
| Spot limit sell | `place_local_limit_sell` | CCXT `create_limit_sell_order`; Indodax `POST /api/v2/order` | Active Spot venue | Exit order | TINGGI; durable intent first |
| Spot emergency market sell | `place_local_market_sell` | CCXT `create_market_sell_order`; Indodax `POST /api/v2/order` | Active Spot venue | Rollback, force-close liquidation | DARURAT; priority queue; ambiguous result wajib reconcile |
| Spot cancel | `cancel_local_order` | CCXT `cancel_order`; Indodax `DELETE /api/v2/order` | Active Spot venue | Cancel/replace, force close, pemulihan | TINGGI; cancel acknowledgement is not terminal proof |
| Spot status order | `fetch_local_order` | CCXT `fetch_order`; Tokocrypto raw `privateGetOpenV1Orders`; Indodax `GET /api/v2/order` | Active Spot venue | Entry/exit monitor, cancel rekonsiliasi, rollback, pemulihan | TINGGI; validate exact order ID |
| Spot lookup by ID klien | `fetch_local_order_by_client_id` / `_find_order_by_client_id` | Native `fetch_order_by_client_id` if present; else open/closed/canceled history, paged archive, `fetch_my_trades` then `fetch_order` | Active Spot venue | Ambiguous create pemulihan, registry rekonsiliasi | KRITIS; incomplete history is UNKNOWN, not absent |
| Spot open-order inventory | `fetch_local_open_orders` | CCXT `fetch_open_orders`; Tokocrypto raw `privateGetOpenV1Orders(type=1)`; Indodax `GET /api/v2/openOrders` | All configured Spot clients swept at pemulihan | Startup pemulihan, Telegram/monitoring through executor | KRITIS; account-wide pagination/completeness required |
| Spot closed/canceled history | Helper lookup | CCXT `fetch_closed_orders`, `fetch_canceled_orders`; Indodax `GET /api/v2/order/histories` | Venue-dependent | Client-ID pemulihan, riwayat order | KRITIS; time-window and pagination limitations |
| Spot fills/trades | `fetch_order_trades` | CCXT `fetch_my_trades` or `fetch_order_trades`; Indodax `GET /api/v2/myTrades` | Active Spot venue | TradeFillReconciliationWorker | TINGGI; dedupe by exchange trade ID and reconcile sum to cumulative fill |
| Spot balance | `get_balances`, `fetch_balance_report` | CCXT `fetch_balance`; Indodax `GET /api/v2/account` | Active Spot and registered Spot balance clients | Funding preflight, sizing, mismatch, Telegram balance | TINGGI; stale cache and account scope wajib be explicit |
| Spot trading fee | `fetch_pair_fee_rates` | Tokocrypto `privateGetOpenV1AccountSpot`; else CCXT `fetch_trading_fee` | Tokocrypto Spot / generic CCXT Spot | Fee preflight | SEDANG; cache with expiry; never treat missing fee as verified |
| Futures open short | `open_global_short_market` | CCXT `create_market_sell_order` with `reduceOnly=False` and venue position-side params | Active Futures venue (OKX/MEXC evidenced; generic CCXT conditional) | Entry hedge | KRITIS; durable intent, one write path, hedge state verification |
| Futures close short | `close_global_short_market` | CCXT `create_market_buy_order` with `reduceOnly=True` and position-side params | Active Futures venue | Exit, force close, rollback/repair | DARURAT/TINGGI; reduce-only invariant |
| Futures position | `fetch_positions` | CCXT `fetch_positions([symbol])`, fallback all positions only on TypeError/AttributeError | Active Futures venue | Hedge ambiguity, rollback guard, force-close, startup pemulihan, exposure rekonsiliasi | KRITIS; malformed/None is UNKNOWN, never flat |
| Futures status order | `fetch_global_order` | CCXT `fetch_order` | Active Futures venue | Hedge/close rekonsiliasi | TINGGI |
| Futures lookup by ID klien | `fetch_global_order_by_client_id` / helper | Native lookup or open/closed/canceled history/archive/private trades then order lookup | Active Futures venue | Ambiguous create pemulihan | KRITIS; exact ID and authoritative history |
| Futures open-order inventory | `fetch_global_open_orders` | CCXT `fetch_open_orders` | Configured Futures venues | Startup pemulihan | KRITIS; account-wide sweep and pagination |
| Futures fills/trades | `fetch_order_trades` | CCXT `fetch_my_trades` / `fetch_order_trades` | Active Futures venue | Fill ledger | TINGGI |
| Futures balance | `get_balances`, `fetch_balance_report` | CCXT `fetch_balance`; MEXC uses `type=swap` | Active Futures and auxiliary Futures clients | Sizing, exposure, Telegram | TINGGI; venue market type explicit |
| Funding history | `fetch_funding_history` | CCXT `fetch_funding_history` | Active Futures venue if supported | Funding rekonsiliasi | PRIVATE/MIXED; verifikasi venue endpoint auth and weight |
| Funding rate | `fetch_funding_rate` | CCXT `fetch_funding_rate` | Active Futures venue | Strategy/funding checks | PUBLIC/MIXED; verifikasi generated endpoint |
| Futures ticker | `fetch_global_ticker` | CCXT `fetch_ticker` | Active Futures venue | Entry revalidation, exit price | PUBLIC data pasar, but wajib share public data budget not private order budget |

## 3. Venue-specific private endpoint detail

### Tokocrypto — Spot
Direct generated CCXT private methods evidenced:
- `privateGetOpenV1Orders(...)` in `executor/exchange_orders.py`: exact order fallback and account-wide open order list (`type=1`).
- `privateGetOpenV1AccountSpot()` in `executor/venues/tokocrypto.py`: account maker/taker commission read.
CCXT standard private methods are also used through the facade for create/cancel/order/balance/trades/history. Tokocrypto rewrites client order IDs; logical-to-wire mapping wajib remain intact.
**Tidak ada Tokocrypto Futures execution route is established in this baseline.**

### Indodax — Spot TAPIv2
All signed requests flow through `IndodaxTAPIv2._request` in `executor/indodax_v2.py`:
- `POST /api/v2/order`: create limit buy/sell and market sell.
- `DELETE /api/v2/order`: cancel.
- `GET /api/v2/order?symbol=...&orderId=...`: status.
- `GET /api/v2/order?symbol=...&origClientOrderId=...`: client-ID lookup.
- `GET /api/v2/order/histories`: closed/canceled/rejected/expired riwayat order (bounded seven-day query window).
- `GET /api/v2/myTrades`: authenticated individual fills.
- `GET /api/v2/account`: account balances.
- `GET /api/v2/openOrders`: account/symbol open-order inventory.
The same adapter also calls public depth/order-book endpoint(s), which are not private. HMAC-SHA256 signing and `X-APIKEY`/`Sign` headers are adapter-specific. Indodax TAPIv2 is not replaceable by legacy CCXT TAPIv1 for these credentials.
**Tidak ada Indodax Futures execution route is established.**

### OKX — Futures
CCXT authenticated operation families:
- create market sell / market buy with `posSide=short` and reduceOnly semantics;
- fetch positions;
- fetch order / order terbuka / client-ID history search;
- private trade history;
- futures balance;
- funding history (if supported by installed CCXT/API account);
- funding rate and ticker are market-data reads, not assumed private.
Position mode wajib match `posSide`; do not strip or alter `reduceOnly`.
**Tidak ada OKX Spot scanner/trading route is certified by this baseline.**

### MEXC — Futures
CCXT authenticated operation families:
- create market sell / market buy;
- fetch positions;
- fetch order / order terbuka / history / client-ID pemulihan;
- private trade history;
- futures balance with explicit `type=swap`.
Funding methods are conditional on CCXT capability.
MEXC uses CCXT market type `swap`; pertahankan symbol resolver and amount/contract conversion.
**MEXC Spot is present in Go scanner data pasar, but Python executor's balance registry maps MEXC as Futures. Jangan infer Spot execution from scanner discovery.**

### Binance and Bybit — Futures auxiliary clients
`BALANCE_MARKETS` registers Binance and Bybit as Futures balance clients when credentials are configured. This proves balance-report/read capability only. It does not certify open-order sweep, positions, order create/cancel, client-ID pemulihan, private trades, or scanner integration.
**Status: NOT CERTIFIED FOR LIVE TRADING.**

## 4. Caller / pekerja map

| Caller or pekerja | Source files | Private API families reached | Criticality |
|---|---|---|---|
| Entry signal validation | `executor/engine_signal.py` | Futures ticker (public), balance/mismatch and fee preflight via engine paths | TINGGI |
| Entry submission / hedge | `executor/engine_entry_submission.py`, `executor/entry_hedge.py`, `executor/entry_orders.py` | Spot limit buy, Futures market sell, balance, status order | KRITIS |
| Pending entry monitor | `executor/entry_monitoring.py`, `executor/entry_orders.py` | Spot fetch order, cancel, order fill | TINGGI |
| Exit pricing / order placement | `executor/exit_pricing.py`, `executor/exit_orders.py`, `executor/exit_execution.py` | Spot order book/ticker, limit sell, status/cancel, Futures close | KRITIS |
| Rollback | `executor/rollback.py` | Spot emergency market sell, Futures positions, order verification | DARURAT |
| Force close | `executor/cancel.py`, `executor/force_close.py`, `executor/force_close_legs.py`, `executor/force_close_lifecycle.py` | Cancel, status, Futures positions, Futures reduceOnly close, Spot market sell | DARURAT |
| Startup and manual pemulihan | `executor/engine_pemulihan.py`, `executor/pemulihan_discovery.py`, `executor/pemulihan_registry.py`, `executor/pemulihan_orphans.py`, `executor/pemulihan_adoption.py`, `executor/order_pemulihan.py` | Account-wide order terbuka, order/client-ID history, balances, positions | KRITIS |
| Fill rekonsiliasi | `executor/trade_fill_rekonsiliasi.py`, `executor/exchange_trades.py` | Private trade history and order lookup | TINGGI |
| Risk/exposure rekonsiliasi | `executor/exposure_rekonsiliasi.py`, `executor/risk_exposure.py`, `executor/risk.py`, `executor/risk_state.py` | Spot balances, Futures positions, sometimes status order | KRITIS |
| Fee preflight | `executor/engine_fee_preflight.py`, `executor/exchange_fee_balance.py`, `executor/venues/tokocrypto.py` | Fee profile and balances | TINGGI |
| Telegram balance request | `internal/telegram/handlers.go` -> Redis request -> `executor/engine_runtime.py` -> ExchangeClient | Balance report | SEDANG/TINGGI |
| Recovery/force-close Telegram command | `internal/telegram/handlers.go` -> Redis Pub/Sub -> `executor/engine_runtime.py` | Triggers pemulihan/cancel/close calls | DARURAT |
| Go scanner | `internal/exchange/live*.go`, `internal/exchange/venue_*.go`, `internal/scanner/*.go` | Public REST market, book, ticker, instrument, funding; public WS | Signal safety |

This map is the initial call-graph boundary. During implementation, code search wajib be rerun for all direct exchange method names and raw HTTP transports to detect newly added bypasses.

## 5. Files containing direct API privat boundary or central dispatch

| File | Direct calls / responsibility | Action |
|---|---|---|
| `executor/exchange_client.py` | CCXT construction, `enableRateLimit=True`, auxiliary balance clients, facade composition | UBAH: pertahankan facade, inject penjadwal boundary |
| `executor/exchange_orders.py` | Spot order create/cancel/status/order terbuka; Tokocrypto raw private order endpoints | UBAH: centralize all dispatch |
| `executor/exchange_futures.py` | Futures create/close/positions/order/order terbuka/funding/ticker | UBAH: centralize dispatch; pertahankan reduceOnly |
| `executor/exchange_trades.py` | CCXT private trade history, order-specific trade lookup | UBAH: penjadwal |
| `executor/exchange_fee_balance.py` | Balance and fee API reads | UBAH: penjadwal and snapshot cache |
| `executor/exchange_common.py` | Client ID pemulihan, history/archive/trades and balance helper | UBAH: penjadwal injection; pertahankan fail-closed behavior |
| `executor/indodax_v2.py` | Own signed HTTP transport and all Indodax TAPIv2 operations | UBAH: penjadwal-aware transport; pertahankan protocol |
| `executor/venues/tokocrypto.py` | Private account fee endpoint and ID mapping | UBAH: penjadwal |
| `executor/venues/indodax.py` | Paginated private trade history | UBAH: bounded penjadwal pacing |
| `executor/venues/okx.py` | OKX ID klien / position mode semantics | PERTAHANKAN + capability tests |
| `executor/venues/mexc.py` | MEXC swap balance params | PERTAHANKAN + capability tests |
| `executor/pemulihan_discovery.py` | Account-wide open-order inventory sweep | UBAH: request penjadwal + durable cursor |
| `executor/trade_fill_rekonsiliasi.py` | Periodic private trade reads | UBAH: penjadwal, fairness and budget |
| `executor/private_stream.py` | Cache only; no venue connection | PERTAHANKAN + implement publisher separately |

## 6. Non-private exchange requests that wajib not consume private budget

Go scanner public requests include:
- Spot/perpetual market discovery and instrument metadata.
- Spot/perpetual order books.
- Ticker and USDT/IDR FX rate.
- Funding rate.
- Public WS book/ticker subscriptions.
- DNS-over-HTTPS in `internal/exchange/live_dns.go`.

Python public reads include:
- `fetch_order_book` (Indodax custom public depth or CCXT public endpoint).
- `fetch_ticker` and `fetch_funding_rate` where venue endpoint is public.
- `load_markets`/market metadata.
These need public market-data request budgets, not private account request buckets. CCXT's generated method auth classification wajib be verified per venue/version.

## 7. Required audit commands for future coverage checks

Search source for (not tests/docs alone):
- `create_order`, `create_limit_`, `create_market_`, `cancel_order`
- `fetch_order`, `fetch_open_orders`, `fetch_closed_orders`, `fetch_canceled_orders`
- `fetch_positions`, `fetch_balance`, `fetch_my_trades`, `fetch_order_trades`
- `fetch_trading_fee`, `privateGet`, `private_`, `_request(`, `session.request`
- Go: `http.NewRequest`, `.Do(req)`, `newOKXRequest`, `okxDo`, WS dial/subscribe.
Every new private call wajib be listed here with venue, endpoint, cost/weight, caller, timeout, retry/idempotency class, and tests.

## 8. Completeness statement

This is a source-derived map of the private operations exposed by the current ExchangeClient/Indodax adapter and the main pekerja call families. It is **not yet a live endpoint certification**. Exact endpoint names, auth classification, request weights, pagination, and supported markets wajib be confirmed against the pinned CCXT version and official venue docs before gateway migration. Binance/Bybit are intentionally marked unverified rather than inferred supported.
