# Private API Call Map — Nafazbot

Baseline: `9cc3dcd11d0999e828309053ba4cffce98a28675` on `feat/unified-dynamic-fee-engine`.
Audit branch: `audit/architecture-baseline-regression-contract`.

## 1. Classification

- **PRIVATE**: authenticated account/order/position/trade/fee operations.
- **PUBLIC**: unauthenticated market data, ticker, instruments, funding rate, public books.
- **MIXED/VERIFY**: CCXT method can be private or public depending on venue/version; verify generated endpoint mapping.
- **NOT A CEX CALL**: Redis, Supabase, Telegram, DNS.

The Python ExchangeClient facade is the principal private exchange boundary. It currently routes directly to CCXT instances or Indodax TAPIv2. The Go scanner exchange adapter inspected here makes public market-data requests; it is not the executor private API gateway.

## 2. Canonical private operation inventory

| Operation | Facade method / direct call | Underlying call or endpoint | CEX scope | Caller families | Risk / required policy |
|---|---|---|---|---|---|
| Spot limit buy | `place_local_limit_buy` | CCXT `create_limit_buy_order`; Indodax `POST /api/v2/order` | Active Spot venue (Tokocrypto/Indodax; generic CCXT only if configured) | Entry submission | HIGH; durable intent first; never blind retry |
| Spot limit sell | `place_local_limit_sell` | CCXT `create_limit_sell_order`; Indodax `POST /api/v2/order` | Active Spot venue | Exit order | HIGH; durable intent first |
| Spot emergency market sell | `place_local_market_sell` | CCXT `create_market_sell_order`; Indodax `POST /api/v2/order` | Active Spot venue | Rollback, force-close liquidation | EMERGENCY; priority queue; ambiguous result must reconcile |
| Spot cancel | `cancel_local_order` | CCXT `cancel_order`; Indodax `DELETE /api/v2/order` | Active Spot venue | Cancel/replace, force close, recovery | HIGH; cancel acknowledgement is not terminal proof |
| Spot order status | `fetch_local_order` | CCXT `fetch_order`; Tokocrypto raw `privateGetOpenV1Orders`; Indodax `GET /api/v2/order` | Active Spot venue | Entry/exit monitor, cancel reconciliation, rollback, recovery | HIGH; validate exact order ID |
| Spot lookup by client ID | `fetch_local_order_by_client_id` / `_find_order_by_client_id` | Native `fetch_order_by_client_id` if present; else open/closed/canceled history, paged archive, `fetch_my_trades` then `fetch_order` | Active Spot venue | Ambiguous create recovery, registry reconciliation | CRITICAL; incomplete history is UNKNOWN, not absent |
| Spot open-order inventory | `fetch_local_open_orders` | CCXT `fetch_open_orders`; Tokocrypto raw `privateGetOpenV1Orders(type=1)`; Indodax `GET /api/v2/openOrders` | All configured Spot clients swept at recovery | Startup recovery, Telegram/monitoring through executor | CRITICAL; account-wide pagination/completeness required |
| Spot closed/canceled history | Helper lookup | CCXT `fetch_closed_orders`, `fetch_canceled_orders`; Indodax `GET /api/v2/order/histories` | Venue-dependent | Client-ID recovery, order history | CRITICAL; time-window and pagination limitations |
| Spot fills/trades | `fetch_order_trades` | CCXT `fetch_my_trades` or `fetch_order_trades`; Indodax `GET /api/v2/myTrades` | Active Spot venue | TradeFillReconciliationWorker | HIGH; dedupe by exchange trade ID and reconcile sum to cumulative fill |
| Spot balance | `get_balances`, `fetch_balance_report` | CCXT `fetch_balance`; Indodax `GET /api/v2/account` | Active Spot and registered Spot balance clients | Funding preflight, sizing, mismatch, Telegram balance | HIGH; stale cache and account scope must be explicit |
| Spot trading fee | `fetch_pair_fee_rates` | Tokocrypto `privateGetOpenV1AccountSpot`; else CCXT `fetch_trading_fee` | Tokocrypto Spot / generic CCXT Spot | Fee preflight | MEDIUM; cache with expiry; never treat missing fee as verified |
| Futures open short | `open_global_short_market` | CCXT `create_market_sell_order` with `reduceOnly=False` and venue position-side params | Active Futures venue (OKX/MEXC evidenced; generic CCXT conditional) | Entry hedge | CRITICAL; durable intent, one write path, hedge state verification |
| Futures close short | `close_global_short_market` | CCXT `create_market_buy_order` with `reduceOnly=True` and position-side params | Active Futures venue | Exit, force close, rollback/repair | EMERGENCY/HIGH; reduce-only invariant |
| Futures position | `fetch_positions` | CCXT `fetch_positions([symbol])`, fallback all positions only on TypeError/AttributeError | Active Futures venue | Hedge ambiguity, rollback guard, force-close, startup recovery, exposure reconciliation | CRITICAL; malformed/None is UNKNOWN, never flat |
| Futures order status | `fetch_global_order` | CCXT `fetch_order` | Active Futures venue | Hedge/close reconciliation | HIGH |
| Futures lookup by client ID | `fetch_global_order_by_client_id` / helper | Native lookup or open/closed/canceled history/archive/private trades then order lookup | Active Futures venue | Ambiguous create recovery | CRITICAL; exact ID and authoritative history |
| Futures open-order inventory | `fetch_global_open_orders` | CCXT `fetch_open_orders` | Configured Futures venues | Startup recovery | CRITICAL; account-wide sweep and pagination |
| Futures fills/trades | `fetch_order_trades` | CCXT `fetch_my_trades` / `fetch_order_trades` | Active Futures venue | Fill ledger | HIGH |
| Futures balance | `get_balances`, `fetch_balance_report` | CCXT `fetch_balance`; MEXC uses `type=swap` | Active Futures and auxiliary Futures clients | Sizing, exposure, Telegram | HIGH; venue market type explicit |
| Funding history | `fetch_funding_history` | CCXT `fetch_funding_history` | Active Futures venue if supported | Funding reconciliation | PRIVATE/MIXED; verify venue endpoint auth and weight |
| Funding rate | `fetch_funding_rate` | CCXT `fetch_funding_rate` | Active Futures venue | Strategy/funding checks | PUBLIC/MIXED; verify generated endpoint |
| Futures ticker | `fetch_global_ticker` | CCXT `fetch_ticker` | Active Futures venue | Entry revalidation, exit price | PUBLIC market data, but must share public data budget not private order budget |

## 3. Venue-specific private endpoint detail

### Tokocrypto — Spot
Direct generated CCXT private methods evidenced:
- `privateGetOpenV1Orders(...)` in `executor/exchange_orders.py`: exact order fallback and account-wide open order list (`type=1`).
- `privateGetOpenV1AccountSpot()` in `executor/venues/tokocrypto.py`: account maker/taker commission read.
CCXT standard private methods are also used through the facade for create/cancel/order/balance/trades/history. Tokocrypto rewrites client order IDs; logical-to-wire mapping must remain intact.
**No Tokocrypto Futures execution route is established in this baseline.**

### Indodax — Spot TAPIv2
All signed requests flow through `IndodaxTAPIv2._request` in `executor/indodax_v2.py`:
- `POST /api/v2/order`: create limit buy/sell and market sell.
- `DELETE /api/v2/order`: cancel.
- `GET /api/v2/order?symbol=...&orderId=...`: status.
- `GET /api/v2/order?symbol=...&origClientOrderId=...`: client-ID lookup.
- `GET /api/v2/order/histories`: closed/canceled/rejected/expired order history (bounded seven-day query window).
- `GET /api/v2/myTrades`: authenticated individual fills.
- `GET /api/v2/account`: account balances.
- `GET /api/v2/openOrders`: account/symbol open-order inventory.
The same adapter also calls public depth/order-book endpoint(s), which are not private. HMAC-SHA256 signing and `X-APIKEY`/`Sign` headers are adapter-specific. Indodax TAPIv2 is not replaceable by legacy CCXT TAPIv1 for these credentials.
**No Indodax Futures execution route is established.**

### OKX — Futures
CCXT authenticated operation families:
- create market sell / market buy with `posSide=short` and reduceOnly semantics;
- fetch positions;
- fetch order / open orders / client-ID history search;
- private trade history;
- futures balance;
- funding history (if supported by installed CCXT/API account);
- funding rate and ticker are market-data reads, not assumed private.
Position mode must match `posSide`; do not strip or alter `reduceOnly`.
**No OKX Spot scanner/trading route is certified by this baseline.**

### MEXC — Futures
CCXT authenticated operation families:
- create market sell / market buy;
- fetch positions;
- fetch order / open orders / history / client-ID recovery;
- private trade history;
- futures balance with explicit `type=swap`.
Funding methods are conditional on CCXT capability.
MEXC uses CCXT market type `swap`; preserve symbol resolver and amount/contract conversion.
**MEXC Spot is present in Go scanner market data, but Python executor's balance registry maps MEXC as Futures. Do not infer Spot execution from scanner discovery.**

### Binance and Bybit — Futures auxiliary clients
`BALANCE_MARKETS` registers Binance and Bybit as Futures balance clients when credentials are configured. This proves balance-report/read capability only. It does not certify open-order sweep, positions, order create/cancel, client-ID recovery, private trades, or scanner integration.
**Status: NOT CERTIFIED FOR LIVE TRADING.**

## 4. Caller / worker map

| Caller or worker | Source files | Private API families reached | Criticality |
|---|---|---|---|
| Entry signal validation | `executor/engine_signal.py` | Futures ticker (public), balance/mismatch and fee preflight via engine paths | HIGH |
| Entry submission / hedge | `executor/engine_entry_submission.py`, `executor/entry_hedge.py`, `executor/entry_orders.py` | Spot limit buy, Futures market sell, balance, order status | CRITICAL |
| Pending entry monitor | `executor/entry_monitoring.py`, `executor/entry_orders.py` | Spot fetch order, cancel, order fill | HIGH |
| Exit pricing / order placement | `executor/exit_pricing.py`, `executor/exit_orders.py`, `executor/exit_execution.py` | Spot order book/ticker, limit sell, status/cancel, Futures close | CRITICAL |
| Rollback | `executor/rollback.py` | Spot emergency market sell, Futures positions, order verification | EMERGENCY |
| Force close | `executor/cancel.py`, `executor/force_close.py`, `executor/force_close_legs.py`, `executor/force_close_lifecycle.py` | Cancel, status, Futures positions, Futures reduceOnly close, Spot market sell | EMERGENCY |
| Startup and manual recovery | `executor/engine_recovery.py`, `executor/recovery_discovery.py`, `executor/recovery_registry.py`, `executor/recovery_orphans.py`, `executor/recovery_adoption.py`, `executor/order_recovery.py` | Account-wide open orders, order/client-ID history, balances, positions | CRITICAL |
| Fill reconciliation | `executor/trade_fill_reconciliation.py`, `executor/exchange_trades.py` | Private trade history and order lookup | HIGH |
| Risk/exposure reconciliation | `executor/exposure_reconciliation.py`, `executor/risk_exposure.py`, `executor/risk.py`, `executor/risk_state.py` | Spot balances, Futures positions, sometimes order status | CRITICAL |
| Fee preflight | `executor/engine_fee_preflight.py`, `executor/exchange_fee_balance.py`, `executor/venues/tokocrypto.py` | Fee profile and balances | HIGH |
| Telegram balance request | `internal/telegram/handlers.go` -> Redis request -> `executor/engine_runtime.py` -> ExchangeClient | Balance report | MEDIUM/HIGH |
| Recovery/force-close Telegram command | `internal/telegram/handlers.go` -> Redis Pub/Sub -> `executor/engine_runtime.py` | Triggers recovery/cancel/close calls | EMERGENCY |
| Go scanner | `internal/exchange/live*.go`, `internal/exchange/venue_*.go`, `internal/scanner/*.go` | Public REST market, book, ticker, instrument, funding; public WS | Signal safety |

This map is the initial call-graph boundary. During implementation, code search must be rerun for all direct exchange method names and raw HTTP transports to detect newly added bypasses.

## 5. Files containing direct private API boundary or central dispatch

| File | Direct calls / responsibility | Action |
|---|---|---|
| `executor/exchange_client.py` | CCXT construction, `enableRateLimit=True`, auxiliary balance clients, facade composition | MODIFY: preserve facade, inject scheduler boundary |
| `executor/exchange_orders.py` | Spot order create/cancel/status/open orders; Tokocrypto raw private order endpoints | MODIFY: centralize all dispatch |
| `executor/exchange_futures.py` | Futures create/close/positions/order/open orders/funding/ticker | MODIFY: centralize dispatch; preserve reduceOnly |
| `executor/exchange_trades.py` | CCXT private trade history, order-specific trade lookup | MODIFY: scheduler |
| `executor/exchange_fee_balance.py` | Balance and fee API reads | MODIFY: scheduler and snapshot cache |
| `executor/exchange_common.py` | Client ID recovery, history/archive/trades and balance helper | MODIFY: scheduler injection; preserve fail-closed behavior |
| `executor/indodax_v2.py` | Own signed HTTP transport and all Indodax TAPIv2 operations | MODIFY: scheduler-aware transport; preserve protocol |
| `executor/venues/tokocrypto.py` | Private account fee endpoint and ID mapping | MODIFY: scheduler |
| `executor/venues/indodax.py` | Paginated private trade history | MODIFY: bounded scheduler pacing |
| `executor/venues/okx.py` | OKX client ID / position mode semantics | KEEP + capability tests |
| `executor/venues/mexc.py` | MEXC swap balance params | KEEP + capability tests |
| `executor/recovery_discovery.py` | Account-wide open-order inventory sweep | MODIFY: request scheduler + durable cursor |
| `executor/trade_fill_reconciliation.py` | Periodic private trade reads | MODIFY: scheduler, fairness and budget |
| `executor/private_stream.py` | Cache only; no venue connection | KEEP + implement publisher separately |

## 6. Non-private exchange requests that must not consume private budget

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
These need public market-data request budgets, not private account request buckets. CCXT's generated method auth classification must be verified per venue/version.

## 7. Required audit commands for future coverage checks

Search source for (not tests/docs alone):
- `create_order`, `create_limit_`, `create_market_`, `cancel_order`
- `fetch_order`, `fetch_open_orders`, `fetch_closed_orders`, `fetch_canceled_orders`
- `fetch_positions`, `fetch_balance`, `fetch_my_trades`, `fetch_order_trades`
- `fetch_trading_fee`, `privateGet`, `private_`, `_request(`, `session.request`
- Go: `http.NewRequest`, `.Do(req)`, `newOKXRequest`, `okxDo`, WS dial/subscribe.
Every new private call must be listed here with venue, endpoint, cost/weight, caller, timeout, retry/idempotency class, and tests.

## 8. Completeness statement

This is a source-derived map of the private operations exposed by the current ExchangeClient/Indodax adapter and the main worker call families. It is **not yet a live endpoint certification**. Exact endpoint names, auth classification, request weights, pagination, and supported markets must be confirmed against the pinned CCXT version and official venue docs before gateway migration. Binance/Bybit are intentionally marked unverified rather than inferred supported.
