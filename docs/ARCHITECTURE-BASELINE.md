# Architecture Baseline — Nafazbot

- Repository: `nzf210/nafaz_arb`
- Baseline branch: `feat/unified-dynamic-fee-engine`
- Baseline commit: `9cc3dcd11d0999e828309053ba4cffce98a28675`
- Audit branch: `audit/architecture-baseline-regression-contract`
- Audit date: 2026-11-13
- Scope: read-only source inspection; documentation only. No trading code, config, migrations, or production resources changed.

## 1. Purpose and guardrails

This document freezes the current architectural baseline before implementation of shared exchange infrastructure. It distinguishes:
- **KEEP** — preserve behavior and interfaces; changes require explicit regression proof.
- **MODIFY** — improve implementation while preserving the externally observable contract.
- **NEW** — add a separate capability behind an adapter/port; do not replace the old path until parity is demonstrated.
- **DEFER** — not part of this architecture work.
- **VERIFY** — source evidence is incomplete or venue/version dependent; do not claim support until verified.

Non-negotiable rules:
1. No rewrite of the strategy/execution state machine as part of gateway or market-data work.
2. Never dual-submit a create-order operation through old and new paths.
3. A create-order timeout is **SUBMISSION_UNKNOWN**, not proof of rejection; reconcile by venue/client ID before any retry.
4. A missing order from an incomplete/unsupported history endpoint is not proof of absence.
5. Unknown wallet, order, or position state is not healthy/flat.
6. Preserve independent Spot and Futures leg accounting and recovery.
7. Keep Supabase/Postgres as durable ledger; Redis is coordination/cache/notification, not the only order record.
8. Roll out read-only first, then cancel/recovery, then create-order only after idempotency and ambiguity tests.
9. Existing REST fallback remains until a per-venue/per-symbol WS feed is proven LIVE and fresh. Stale/gapped books cannot create signals.
10. No production mutation from this audit branch.

## 2. Repository snapshot

The branch was read from GitHub at the commit above. Recursive tree contained **213 tracked paths**, with no truncation. This is a source inventory, not a claim that all 213 files were line-by-line audited. Critical runtime paths are inspected; secondary files are classified by responsibility and marked for deeper verification in the implementation plan.

The branch head commit message is:
`fix(executor): support USDT spot quotes and resolve queue stall bottlenecks`.

Important current code facts:
- Python executor builds CCXT async clients with `enableRateLimit=True`; this is per CCXT instance, not a shared cross-process limiter.
- Indodax Spot uses a custom TAPIv2 adapter with its own `aiohttp.ClientSession` and HMAC-SHA256 request signing.
- Balance-only client inventory includes Tokocrypto Spot, Indodax Spot, OKX Futures, MEXC Futures, Binance Futures, and Bybit Futures.
- The Go scanner's exchange adapter exposes public market discovery, books, ticker/FX, and funding data; no authenticated create/cancel/order/balance/position path was found in the inspected Go exchange adapter. Go exchange API traffic in this baseline is public market-data traffic.
- Executor private stream cache exists, but the compose comment says no publisher/subscriber is wired to populate it yet. Do not treat it as an operational private WS feed.
- Redis Pub/Sub is used for emergency cancel/force close notifications; Pub/Sub alone is not durable delivery.
- Signal priority queue uses Redis sorted-set pop; it is an ephemeral opportunity queue, not an execution command journal.
- Pair serialization in the Python engine is process-local; recovery has a distributed Redis lock. They are different protections.

## 3. Runtime architecture baseline

```text
Public exchange market data
  CEX REST + public WebSocket
        |
        v
Go scanner / market discovery / local books / funding
        |
        v
Redis opportunity payload + priority ZSET + notify Pub/Sub
        |
        v
Python executor (signal validation, risk, sizing, maker/hedge, exits)
        |
        +--> ExchangeClient facade
        |      +--> CCXT async clients (venue-specific)
        |      +--> Indodax TAPIv2 direct HTTP adapter
        |      +--> extra read-only balance clients
        |
        +--> Universal Order Registry / Supabase durable records
        +--> Redis operational state, locks, queues, Pub/Sub
        +--> recovery / exposure / fill reconciliation workers
        |
        v
Telegram Go API reads Redis/Supabase state and sends operator commands
```

The architecture is currently hybrid: Go owns scanner/public market data and Telegram service; Python owns execution, private exchange access, recovery, risk, and order lifecycle.

## 4. Preserve / modify / build matrix

| Component | Decision | Baseline contract |
|---|---|---|
| Go scanner and opportunity evaluation | KEEP + MODIFY | Preserve signal payload semantics, profitability/fee checks, idempotency, stale-data rejection. Add WS coverage without changing strategy formulas. |
| Exchange pair/market normalization | KEEP + MODIFY | Persisted scanner venue tags and native IDs must continue resolving to exact venue market; ambiguous mapping fails closed. |
| Python execution engine orchestration | KEEP | Preserve entry, hedge, maker, exit, rollback, force-close, and recovery state transitions. |
| ExchangeClient facade | MODIFY | Keep current call interface during scheduler migration; make every private request pass through a shared policy boundary. |
| CCXT clients | KEEP initially | Keep adapter behavior; per-instance limiter is not sufficient as global control. |
| Indodax TAPIv2 | KEEP | Preserve signature, endpoints, decimal formatting, status normalization, post-only/TIF behavior, and error classification. Add scheduler transport injection, not a rewrite. |
| Venue modules | KEEP + MODIFY | Retain Tokocrypto client-ID mapping, Indodax history pagination, OKX position-side params, MEXC swap balance params. Add capability metadata. |
| Universal Order Registry | KEEP | Durable order identity and lifecycle remain canonical. |
| Durable order intent | KEEP | Persist intent before LIVE create order. Persistence failure blocks submission. |
| Two-leg execution/recovery | KEEP | Spot and Futures progress remain independent. |
| Risk state/circuit breaker | KEEP | Unknown state blocks new exposure; durable restore remains mandatory. |
| Trade fill reconciliation | KEEP + MODIFY | Immutable venue fill IDs and cumulative-fill cross-check remain required; route reads through scheduler. |
| Exposure reconciliation | KEEP + MODIFY | Keep actual balance/position verification and fail-closed mismatch handling. |
| Adaptive Maker Pegging | KEEP | Preserve strategy and Indodax-specific behavior; regression tests required for every exchange transport change. |
| Fee engine | KEEP | Preserve fee source/verification semantics and net-profit calculation. |
| Telegram command UX | KEEP | Preserve commands and operator semantics; route all private reads/commands through executor scheduler/journal. |
| Redis opportunity queue | KEEP | Ephemeral signals remain TTL-bounded; do not repurpose as durable execution queue. |
| Emergency Pub/Sub | MODIFY | Keep as wake-up only; add durable command journal and acknowledgement. |
| PrivateStreamCache | KEEP + NEW producer | Preserve cache API/freshness fallback; add actual venue private-stream ingestion separately. |
| Shared private API scheduler | NEW | Cross-worker/cross-process request policy, rate buckets, cooldown, priority, bounded retries, telemetry. |
| Unified market-data subscription manager | NEW incrementally | Per-symbol lifecycle, sequence/freshness/resync, feed coverage and REST budget. Existing venue managers adapt into contract. |
| Distributed execution lease | NEW/MODIFY | Extend beyond recovery lease; pair/account ownership, fencing token, atomic order-slot reservation. |
| Durable emergency command journal | NEW | Persist, claim, acknowledge, retry, idempotent command IDs. |
| Full Go migration of Python executor | DEFER | No justification until staged gateway proves value and parity. |

## 5. CEX capability baseline

Capabilities are split into **scanner public market-data support**, **executor private trading route**, and **auxiliary balance client registration**. These are not interchangeable.

| CEX | Go scanner public markets/books | Python executor private route evidenced | Auxiliary balance registry | Status |
|---|---|---|---|---|
| Tokocrypto | Spot market discovery, Spot REST order book; WS order-book coverage not established | Spot create limit buy/sell, market sell, cancel, order lookup, open-order listing, client-ID recovery, private trades, balance, account fee | Spot | Partial full-trade path; verify all CCXT endpoint variants and WS |
| Indodax | Spot market discovery, Spot REST order book, dedicated public book WS and USDT/IDR ticker WS | TAPIv2 Spot create/cancel/order lookup/client-ID lookup/open orders/terminal history/myTrades/account balance; post-only limit support | Spot | Strong custom adapter; verify live endpoint semantics and WS |
| OKX | Perpetual SWAP discovery, order book REST + dedicated public WS, funding rate | Futures market sell short, reduceOnly buy close, positions, order lookup/open orders/client-ID recovery, private trades, balance, funding history/rate, ticker | Futures | Executor path evidenced; verify account position mode and CCXT version |
| MEXC | Spot and perpetual discovery, Spot/Futures REST order book and funding | Futures market sell/buy, positions, order lookup/open orders/client-ID recovery, private trades, balance using `type=swap`, funding methods when supported | Futures | Executor path evidenced; verify swap-specific endpoint/capability |
| Binance | No scanner market route established in this audit | No dedicated active pair route established; generic CCXT path may be selected by config | Futures | Auxiliary balance client only evidenced; trading support NOT certified |
| Bybit | No scanner market route established in this audit | No dedicated active pair route established; generic CCXT path may be selected by config | Futures | Auxiliary balance client only evidenced; trading support NOT certified |

**Important qualification:** `BALANCE_MARKETS` proves auxiliary balance-client registration, not end-to-end order execution/recovery support. Generic CCXT method availability does not prove the account, market type, client-ID, position mode, pagination, or endpoint is production-ready. No CEX should be marked LIVE-supported until capability tests pass.

### Scanner public data coverage
- Tokocrypto Spot: REST order-book fetch path.
- Indodax Spot: dedicated `IndodaxBookManager` with dynamic pair subscriptions.
- OKX Futures: dedicated `OKXBookManager` with instrument subscriptions/snapshot recovery.
- MEXC Spot/Futures: REST book paths; a generic WS stream exists but complete per-symbol book lifecycle was not established.
- No private order/account API is identified in the Go scanner exchange adapters examined.

## 6. Shared request scheduler design boundary

The scheduler must not become a new trading engine. It owns transport admission/control only:
- identity scope: `venue + credential/account + market/API category + endpoint bucket`;
- request classes: emergency write, recovery read, order monitor, position/balance risk read, order write, analytics/fill history, fee/metadata;
- venue-specific weight/cost and concurrency;
- shared cooldown, Retry-After, exponential backoff with jitter, circuit state;
- bounded queues and deadlines;
- read deduplication only for semantically identical safe reads;
- telemetry and trace/request IDs;
- no blind retries of non-idempotent create order;
- no request is silently dropped; caller receives typed timeout/rejection/deferred result.

Transport adapters:
- CCXT: wrap authenticated methods at the ExchangeClient/transport boundary; do not monkey-patch undocumented internals without tests.
- Indodax: inject a scheduler-aware request executor into TAPIv2 `_request`.
- Go public scanner: separate public data limiter; do not share private credential bucket with executor.

## 7. Market data manager boundary

Normalized book contract must include venue, market type, native symbol, bid/ask levels, exchange timestamp, local receive timestamp, sequence/update ID where available, source, state, and freshness.
State machine: `UNSUBSCRIBED -> SYNCING -> LIVE`; any stale timestamp, sequence gap, disconnect, checksum failure, or malformed payload transitions to `STALE/GAP/FAILED` and blocks signal generation until resync.
REST budget is explicit per venue and purpose: initial snapshot, resync, metadata, and controlled fallback. A generic socket-connected flag is not proof that every symbol is live.

## 8. Execution ownership boundary

Add a distributed lease for normal execution separate from recovery lock:
- key scope includes account/credential and canonical pair;
- lease owner and monotonically increasing fencing token;
- lease heartbeat and bounded TTL;
- every state-changing command validates current fencing token;
- pending/open Spot order slot reservation is atomic per CEX/account;
- lock loss blocks new order writes and triggers safe reconciliation;
- preserve existing local asyncio lock as an optimization, never as distributed correctness.

## 9. Known risk / open verification items

1. Exact authenticated endpoint weights and burst limits must be verified against current official docs for each enabled CEX and account tier.
2. CCXT version and generated endpoint names must be pinned and capability-tested.
3. Tokocrypto private fallback uses generated CCXT methods `privateGetOpenV1Orders` and `privateGetOpenV1AccountSpot`; verify exact response shape and order type semantics against the installed CCXT version.
4. Indodax TAPIv2 order/history/myTrades endpoint behavior, timestamp windows, and post-only semantics require live-safe or sandbox/recorded-fixture verification.
5. OKX `posSide`, `reduceOnly`, position mode, and client ID behavior must be tested against actual account mode.
6. MEXC `defaultType=swap`, `type=swap`, order symbol resolution, and position normalization require venue-specific tests.
7. Binance/Bybit are not certified for trading by balance registry presence.
8. Private WS credentials, login, subscription, reconnect, deduplication, and event-to-registry mapping are not yet evidenced as complete.
9. Multi-executor normal pair ownership is not proven distributed.
10. Recovery sweep pagination and account-wide endpoint semantics require per-venue tests.
11. Exact private API call coverage must be re-scanned whenever new direct exchange method or raw HTTP use is added.

## 10. Change policy

Every future PR touching exchange access must:
- update `PRIVATE_API_CALL_MAP.md` if a private endpoint/callsite changes;
- update `REGRESSION_CONTRACT.md` if behavior or test obligations change;
- demonstrate no bypass around scheduler;
- include mock/fixture tests and venue capability test;
- preserve old path behind explicit rollout flag until parity is demonstrated;
- include rollback procedure and operational metrics.
