# Regression Contract — Nafazbot

Baseline: `9cc3dcd11d0999e828309053ba4cffce98a28675`.
Audit branch: `audit/architecture-baseline-regression-contract`.
This contract is mandatory for changes to ExchangeClient, venue adapters, private request scheduling, WS ingestion, queue delivery, recovery, or execution coordination.

## 1. Global invariants

| ID | Invariant | Failure behavior |
|---|---|---|
| G-01 | No LIVE order is submitted before durable order intent is confirmed | Block submission; do not fall back to unregistered write |
| G-02 | Ambiguous create response is `SUBMISSION_UNKNOWN` | Search exact client ID/history and reconcile; never blind retry |
| G-03 | Cancel acknowledgement is not terminal order proof | Fetch final status/fills; block replacement/force close until reconciled |
| G-04 | Unknown/malformed order inventory is not empty inventory | Mark scope unverified and block new exposure |
| G-05 | Unknown/malformed futures positions are not flat | Halt scope; do not spot rollback or declare force-close complete |
| G-06 | Spot and Futures legs retain independent fill/progress quantities | Reconcile each leg independently; no aggregate shortcut |
| G-07 | Every fill is deduplicated by venue trade ID | Duplicate events cannot double count exposure/PnL |
| G-08 | Sum of verified fills reconciles to cumulative order fill within configured tolerance | Keep `RECONCILIATION_REQUIRED`; no silent correction |
| G-09 | Orphan orders are recorded and block scope until resolved | Never cancel solely because registry record is absent |
| G-10 | Registry/Supabase durable state is not replaced by Redis-only state | Redis loss must not erase order ownership |
| G-11 | Per-CEX max open/pending Spot order cap is enforced atomically | Concurrent signals cannot exceed cap |
| G-12 | A stale, crossed, malformed, gapped, or unverified book cannot create a signal | Reject candidate and emit reason/metric |
| G-13 | A scheduler cooldown is shared by all workers using the same credential scope | No other worker continues into known venue throttle |
| G-14 | Emergency commands survive consumer downtime/restart | Persisted command is retried until terminal acknowledgement |
| G-15 | Loss of distributed execution lease blocks further order writes | Reconcile before any new submission |
| G-16 | No old/new dual write path exists for one venue operation | Exactly one active writer per operation and rollout stage |

## 2. Feature preservation contracts

### Trading strategy and pricing
- Preserve current opportunity schema and scanner/executor compatibility.
- Preserve fee engine maker/taker rates, source, verification state, and net-profit calculations.
- Preserve adaptive maker pegging, aggressive strategy selection, dynamic target profit, stale quote refresh, and cancel/replace semantics.
- Preserve IDR/USDT/USDC conversion and base/quote/contract amount normalization.
- Preserve market-specific precision, min amount/notional, lot step, tick size, and post-only constraints.
- Preserve Indodax post-only/TIF handling; venue rejection must not silently downgrade to taker.

### Entry and two-leg execution
- Spot entry intent is durable before create.
- Spot partial fill hedges only the verified delta; repeated polling/event must not hedge same fill twice.
- Futures hedge failure triggers the existing conservative rollback/verification path.
- If Futures position query is unavailable/unknown, do not assume no short and do not perform unsafe Spot rollback.
- Entry state, pending order, active position, and registry transitions remain consistent across restart.

### Exit and force close
- Exit order cancel races with fills; fetch final order and reconcile all fills before replacement.
- Futures close must remain reduce-only and short-side-specific.
- Force close cannot report success until Futures position is verified flat and Spot exposure is reconciled.
- Emergency Spot liquidation quantity is bounded by actual unhedged/verified Spot exposure.
- Any ambiguous emergency write remains an incident requiring reconciliation, not a reason for another blind write.

### Recovery and durable state
- Startup recovery runs before new entry readiness.
- Sweep every configured account/market that can hold executor-managed orders, including account-wide inventory where supported.
- Recover orders absent from local registry as orphans; never silently discard.
- Reconcile active positions against actual exchange positions/balances.
- Restore durable risk/circuit-breaker state before allowing new entries.
- Recovery lock owner/heartbeat/expiry behavior remains intact.
- Recovery pagination failure or endpoint unsupported means incomplete inventory, not success.
- Restart during each recovery stage is resumable and idempotent.

### Telegram and operations
- Existing `/orders`, `/positions`, `/pending_orders`, `/balance`, recovery, kill-switch, cancel, and force-close flows retain operator semantics.
- Offline executor cannot lose emergency command.
- Telegram read commands must not bypass scheduler for private reads.
- Status must distinguish healthy, degraded, unknown, halted, and recovery-required.
- Spot max open order configuration default remains 2 unless explicitly changed; per-CEX overrides survive restart.

## 3. Request scheduler tests

| ID | Scenario | Expected result |
|---|---|---|
| RS-01 | Two executor processes issue private reads for same credential | Shared concurrency/rate budget applies |
| RS-02 | Tokocrypto returns HTTP 1008 / 429 | Shared cooldown blocks all same-scope callers; bounded retry only |
| RS-03 | Indodax 429/503 with Retry-After | Respect Retry-After; no hot loop |
| RS-04 | OKX endpoint weight exhausted | Endpoint bucket cools down; unrelated venue remains usable |
| RS-05 | MEXC throttles Futures endpoint | Futures requests back off; no duplicate create |
| RS-06 | Balance, recovery and monitor request concurrently | Priority/fairness prevent recovery starvation and runaway traffic |
| RS-07 | Scheduler/Redis unavailable | Private write fails closed; read policy explicit and bounded |
| RS-08 | Request deadline expires while queued | Caller receives typed deferred/timeout; command not silently dropped |
| RS-09 | Read dedupe with same scope/symbol/params | One request only when response semantics permit |
| RS-10 | Same read but different account/credential | No cross-account cache/response leakage |
| RS-11 | Create order response timeout after venue acceptance | Mark unknown; lookup by exact client ID before any retry |
| RS-12 | Cancel response timeout | Re-fetch status; never assume cancel succeeded |
| RS-13 | Process restarts during cooldown | Cooldown survives according to configured durability policy |
| RS-14 | Rate-limit error classification differs by adapter | Normalized typed throttle and retry metadata |

## 4. CEX capability and contract tests

### Tokocrypto Spot
- CCXT client construction and Spot market resolution.
- Native symbol wire conversion and `toko:` prefix stripping.
- Logical client order ID -> venue wire ID -> logical ID mapping.
- `privateGetOpenV1Orders` exact order fallback must not return a different open order.
- Account-wide open-order response is validated and complete.
- Private account fee response maker/taker units are normalized correctly.
- Spot limit buy/sell, market sell, cancel, status, balance and trade history.
- HTTP 1008/429 cooldown and ambiguous create recovery.
- No Futures capability is assumed.

### Indodax Spot TAPIv2
- HMAC-SHA256 signs exact URL-encoded query string; timestamp/recvWindow preserved.
- `X-APIKEY` and `Sign` headers present; secrets never logged.
- Create/cancel/order/client-ID lookup/openOrders/order histories/myTrades/account endpoints map to CCXT-like normalized schema.
- Partial fill remains open with correct cumulative `filled`.
- Decimal formatting rounds down to exact quantity increment; zero-after-rounding rejected.
- Post-only/TIF is preserved; rejected post-only does not silently become taker.
- Seven-day order history boundary and trade history pagination are tested.
- 429/503 and Retry-After classification works.
- Session reuse/close lifecycle has no leaked ClientSession.
- No Indodax Futures capability assumed.

### OKX Futures
- Market ID resolves to the exact SWAP instrument; no ambiguous candidate.
- Position mode and `posSide=short` are respected.
- Open short is SELL with reduceOnly false; close is BUY with reduceOnly true.
- Fetch positions distinguishes empty valid response from malformed/unknown.
- Client ID, open-order, terminal-history and private-trade recovery work.
- Contract size conversion and amount specification are correct.
- Funding history, funding rate and ticker endpoint capabilities are tested separately.
- Public WS sequence/snapshot/stale/reconnect handling is correct.

### MEXC Futures
- `defaultType=swap` remains enforced even with legacy config.
- Futures balance uses `type=swap`.
- Native market ID and CCXT unified symbol resolution are exact.
- Contract amount/size conversion is correct.
- Short/close reduceOnly semantics and positions are verified.
- Client-ID lookup, order history and private fills are authoritative.
- Endpoint not supported or incomplete pagination remains UNKNOWN.
- REST book fallback and WS coverage are measured; no unverified book is tradable.

### Binance / Bybit auxiliary balance
- Current baseline certifies only auxiliary Futures balance client registration.
- Balance endpoint auth, market params, response schema, throttling, and account isolation must pass.
- Until dedicated order/position/open-order/client-ID/trade-history tests exist, these venues must not be exposed as active LIVE trading venues.
- No inference from generic CCXT methods is accepted.

## 5. Market-data / WebSocket regression tests

| ID | Scenario | Expected result |
|---|---|---|
| MD-01 | WS disabled | Existing controlled REST path remains functional |
| MD-02 | WS connects but symbol not subscribed | Symbol is not LIVE |
| MD-03 | Snapshot then valid updates | State transitions SYNCING -> LIVE |
| MD-04 | Sequence gap/checksum mismatch | State GAP/FAILED; candidate blocked; resync requested |
| MD-05 | Socket disconnect | All affected symbols become STALE/FAILED |
| MD-06 | Reconnect with changed active pair set | Correct subscribe/unsubscribe; no duplicate subscriptions |
| MD-07 | One symbol stale, other symbol healthy | Health remains per-symbol |
| MD-08 | REST fallback budget exhausted | Candidate blocked; no unbounded REST loop |
| MD-09 | Exchange timestamp old/future/invalid | Freshness validation rejects data |
| MD-10 | WS event duplicates | Book update idempotent |
| MD-11 | Indodax/OKX dedicated manager restart | Snapshot/resubscription restores correct book |
| MD-12 | MEXC/Tokocrypto missing WS implementation | Explicit REST-only capability; not mislabeled WS-ready |

## 6. Queue and distributed coordination tests

| ID | Scenario | Expected result |
|---|---|---|
| Q-01 | Signal expires before executor handles it | Dropped as stale; no order |
| Q-02 | Executor crashes after ZPOPMAX | Opportunity may expire; no claim of durable delivery |
| Q-03 | Durable execution command consumer crashes after claim | Lease expires and command is retried idempotently |
| Q-04 | Emergency cancel published while executor offline | Command remains durable and is processed after restart |
| Q-05 | Duplicate emergency command delivery | Idempotency key prevents duplicate effect |
| Q-06 | Two executors receive same pair signal | Only one distributed lease owner can write orders |
| Q-07 | Lease expires while old owner is paused | Fencing token prevents stale owner write |
| Q-08 | Spot max order slot claimed concurrently | Atomic reservation never exceeds per-CEX cap |
| Q-09 | Redis notification lost | Durable command polling still discovers command |
| Q-10 | Supabase/outbox unavailable | Existing durability/fail-closed behavior retained; no silent order loss |

## 7. Crash / failure-injection matrix

Must test process restart at each boundary:
1. Before durable intent write.
2. After intent write, before exchange request.
3. During request before response.
4. After venue accepted order, before response reached executor.
5. After response, before registry acknowledgement.
6. After registry update, before Redis cache update.
7. During partial Spot fill, before Futures hedge.
8. After Futures hedge, before position state persistence.
9. During cancel while fill races.
10. During force-close Futures buyback.
11. After venue flat, before durable terminal state.
12. During recovery account-wide pagination.
13. During fill reconciliation before all trade IDs persisted.
14. During emergency command claim/ack.
15. During scheduler cooldown and WS resync.

For every case, expected result is one of: safely resumed, reconciled to venue truth, or halted with explicit `RECONCILIATION_REQUIRED`. Never silent READY.

## 8. Existing tests that form the starting regression suite

The following existing tests are mandatory starting points (do not replace them with new-only tests):
- `tests/test_adaptive_maker.py`
- `tests/test_adaptive_maker_paper_validation.py`
- `tests/test_entry.py`
- `tests/test_exits.py`
- `tests/test_two_leg_execution_recovery.py`
- `tests/test_order_persistence_guarantees.py`
- `tests/test_order_recovery.py`
- `tests/test_recovery.py`
- `tests/test_distributed_recovery_lock.py`
- `tests/test_force_close.py`
- `tests/test_force_clean_orphan.py`
- `tests/test_exposure_reconciliation.py`
- `tests/test_trade_fill_reconciliation.py`
- `tests/test_rate_limit_resilience.py`
- `tests/test_exchange_market_resolution_recovery.py`
- `tests/test_exchange_order_id_normalization.py`
- `tests/test_indodax_v2.py`
- `tests/test_exchange_fee_provider.py`
- `tests/test_fee_engine.py`
- `tests/test_stream_cache.py`
- `tests/test_signal_priority.py`
- `tests/test_venues_modular.py`
- `tests/test_mexc_futures_balance.py`
- `tests/test_production_failure_injection.py`
- Go: `go test ./...`
- Supabase: `supabase/tests/schema_smoke.sql` and migration ordering/uniqueness validation.

## 9. CI / release gate

A change is not accepted merely because lint or unit tests pass. Required gates:
1. Python full test suite and static analysis.
2. Go full test suite and race-sensitive tests for WS/cache/coordination.
3. SQL schema smoke and migration sequence validation.
4. CEX adapter contract tests for each touched venue.
5. Failure injection for touched order lifecycle.
6. Search-based no-bypass check for direct private methods outside approved adapter boundary.
7. CI green on the audit/implementation branch.
8. Staging paper/shadow validation with no production write.
9. Explicit operator approval before enabling LIVE write path.

## 10. Acceptance and rollback

Acceptance requires all applicable contracts above to pass and operational metrics to show:
- request count/weight by venue/account/endpoint;
- throttle count, cooldown duration, retry count;
- order lookup authoritative/unknown rates;
- recovery sweep progress and coverage;
- WS coverage/freshness/gap/resync per symbol;
- emergency command age/ack/retry;
- active leases and fencing rejections;
- per-CEX pending/open Spot reservation usage.

Rollback must be a feature-flag/config switch to the last proven single path. Do not keep two active write paths as fallback. If the new read path is unhealthy, disable it and use the old read path under its bounded rate budget. If write-path ambiguity occurs, halt new entries and reconcile; do not auto-failover create-order.
