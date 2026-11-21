# NAFAZ Executor V3 — PR-00 Deep Source Audit Report

Repository: `nzf210/nafaz_arb`  
Audit branch: `feat/executor-v3-enterprise`  
Parent branch: `feat/v2-bootstrap-config-stop-fencing`  
Audited HEAD: `32d0fd75d2de31f5737975abf0c3d837a7cf8699`  
Audit date: 2026-11-21  
Audit mode: READ-ONLY static source and CI evidence review  
Release status: V2 baseline CI-green; NOT LIVE-CERTIFIED

## 1. Executive decision

**PR-00 static source mapping is substantially complete, but PR-00 is not fully closed for implementation authorization.**

The audit inspected the complete Executor Python module tree (including entry, hedge, exit, rollback, force-close, recovery, registry, exchange wrappers, ledger, reconciliation, risk, runtime and venue adapters), the relevant schema migrations, the V2 architecture/release checklist, and the latest GitHub Actions result.

No trading source, test, migration, runtime configuration, production database or exchange was changed or contacted.

The most important conclusion is that V2 is not missing all enterprise building blocks. It already has PostgreSQL STOP/fencing, durable submit attempts, hedge claims, an order registry, an execution fill ledger, order/fill/exposure reconciliation, and an exit decision engine. The gap is that these components do not yet form one consistently enforced lifecycle authority. Some responsibilities are implemented in parallel paths or have weaker guarantees on de-risk and operator-command flows.

**Do not begin PR-01 implementation until the P0 findings in §4 have explicit decisions and regression tests.** This is an architecture/readiness recommendation, not a claim that the current V2 LIVE system is unsafe in every path.

## 2. Baseline and verification evidence

### 2.1 Branch identity

- `feat/executor-v3-enterprise` and `feat/v2-bootstrap-config-stop-fencing` were identical at audit start.
- Both resolved to HEAD `32d0fd75d2de31f5737975abf0c3d837a7cf8699`.
- GitHub compare reported `identical`, 0 commits ahead, 0 behind.
- The branch contains the three V3 planning documents; no V3 trading implementation commit exists.

### 2.2 CI evidence

Latest workflow observed for the audited HEAD:

- Workflow run: [44906294211](https://github.com/nzf210/nafaz_arb/actions/runs/44906294211)
- Result: **5 of 5 jobs succeeded**
  - `python`
  - `go`
  - `supabase-schema`
  - `vps-migration-runner`
  - `docker-images`

The latest checklist records source/test head `817eec2bafac85f2b0f242b3681cc4e06574f46b`, with **754 Python tests passed, 10 skipped**, PostgreSQL schema/integration and multi-process disconnect checks passed, plus Go tests/race/vet, migration-runner and image builds. The audited HEAD is a documentation commit on top of the same V2 source baseline; the latest CI run is green.

CI success proves the checked-in automated suite and disposable database workflows passed. It does **not** prove:
- the production VPS database schema is at the same migration version;
- actual Tokocrypto, Indodax, OKX or MEXC sandbox/live order semantics;
- the executor's multi-process behavior under every real network/database failure;
- production operational runbooks or operator identity governance;
- LIVE certification.

### 2.3 Audit limitations

This was a remote static audit of the repository tree and source contents. No local checkout was built or executed in this task. I did not run the test suite again, connect to the production VPS/database, inspect secrets or query CEX accounts. Runtime call graphs that depend on dynamic dispatch still require staging instrumentation and tests.

## 3. Private mutation call-site inventory

The following is the production Executor path map found in the Python source tree. Adapter methods ultimately perform the raw CCXT/private API request; orchestration call sites are responsible for intent, fencing, recovery and outcome handling.

| Operation | Orchestration call site | Adapter/raw request | Existing protection observed | PR-00 assessment |
|---|---|---|---|---|
| Spot entry BUY | `executor/engine_entry_submission.py:_submit_entry_order` (calls `place_local_limit_buy`) | `executor/exchange_orders.py:place_local_limit_buy` → `local.create_limit_buy_order` | Required order intent, PostgreSQL PREPARED submit attempt, shared `order_submit_fence`, exact client-ID recovery | Strongest current path; not routed through MutationGateway |
| Futures entry short SELL | `executor/entry_hedge.py:_submit_entry_hedge_order` (calls `open_global_short_market`) | `executor/exchange_futures.py:open_global_short_market` → `global_fut.create_market_sell_order` | Hedge generation claim, required order intent, PostgreSQL PREPARED attempt, shared submit fence, unknown-submit recovery | Strongest current path; not routed through MutationGateway |
| Normal Spot exit limit SELL | `executor/exit_orders.py` (dynamic exit placement) | `executor/exchange_orders.py:place_local_limit_sell` → `local.create_limit_sell_order` | Order intent is registered with `required=False`; `derisk_fence`; fill/cancel reconciliation | No durable `executor_submit_attempts` row; normal exit ownership is not cross-replica session ownership |
| Normal Spot exit market SELL | `executor/exit_orders.py:_execute_taker_sniper_exit` | `executor/exchange_orders.py:place_local_market_sell` → `local.create_market_sell_order` | `required=False` intent; `derisk_fence`; follow-up exit processing | Same de-risk persistence/ownership gap |
| Normal Futures close BUY | `executor/exit_execution.py` | `executor/exchange_futures.py:close_global_short_market` → `global_fut.create_market_buy_order` with `reduceOnly=True`, short position-side params | `required=False` intent; `derisk_fence`; order status/fill checks | No durable submit-attempt row; position-flat verification is not part of normal close finalization |
| Hedge-failure Spot rollback SELL | `executor/rollback.py:_liquidate_unhedged_spot` | `place_local_market_sell` | `required=False` intent; `derisk_fence`; stable ID reused for explicit rate-limit/settlement retries; fill verification | No durable submit-attempt row; retry classification and venue idempotency need per-venue proof |
| Force-close Futures BUY | `executor/force_close_legs.py:_close_global_short_leg` | `close_global_short_market` | Required durable order-registry intent; prior-intent lookup inside de-risk fence; reduceOnly; terminal fill and futures-flat verification | Better protected than normal exit; still uses a separate de-risk path rather than a single lifecycle gateway |
| Force-close Spot SELL | `executor/force_close_legs.py` spot liquidation path | `place_local_market_sell` | Required durable order-registry intent; prior-intent lookup inside de-risk fence; stable position-bound client ID; fill verification | Better protected than rollback/normal exit; still separate mutation path |
| Entry cancel | `executor/entry_orders.py:_cancel_entry_order_and_reconcile` | `executor/exchange_orders.py:cancel_local_order` → `local.cancel_order` | Cancel request timestamp/reason persisted; then venue order fetch and terminal/fill reconciliation | No shared durable cancel-intent/ownership contract |
| Dynamic exit cancel/replace | `executor/exit_orders.py:_cancel_existing_exit_order` | `cancel_local_order` | Fetches final venue state; applies racing fills; blocks replacement unless terminal | Good single-process safety; pair lock is process-local, not a distributed session lease |
| Force-close pre-cancel | `executor/cancel.py:_cancel_pending_order_for_force_close` | `cancel_local_order` | Fetches terminal order, processes racing fills, blocks force-close if uncertain | Good local reconciliation behavior; no common durable cancel intent |
| Operator `CANCEL_ORDER` | `executor/command_processor.py:process_one_command` | `MutationGateway.execute_cancel_order` | Command journal claim; calls generic gateway | **P0 defect:** gateway can report CONFIRMED without invoking a cancel method; see §4.1 |

### 3.1 MutationGateway status

- `MutationGateway` is instantiated in `executor/engine.py`.
- `execute_create_order` has no production orchestration caller identified in the Executor source tree inspected. The production entry/hedge paths use specialized exchange wrappers with their own PostgreSQL submit fencing.
- `execute_cancel_order` is called by the durable operator command processor.
- Do not simply reroute every existing call to the current gateway. Its cancel contract is not safe enough yet, and its generic create contract does not model the existing typed Spot/Futures wrapper capabilities and all current recovery metadata.

The correct V3 objective is one mutation authority with explicit operation contracts, not merely more call sites invoking the current class.

## 4. Findings requiring action before implementation

Severity meanings: **P0** = correctness/control defect to resolve before relying on the affected path; **P1** = material enterprise-scale or recovery gap; **P2** = hardening/consistency improvement.

### P0-01 — Operator CANCEL_ORDER can return false success

**Evidence**
- `executor/command_processor.py` passes `self.engine.client` to `MutationGateway.execute_cancel_order`.
- The facade in `executor/exchange_client.py` exposes `cancel_local_order`, not the generic `cancel_order` or `cancel_order_direct` method checked by the gateway.
- `execute_cancel_order` has no `else` branch when neither method exists. It still releases a Spot slot and returns `MutationResultType.CONFIRMED`.
- The gateway does not fetch final order state before reporting success.

**Impact**
A durable operator cancel command may be marked COMPLETED even though no venue cancel request was sent. Even with a compatible client, cancel acknowledgement is not proof the order is terminal.

**Required fix/test**
- Require a typed venue/market-bound cancel capability; missing capability must return REJECTED.
- Route by exact exchange + market/account, not an unbound composite facade.
- Persist cancel intent and fencing/ownership metadata.
- Fetch final order state and reconcile fills before slot release or replacement.
- Regression: facade exposing only `cancel_local_order` must call the correct adapter or fail closed; it must never return CONFIRMED without a venue call and terminal proof.

### P0-02 — Fill-ledger append is explicitly best-effort and callers ignore failure

**Evidence**
- `executor/ledger_hooks.py` documents ledger helpers as best-effort.
- `_ledger()` returns None when the ledger is disabled.
- `_record_ledger_fill()` returns None for missing/unverifiable order fill data and catches append errors.
- `ExecutionFillLedger.record_fill()` increments `_dropped` and returns None on database errors.
- Entry/exit call sites await the helper but do not require a durable fill ID before advancing the lifecycle.

**Impact**
A position can continue through hedge/exit accounting while its primary execution-fill ledger is incomplete. A later trade-history reconciliation may catch some missing fills, but it is a separate writer/path and does not turn the initial append into an atomic prerequisite.

**Required fix/test**
- Distinguish duplicate/replay from disabled, invalid and persistence-failed outcomes with typed results.
- For confirmed fills, persist a durable accounting event or durable reconciliation debt before continuing normal state transitions.
- Block new exposure in the affected scope when critical fill evidence is missing; keep protective de-risk available.
- Failure injection at every fill append boundary; restart must reconstruct exact exposure and accounting.

### P1-02 — Execution state has multiple authorities; PostgreSQL is not yet canonical end-to-end

**Evidence**
- `engine.py` owns in-memory `active_positions`, `pending_entry_orders`, `pending_exit_orders`.
- `engine_recovery.py` restores active positions and pending entries from Redis keys such as `arb:active_pos:*` and `arb:pending_entry:*`.
- Normal position/exit progress is written back to Redis.
- `GlobalStopService`, hedge claims and `ExecutionFillLedger` use PostgreSQL.
- `UniversalOrderRegistry` uses Redis as a hot cache/index and a SupabaseWriter durable order-record path backed by local SQLite outbox delivery.
- Supabase position/order/trade writes are asynchronous reporting/projection writes.
- Migration `20261114000100_add_executor_v2_core.sql` creates `execution_sessions` and `execution_legs`; the inspected runtime uses in-memory `TwoLegCoordinator` and Redis positions. No authoritative session/leg writer was established in this audit.

**Impact**
A Redis loss, partial outbox delivery, restart or concurrent worker can expose different notions of current position/order state. The existing architecture has durable components, but no demonstrated single transactionally consistent aggregate root across session, legs, orders, fills, hedge obligations, exit and PnL.

**Required fix**
Define authority by domain. V3 target: PostgreSQL transactional canonical session/leg/intent/order/fill/obligation/position/exit/risk state; Redis rebuildable cache/notification only; SQLite outbox only for downstream reporting. Prove that all active state can be rebuilt from PostgreSQL plus venue reconciliation before treating Redis as disposable.

### P1-03 — Normal exit/rollback paths do not use the durable submit-attempt protocol

**Evidence**
- Entry BUY and Futures hedge SELL persist `executor_submit_attempts` PREPARED, then transition to SUBMITTING under `order_submit_fence`.
- Normal Spot exit, normal Futures close and hedge rollback register order intent with `required=False` and use `derisk_fence`; they do not call `prepare_submit_attempt`.
- Force-close has stronger prior-intent lookup and required registry intent, but still uses a distinct de-risk path.

**Impact**
Entry and hedge have a more complete unknown-submit ledger than the exit/rollback paths. An ambiguous exit/rollback can therefore have different persistence and recovery semantics from an ambiguous entry. This is particularly important because a half-closed pair is still financial exposure.

**Required fix**
Create a separate durable risk-reducing intent state machine that is allowed under STOP but is still idempotent, recoverable and serialized. Do not reuse entry STOP semantics to block exits.

### P1-04 — Multi-instance exit ownership is not proven

**Evidence**
- `engine.py` uses in-process `asyncio.Lock` maps for pair/entry synchronization.
- `exit_orders.py` relies on pair-bound local locks for normal exit monitoring and cancel/replace.
- The PostgreSQL de-risk advisory lock serializes the actual de-risk critical section, but the normal exit path does not perform a durable prior-intent/exit-claim check under that lock before each new exit order.
- Force-close specifically performs prior-intent lookup inside the de-risk fence; normal exit does not follow the same pattern.

**Impact**
Two executor replicas can independently evaluate the same position and create separate normal exit orders with different client IDs. A global de-risk lock serializes requests but does not by itself prove the second worker will adopt the first worker's order instead of submitting another.

**Required fix**
Durable session/position exit lease or CAS claim; deterministic exit intent identity; reservation of exact close quantity; stale-owner fencing; cross-process duplicate-exit tests.

### P1-05 — De-risk DB-outage fallback trades availability for serialization

**Evidence**
- `derisk_fence()` intentionally allows a protective order to proceed without shared PostgreSQL serialization when a database outage is detected.
- This avoids leaving existing exposure unmanaged, but no durable distributed emergency-owner lease is acquired in the fallback.

**Impact**
If multiple replicas are alive during a PostgreSQL outage, each may attempt a risk-reducing action for the same exposure. ReduceOnly limits some Futures consequences, but Spot sells and duplicate rollback/exit requests are not inherently safe.

**Required fix**
Define a designated emergency owner/election or a single-instance emergency mode, stable idempotency IDs, bounded action rules and immediate post-recovery reconciliation. Preserve the ability to de-risk; do not solve this by simply blocking every close when PostgreSQL is down.

### P1-06 — One global PostgreSQL advisory lock serializes all entry submissions

**Evidence**
- `FENCE_LOCK_KEY` is one stable application-wide advisory key.
- `order_submit_fence()` calls `pg_try_advisory_lock(FENCE_LOCK_KEY)` for every create path using the shared entry fence.
- The unresolved-attempt query can be scoped to exchange/symbol, but the lock itself remains global.

**Impact**
A slow or rate-limited order on one venue/instrument can block unrelated venue/instrument submissions. This is safe but is a throughput bottleneck as concurrent accounts, markets and workers scale.

**Required fix**
Retain a globally ordered STOP epoch/admission barrier, but use scoped mutation locks for independent account/venue/instrument operations. Prove STOP-vs-submit linearizability and prevent stale fencing generations.

### P1-07 — Durable command PROCESSING state has no lease/reclaim path

**Evidence**
- `command_journal` schema stores state/attempts/timestamps but no owner or lease expiry.
- `poll_and_execute_pending_commands()` claims only rows in PENDING and changes them to PROCESSING.
- A process crash after claim leaves the command in PROCESSING; subsequent pollers only select PENDING.

**Impact**
Emergency/operator commands can remain stuck indefinitely after a worker crash.

**Required fix**
Add owner, lease token, lease expiry, heartbeat/attempt metadata and safe reclaim. Side effects must be idempotent; command completion must be fenced by the current lease token.

### P1-08 — EventInbox is not a durable, wired event-processing pipeline

**Evidence**
- `EventInbox` deduplicates in an in-memory set with bounded history.
- `engine.py` constructs `self.event_inbox`, but the inspected runtime/private-stream worker path updates `PrivateStreamCache` directly; no runtime `ingest_event` dispatch path was found.
- `PrivateOrderStreamWorker` uses `watch_orders` where the configured client exposes it and falls back to REST otherwise. It does not establish durable fill inbox processing or position stream reconciliation.

**Impact**
The class name and architecture docs suggest a normalized event pipeline, but actual event ingestion is currently a cache/REST acceleration path. It should not be treated as durable fill delivery or exactly-once event processing.

**Required fix**
Persist normalized event identity and payload before acknowledgement; process idempotently; use WS as low-latency input and REST/trade-history reconciliation as completeness authority. Track actual per-venue stream capability and coverage.

### P1-09 — Exit policy exists, but durable trailing state is absent

**Evidence**
- `ExitDecisionEngine.evaluate_exit()` implements emergency unwind, stop loss, unhedged exposure repair, profit target, time decay, breakeven, capital recovery and time stop.
- Defaults include $5 absolute stop-loss, 0.50% target net ROI, $0.50 minimum profit, decay phases and 90/180-minute recovery thresholds; runtime/config may override them.
- `PairPnLEngine` computes combined Spot/Futures net liquidation PnL and ROI on total deployed capital (Spot notional + actual Futures margin + fee reserve).
- No trailing high-water mark/peak drawdown state or durable trailing trigger was found in `ExitDecisionEngine`, exit order state or position model.
- The decision engine returns actions; exit order execution remains in the existing `exit_orders.py` / `exit_execution.py` mixins.

**Impact**
The current policy is not a blank slate and should be reused. However, it does not yet provide a restart-safe trailing-profit exit, and the exit decision + execution lifecycle is not separated by a durable coordinator boundary.

**Required fix**
Keep the current evaluator as the decision authority. Add policy snapshot, persisted high-water mark, explicit exit intent/coordinator, quantity reservation and verified final flatness. Profit target remains based on combined net liquidation economics and total deployed capital; no universal hard-coded percentage should be introduced.

### P1-10 — Fill/accounting pipelines need one declared source of truth

**Evidence**
- `ExecutionFillLedger` writes to PostgreSQL `execution_fill_ledger`.
- `ledger_hooks.py` can synthesize a cumulative order-average fill ID when an order snapshot contains multiple fills or no single trade row.
- `TradeFillReconciliationWorker` separately fetches venue trade history and writes individual fills through `SupabaseWriter.order_fill()`, then checks cumulative quantity and fee completeness.
- The trade-history reconciliation path and the PostgreSQL execution-fill ledger are distinct persistence paths in the inspected code.

**Impact**
The system has both order-snapshot-derived fill events and venue-trade-history-derived fill rows. Their relationship, correction rules, and which one feeds realized PnL must be explicit to avoid double counting or silent divergence.

**Required fix**
Make venue trade ID the canonical execution identity. Keep order cumulative fills as progress snapshots only. Define a single canonical fill ledger and treat all other records as projections/reconciliation evidence. Add multi-fill/partial-fill replay tests.

### P1-11 — Normal exit finalization is weaker than force-close flat verification

**Evidence**
- Force-close futures path calls explicit position-flat verification.
- Normal exit execution validates the close order terminal status and fill quantity, then updates position state and eventually `_finalize_exit_position()`.
- `_finalize_exit_position()` removes the Redis active-position key when the local quantity is below its dust threshold; no equivalent explicit venue position-flat query is performed in that finalizer.

**Impact**
A fully filled reduceOnly close order is strong evidence, but it is not the same as reconciling actual position inventory and all related open orders before marking the session CLOSED.

**Required fix**
Require final order inventory + trade fills + Spot balance attribution + Futures position zero + no outstanding hedge allocation before CLOSED. If venue inventory cannot be verified, retain RECONCILIATION_REQUIRED.

### P2-01 — Floating-point types remain throughout correctness-critical execution

Confirmed in quantity, hedge claims, order registry, fill ledger hooks, pair PnL, exit decision, recovery and reconciliation. PostgreSQL uses NUMERIC in parts of the schema, but values are converted to Python float before calculations and writes. V3 should introduce Decimal/typed quantity/notional/FX values at domain boundaries, with explicit venue precision conversion.

### P2-02 — Venue capability and stream readiness need measured certification

The modular `executor/venues/*` package does not contain the main create/cancel call sites; actual private writes are in `exchange_orders.py` and `exchange_futures.py`. The stream worker only uses clients that expose `watch_orders`. A venue being registered or exposing a generic CCXT method is not proof of exact client-ID lookup, trade-history pagination, reduceOnly semantics, position mode or private-stream completeness.

### P2-03 — Schema identity needs account scoping for enterprise multi-account use

The execution fill unique index is `(exchange_name, market_type, exchange_fill_id)`. It does not include an explicit credential/account identity. Before multi-account operation, uniqueness and idempotency scopes must be validated against each venue's trade-ID guarantees and account model.

## 5. Existing assets to preserve

| Existing component | V3 decision |
|---|---|
| `GlobalStopService` / `executor_submit_attempts` | Preserve entry admission and unknown-submit safety; evolve to global STOP epoch + scoped mutation ownership |
| `HedgeIntentClaimService` | Preserve per-parent fill-generation CAS; add pooled obligations/allocation only as a separate domain |
| `UniversalOrderRegistry` | Preserve identifiers and recovery behavior; clarify durable writer and canonical state ownership |
| `ExecutionFillLedger` | Preserve append-only concept; make append result explicit and canonical |
| `TradeFillReconciliationWorker` | Preserve venue trade-history verification; unify with canonical ledger |
| `ExposureReconciliationWorker` | Preserve Spot balance/Futures position checks; extend to account/session/exit verification |
| `ExitDecisionEngine` | Preserve current stop/target/time-decay policy; add durable trailing and policy snapshots |
| `PairPnLEngine` | Preserve combined liquidation PnL and deployed-capital ROI; enforce verified fee/FX/funding inputs |
| `derisk_fence` | Preserve STOP-independent protective actions; replace unowned DB-outage fallback with explicit emergency ownership |
| Venue adapters | Preserve tested venue semantics; do not rewrite adapters without contract failures |

## 6. PR-00 acceptance checklist

| Item | Result | Notes |
|---|---|---|
| Confirm branch and HEAD | PASS | Branch and parent identical at `32d0fd7` |
| Inspect complete Executor Python file tree | PASS | All Executor Python modules in repository tree fetched/scanned for private mutation calls |
| Map create/cancel/rollback/force-close call sites | PASS WITH FINDINGS | See §3; operator cancel P0 identified |
| Inspect submit fencing and de-risk fencing | PASS WITH FINDINGS | Global lock; de-risk DB outage fallback |
| Inspect exit/PnL lifecycle | PASS WITH FINDINGS | Existing evaluator; no durable trailing; normal close lacks final flat query |
| Inspect fill and reconciliation lifecycle | PASS WITH FINDINGS | Best-effort ledger + separate trade-history fill writer |
| Inspect relevant schema migrations | PASS WITH FINDINGS | Session/leg schema exists; runtime writer/canonical authority unclear |
| Inspect V2 release checklist and regression contract | PASS | Existing open release gates retained |
| Check latest CI | PASS | Latest workflow 5/5 jobs succeeded |
| Re-run full tests locally | NOT RUN | No local checkout/runtime execution in this audit |
| Verify production DB migration state | NOT RUN | No production database access |
| Verify CEX sandbox/live contracts | NOT RUN | No exchange requests performed |
| Runtime-instrumented multi-replica call graph | OPEN | Must be part of implementation/certification tests |

**PR-00 disposition: STATIC AUDIT DELIVERABLE COMPLETE; implementation gate remains CONDITIONAL.** Resolve P0-01 with a focused regression before relying on the operator cancel path. Carry P1-01 and the remaining P1 findings into the V3 implementation backlog and acceptance tests. Keep all P1 findings in the V3 backlog and do not claim LIVE certification.

## 7. Immediate implementation order after PR-00

1. **PR-00A — Close P0 controls:** fix/test operator cancellation semantics; define typed fill-ledger append result and accounting-debt behavior. This is still audit follow-up, not a request to patch V2 on this branch.
2. **PR-01 — Canonical state and lifecycle contract:** identify one authoritative writer for sessions, legs, orders, fills, positions, hedge obligations and exits; map existing tables before migrations.
3. **PR-02 — Scoped ownership/fencing:** global STOP epoch plus scoped account/venue/instrument/session leases; include normal exit and de-risk.
4. **PR-03 — Unified mutation contract:** route all create/cancel/replace/reduce-only close/rollback/force-close through typed operation contracts with durable intents and unknown outcome reconciliation.
5. Continue with the approved V3 roadmap phases for fill ledger, hedge obligations, liquidity scheduler, PnL/exit, reconciliation, operations and certification.

No scanner source or scanner contract changes are included in this plan.
