# NAFAZ Executor V3 — Implementation Blueprint

- Repository: nzf210/nafaz_arb
- Target branch: feat/executor-v3-enterprise
- Status: PROPOSED — source inventory and design approval required before implementation
- Scope: Executor rebuild only; scanner behavior is frozen and out of scope
- Reviewed branch baseline: 32d0fd75d2de31f5737975abf0c3d837a7cf8699 (source baseline; documentation updates follow)
- Language: English

> This is an implementation plan, not proof that the design is implemented or LIVE-certified. Creating this document does not change trading code, runtime configuration, database state, or production resources.

---

## 1. Executive summary

NAFAZ Executor V3 is a controlled internal rebuild of the execution lifecycle. Its purpose is to make execution state durable, recoverable, idempotent, observable, and safe under partial fills, concurrent events, exchange timeouts, process restarts, and partial exits.

The scanner is treated as proven for this workstream. V3 must consume the existing opportunity contract without changing scanner discovery, market-data collection, pair matching, opportunity scoring/ranking, profit estimation, Redis publication, or scanner APIs. The executor may perform final execution-time validation because prices, books, fees, balances, and venue state can change after scanning.

The rebuild must not preserve weak internal orchestration merely for backward compatibility. It must preserve externally relied-upon contracts and already-correct business rules. Existing executor components are reused only after their behavior is verified against this blueprint.

### Target outcomes

1. Every accepted opportunity has one durable execution session.
2. Spot and Futures legs have independent order and fill lifecycles.
3. Every confirmed Spot fill creates a durable hedge obligation.
4. Hedge calculations use confirmed fills and account for in-flight reservations.
5. Small compatible hedge obligations may be aggregated safely, with deterministic allocation back to source sessions.
6. Liquidity-aware scheduling waits when executable depth is insufficient, but never waits without risk limits.
7. Ambiguous submissions are reconciled before retry.
8. Exit is coordinated across both legs and cannot report completion while exposure remains.
9. Restart recovery reconstructs sessions from durable records and exchange evidence.
10. PnL is attributable to fills, fees, funding, conversion, and actual exit execution.
11. Multiple sessions progress concurrently without duplicate ownership or cross-session contamination.
12. Every exchange write is auditable and protected by durable intent, idempotency, and fencing.

### Non-goals

- Rebuild or replace the Go scanner.
- Change scanner ranking or strategy formulas.
- Silently change strategy parameters, leverage, risk limits, venue selection, fee assumptions, or sizing semantics.
- Rewrite all exchange adapters without evidence that an adapter is incorrect.
- Move the executor to another repository or programming language.
- Claim that unit tests alone prove LIVE readiness.
- Promise guaranteed profit.

---

## 2. Repository baseline and source-review facts

The branch already contains meaningful V2 work. V3 is a rebuild of lifecycle/orchestration, not a blind deletion of existing code.

### Existing files confirmed on the reviewed branch

| Existing path | Observed responsibility / current role | V3 treatment |
|---|---|---|
| executor/entry_hedge.py | EntryHedgeMixin._handle_entry_fills calculates cumulative Spot fill, hedge target/residual, fill threshold, and venue minimum checks. | Refactor behind a durable Hedge Obligation service. Preserve verified sizing and hedge-ratio semantics; remove reliance on transient order dictionaries/Redis-only progress. |
| executor/fill_ledger.py | Fill ledger and fill attribution support. | Audit schema, idempotency key, transaction boundaries, fee handling, correction and replay behavior. Reuse or evolve; do not create a competing fill ledger. |
| executor/event_inbox.py | Normalizes private-stream order/trade events and deduplicates with an in-memory set. | Replace in-memory-only deduplication as the correctness boundary with durable inbox identity and transactional processing. |
| executor/engine_entry_flow.py | Entry orchestration mixin. | Reduce to thin application orchestration; move business rules into explicit services. |
| executor/engine_entry_submission.py | Entry order submission and preparation/error paths. | Route all order writes through durable intent + mutation gateway; preserve existing risk/STOP checks. |
| executor/engine_recovery.py | Startup/recovery logic, including unresolved submit-attempt recovery by client order ID. | Evolve into resumable Recovery Coordinator. Preserve exact-ID recovery and fail-closed behavior. |
| executor/exposure_reconciliation.py | Periodic read-only exposure reconciliation using active positions and exchange balance reports. | Extend into order, fill, position, and PnL reconciliation. Aggregate position comparison alone is insufficient. |
| executor/global_stop.py and related fencing code | Durable STOP and submit-fencing controls. | Preserve and integrate into every order mutation path: entry, hedge, exit, rollback, and emergency de-risking. |
| executor/quantity.py | Amount specifications, order sizing, canonical hedge target, and Futures sizing validation. | Preserve as shared quantity/contract normalization foundation after Decimal and venue-contract audit. |
| executor/private_stream.py and venue stream adapters | Private-stream cache/event support. | Treat streams as event acceleration, not sole source of truth. Verify actual subscription/reconnect behavior per venue. |
| executor/order_registry.py (if present at implementation baseline) | Durable order registry referenced by recovery paths. | Inspect and reuse if its uniqueness and transaction guarantees satisfy this blueprint. Do not create a parallel registry. |

### Observations from inspected source

- entry_hedge.py uses float conversions in its fill path and keeps progress fields such as filled/hedged quantities in order metadata. V3 must move correctness-critical quantity/notional calculations to exact decimal representation and durable ledgers.
- event_inbox.py currently uses an in-memory _seen_event_ids set and clears it when its history limit is reached. Its event IDs are composed from selected order/trade fields. This is useful as a process-local optimization, but cannot guarantee deduplication across restarts or executor instances.
- exposure_reconciliation.py compares expected exposure derived from engine.active_positions with exchange balance/position reports. This is a useful safety monitor, but does not replace per-order and per-fill reconciliation.
- engine_recovery.py already has an exact client-order-ID recovery path and refuses to acknowledge a recovered venue order without a durable registry record. Preserve this behavior.
- The V2 checklist explicitly leaves some cross-instance create-order path coverage and multi-instance integration evidence open. V3 must close those gaps with tests, not documentation-only claims.
- README material describes both scanner and executor. Scanner claims are not a reason to change scanner code in this workstream.

### Existing documents to treat as constraints

- docs/EXECUTOR-ARCHITECTURE-V2.md
- docs/V2-BOOTSTRAP-CONFIG-STOP-FENCING-CHECKLIST.md
- docs/AUDIT-FINDINGS.md
- docs/REGRESSION-CONTRACT.md
- docs/EXPOSURE-RECONCILIATION.md
- docs/TAHAP4-FILL-LEDGER-HARDENING.md

Before coding, review their complete current contents and reconcile conflicts. This blueprint supersedes V2 only for the approved executor implementation scope; it does not silently invalidate existing safety contracts.

### Required inventory before implementation

The reviewed file list is not a complete repository tree. Before assigning exact line-level changes, inventory:

- all modules under executor/;
- all test files under tests/ and executor-specific test directories;
- all database migrations and SQL schema files;
- all Redis queue/consumer/publisher code;
- all exchange adapter and private-stream modules;
- all Telegram commands that read or mutate executor state;
- all deployment/compose files and worker entrypoints;
- all references to create/cancel/close order methods;
- all direct writes to Redis, PostgreSQL/Supabase, order registry, active positions, and pending-order records.

Do not assume a proposed V3 module already exists. Do not create a new module if an existing module can be safely evolved to own the same responsibility.

---

## 3. Locked business and execution invariants

### 3.1 Scanner boundary

The scanner is out of scope. Preserve:

- current scanner API and opportunity payload;
- opportunity identifiers and dispatch semantics;
- pair/venue identity mapping;
- profit-estimation and ranking behavior;
- Redis publication contract;
- scanner configuration and operational behavior.

The executor may reject an opportunity at admission if it is stale, malformed, no longer profitable under execution-time checks, exceeds risk limits, has invalid venue capabilities, or cannot be sized safely. Rejection must include a stable reason code and must not mutate scanner ranking logic.

### 3.2 Notional and hedge policy

These are internal policy defaults, not overrides of exchange rules:

| Policy | Default |
|---|---:|
| Minimum Spot order notional | USD 10 equivalent |
| Minimum Futures order notional | USD 5 equivalent |
| Default hedge ratio | 1.0 (1:1 base-equivalent exposure) |
| Over-hedge | Prohibited by default |
| Small hedge obligations | May be accumulated/aggregated when compatible |
| Hedge basis | Confirmed Spot fills |
| Hedge completion basis | Confirmed Futures fills, not submitted quantity |
| Exchange minimums | Always authoritative when stricter |
| Unhedged exposure | Must remain within configured notional, duration, and price-risk budgets |

A venue's actual minimum amount, minimum cost/notional, contract size, amount step, and precision may make a market ineligible even when it meets internal USD policy. Never fabricate a lower exchange minimum.

### 3.3 Quantity and notional definitions

- Keep base quantity, contract quantity, quote notional, margin, and USD-equivalent notional as distinct typed concepts.
- Normalize linear/inverse contracts using authoritative contract metadata.
- Use exact decimal arithmetic for quantity, price, fee, and notional decisions.
- Never compare contract count directly with base-asset quantity unless the adapter has normalized it.
- Keep FX rate, source, timestamp, and validity with IDR-to-USD-equivalent valuation.
- Missing/stale/invalid FX makes the affected valuation unknown; never use a hard-coded fallback.

### 3.4 Core invariants

1. A confirmed Spot fill creates or increases a hedge obligation exactly once.
2. A submitted order is not a confirmed fill.
3. An exchange acknowledgement is not proof of a fill or terminal order state.
4. A cancel acknowledgement is not proof that the order is canceled or that no late fill occurred.
5. A Futures in-flight reservation reduces the amount another worker may submit.
6. A hedge fill is allocated to source obligations exactly once.
7. An unconfirmed Futures order cannot satisfy an obligation.
8. A session cannot complete while any leg, order, obligation, or managed exchange exposure remains unresolved.
9. Unknown order inventory is not empty inventory; unknown Futures position is not flat.
10. Database/lease/fencing uncertainty blocks new exposure.
11. Redis loss cannot erase an order, fill, session, or hedge obligation.
12. Restart cannot reset cumulative fill, hedge, exit, or risk progress.
13. No order write may bypass the mutation gateway and durable intent path.
14. Each venue/account/market scope has one serialized mutation authority at a time.
15. One session's failure must not corrupt another session's state.

---

## 4. Target architecture

### 4.1 Logical components

1. Opportunity Admission Gate — validates incoming scanner payload, freshness, venue mapping, current book, fee assumptions, account state, minimums, and risk.
2. Execution Coordinator — owns session creation, durable state transitions, leases/fencing, and orchestration.
3. Spot Leg Manager — owns Spot order lifecycle and actual Spot exposure.
4. Futures Leg Manager — owns Futures order lifecycle, contract conversion, and short exposure.
5. Fill Ledger — immutable source of confirmed trades/fills and associated fees.
6. Hedge Obligation Manager — derives obligations from Spot fills and tracks confirmed/in-flight Futures coverage.
7. Hedge Allocation Service — allocates aggregated Futures fills deterministically to originating obligations.
8. Liquidity Evaluator — calculates executable depth, VWAP, slippage, freshness, and hedge eligibility.
9. Hedge Scheduler — wakes obligations on fills/book updates/timers and submits only when minimum and risk constraints permit.
10. Pair Exposure and PnL Engine — calculates combined Spot/Futures exposure and attributable economics.
11. Exit Coordinator — closes both legs with independent progress and recovery.
12. Exchange Mutation Gateway — the only path for create/cancel/replace/close mutations.
13. Reconciliation Engine — reconciles orders, fills, positions, balances, and PnL.
14. Recovery Coordinator — resumes incomplete sessions and resolves ambiguous/unknown states.
15. Operational Read Model — exposes timelines, pending orders, obligations, positions, incidents, and metrics to existing operator consumers.

### 4.2 Deployment model

- One active logical coordinator and one standby coordinator.
- PostgreSQL lease and monotonically increasing fencing generation determine ownership.
- Multiple execution sessions may run concurrently.
- Per-session and per-instrument/account serialization prevents conflicting writes without globally serializing all trading.
- Workers may scale horizontally only after durable claiming, fencing, idempotency, and cross-process tests pass.
- Redis is cache, wake-up queue, notification, and acceleration. PostgreSQL remains canonical.
- Private WebSocket events are preferred for low-latency updates; REST reconciliation is mandatory fallback and verification.

### 4.3 Dependency direction

Business services must not directly call venue SDK/CCXT methods. They call typed adapter interfaces. Only the mutation gateway may invoke private order mutation methods. Read-only market/account queries may use a shared read gateway/scheduler.

---

## 5. Execution session and state model

### 5.1 Session identity

Persist at least:

- execution_session_id;
- source opportunity_id;
- account/credential-scope identifier (never credential material);
- Spot venue, market type, native symbol, normalized symbol, base/quote;
- Futures venue, market type, native symbol, normalized symbol, contract type;
- strategy/config version and immutable execution-parameter snapshot;
- target notional/quantity and hedge ratio;
- creation/update timestamps;
- current state and monotonically increasing state_version;
- coordinator fencing generation;
- terminal reason and completion timestamp, when applicable.

Do not use a mutable pair label as the only primary identity.

### 5.2 Session states

Recommended lifecycle:

CREATED → VALIDATING → ADMITTED → ENTRY_IN_PROGRESS → HEDGE_PENDING / PARTIALLY_HEDGED → HEDGED → EXIT_IN_PROGRESS → EXIT_PARTIAL → RECONCILIATION_REQUIRED / RECOVERY_IN_PROGRESS → COMPLETED

Additional blocked/terminal states may include REJECTED, ABORTED, and FAILED, but they must never imply exposure is zero unless verified.

### 5.3 Independent leg states

Each Spot and Futures leg has its own state:

NOT_STARTED, PREPARED, SUBMITTING, SUBMISSION_UNKNOWN, OPEN, PARTIALLY_FILLED, FILLED, CANCEL_PENDING, CANCELED, REJECTED, CLOSING, CLOSED, UNKNOWN.

Session state is derived from leg states, obligations, positions, and reconciliation status. Do not force both legs into one shared order status.

### 5.4 Transition rules

- Every transition has an expected prior state/version and actor/fencing token.
- Illegal transitions are rejected and logged.
- State update and corresponding outbox event are committed atomically.
- Late events are reconciled against order/fill identity; they are not discarded merely because a session advanced.
- Terminal states are immutable except for explicit reconciliation corrections with audit history.
- COMPLETED requires verified flat exposure on both legs, no live/open order, no unresolved intent, no pending hedge obligation, and successful final reconciliation.

---

## 6. Durable data model

This is a logical schema proposal. Map it to existing tables before migrations and reuse compatible structures. Avoid duplicate sources of truth.

| Logical entity | Purpose and key constraints |
|---|---|
| execution_sessions | One durable record per session; unique source/admission key prevents duplicate session creation. |
| execution_legs | Independent Spot/Futures leg state, target, confirmed quantity, remaining quantity, venue and instrument identity. |
| execution_order_intents | Durable pre-submit intent, stable idempotency/client ID, request hash, fencing generation, status. |
| execution_orders | Venue order identity, client ID, side/type/price/quantity, status, cumulative fill and remaining quantity. |
| execution_order_events | Append-only order lifecycle observations and cancel/replace outcomes. |
| execution_fills | Immutable confirmed trade facts, venue trade ID, order identity, exact price/qty, fee and fee currency. |
| hedge_obligations | Target hedge quantity, confirmed allocation, reserved in-flight quantity, remaining quantity, deadline and risk state. |
| hedge_allocations | Deterministic mapping of Futures fills to source obligations; no over-allocation. |
| execution_positions | Session/instrument expected exposure and verified exchange exposure snapshots. |
| execution_pnl_ledger | Fill-level realized PnL, fee, funding, conversion, adjustment and settlement entries. |
| execution_commands | Durable commands with claim, retry, lease and terminal status. |
| execution_inbox | Durable deduplicated private-stream/REST events and processing state. |
| execution_outbox | Transactional event publication records. |
| execution_leases | Coordinator/worker ownership and monotonic fencing generation. |
| execution_reconciliation_runs | Scope, source watermarks, result, mismatch and remediation status. |
| execution_incidents | Operator-visible unknown/mismatch/risk incidents and acknowledgements. |

### 6.1 Database transaction requirements

- Persist intent before any network mutation.
- Hedge reservation and corresponding order intent must be committed atomically.
- Fill insertion, cumulative quantity update, obligation update, allocation update, and outbox event must be transactionally coordinated.
- Unique constraints are the final defense against duplicate fills/events; in-memory dedupe is only an optimization.
- Use row locks or conditional updates scoped to obligation/session/instrument. Do not use one global lock for all sessions.
- Use optimistic concurrency (state_version) for session transitions.
- Every mutating command carries fencing generation.
- Do not hold a database transaction open during slow exchange calls except an explicitly reviewed submit-fence critical section. Prefer durable pre-submit reservation plus fenced mutation gateway.
- Database outage or uncertain commit outcome blocks new exposure. Recovery inspects durable intent and venue before retry.

### 6.2 Numeric and time representation

- Use PostgreSQL NUMERIC or an equivalently exact representation for financial values.
- Use Python Decimal for executor business calculations; do not convert to binary float for trading decisions.
- Preserve redacted raw venue payloads where useful; exclude credentials and sensitive headers.
- Persist UTC timestamps, source timestamp, receive timestamp, and event sequence/watermark when available.
- Persist fee currency and amount separately from fee valuation currency/rate.

---

## 7. Order intent and mutation gateway

### 7.1 Submission protocol

1. Validate session and leg state.
2. Validate lease/fencing token and global STOP.
3. Validate venue capability, account scope, amount spec, risk budget, and current price/book.
4. Generate stable local intent ID and client order ID where supported.
5. Persist complete intent and request hash.
6. Reserve relevant quantity/notional and concurrency slot.
7. Commit durable intent.
8. Submit through the single mutation gateway.
9. Persist acknowledgement or mark SUBMISSION_UNKNOWN if response is ambiguous.
10. Resolve exact order by client ID/order ID and reconcile fills before retry or replacement.

### 7.2 Idempotency

- Same logical intent must never create a second exchange order due to retry.
- Client IDs are deterministic per logical attempt and comply with venue constraints.
- If a venue lacks reliable client-ID lookup, use strongest available order/trade correlation and keep scope blocked until ambiguity is resolved.
- A new client ID is not permission to retry an ambiguous prior submission.
- Reusing an idempotency key with different normalized parameters is rejected.

### 7.3 Cancellation and replacement

- Cancel acknowledgement is not terminal proof.
- Fetch exact order and reconcile all fills after cancellation.
- Replacement is forbidden until prior order is terminal and cumulative fill is reconciled.
- Cancel/fill races are first-class cases.
- Do not release reserved quantity or balance until final order state is verified.

---

## 8. Fill ledger and event processing

### 8.1 Fill as canonical execution fact

Store venue, account, market type, symbol, order ID, client ID, trade ID, side, exact quantity, price, fee, fee currency, venue timestamp, receive timestamp, and raw-event hash.

- Use venue trade identity as primary dedupe key where available.
- If trade ID is unavailable, define a documented venue-specific composite identity and test collision behavior.
- Order cumulative filled quantity is a consistency check, not a substitute for individual fill records.
- If cumulative fill and sum of unique fills disagree beyond venue-specific tolerance, mark mismatch; do not silently overwrite either value.

### 8.2 Durable inbox

executor/event_inbox.py is a starting point for normalization, but its in-memory dedupe set is not a durable boundary.

Processing flow:

1. Normalize event with venue adapter and preserve source metadata.
2. Insert into durable inbox using unique event identity.
3. Claim event with lease/fencing token.
4. Apply event and associated ledger/obligation updates in one transaction.
5. Mark processed and publish outbox event in the same transaction.
6. Retry unprocessed events after restart.
7. Route malformed, out-of-order, or uncorrelatable events to explicit unknown/reconciliation state.

Handlers must be idempotent. Duplicate delivery must not duplicate fills, obligations, allocations, or PnL.

---

## 9. Hedge Obligation Manager

This is the highest-priority business subsystem in V3.

### 9.1 Obligation creation

Every confirmed Spot fill creates or increases a hedge obligation for its session and compatible Futures instrument.

For default 1:1 base-equivalent hedge:

target_hedge_qty = confirmed_spot_qty × hedge_ratio

Calculate in normalized base-equivalent quantity after contract conversion and venue rules.

### 9.2 Residual calculation

hedge_residual = max(0, target_hedge_qty - confirmed_hedge_qty - reserved_inflight_qty)

- target_hedge_qty comes from confirmed Spot fills and hedge ratio.
- confirmed_hedge_qty is confirmed Futures fills allocated to this obligation.
- reserved_inflight_qty is quantity reserved by durable unresolved Futures intents.

Never count requested order quantity as confirmed hedge.

### 9.3 Minimum threshold and partial fills

- Internal minimum Spot order notional: USD 10 equivalent.
- Internal minimum Futures order notional: USD 5 equivalent.
- Actual exchange minimum amount/notional/contract rules take precedence when higher.
- A Spot fill below Futures minimum remains a durable pending obligation.
- Do not submit Futures larger than the obligation just to satisfy minimum.
- Do not immediately unwind solely because current Futures depth is temporarily insufficient.
- Accumulate fills while the Spot order is active and risk budget permits.
- Below-minimum obligations may be combined only under the aggregation rules below.
- If Spot becomes terminal while obligation remains below minimum, apply bounded recovery: compatible aggregation, bounded wait, risk-approved unwind, or operator intervention. Never wait forever or silently mark hedged.

### 9.4 Safe aggregation

Aggregate only when all required keys match:

- same account/credential scope;
- same Futures venue and market type;
- same exact contract/instrument and contract-specification version;
- same hedge direction;
- same margin/position mode where relevant;
- compatible hedge ratio and risk policy;
- compatible settlement/quote and allocation rules;
- no conflicting reduce-only/position ownership semantics.

Workflow:

1. Select eligible obligations using deterministic ordering (e.g. oldest risk exposure first).
2. Lock/claim selected obligations.
3. Calculate aggregate residual and validate minimum, precision, depth and risk.
4. Reserve each obligation's contribution atomically.
5. Create one Futures order intent with allocation plan.
6. Allocate each confirmed Futures fill deterministically to source obligations.
7. Release unused reservations only after final order reconciliation.
8. Recalculate residuals and requeue remaining obligations.

Aggregation must not erase per-session accounting. Each session retains its obligation, allocation, exposure, and PnL attribution.

### 9.5 In-flight reservation race protection

Two workers processing the same Spot fill must not both create hedge intents for the same residual.

The reservation transaction atomically:

- locks or conditionally updates relevant obligation rows;
- calculates target minus confirmed minus existing in-flight;
- verifies minimum and risk policy;
- creates hedge intent;
- records reservation allocations;
- increments state version;
- writes outbox event.

If a worker loses the concurrency race, it reloads and recalculates. It must not submit using stale in-memory residual.

---

## 10. Liquidity-aware hedge scheduler

### 10.1 Liquidity evaluation

Do not use only best bid/ask quantity. Evaluate executable depth across levels up to configured maximum slippage/price boundary.

Inputs include venue, book source, sequence/update ID, exchange timestamp, local receive timestamp, normalized levels, contract metadata, residual, amount step/minimum, fees, slippage limits and risk budget.

Outputs include executable base quantity, USD-equivalent notional, expected VWAP, slippage, book age/freshness, eligible quantity, and wait/rejection reason.

Reject crossed, malformed, stale, sequence-gapped, or unverified books. Re-fetch/rebuild before use.

### 10.2 Scheduler behavior

- Wake on new obligation, Spot fill, Futures fill, book update, order terminal event, timer and recovery event.
- Coalesce redundant wake-ups; durable obligation remains the source of work.
- Use bounded backoff and venue rate-limit scheduling.
- Re-evaluate book and risk immediately before submission.
- Do not busy-loop while waiting.
- Track wait reason and duration.

Suggested states:

WAITING_FOR_MINIMUM, WAITING_FOR_LIQUIDITY, READY_TO_HEDGE, HEDGE_SUBMITTING, HEDGE_PARTIAL, HEDGE_COMPLETE, RISK_LIMIT_BREACHED, RECOVERY_REQUIRED.

### 10.3 Unhedged risk budget

Every obligation/session has explicit configurable limits:

- maximum unhedged USD-equivalent notional;
- maximum unhedged duration;
- maximum adverse price movement;
- maximum basis deterioration;
- maximum hedge attempts and venue cooldown;
- maximum aggregate pending obligations per account/instrument;
- maximum permitted stale-book duration.

Actual values must be imported from approved runtime configuration and compared exactly. This blueprint does not authorize changing risk values.

On breach:

1. Stop adding new Spot exposure for affected scope.
2. Cancel remaining Spot quantity where safe and verify final status/fills.
3. Recalculate final Spot exposure from confirmed fills.
4. Attempt approved hedge/recovery route if within policy.
5. Otherwise execute approved unwind or require operator action.
6. Keep incident open until exchange exposure is verified and reconciled.

---

## 11. Pair exposure, PnL, and exit

### 11.1 Separate execution state from market valuation

Maintain separate versions/snapshots for execution/order state, fill ledger state, position/exposure state, market-data state, and PnL valuation state.

A market price update may change unrealized PnL, but must not mutate order/fill state or trigger false reconciliation.

### 11.2 PnL requirements

Track separately:

- estimated PnL at admission;
- realized PnL from actual fills and exits;
- unrealized PnL from executable liquidation prices;
- fees by fill and fee currency;
- funding payments/receipts and settlement identity;
- conversion valuation and FX source/timestamp;
- slippage versus decision and order prices;
- corrections/adjustments with reason and audit actor.

Use actual fill prices, not scanner snapshots, as accounting basis after execution. Funding must be idempotently linked to exchange settlement records where available.

### 11.3 Exit decision

Use pair-level net liquidation economics and risk:

- executable Spot sale proceeds;
- executable Futures buy-to-close cost;
- entry/exit fees;
- funding already settled and expected before next settlement;
- conversion cost/rate;
- expected slippage and depth;
- risk/timeout/emergency constraints.

A positive unrealized mark based on mid-price is not proof that the pair can be closed profitably.

### 11.4 Exit execution

- Each leg has independent exit intent and fill progress.
- Futures close uses correct short-side semantics and reduce-only where supported/required.
- Spot sell quantity is bounded by verified exposure and inventory ownership.
- Partial exit is a first-class state.
- If one leg closes and the other fails, keep session open in EXIT_PARTIAL / RECONCILIATION_REQUIRED.
- Retry only after exact order reconciliation.
- Completion requires both legs verified flat, all relevant orders terminal, no remaining obligations/reservations, and final PnL reconciliation.

---

## 12. Reconciliation and recovery

### 12.1 Four reconciliation domains

| Domain | Compare | Failure result |
|---|---|---|
| Order | Durable intents/orders versus venue open and historical order state | UNKNOWN or MISMATCH; block conflicting writes |
| Fill | Durable fill ledger versus venue trade history and order cumulative fill | MISMATCH; do not silently rewrite fills |
| Position | Session/leg exposure versus exchange position/balance/inventory | MISMATCH / ORPHAN; freeze affected scope |
| PnL | Fill/fee/funding ledger versus exchange statements/settlements | PENDING_SYNC or MISMATCH; keep accounting provisional |

### 12.2 Reconciliation triggers

- process startup before admission;
- order submission timeout;
- cancel/replace timeout;
- private stream disconnect/reconnect;
- late or out-of-order event;
- cumulative fill mismatch;
- periodic safety sweep;
- session exit;
- ownership/fencing change;
- database recovery after outage.

### 12.3 Recovery order

1. Acquire valid lease/fencing ownership.
2. Load unresolved sessions, intents, orders, fills, obligations, allocations, positions and incidents.
3. Query venue open orders and relevant historical orders/trades with pagination.
4. Resolve ambiguous submissions by exact client ID/order ID first.
5. Reconcile order cumulative quantities against unique fills.
6. Reconcile actual Spot inventory and Futures positions for managed account scope.
7. Rebuild obligation residuals and in-flight reservations from durable facts.
8. Reconcile exits and reduce-only orders.
9. Reconcile fee/funding/PnL entries.
10. Mark each scope healthy, degraded, unknown, or recovery-required.
11. Open admission only for scopes whose required checks pass.

Unknown/malformed inventory or unsupported pagination is incomplete reconciliation, not an empty result.

### 12.4 Recovery states

Use SYNCED, PENDING_SYNC, DEGRADED, MISMATCH, UNKNOWN, RECOVERY_IN_PROGRESS, and RESOLVED. Preserve incident history and operator actions.

---

## 13. Exchange adapter and capability contract

All venue-specific behavior is isolated behind adapters. Business services must not contain scattered venue-name conditionals.

### Required adapter operations

Read operations, where supported:

- fetch market metadata/capabilities;
- fetch order book;
- fetch balance;
- fetch open orders;
- fetch exact order;
- fetch order by client ID;
- fetch trade history;
- fetch positions;
- fetch funding history/settlements;
- fetch account/position mode.

Mutation operations, only through mutation gateway:

- create order;
- cancel order;
- amend/replace order if supported;
- close/reduce position where venue semantics require a dedicated method.

### Capability profile

Per venue/account/market, explicitly declare:

- minimum amount and minimum notional/cost;
- amount unit (base or contract), amount step, precision, contract size;
- price tick and price precision;
- linear/inverse contract type;
- one-way/hedge position mode;
- reduce-only support and semantics;
- client order ID support, uniqueness scope, length and lookup support;
- private stream support, event types, sequence behavior and reconnect behavior;
- trade-history availability and pagination;
- order status/cancel semantics;
- rate-limit bucket and retry-after behavior;
- supported order types and time-in-force;
- maintenance/trading status.

Unknown capability fails closed for the operation that depends on it. Do not assume a capability merely because CCXT exposes a generic method.

Spot and Futures on the same exchange are separate capability profiles. MEXC Spot and MEXC Contract must not be conflated.

---

## 14. Risk admission, STOP, and de-risking

- Preserve existing approved risk limits and venue settings exactly.
- Admission validates account balance/margin, existing managed exposure, open-order caps, per-venue cooldown, pair limits, and global STOP.
- STOP blocks new exposure, but must not disable safe monitoring/reconciliation of existing positions.
- De-risking uses a dedicated fenced path and never bypasses durable intent.
- Emergency actions are bounded by verified exposure; unknown position is not permission to send an oversized close.
- If PostgreSQL, lease, fencing, config snapshot, or STOP state cannot be verified, block new entry.
- Do not automatically resume after confirmed IP block or critical STOP without documented operator resolution/acknowledgement.

---

## 15. Observability and operator interface

The executor publishes a read model; Telegram/dashboard must not infer state from transient Redis keys.

### Per-session timeline

Record admission/rejection, Spot intent/submission/ack/fill/cancel, obligation creation/reservation/allocation, scheduler wait reason and book reference, Futures intent/submission/fill/cancel, exit trigger and leg progress, reconciliation result, recovery/operator action, and final PnL/completion.

### Metrics

- active sessions by state;
- active Spot/Futures orders;
- Spot unhedged notional;
- pending hedge obligation notional and age;
- hedge in-flight notional;
- hedge completion latency;
- waiting-for-minimum and waiting-for-liquidity counts;
- unknown submissions;
- order/fill/position reconciliation mismatches;
- orphan order/position counts;
- partial exits;
- recovery backlog and age;
- realized pair PnL, fees, funding, and estimated liquidation PnL;
- per-venue request latency, rate-limit responses, and private-stream health.

Mutating operator commands must become durable commands with actor, authorization, idempotency, audit trail, and result status. Existing command semantics must be preserved.

---

## 16. File-level implementation map

This is a responsibility map, not a claim that all proposed files already exist. Confirm exact files, classes, functions, and call sites in Phase 0.

### 16.1 Existing modules

| File | Required change | Functions/responsibilities to inspect |
|---|---|---|
| executor/entry_hedge.py | Replace authoritative hedge accounting in transient order dictionaries with obligation service calls. | EntryHedgeMixin._handle_entry_fills; all helpers submitting Futures hedge or updating hedged_short_qty. |
| executor/fill_ledger.py | Enforce durable fill uniqueness, exact numeric values, atomic ledger/obligation updates, replay-safe attribution. | All insert/upsert, dedupe, cumulative-fill, fee and allocation functions. |
| executor/event_inbox.py | Persist normalized events; durable claim/process/retry; strengthen event identity and numeric validation. | EventNormalizer.normalize_order, normalize_trade, EventInbox.ingest_event, handler registration/dispatch. |
| executor/engine_entry_flow.py | Thin orchestration only; no embedded hedge/position truth. | Entry lifecycle methods and calls into submission/hedge/registry. |
| executor/engine_entry_submission.py | Use order intent service and mutation gateway; ensure all paths are fenced. | Spot create path (including _submit_entry_order if present), timeout/error handling, pending-order persistence. |
| executor/engine_recovery.py | Resume durable session artifacts; exact-ID recovery; no silent pending-key deletion. | _persist_reconciled_submit_order, _reconcile_durable_submit_attempts, startup recovery methods. |
| executor/exposure_reconciliation.py | Retain as compatibility wrapper if needed; delegate to multi-domain reconciliation. | ExposureReconciliationWorker.reconcile_once, balance parsing, active-position comparison and halt behavior. |
| executor/quantity.py | Standardize Decimal and base-equivalent conversion; validate amount spec and contract metadata. | AmountSpec, calculate_order_size, canonical_hedge_target, Futures sizing validation. |
| executor/global_stop.py and submit-fence modules | Ensure every mutation path uses one fencing/STOP authority. | STOP request/read, lease/fencing validation, submit/de-risk fence entry points. |
| executor/order_registry.py if present | Reuse as canonical durable order registry; strengthen unique keys, transitions and recovery queries. | Register/update/find-by-client-ID/list-unresolved methods. |
| executor/private_stream.py and venue stream adapters | Normalize source timestamps/sequences and reconnect state; stream is not canonical. | Subscription lifecycle, cache updates, reconnect/resubscribe and callbacks. |
| Exchange client/adapter modules | Standardize capability and order semantics; route mutations through gateway. | All create/cancel/close methods and venue-specific client resolution. |
| Existing exit modules (exact names to inventory) | Integrate independent leg exit state, verified terminal status and pair-level coordinator. | Every exit/force-close/rollback/reduce-only path. |
| Existing Telegram/operator modules | Read durable read model; persist operator mutations. | Pending orders, positions, balance, cancel, force-close, STOP and recovery commands. |

### 16.2 Proposed new modules (create only if no equivalent exists)

| Proposed path | Responsibility | Dependencies |
|---|---|---|
| executor/execution_coordinator.py | Admission and session lifecycle orchestration. | Session repository, risk gate, lease, outbox. |
| executor/session_manager.py | Session state transitions and version checks. | Execution repository. |
| executor/order_intent_service.py | Durable intent, idempotency, reservations and submission ambiguity. | Repository, fencing. |
| executor/exchange_mutation_gateway.py | Sole private order mutation path. | Adapter, intent service, STOP/fencing. |
| executor/spot_leg_manager.py | Spot order/fill/position lifecycle. | Gateway, fill ledger, repository. |
| executor/futures_leg_manager.py | Futures order/contract/position lifecycle. | Gateway, fill ledger, capability profile. |
| executor/hedge_obligation_manager.py | Obligation creation and residual accounting. | Fill ledger, repository. |
| executor/hedge_allocation.py | Allocation of aggregated Futures fills. | Obligation manager, fill ledger. |
| executor/liquidity_evaluator.py | Depth/VWAP/slippage/freshness evaluation. | Order-book adapter, amount spec. |
| executor/hedge_scheduler.py | Durable obligation scheduling and bounded waiting. | Obligation manager, liquidity evaluator, risk gate. |
| executor/pair_pnl_engine.py | Pair-level realized/unrealized/net liquidation PnL. | Fill ledger, fee/funding/conversion data. |
| executor/exit_coordinator.py | Two-leg exit orchestration and partial exit recovery. | Leg managers, PnL, gateway. |
| executor/reconciliation_engine.py | Order/fill/position/PnL reconciliation. | Adapters, repositories, ledger. |
| executor/recovery_coordinator.py | Startup/event-driven/periodic recovery. | Reconciliation, session repository, leases. |
| executor/risk_admission.py | Admission and unhedged exposure budgets. | Runtime config, positions, obligations. |
| executor/execution_repository.py | Transactional repository interfaces/implementation. | PostgreSQL driver/schema. |
| executor/execution_read_model.py | Operator/dashboard read projections. | Durable session/order/position state. |

### Code hygiene

- Keep modules cohesive; do not create one giant engine class.
- Avoid cyclic imports; use narrow interfaces and dependency injection.
- Keep exchange-specific conditionals inside adapters/capability profiles.
- Keep business logic independent from Redis and Telegram.
- Use typed models and Decimal fields for financial values.
- Use stable reason codes rather than scattered error-string matching.
- Every public service method defines idempotency, transaction boundary, and failure semantics.
- Disable old order-write paths after cutover; never leave dual writers active.

---

## 17. Implementation phases and pull-request plan

Do not merge a single massive rebuild. Each phase has a reviewable diff and explicit acceptance evidence.

### Phase 0 — Source inventory and contract freeze

Deliverables:
- complete executor file/function/call-site inventory;
- scanner-to-executor input contract snapshot;
- existing DB/Redis schema and migration inventory;
- exchange mutation call graph;
- existing tests and CI workflow inventory;
- current runtime config and protected-field diff baseline;
- V2 open checklist mapped to V3 work items.

Acceptance:
- every create/cancel/close call site is identified;
- every persistent state owner is identified;
- no duplicate writer path is omitted;
- scanner contract is documented and frozen;
- no trading code changes in this phase.

### Phase 1 / PR 1 — Durable execution core

Deliverables: session model/state machine, repository interfaces, PostgreSQL transactions, session/version/lease/fencing contract, transactional outbox and durable command pattern, schema migration and backfill plan.

Tests: atomic session creation, duplicate admission, concurrent state transition, stale fencing token, lease takeover, database failure/ambiguous commit, active-session migration.

Acceptance: sessions survive restart without Redis; two coordinators cannot own/mutate the same scope; existing positions are not discarded/reset.

### Phase 2 / PR 2 — Order intent and mutation gateway

Deliverables: durable intent service, stable idempotency/client IDs, single mutation gateway, create/cancel/replace classification, STOP/fencing on every mutation path.

Tests: lost create ACK with/without accepted order, duplicate command replay, cancel ACK while still open, cancel/fill race, two workers submitting same intent, STOP racing submit.

Acceptance: no blind retry on unknown submission; no private order write bypasses gateway; every mutation has durable intent/audit record.

### Phase 3 / PR 3 — Independent legs and fill ledger

Deliverables: independent leg state, durable inbox, immutable fill ledger, venue identity normalization, exact quantity and fee representation.

Tests: duplicate/out-of-order events, cumulative fill mismatch, late fill after cancel, partial fill/restart, trade-history pagination, fee currency normalization.

Acceptance: every confirmed fill stored exactly once; leg state reconstructible; float conversion cannot alter financial decisions.

### Phase 4 / PR 4 — Hedge obligation and aggregation

Deliverables: durable obligation manager, confirmed/in-flight/residual calculation, minimum checks, safe aggregation/allocation, unhedged exposure budgets.

Tests: Spot $3 obligation below Futures $5; accumulation to $5; compatible $3+$2 aggregation; incompatible obligations rejected; concurrent reservation race; Futures partial fill; rejected/unknown aggregate order; late Spot fill after cancel.

Acceptance: no duplicate hedge; no default overhedge; every aggregate fill allocated to source sessions; no obligation disappears on terminal Spot order or restart.

### Phase 5 / PR 5 — Liquidity-aware scheduler

Deliverables: depth/VWAP/slippage evaluator, freshness/sequence validation, durable wait/retry schedule, risk deadline/breach policy, event-driven wake-up and bounded fallback.

Tests: depth $3 for $5 hedge; depth increases; stale/gapped/crossed book; price exceeds slippage; venue rate limit; risk deadline with insufficient liquidity; scheduler restart.

Acceptance: temporary low liquidity causes bounded waiting, not immediate blind unwind; no unbounded waiting; no hedge from stale/insufficient depth; risk breach creates explicit recovery incident.

### Phase 6 / PR 6 — Pair PnL and exit coordinator

Deliverables: pair PnL/net liquidation valuation, fee/funding/conversion attribution, independent two-leg exit, partial-exit/emergency recovery.

Tests: one leg closes and other fails; partial fills on exit; reduce-only semantics; duplicate funding settlement; late fee update; PnL market update without execution-state mutation.

Acceptance: no completion with residual exposure; PnL reproducible from ledger; exit uses actual fills and executable liquidation estimates.

### Phase 7 / PR 7 — Reconciliation and recovery

Deliverables: four reconciliation domains, startup recovery gate, periodic sweep, WebSocket reconnect recovery, orphan handling, incident/operator-resolution workflow.

Tests: restart at every lifecycle state; venue order absent locally; local order with venue history unavailable; unknown inventory; pagination failure; DB outage/recovery; stream replay; position mismatch.

Acceptance: unknown state blocks conflicting new exposure; recovery is idempotent/resumable; no order/position silently discarded.

### Phase 8 / PR 8 — Integration and certification

Deliverables: scanner contract integration with scanner unchanged, Redis outbox/dispatch integration, Telegram/dashboard read model, venue certification, shadow mode, fault injection, soak test and runbook.

Acceptance: all venue profiles and global regression contracts pass; no unapproved strategy/risk/config changes; explicit LIVE approval follows evidence review.

---

## 18. Test strategy and quality gates

### Required test layers

1. Unit tests for formulas, Decimal arithmetic, state transitions and capability checks.
2. Property-based tests for fill ordering, duplicate delivery, hedge residual invariants and allocation conservation.
3. Concurrency tests for reservation, ownership, duplicate commands and multi-worker scheduling.
4. PostgreSQL integration tests for unique constraints, transaction atomicity, outbox, lease and fencing.
5. Multi-process tests using independent executor processes against the same PostgreSQL instance.
6. Adapter contract tests for each venue and market type.
7. Replay tests using redacted real event sequences.
8. Fault injection for timeouts, lost ACKs, disconnects, restarts, database outages and rate limits.
9. Soak tests for long-running sessions/events/queues.
10. Shadow execution tests that calculate intended actions without sending trading mutations.

### Mandatory invariants to assert

- Sum of allocations to a Futures fill never exceeds its quantity.
- Confirmed hedge plus reserved in-flight hedge never exceeds permitted target plus explicitly approved tolerance.
- Every confirmed Spot fill maps to exactly one obligation contribution.
- Every confirmed exchange trade maps to one unique fill ledger row.
- Duplicate event replay leaves all financial totals unchanged.
- Stale fencing token cannot mutate exchange or durable state.
- Session cannot complete with nonzero exposure or unresolved orders/obligations.
- Market-data update cannot mutate order/fill state.
- Failed Redis publish does not lose a committed command.
- Database failure cannot produce untracked new exposure.

### CI gates

Preserve existing CI and add relevant checks:

- Python unit/integration tests;
- existing Go scanner tests unchanged;
- Python type/lint/static checks as configured;
- Go tests/race tests for Go integration code only when touched;
- SQL migration validation and rollback tests;
- PostgreSQL multi-process integration suite;
- critical state-transition/recovery coverage;
- existing dependency/security checks.

Do not weaken or skip existing tests to make V3 green.

---

## 19. Rollout, migration, and rollback

### No destructive cutover

- Inventory existing active orders, positions, pending intents and Redis records before migration.
- Do not delete legacy state during deployment.
- Backfill legacy records with source/provenance and reconciliation status.
- Unknown legacy records remain blocked for new exposure until resolved.
- Do not run old and new writers simultaneously for the same venue/account/operation.
- Define one authoritative writer per scope.

### Staged rollout

1. Offline tests and replay.
2. Shadow-read existing live state and compare V3 projections.
3. Shadow decisions without order mutations.
4. Controlled test account or venue sandbox where available.
5. Small explicitly approved canary after all safety gates pass.
6. Expand only after reconciliation, latency, error and PnL evidence is reviewed.

This blueprint does not authorize LIVE deployment or changes in capital allocation.

### Rollback

Rollback preserves durable intents, fills, obligations and positions. Never switch the old executor back on while V3 has in-flight or unknown orders.

1. Close admission for affected scope.
2. Reconcile all in-flight and unknown orders.
3. Verify positions and obligations.
4. Stop V3 writers with fencing.
5. Transfer ownership under a new fencing generation.
6. Enable prior writer only if its state is reconciled and it cannot duplicate V3 actions.
7. Preserve V3 audit records.

---

## 20. Definition of done and LIVE certification gate

- [ ] Scanner contract preserved and verified without scanner logic changes.
- [ ] All order mutations pass through durable mutation gateway.
- [ ] All confirmed fills are durable and idempotent.
- [ ] Hedge obligations are durable and derived from confirmed Spot fills.
- [ ] In-flight reservation prevents duplicate hedge.
- [ ] Aggregated fills are allocated correctly to originating sessions.
- [ ] Internal minimums and actual exchange minimums are enforced.
- [ ] Liquidity waiting is bounded by explicit risk budgets.
- [ ] Unknown submission/cancel states are reconciled before retry.
- [ ] Both leg lifecycles survive restart at every tested transition.
- [ ] Pair PnL is attributable to actual fills, fees, funding and conversion.
- [ ] Exit cannot complete with open exposure or unresolved orders.
- [ ] Startup and periodic reconciliation pass for every enabled venue/account scope.
- [ ] Multi-process fencing and concurrency tests pass.
- [ ] Telegram/dashboard read models reflect durable state.
- [ ] Relevant open V2 checklist items are closed with test evidence.
- [ ] No protected risk, sizing, leverage, fee or venue configuration changed without explicit approval.
- [ ] Runbook, incident handling, rollback and venue capability matrix are reviewed.
- [ ] Explicit human approval is recorded before enabling LIVE.

Passing tests is necessary but insufficient. LIVE certification also requires venue-specific behavior evidence and operational review.

---

## 21. Required implementation artifacts

Maintain these documents:

1. docs/NAFAZ-EXECUTOR-V3-IMPLEMENTATION-BLUEPRINT.md — this plan.
2. docs/NAFAZ-EXECUTOR-V3-SOURCE-INVENTORY.md — exact files, classes, functions, call sites and current ownership.
3. docs/NAFAZ-EXECUTOR-V3-DATA-MODEL.md — approved schema, constraints, migrations and backfill.
4. docs/NAFAZ-EXECUTOR-V3-STATE-MACHINE.md — session/leg/obligation/order transitions.
5. docs/NAFAZ-EXECUTOR-V3-EXCHANGE-CAPABILITIES.md — per-venue/market capability matrix.
6. docs/NAFAZ-EXECUTOR-V3-TEST-MATRIX.md — test IDs, fixtures, expected results and evidence.
7. docs/NAFAZ-EXECUTOR-V3-OPERATIONS-RUNBOOK.md — startup, recovery, STOP, incident, rollback and certification procedures.

Do not begin broad code changes until artifacts 2–4 are reviewed against the actual branch and existing database schema.

---

## 22. Immediate next actions

1. Re-fetch target branch HEAD and confirm it has not moved.
2. Generate complete source inventory and order-mutation call graph.
3. Map every current executor table/Redis key to the proposed durable model.
4. Identify existing tests and exact V2 checklist items still open.
5. Resolve schema and state-machine design before creating migrations.
6. Approve PR 1 scope and acceptance tests.
7. Implement one vertical slice at a time; do not change scanner behavior.

**Final rule:** V3 is not successful because it submits more orders or opens more positions. It is successful when every accepted execution can be explained, reconstructed, hedged, exited, reconciled, and accounted for without duplicate exposure or lost state.


---

# Enterprise Scale Architecture Addendum — 2026-11-21

This addendum extends the original blueprint after inspecting the current branch source. It reuses existing V2 components and does not authorize a code rewrite or imply LIVE certification.

## 1. Target operating model

NAFAZ V3 is a modular execution platform with a logical Control Plane and scalable execution workers. It is not an early microservice migration.

- One active logical coordinator and one standby coordinator.
- Multiple session workers may process independent sessions concurrently.
- Mutations are serialized by venue account, market/instrument and ownership scope, not by one global queue.
- PostgreSQL is canonical for session, intent, order, fill, hedge, position, exit, risk and audit state.
- Redis is disposable acceleration: cache, wake-up, notification and ephemeral queue.
- Private WebSocket is a low-latency event source; REST/history reconciliation remains mandatory.
- Go scanner and its opportunity contract are frozen.

## 2. Logical architecture

    GO SCANNER (FROZEN)
      opportunity payload / source opportunity ID
                  |
                  v
    OPPORTUNITY ADMISSION GATE
      freshness / pair identity / venue capability / balance / fees / limits
                  |
                  v
    EXECUTION CONTROL PLANE
      session coordinator | ownership + fencing | policy snapshots
      independent risk control | durable command journal
                  |
                  v
    EXECUTION DOMAIN
      Spot leg | Futures leg | Fill ledger | Hedge obligations
      pooled hedge allocator | liquidity scheduler
      position accounting | PnL | Exit Decision Engine
      Exit Coordinator | recovery + reconciliation
                  |
                  v
    MUTATION GATEWAY (ONLY PRIVATE WRITE BOUNDARY)
                  |
                  v
    TOKOCRYPTO SPOT | INDODAX SPOT | OKX SWAP | MEXC CONTRACT
                  |
                  v
    POSTGRESQL CANONICAL STATE
      Redis cache/notifications are rebuildable projections

Observability and incident management consume durable lifecycle events and expose read-only projections. Operator mutations enter through a durable command journal, not direct dashboard writes.

## 3. Ownership and delivery guarantees

1. One authoritative writer owns a session transition at a time.
2. One mutation owner is authorized for a venue account/instrument scope at a time.
3. Every write carries session ID, intent ID, stable client order ID, fencing generation, policy version and idempotency key.
4. Mutation Gateway validates current fencing at the write boundary.
5. A stale worker may continue running but cannot mutate after losing ownership.
6. Risk-reducing operations remain possible under STOP. If PostgreSQL serialization is unavailable, use a designated emergency owner, stable client ID and immediate reconciliation; do not permit uncontrolled multi-worker emergency writes.
7. Do not promise exactly-once exchange delivery. The contract is durable intent, at-least-once event delivery, idempotent processing and exact-ID reconciliation.

## 4. Independent Risk Control Plane

Risk control is a separate logical authority from strategy and execution orchestration. It can deny new exposure while allowing recovery, hedge repair and protective exit.

### Risk dimensions

- Global portfolio gross/net exposure and deployed capital.
- Venue/account exposure and available balance.
- Pair/instrument exposure.
- Per-session unhedged notional and duration.
- Outstanding entry/hedge/exit order count and notional.
- Daily realized loss and drawdown.
- Slippage budget and execution quality.
- Funding cost and projected holding cost.
- Market-data freshness and exchange health.
- Database/fencing health and reconciliation completeness.

### Risk decisions

ALLOW_ENTRY, DEFER_ENTRY, BLOCK_ENTRY, ALLOW_HEDGE, ALLOW_EXIT, REQUIRE_DE_RISK, HALT_SCOPE and REQUIRE_OPERATOR.

Risk control fails closed for new exposure when canonical state, balances, policy, market data or ownership cannot be verified. It must not disable safe risk-reducing operations merely because global entry STOP is active.

Persist each decision's input snapshot, result, reason code and timestamp. Risk limits remain policy inputs; do not hard-code new production values in architecture work.

## 5. Immutable policy snapshots

Each session stores immutable versions or snapshots for:

- strategy parameters and hedge ratio;
- Spot/Futures minimum notional policy;
- exchange market metadata version;
- risk limits and account scope;
- maker/taker fee schedule and verification state;
- exit target, stop, trailing and time-decay policy;
- FX rate/source/time and reporting currency;
- position mode, leverage and contract multiplier.

Existing internal defaults remain USD 10 equivalent minimum Spot, USD 5 equivalent minimum Futures hedge, 1:1 base-equivalent hedge ratio and no overhedge by default. Stricter exchange minimums and precision rules always win. This is a policy design statement, not a LIVE configuration change.

## 6. Exit Decision and Position Lifecycle — mandatory core domain

The repository already contains:
- executor/exit_decision_engine.py
- executor/pair_pnl_engine.py
- executor/exit_pricing.py
- executor/exit_orders.py
- executor/exit_execution.py
- executor/force_close.py and related leg/lifecycle modules

V3 must extend this path, not create a second exit engine.

### Separation of responsibilities

- Position Monitor: builds a fresh, verified position and market snapshot.
- Pair PnL Engine: computes estimated net liquidation economics for the complete pair.
- Exit Decision Engine: pure policy evaluator; returns action and reason; never calls an exchange.
- Exit Coordinator: owns exit session, quantity reservation, per-leg progress and state transitions.
- Mutation Gateway: submits cancel/close/rollback writes.
- Exit Reconciliation: verifies terminal orders, fills, allocated hedge and actual exchange exposure before CLOSED.

### Required decision inputs

- Verified Spot quantity and cost basis.
- Verified Futures base-equivalent quantity and contract metadata.
- Executable Spot bid VWAP and Futures ask VWAP for intended close quantity.
- Entry and expected exit fees, actual fee currency and verification status.
- Accrued/estimated funding and funding settlement provenance.
- FX conversion rates with source and timestamp.
- Deployed capital and actual Futures margin.
- Executable entry/exit spread and spread convergence.
- Position age, policy snapshot and durable trailing high-water mark.
- Book freshness, sequence validity, depth, slippage and venue health.
- Hedge drift, pending orders and unknown reconciliation state.

If a critical input is unknown or stale, do not treat it as zero cost or a healthy/flat state. Return WAIT_FOR_RECONCILIATION or a risk-directed action based on verified exposure.

### PnL and ROI contract

Primary profit trigger is estimated net liquidation PnL in one reporting currency:

    NetLiquidationPnL =
      SpotLiquidationPnL + FuturesLiquidationPnL + NetFunding
      - EntryFees - EstimatedExitFees - Taxes
      - ConversionCosts - OtherKnownCosts

Spot close uses executable bid depth. Closing a Futures short uses executable ask depth. Estimated liquidation PnL must be distinguished from realized PnL.

Report at least two ROI measures:
- Strategy/spread ROI against an explicitly defined Spot notional basis.
- Return on total deployed capital, including Spot capital, actual Futures margin and allocated fee reserve.

Never calculate pair ROI using Futures margin alone. Do not mix a margin-only ROI with full-pair PnL.

### Exit trigger priority

1. Emergency risk: verified exposure mismatch, unhedged risk-budget breach, inability to protect exposure, or critical state uncertainty.
2. Protective stop: net liquidation loss reaches session stop policy or strategy thesis is invalidated.
3. Profit target: net liquidation PnL and/or deployed-capital ROI reaches configured threshold after exit costs and safety buffer.
4. Trailing profit: activate after configured profit threshold; persist high-water mark and exit when peak-to-current retracement exceeds configured bound.
5. Spread convergence/edge decay: executable close spread reaches target or remaining edge no longer covers cost and risk.
6. Funding/time stop: funding drag or time-decayed required return makes continued holding inferior.
7. Otherwise HOLD.

Thresholds are pair/policy inputs, not universal hard-coded percentages. A profit target must exceed estimated full exit costs plus a safety buffer.

### Partial and two-leg exit

Partial exit is a quantity operation, not a boolean. The coordinator reserves close quantity against verified exposure and allocated pooled hedge. It must:
- reduce Spot and Futures consistently with hedge ratio and contract size;
- honor reduceOnly and position-side semantics for Futures;
- preserve remaining session exposure;
- allocate pooled hedge reduction only to the originating session;
- recalculate residual hedge after each confirmed exit fill;
- survive cancel/fill races and worker restarts.

Session is CLOSED only when managed Spot/Futures exposure is verified zero, relevant orders are terminal, no hedge obligation/allocation remains, and final accounting is reconciled.

### Exit state model

    MONITORING
      -> EXIT_DECIDED
      -> EXIT_INTENT_DURABLE
      -> EXIT_SUBMITTING
      -> EXIT_SUBMISSION_UNKNOWN / EXIT_WORKING
      -> EXIT_PARTIAL
      -> EXIT_RECONCILING
      -> CLOSED

Any ambiguous result moves to reconciliation, not blind retry. Emergency de-risk may close one leg before the other, but residual exposure remains an active risk incident until flat.

## 7. Pooled hedge allocation accounting

The existing hedge claim table protects one parent Spot order/fill generation. It does not by itself prove cross-session pooled hedge allocation.

Aggregation is allowed only when Futures account, venue/contract, position mode, side, hedge ratio, quote/settlement asset, risk policy and lifecycle semantics are compatible.

For each pool:
- source obligations remain individually durable;
- pooled Futures order has its own intent/order/fill identity;
- every fill is allocated deterministically to source obligations;
- allocated quantity plus unallocated residual equals verified Futures fill quantity;
- fee, funding and realized PnL are apportioned by documented deterministic rules;
- a session may exit only its allocated share;
- adjustments and orphan fills remain auditable.

Never aggregate incompatible accounts, contracts, margin modes, sides or policy snapshots.

## 8. Exchange failure isolation

Each venue/account/endpoint has explicit health, rate budget, circuit breaker and capability state. An outage must not stop unrelated venues, but sessions depending on the failed venue for hedge/exit are escalated to Risk Control.

Unknown or incomplete endpoint inventory is never interpreted as empty. No blind create-order retry. Recovery first uses exact client ID, exchange order ID and complete venue history.

## 9. Persistence and accounting authority

- PostgreSQL native transactions: canonical session/order intent/fencing/claims/fill/position/exit/risk/audit state.
- Redis: cache, notification and wake-up only.
- SQLite outbox: durable delivery of non-critical reporting projections; not a substitute for canonical fills or positions.
- Supabase REST: reporting/read projection where applicable; never a competing writer for the same canonical row.

Critical canonical write failure blocks new exposure. Protective de-risk remains available under explicit emergency ownership/fencing policy. A fill that cannot be persisted creates an accounting-debt incident and triggers reconciliation; it is not silently ignored.

## 10. Enterprise operations and disaster recovery

Required capabilities:
- Correlation ID from opportunity through final PnL.
- Metrics for submit latency, fill-to-hedge latency, hedge backlog/age, unhedged notional, exit latency, unknown orders, reconciliation drift, fee/funding drift, fencing rejects and exchange circuit state.
- Structured session/order event timeline.
- Durable operator commands and audited manual resolution.
- Database backup/PITR and restore drills.
- Secret isolation and credential rotation.
- Incident severity, owner, recovery action and resolution.
- Dashboard as read-only projection; mutations go through durable commands.

## 11. Updated delivery gates

Follow NAFAZ-EXECUTOR-V3-IMPLEMENTATION-ROADMAP.md. No implementation starts before PR-00 inventory and mutation-path mapping are accepted. No LIVE cutover occurs without end-to-end failure injection, multi-process fencing tests, reconciliation proof, operational runbooks and explicit operator approval.

## 12. Companion documents

- docs/NAFAZ-EXECUTOR-V3-SOURCE-INVENTORY.md — verified existing files, functions, schema, tests and reuse/rebuild decisions.
- docs/NAFAZ-EXECUTOR-V3-IMPLEMENTATION-ROADMAP.md — PR dependencies, source ownership, tasks, acceptance and release gates.


## PR-00 audit status

The detailed static audit and mutation-path map are recorded in `docs/NAFAZ-EXECUTOR-V3-PR00-AUDIT-REPORT.md`. Do not begin PR-01 until the two P0 findings in that report have an agreed remediation and regression coverage. The report does not certify production DB state or LIVE CEX behavior.
