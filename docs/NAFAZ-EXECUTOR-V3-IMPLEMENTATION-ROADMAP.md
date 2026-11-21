# NAFAZ Executor V3 — Enterprise Implementation Roadmap

Repository: nzf210/nafaz_arb  
Target branch: feat/executor-v3-enterprise  
Planning baseline: 32d0fd75d2de31f5737975abf0c3d837a7cf8699  
Status: PROPOSED — no implementation authorization implied  
Scope: Executor V3 only; scanner behavior is frozen.

## 1. Delivery strategy

Use a controlled rebuild, not a blind rewrite and not an early microservice split.

- Preserve the Go scanner and existing opportunity contract.
- Preserve proven exchange adapter behavior and regression contracts.
- Reuse V2 modules when they pass V3 acceptance tests.
- Replace weak orchestration and transient correctness state only where evidence requires it.
- PostgreSQL is canonical execution state; Redis is cache, notification and wake-up.
- All private exchange writes use durable intent and one mutation gateway.
- No LIVE cutover is part of this roadmap. It requires separate certification and operator approval.

A PR is complete only when source changes, migrations (if any), tests, telemetry, rollback behavior and evidence are complete.

## 2. Dependency graph

    PR-00 Inventory and contract freeze
       |
       v
    PR-01 Durable domain/state and schema gap analysis
       |
       +----> PR-02 Ownership/fencing and Risk Control Plane
       |                         |
       v                         v
    PR-03 Mutation Gateway and exchange isolation
       |
       v
    PR-04 Fill ledger and independent leg lifecycle
       |
       v
    PR-05 Hedge obligations and pooled allocation
       |
       v
    PR-06 Liquidity scheduler and bounded waiting
       |
       +-------------------------------+
       v                               v
    PR-07 Position/PnL             PR-08 Exit decision/coordinator
       |                               |
       +---------------+---------------+
                       v
                 PR-09 Reconciliation/recovery
                       |
                       v
                 PR-10 Observability/DR/operations
                       |
                       v
                 PR-11 End-to-end certification

PR-07 and PR-08 may be developed in parallel after PR-04 ledger contracts are stable. They must integrate before PR-09 certification.

## 3. PR-00 — Source inventory and contract freeze

**Purpose:** documentation and read-only audit only.

**Primary artifact:** NAFAZ-EXECUTOR-V3-SOURCE-INVENTORY.md.

Tasks:
- Confirm current branch HEAD and exact source tree.
- Trace opportunity → session → order intent → exchange order → fill → hedge → position → exit → final PnL.
- Trace every create/cancel/replace/close API call and identify its MutationGateway path.
- Map Redis keys, PostgreSQL tables and reporting outbox to one authority each.
- Inspect V2 architecture, STOP/fencing checklist, regression contract and migration history.
- Record existing test/CI baseline and open defects.
- Freeze scanner payload and behavior contract.

Acceptance:
- Each order mutation has one named owner.
- Each durable table has one authoritative writer.
- Existing regression suite baseline is recorded.
- Scanner contract is frozen.
- No trading code, migration, runtime config or production resource changed.

## 4. PR-01 — Durable execution domain and session state

**Reuse:** executor/engine.py, engine_entry_flow.py, engine_order_intents.py, models.py, position_models.py, existing V2 core schema.

Tasks:
- Define durable identities for opportunity, session, leg, account, venue market, intent, order and fill.
- Add monotonic state version/optimistic concurrency where absent.
- Pin immutable strategy, execution, risk and exit policy snapshots per session.
- Preserve local Spot venue/native symbol/quote and foreign Futures venue/native contract identities.
- Derive session state from actual legs, orders, fills, obligations and exposure.
- Keep Decimal quantities and typed quote/base/notional/margin values internally.
- Extend existing session/leg schema only after migration and constraint audit; do not duplicate tables.

Acceptance:
- Session/leg state reconstructs after restart.
- Duplicate opportunity cannot create duplicate session under the defined idempotency scope.
- Stale state version cannot overwrite newer state.
- Pair display label is never the sole market identity.

## 5. PR-02 — Ownership, fencing and Risk Control Plane

**Reuse:** executor/global_stop.py, derisk_fence.py, risk.py, risk_state.py, postgres_risk.py, order_slots.py, hedge_claims.py.

Tasks:
- Define mutation ownership scope: credential/account + venue + market/instrument.
- Use monotonically increasing fencing generations checked at mutation boundary.
- Keep one active logical coordinator and standby; permit independent sessions concurrently.
- Define global, venue/account, pair and session risk budgets.
- Cover gross/net exposure, deployed capital, unhedged notional/duration, open order count, daily loss, drawdown, slippage, funding, data age and venue health.
- Separate ENTRY_BLOCKED from DE_RISK_ALLOWED.
- Restore durable risk state before new-entry readiness.
- Resolve DB-unavailable de-risk fallback with designated emergency ownership, stable client ID and immediate reconciliation.

Acceptance:
- Two workers cannot mutate the same scope concurrently.
- Stale worker is rejected after ownership transfer.
- STOP blocks new exposure but permits safe protective actions.
- Risk persistence failure blocks new exposure.
- Multi-process STOP-vs-submit and fencing stress tests pass.

## 6. PR-03 — Mutation Gateway and exchange isolation

**Reuse:** executor/mutation_gateway.py, exchange_client.py, exchange_orders.py, exchange_capabilities.py, order_registry.py, engine_entry_submission.py, cancel.py.

Tasks:
- Route entry, hedge, cancel, replace, reduce-only close, rollback and force-close through one private mutation contract.
- Persist intent before exchange request; bind stable client ID, intent ID and fencing generation.
- Distinguish definitive rejection from timeout/unknown.
- Reconcile ambiguous create by exact client ID and venue history before retry.
- Add per-venue/account/endpoint rate budget, circuit breaker, deadlines, Retry-After and bounded backoff.
- Isolate unrelated venue failures.
- Define supported/unsupported/incomplete/degraded/unknown endpoint capability states.
- Preserve venue-specific post-only, reduceOnly, position mode, contract size, precision and client-ID semantics.

Acceptance:
- Static call-site scan finds no unapproved direct private mutation.
- Timeout after venue acceptance never produces blind duplicate create.
- Cancel/fill race is reconciled before replacement.
- One venue throttle does not pause unrelated venues.
- Unknown inventory blocks new exposure in the affected scope.

## 7. PR-04 — Fill ledger and independent leg lifecycle

**Reuse:** executor/fill_ledger.py, entry_hedge.py, leg_managers.py, trade_fill_reconciliation.py, order_sync_worker.py, existing fill-ledger migration.

Tasks:
- Use venue trade identity + venue/account/market as fill idempotency key.
- Keep ledger append-only; corrections are adjustment rows with reason/source.
- Persist fee amount/currency/verification, FX source/time, native and base-equivalent quantity.
- Keep Spot and Futures legs independent.
- Reconcile order cumulative fill against individual verified fills.
- Treat critical fill-ledger write failure as durable accounting debt, block new exposure in that scope, and keep protective position management available.
- Make in-memory leg managers rebuildable projections.

Acceptance:
- Duplicate/out-of-order fills do not double count.
- Partial fills survive restart/replay.
- Missing fill history is UNKNOWN, not zero.
- Fee currency and FX provenance survive recovery.
- No open position depends only on in-memory state.

## 8. PR-05 — Hedge obligations and pooled allocation

**Reuse:** executor/entry_hedge.py, hedge_claims.py, quantity.py, leg_managers.py and existing hedge claim schema.

Locked business policy:
- Internal Spot minimum: USD 10 equivalent.
- Internal Futures minimum hedge: USD 5 equivalent.
- Stricter venue min notional/amount, lot step, precision and contract multiplier take precedence.
- Default hedge ratio: 1:1 base-equivalent.
- Overhedge prohibited by default.
- Obligation is based on confirmed Spot fills; completion on confirmed Futures fills.
- In-flight reservation is subtracted from residual.

Tasks:
- Persist each confirmed Spot fill delta as hedge obligation.
- Track target, confirmed, reserved, deferred, failed and remaining quantities.
- Keep current per-parent fill-generation CAS claim.
- Add pool compatibility key: account, Futures venue/contract, position mode, side, quote/settlement asset, hedge ratio and risk policy.
- Add deterministic allocation of pooled Futures fills back to source sessions.
- Attribute fee, funding, slippage and exit to source session.
- Ensure one session's exit cannot close hedge allocated to another session.

Acceptance:
- No duplicate claim for one fill generation.
- Aggregate hedge does not exceed confirmed obligations less reservations.
- Allocation sums reconcile to verified Futures fills.
- Incompatible sessions never share a hedge pool.
- One session can exit independently of other pool members.

## 9. PR-06 — Liquidity scheduler and bounded waiting

**Reuse:** executor/market_data_gate.py, exit_pricing.py, adaptive_maker.py and venue order-book adapters.

Tasks:
- Validate freshness, sequence/checksum, depth, executable VWAP, fees and slippage.
- Defer hedge if depth is insufficient; e.g. required minimum $5 while executable depth is $3.
- Persist deferred obligation and wake on fill, book update, timer, recovery or risk event.
- Bound waiting by unhedged notional, duration, adverse movement, funding and session deadline.
- Execute documented de-risk policy when wait budget is exhausted.
- Respect venue request budget; no unbounded polling.

Acceptance:
- Insufficient depth creates durable deferred work, not lost state.
- Scheduler restart restores obligations.
- Stale/gapped book cannot authorize order.
- Expiry invokes deterministic risk action.
- No rate-limit hot loop.

## 10. PR-07 — Position accounting and PnL

**Reuse:** executor/pair_pnl_engine.py, exit_execution.py, fee_engine.py, fill_ledger.py, financial_reconciliation.py.

Tasks:
- Separate realized, unrealized, estimated liquidation and funding PnL.
- Spot liquidation uses executable bid depth; Futures short close uses executable ask depth.
- Include entry/exit fees, fee currency conversion, funding, tax, FX provenance, slippage and exit reserve.
- Report strategy ROI and return on total deployed capital separately.
- Never use Futures margin alone as pair ROI denominator.
- Derive realized PnL from verified fills and ledger adjustments.
- Unknown/stale fee, FX, funding or depth must be marked unknown/low-confidence, not silently zero.
- Attribute expected-vs-realized difference by fees, slippage, funding and holding time.

Acceptance:
- PnL reproducible from fill ledger and policy snapshot.
- Partial exit PnL allocated only to closed quantity.
- Estimated liquidation PnL is not mislabeled as realized.
- Reconciliation identifies fee/funding/FX drift.

## 11. PR-08 — Exit Decision Engine and Exit Coordinator

**Reuse:** executor/exit_decision_engine.py, pair_pnl_engine.py, exit_orders.py, exit_pricing.py, exit_execution.py, force_close modules and derisk_fence.py.

Tasks:
- Extend the existing ExitDecisionEngine; no duplicate decision engine.
- Pin exit policy version per session.
- Evaluate net liquidation PnL, ROI on deployed capital, executable spread convergence, trailing peak/drawdown, stop loss, funding drag, time decay, liquidity and hedge drift.
- Emergency and protective risk outrank profit targets.
- Persist trailing high-water mark and trigger state; restore after restart.
- Return explicit actions: HOLD, CANCEL_REMAINING_ENTRY, REPAIR_HEDGE, PARTIAL_EXIT, FULL_EXIT, EMERGENCY_DE_RISK, WAIT_FOR_RECONCILIATION.
- Decision engine is pure and never sends exchange orders.
- Exit Coordinator reserves exact quantities, honors reduceOnly, routes mutations through gateway and tracks legs independently.
- Exit completion requires all orders terminal, Spot/Futures exposure verified flat and pooled allocations released/reconciled.

Exit trigger hierarchy:
1. Emergency risk: exposure mismatch, unhedged breach, inability to protect exposure, critical state uncertainty.
2. Protective stop: net liquidation loss or strategy invalidation.
3. Profit target: combined net liquidation PnL and/or deployed-capital ROI after exit costs and safety buffer.
4. Trailing profit: activate at policy threshold; exit on durable peak-to-current retracement.
5. Spread convergence/edge decay: executable close spread reaches target or remaining edge no longer covers costs and risk.
6. Funding/time stop: holding cost or time decay makes further holding inferior.
7. Otherwise HOLD.

Targets are pair/policy inputs, not universal hard-coded percentages. A target must exceed estimated full exit costs plus a safety buffer.

Acceptance:
- Combined pair economics, not single-leg PnL, trigger profit.
- Emergency/stop outrank take profit.
- Trailing state survives restart and stale snapshots cannot move the peak backward.
- Partial exit preserves hedge allocation.
- Exit timeout cannot create duplicate close.
- CLOSED requires verified zero managed exposure and terminal order inventory.

## 12. PR-09 — Reconciliation and resumable recovery

**Reuse:** engine_recovery.py, order_recovery.py, recovery_*.py, exposure_reconciliation.py, trade_fill_reconciliation.py, financial_reconciliation.py.

Tasks:
- Reconcile four domains independently: orders, fills, positions/balances, PnL/fees/funding.
- Persist scope, cursor, time watermarks and pagination completeness.
- Unsupported/incomplete venue history is UNKNOWN, not empty.
- Adopt orphan orders only after exact identity/ownership verification.
- Startup sequence: DB/fencing → risk restore → unresolved submit recovery → order inventory → fill replay → positions → hedge obligations → exit monitor → reconciliation → new-entry readiness.
- Prevent periodic sweeps from racing active mutations.
- Scanner downtime must not stop management of open positions.

Acceptance:
- Crash at every mutation boundary resumes safely or halts explicitly.
- Unknown inventory blocks new exposure.
- Orphans are surfaced, not discarded.
- Position never closes while exchange exposure or allocated hedge remains.
- Recovery is repeatable and idempotent.

## 13. PR-10 — Observability, incident response and disaster recovery

**Reuse:** engine_status.py, system_readiness.py, supabase_outbox.py, Telegram status/order/position commands and deployment files.

Tasks:
- Correlation ID from opportunity to final PnL.
- Metrics: submit latency, fill-to-hedge latency, hedge backlog/age, unhedged notional, exit latency, unknown orders, reconciliation drift, fee/funding drift, fencing rejects and venue circuit state.
- Structured event timeline and incident taxonomy.
- Durable operator commands: stop entries, resume, reconcile, force de-risk and manual resolution with signed reason.
- Dashboard is read-only; writes go through durable command journal.
- Backup/PITR, restore drill, secrets isolation and credential rotation.
- Runbooks for venue outage, DB failover, unknown order, orphan exposure and partial exit.

Acceptance:
- Every open position has traceable lifecycle timeline.
- Alerts cover unhedged exposure, aged obligations, unknown orders, ledger debt, stale books and reconciliation mismatch.
- Restore drill reconstructs executor state.
- Operator commands survive executor downtime and are idempotent.

## 14. PR-11 — Certification and staged rollout

Stages:
1. Static contract audit and full unit/integration suite.
2. Deterministic exchange simulators and fault injection.
3. Multi-process PostgreSQL integration and fencing stress tests.
4. Paper mode with real market data and no exchange writes.
5. Shadow decision mode for entry/hedge/exit with no writes.
6. Separately authorized controlled exchange test under strict caps.
7. Gradual enablement by venue/account/pair after evidence review.
8. Rollback to one known write path; never run old/new create-order paths simultaneously.

Mandatory fault scenarios:
- Worker crash before/after durable intent.
- Timeout after venue accepted create.
- Duplicate/out-of-order fill.
- Partial Spot fill while Futures minimum is not met.
- Insufficient depth and wait-budget expiry.
- Two workers claim same hedge.
- Pooled hedge allocation and one-session exit.
- Cancel/fill race.
- One-leg exit failure.
- Private stream loss and REST fallback.
- Venue throttle/IP restriction.
- Redis loss.
- PostgreSQL disconnect/failover.
- Fill-ledger write failure.
- Missing/stale FX, fee or funding.
- Old worker resumes after ownership transfer.
- STOP races with entry and emergency close.

Release gate:
- All required tests pass with CI evidence.
- No unresolved P0/P1 defect in touched execution path.
- All active orders and positions reconcile.
- No unresolved unknown submission.
- Risk and exit policy snapshots restore correctly.
- Runbooks, metrics, alerts, backup and restore have been exercised.
- Operator explicitly approves LIVE enablement.

## 15. Definition of Done for every PR

Every PR must include:
1. Exact source files/functions changed.
2. Reason for each change and reuse-vs-rebuild justification.
3. Migration and rollback/forward-fix plan if schema changes.
4. Unit, integration, failure-injection and regression tests.
5. Proof all mutation call sites still use MutationGateway.
6. Updated state diagrams and operational telemetry.
7. Scanner and Telegram compatibility statement.
8. Unresolved risk list.
9. No unrelated refactor.

## 16. Non-negotiable rules

- Do not change scanner behavior in V3.
- Do not change LIVE config, leverage, credentials, capital or risk thresholds as part of architecture PRs.
- Do not duplicate an existing module/table responsibility.
- Do not use Redis as canonical execution state.
- Do not equate HTTP acknowledgement with fill or terminal order.
- Do not blindly retry ambiguous writes.
- Do not interpret missing data as zero, flat or healthy.
- Do not declare CLOSED before reconciliation.
- Do not merge untested order mutation paths.
- Keep PRs focused; no giant all-at-once rewrite.

## 17. Immediate next action

PR-00 static mapping is documented in `docs/NAFAZ-EXECUTOR-V3-PR00-AUDIT-REPORT.md`. Before PR-01, close P0-01 operator cancellation false-success and P0-02 fill-ledger append failure semantics with focused tests. No trading code changes until these two controls are approved.
