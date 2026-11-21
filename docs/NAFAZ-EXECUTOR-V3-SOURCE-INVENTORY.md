# NAFAZ Executor V3 — Source Inventory and Rebuild Decision Register

Repository: nzf210/nafaz_arb  
Target branch: feat/v2-bootstrap-config-stop-fencing  
Reviewed HEAD: a041d1dddc74b0e92eccd9cf6b21545d22e457f9  
Review date: 2026-11-21  
Status: READ-ONLY INVENTORY; not a full code audit or LIVE certification.

> This inventory is based on the target branch tree and selected source-file reads. File presence does not mean every line or runtime path has been audited. Function names are included where verified. Re-fetch HEAD and inspect complete call sites before implementation.

## 1. Executive findings

V2 already contains substantial execution infrastructure. V3 must reuse and harden it rather than create parallel owners for exit, PnL, mutation, fill, risk, hedge claims, or reconciliation.

| Capability | Existing source | Initial V3 decision |
|---|---|---|
| Exit decisions | executor/exit_decision_engine.py — ExitDecisionEngine.evaluate_exit | Reuse and extend; no second exit engine |
| Pair liquidation PnL | executor/pair_pnl_engine.py — PairPnLEngine.compute_pair_pnl | Reuse; harden precision and input provenance |
| Exit order lifecycle | executor/exit_orders.py — ExitOrderManagementMixin | Refactor behind coordinator; preserve venue semantics |
| Exit accounting | executor/exit_execution.py — _build_closed_trade_record | Reconcile with fill-ledger-derived realized PnL |
| Exit VWAP/depth | executor/exit_pricing.py — compute_vwap_exit_fill | Reuse; add freshness and impact controls |
| Mutation gateway | executor/mutation_gateway.py — MutationGateway | Keep as sole private mutation boundary |
| STOP/fencing | executor/global_stop.py — GlobalStopService | Preserve; expand ownership/concurrency tests |
| De-risk fencing | executor/derisk_fence.py — derisk_fence | Preserve protective path; resolve DB outage concurrency fallback |
| Hedge claims | executor/hedge_claims.py — HedgeIntentClaimService | Reuse; add pooled obligation/allocation domain |
| Fill ledger | executor/fill_ledger.py — ExecutionFillLedger | Reuse; harden critical persistence-failure handling |
| Order registry | executor/order_registry.py — UniversalOrderRegistry | Reuse; establish one canonical write authority |
| Risk state | executor/risk_state.py, executor/postgres_risk.py | Reuse; add independent policy/limit evaluation contract |
| Exposure reconciliation | executor/exposure_reconciliation.py | Reuse for balance/position domain; not full reconciliation |
| Fill reconciliation | executor/trade_fill_reconciliation.py | Reuse and expand |
| Financial reconciliation | executor/financial_reconciliation.py | Extend to fee currency, funding, FX and adjustments |
| Leg managers | executor/leg_managers.py | Audit wiring; current managers use in-memory dictionaries |
| Quantity normalization | executor/quantity.py | Reuse; Decimal and contract conversion audit |
| Venue capability | executor/exchange_capabilities.py | Complete capability matrix |
| Durable reporting outbox | executor/supabase_outbox.py | Reporting delivery only, not canonical execution state |

## 2. Architecture boundaries

### Must remain unchanged
- Go scanner discovery, pair matching, ranking, market-data collection, opportunity profit formula, Redis publication and scanner API.
- Scanner-to-Executor payload shape, identifiers and dispatch semantics.
- Venue-bound identity: local Spot venue/native symbol/quote asset and foreign Futures venue/native contract remain explicit.
- Existing strategy sizing and fee semantics unless separately approved.
- Telegram operator command semantics; dashboard remains a read model.

### Canonical state policy
PostgreSQL is the canonical execution state authority using native transactions for critical session, intent, order, fill, hedge, position, exit, risk and audit mutations. Redis is cache, notification and wake-up only. SQLite outbox is for durable reporting delivery, not exchange-order lifecycle authority. Supabase REST must not become a competing writer for canonical execution rows.

Migration files in Git do not prove they are applied to a live database. Verify schema state in a non-production environment before migrations are designed.

## 3. Verified critical source inventory

### Engine and admission
| File | Verified responsibility/symbols | Decision |
|---|---|---|
| executor/engine.py | ArbitrageEngine.__init__, initialize; composition root | Preserve public surface; map call graph |
| executor/engine_runtime.py | EngineRuntimeMixin.start/stop, signal queue, command journal, kill switch, recovery and exit scheduling | Evolve carefully; preserve runtime contracts |
| executor/engine_entry_flow.py | Entry orchestration | Thin coordinator after call graph review |
| executor/engine_entry_sizing.py | Entry sizing | Reuse; audit precision and quote conversion |
| executor/engine_entry_submission.py | Entry preparation/submission | All writes through durable intent + gateway |
| executor/engine_order_intents.py | OrderRegistrationMetadata; EngineOrderIntentMixin._register_order | Separate mandatory pre-submit intent from post-ack projection |
| executor/engine_recovery.py | _persist_reconciled_submit_order; _reconcile_durable_submit_attempts | Preserve exact client-ID recovery and fail-closed behavior |
| executor/engine_signal.py | Signal admission/normalization; file confirmed, symbols need line-level review | Preserve scanner contract |

### Mutation, ownership and recovery
| File | Verified responsibility/symbols | Decision |
|---|---|---|
| executor/mutation_gateway.py | MutationGateway, MutationResultType, generate_client_order_id, unknown-submit handling | Sole private write boundary; audit every call site |
| executor/global_stop.py | GlobalStopService; PostgreSQL advisory-lock STOP/fencing | Preserve; certify stale-owner rejection |
| executor/derisk_fence.py | derisk_fence | Preserve protective action during STOP; test DB-loss fallback |
| executor/order_registry.py | UniversalOrderRegistry facade | Preserve API; audit storage/recovery mixins |
| executor/order_registry_storage.py | Registry persistence | Verify uniqueness and transaction guarantees |
| executor/order_registry_recovery.py | Registry recovery/adoption | Verify complete inventory and orphan policy |
| executor/order_recovery.py and executor/recovery_*.py | Exact order recovery, discovery, adoption, orphan handling | Consolidate only after behavior parity |
| executor/order_slots.py | Per-venue/account Spot slot reservations | Preserve atomic reservation |
| executor/order_sync_worker.py | OrderSyncWorker.sync_once, maybe_sync | Projection/synchronization only; not second state machine |
| executor/command_processor.py | Durable operator command processing | Preserve durable emergency commands |

### Legs, fills and hedge
| File | Verified responsibility/symbols | Decision |
|---|---|---|
| executor/entry_hedge.py | EntryHedgeMixin._handle_entry_fills; cumulative Spot fill and hedge residual | Preserve business semantics; replace float/transient correctness state |
| executor/hedge_claims.py | claim_generation, mark_submitted, mark_confirmed, mark_abandoned, mark_zero_fill_terminal | Reuse CAS claim; add pooled allocation separately |
| executor/leg_managers.py | SpotLegManager, FuturesLegManager, TwoLegCoordinator | Runtime projection only until durable hydration proven |
| executor/fill_ledger.py | ExecutionFillLedger; PostgreSQL append-only fills and PnL deviations | Reuse; accounting debt must be explicit and block new exposure |
| executor/trade_fill_reconciliation.py | TradeFillReconciliationWorker.reconcile_once | Extend pagination, fee/funding provenance and completeness |
| executor/financial_reconciliation.py | FinancialReconciliationEngine.reconcile_order_fills | Extend per-fill identity and currency-aware comparisons |
| executor/quantity.py | AmountSpec, calculate_order_size, canonical_hedge_target, Futures sizing validation | Shared quantity authority; Decimal migration |
| executor/market_models.py, market_data_gate.py | Market metadata and data admission | Add freshness/sequence/depth contracts |

### Exit, PnL and position
| File | Verified responsibility/symbols | Decision |
|---|---|---|
| executor/exit_decision_engine.py | ExitDecisionEngine.evaluate_exit; emergency, stop, unhedged repair, time decay, capital recovery, hold | Existing decision authority; extend, do not duplicate |
| executor/pair_pnl_engine.py | PairPnLEngine.compute_pair_pnl; combined Spot/Futures liquidation PnL, fees, funding, FX and ROI | Reuse; verify inputs, precision and cost provenance |
| executor/exit_pricing.py | compute_vwap_exit_fill, calculate_chunk_exit_qty, ExitPricingMixin | Reuse; add freshness and max impact |
| executor/exit_orders.py | _check_dynamic_exit_pair_inner, _check_dynamic_exits, _process_pending_exit_inner and cancel/fill synchronization | Main integration point for Exit Coordinator |
| executor/exit_execution.py | _build_closed_trade_record | Reconcile with immutable fill ledger |
| executor/exits.py | Exit lifecycle helpers | Preserve tested behavior |
| executor/force_close.py, force_close_legs.py, force_close_lifecycle.py | Operator/emergency close | Unify under exit coordinator without losing emergency semantics |
| executor/unwind_planner.py | Unwind planning | Audit actual exposure and risk priority |
| executor/models.py, position_models.py | Order/position models | Add typed identity, policy snapshot and state version |

### Risk, adapters and operations
| File | Responsibility | Decision |
|---|---|---|
| executor/risk.py, risk_exposure.py, risk_funding.py, risk_state.py | Risk composition, exposure, funding preflight, durable state | Reuse; add independent risk decision interface |
| executor/postgres_risk.py | PostgreSQL risk-state and realized-loss persistence | Reuse; define shared pool/transaction ownership |
| executor/capital_manager.py | Capital allocation/reservation | Reconcile with portfolio/session exposure |
| executor/exchange_capabilities.py | Venue capability definitions | Complete per venue/account/market matrix |
| executor/exchange_client.py, exchange_*.py, venues/ | Exchange facade/adapters | Preserve adapters; normalized capability contracts |
| executor/private_stream.py | Private order stream cache/worker | Verify venue wiring; REST remains reconciliation authority |
| executor/fee_engine.py | Dynamic fee source/rates | One fee authority for entry, hedge, exit and PnL |
| executor/supabase_outbox.py, supabase_worker.py, supabase_*.py | Local outbox and reporting writers/readers | Reporting projection only |
| executor/system_readiness.py, engine_status.py | Readiness/status | Expand degraded/unknown/blocked state |
| executor/operator_resolution.py | Manual resolution | Preserve signed reason and audit |

## 4. Existing migrations confirmed

Target branch contains:
- 20261107000100_create_nafaz_core_schema.sql
- 20261109000100_add_scanner_opportunity_idempotency.sql
- 20261111000100_add_live_order_fill_reconciliation.sql
- 20261111000200_add_durable_risk_state.sql
- 20261111000300_add_idempotent_risk_pnl_events.sql
- 20261111000400_add_reconciliation_audit_trail.sql
- 20261111000500_add_universal_order_registry.sql
- 20261112000100_add_venue_client_order_id.sql
- 20261113000100_add_global_stop_fencing.sql
- 20261113000200_add_durable_submit_attempts.sql
- 20261113000300_add_submit_resolution_audit.sql
- 20261114000100_add_executor_v2_core.sql
- 20261114000200_allow_partial_position_status.sql
- 20261115000100_add_execution_fill_ledger.sql
- 20261116000100_add_hedge_intent_claims.sql

Existing concepts include execution_sessions, execution_legs, pair_pnl_snapshots, command_journal, execution_fill_ledger, pair_pnl_deviations and executor_hedge_intent_claims. Do not create duplicates. Audit complete DDL, indexes, foreign keys, RLS and actual applied migration state first.

## 5. Existing regression test inventory

- Entry/hedge/partial fill: test_entry.py, test_partial_fill_min_notional.py, test_hedge_claims.py, test_two_leg_execution_recovery.py.
- Exit/PnL: test_exits.py, test_exit_priority_improvements.py, test_pnl.py, test_tahap2_exitability.py, test_force_close.py, test_force_clean_orphan.py.
- Persistence/fencing/recovery: test_order_persistence_guarantees.py, test_order_recovery.py, test_global_stop_fencing.py, test_distributed_recovery_lock.py, test_postgres_submit_fencing_integration.py, test_production_failure_injection.py.
- Accounting/reconciliation: test_tahap4_fill_ledger.py, test_trade_fill_reconciliation.py, test_exposure_reconciliation.py.
- Venue/quantity/fees: test_clients.py, test_venues_modular.py, test_venue_recovery_contracts.py, test_exchange_market_resolution_recovery.py, test_exchange_order_id_normalization.py, test_indodax_v2.py, test_mexc_futures_balance.py, test_quantity.py, test_fee_engine.py, test_exchange_fee_provider.py.
- Risk/operations/runtime: test_risk.py, test_config.py, test_signal_priority.py, test_single_flight_market_gate.py, test_stream_cache.py, test_supabase_writer.py, test_executor_v2_core.py.

These are test files present in the branch, not tests run or certified during this documentation task.

## 6. Source-level risks requiring explicit V3 decisions

1. Critical financial/quantity paths use floats in exit decision, pair PnL, hedge progress, order registry and reconciliation. Establish Decimal for correctness-critical calculations.
2. Order registration is described as best-effort after acknowledgement in engine_order_intents.py, while GlobalStopService owns durable pre-submit attempts. Make mandatory pre-submit intent the authority and post-ack registry a projection.
3. fill_ledger.py documents that ledger failure may be logged/counted while trading continues. V3 must create durable accounting-debt incident, block new exposure for affected scope, and continue protective position management.
4. leg_managers.py maintains in-memory dictionaries. These may be projections but cannot be canonical lifecycle state; prove hydration/replay.
5. ExitDecisionEngine already has emergency, stop-loss, unhedged repair, time-decay and capital recovery. Inspected evaluate_exit does not prove durable trailing high-water state or policy pinning; add only missing functionality.
6. PairPnLEngine accepts fees, funding and FX, but source freshness/verification and missing-data behavior must be validated. Unknown cost must not silently become zero.
7. HedgeIntentClaimService protects a parent order/fill generation; it does not itself provide cross-session pooled hedge allocation.
8. derisk_fence intentionally permits protective writes when PostgreSQL serialization is unavailable. This protects exposure but weakens multi-instance serialization; define designated emergency ownership and immediate reconciliation.
9. Existing exposure and financial workers do not prove complete order, fill, position, balance, funding and PnL reconciliation for every venue.
10. Private stream cache is not proof of active, sequence-safe per-venue subscription.
11. Migration presence in Git does not prove deployment. No production DB was queried.

## 7. Required pre-implementation checks

- Re-fetch target HEAD and compare with this baseline.
- Fetch full contents and map all call sites for every touched file.
- Trace every private create/cancel/replace/close call to MutationGateway.
- Verify startup ordering: DB/fencing → risk restore → submit/order recovery → fills → positions/hedges → exit monitor → reconciliation → new-entry readiness.
- Record baseline test/CI results before editing.
- Mark every source row REUSE, MODIFY, REPLACE or NEW with reason and tests.
- No Go scanner source or scanner behavior changes.

## 8. Review limitations

This is a branch-specific source inventory based on tree inspection and selected source reads. It is not a complete line-by-line audit, test run, live exchange probe, production DB migration check, or LIVE safety certification.
