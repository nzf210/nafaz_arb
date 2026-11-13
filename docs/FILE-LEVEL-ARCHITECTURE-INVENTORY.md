# File-Level Architecture Inventory

- Repository: `nzf210/nafaz_arb`
- Base: `feat/unified-dynamic-fee-engine` @ `9cc3dcd11d0999e828309053ba4cffce98a28675`
- Audit branch: `audit/architecture-baseline-regression-contract`
- Recursive tree: 213 tracked files, not truncated.
- Purpose: file-by-file change boundary for the architecture work.

## How to read this inventory

- **Source inspected** means the file was fetched from the exact baseline commit and examined for its architectural responsibility. It does not imply a runtime test was run.
- **Inventory / scope only** means the file exists in the baseline tree and is assigned a preliminary preservation/change policy from its role/name. It must receive detailed review before implementation touches it.
- **KEEP** protects current behavior; it does not mean the file is bug-free.
- **MODIFY** means preserve behavior and public contracts while changing implementation.
- No source file was modified by this audit.

## Complete tracked-file matrix

| File | Decision | Review depth | Contract / reason |
|---|---|---|---|
| `.dockerignore` | KEEP + MODIFY | Inventory / scope only | Deployment/feature flags/CI must support staged rollout |
| `.github/workflows/test.yml` | KEEP + MODIFY | Inventory / scope only | Deployment/feature flags/CI must support staged rollout |
| `.gitignore` | KEEP | Inventory / scope only | Documentation/tooling baseline |
| `Dockerfile` | KEEP + MODIFY | Inventory / scope only | Deployment/feature flags/CI must support staged rollout |
| `Dockerfile.executor` | KEEP + MODIFY | Inventory / scope only | Deployment/feature flags/CI must support staged rollout |
| `LICENSE` | KEEP | Inventory / scope only | Documentation/tooling baseline |
| `PODMAN.md` | KEEP | Inventory / scope only | Documentation/tooling baseline |
| `README.md` | KEEP | Inventory / scope only | Documentation/tooling baseline |
| `cache/indodax_spot_markets.json` | KEEP / VERIFY | Inventory / scope only | Inspect before changing |
| `cmd/nafaz-scanner-outbox/main.go` | KEEP + MODIFY | Inventory / scope only | Preserve opportunity contract; improve WS coverage/telemetry |
| `cmd/nafaz-scanner/main.go` | KEEP + MODIFY | Source inspected | Preserve opportunity contract; improve WS coverage/telemetry |
| `config.example.json` | KEEP + MODIFY | Source inspected | Deployment/feature flags/CI must support staged rollout |
| `data/.gitkeep` | KEEP / VERIFY | Inventory / scope only | Inspect before changing |
| `docker-compose.yml` | KEEP + MODIFY | Source inspected | Deployment/feature flags/CI must support staged rollout |
| `docs/AUDIT-FINDINGS.md` | REFERENCE | Inventory / scope only | Update if architecture contract changes |
| `docs/EXPOSURE-RECONCILIATION.md` | REFERENCE | Inventory / scope only | Update if architecture contract changes |
| `docs/GAP-01-03-IMPLEMENTATION.md` | REFERENCE | Inventory / scope only | Update if architecture contract changes |
| `docs/IMPROVEMENT-PLAN.md` | REFERENCE | Inventory / scope only | Update if architecture contract changes |
| `docs/RISK-STATE-PERSISTENCE.md` | REFERENCE | Inventory / scope only | Update if architecture contract changes |
| `docs/UNIFIED-FEE-ENGINE.md` | REFERENCE | Inventory / scope only | Update if architecture contract changes |
| `executor/__init__.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/adaptive_maker.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/cancel.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/config.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/engine.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/engine_common.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/engine_entry_flow.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/engine_entry_sizing.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/engine_entry_submission.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/engine_fee_preflight.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/engine_order_intents.py` | KEEP + MODIFY | Source inspected | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/engine_recovery.py` | KEEP + MODIFY | Inventory / scope only | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/engine_runtime.py` | KEEP + MODIFY | Source inspected | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/engine_signal.py` | KEEP + MODIFY | Source inspected | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/engine_status.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/entry.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/entry_common.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/entry_hedge.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/entry_monitoring.py` | KEEP + MODIFY | Source inspected | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/entry_orders.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/exchange_client.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/exchange_common.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/exchange_fee_balance.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/exchange_futures.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/exchange_orders.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/exchange_trades.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/exit_execution.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/exit_orders.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/exit_pricing.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/exits.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/exposure_reconciliation.py` | KEEP + MODIFY | Inventory / scope only | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/fee_engine.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/force_close.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/force_close_legs.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/force_close_lifecycle.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/helpers.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/indodax_v2.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/main.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/market_models.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/models.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/order_models.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/order_recovery.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/order_registry.py` | KEEP + MODIFY | Inventory / scope only | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/order_registry_common.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/order_registry_recovery.py` | KEEP + MODIFY | Inventory / scope only | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/order_registry_storage.py` | KEEP + MODIFY | Inventory / scope only | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/pair_registry.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/paper_exchange_client.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/position_models.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/private_stream.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/quantity.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/recovery_adoption.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/recovery_discovery.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/recovery_orphans.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/recovery_registry.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/recovery_utils.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/risk.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/risk_exposure.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/risk_funding.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/risk_state.py` | KEEP + MODIFY | Inventory / scope only | Preserve state contract; add durable command/coordination or scheduler integration |
| `executor/rollback.py` | KEEP | Source inspected | Trading/recovery feature; regression protected, no rewrite |
| `executor/simulator.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_common.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_orders.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_outbox.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_positions.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_queries.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_risk.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_trades.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/supabase_worker.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/trade_fill_reconciliation.py` | MODIFY | Source inspected | Private API boundary; scheduler integration without changing lifecycle semantics |
| `executor/trade_models.py` | KEEP | Inventory / scope only | Trading/recovery feature; regression protected, no rewrite |
| `executor/venues/__init__.py` | KEEP + MODIFY | Inventory / scope only | Retain venue quirks; add capability/scheduler contract |
| `executor/venues/base.py` | KEEP + MODIFY | Source inspected | Retain venue quirks; add capability/scheduler contract |
| `executor/venues/indodax.py` | KEEP + MODIFY | Source inspected | Retain venue quirks; add capability/scheduler contract |
| `executor/venues/mexc.py` | KEEP + MODIFY | Source inspected | Retain venue quirks; add capability/scheduler contract |
| `executor/venues/okx.py` | KEEP + MODIFY | Source inspected | Retain venue quirks; add capability/scheduler contract |
| `executor/venues/registry.py` | KEEP + MODIFY | Source inspected | Retain venue quirks; add capability/scheduler contract |
| `executor/venues/tokocrypto.py` | KEEP + MODIFY | Source inspected | Retain venue quirks; add capability/scheduler contract |
| `go.mod` | KEEP + VERIFY | Inventory / scope only | Dependency pinning and CCXT API surface must be verified |
| `go.sum` | KEEP + VERIFY | Inventory / scope only | Dependency pinning and CCXT API surface must be verified |
| `internal/api/server.go` | KEEP | Inventory / scope only | Preserve API/ledger contracts; additive changes only |
| `internal/api/server_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/config/config.go` | KEEP / VERIFY | Inventory / scope only | Inspect before changing |
| `internal/config/config_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/domain/domain.go` | KEEP / VERIFY | Inventory / scope only | Inspect before changing |
| `internal/exchange/adapter.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/adapter_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/funding_worker.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/funding_worker_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/health.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/health_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/indodax_limiter.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/indodax_resilience_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/indodax_ws.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_books.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_dns.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_funding.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_perp.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_spot.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/mock.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/okx_books_ws.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/okx_books_ws_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/okx_limiter.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/okx_limiter_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/venue_indodax.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venue_mexc.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venue_okx.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venue_tokocrypto.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venues.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venues_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/ws_book.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/ws_book_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/exchange/ws_client.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/ws_decode.go` | KEEP + MODIFY | Inventory / scope only | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/ws_streams.go` | KEEP + MODIFY | Source inspected | Preserve venue adapter; unify market-data lifecycle and public request policy |
| `internal/history/store.go` | KEEP | Inventory / scope only | Preserve API/ledger contracts; additive changes only |
| `internal/history/store_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/redishandler/client.go` | MODIFY | Source inspected | Preserve signal semantics; add durable command path separately |
| `internal/redishandler/client_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/scanner/evaluate.go` | KEEP + MODIFY | Source inspected | Preserve opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/exchange_health_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/scanner/hybrid_filter_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/scanner/liquidity.go` | KEEP + MODIFY | Inventory / scope only | Preserve opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/liquidity_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/scanner/service.go` | KEEP + MODIFY | Source inspected | Preserve opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/service_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/scanner/validation.go` | KEEP + MODIFY | Inventory / scope only | Preserve opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/validation_freshness_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/supabase/client.go` | KEEP | Inventory / scope only | Preserve API/ledger contracts; additive changes only |
| `internal/supabase/client_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/supabase/outbox.go` | KEEP | Inventory / scope only | Preserve API/ledger contracts; additive changes only |
| `internal/supabase/outbox_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/telegram/balances_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/telegram/bot.go` | KEEP + MODIFY | Inventory / scope only | Preserve operator UX; emergency commands become durable |
| `internal/telegram/bot_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/telegram/clean_orphan_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/telegram/formatters.go` | KEEP + MODIFY | Inventory / scope only | Preserve operator UX; emergency commands become durable |
| `internal/telegram/handlers.go` | KEEP + MODIFY | Source inspected | Preserve operator UX; emergency commands become durable |
| `internal/telegram/monitor.go` | KEEP + MODIFY | Source inspected | Preserve operator UX; emergency commands become durable |
| `internal/telegram/monitor_test.go` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `internal/telegram/positions.go` | KEEP + MODIFY | Source inspected | Preserve operator UX; emergency commands become durable |
| `pytest.ini` | KEEP | Inventory / scope only | Documentation/tooling baseline |
| `requirements.txt` | KEEP + VERIFY | Inventory / scope only | Dependency pinning and CCXT API surface must be verified |
| `session-files/20261108-okx-ratelimit-note.md` | REFERENCE | Inventory / scope only | Update if architecture contract changes |
| `session.db` | KEEP / VERIFY | Inventory / scope only | Inspect before changing |
| `supabase/README.md` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/config.toml` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261107000100_create_nafaz_core_schema.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261109000100_add_scanner_opportunity_idempotency.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261111000100_add_live_order_fill_reconciliation.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261111000200_add_durable_risk_state.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261111000300_add_idempotent_risk_pnl_events.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261111000400_add_reconciliation_audit_trail.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261111000500_add_universal_order_registry.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/migrations/20261112000100_add_venue_client_order_id.sql` | KEEP | Inventory / scope only | Durable schema/migration contract; additive migrations only |
| `supabase/tests/schema_smoke.sql` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/conftest.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_adaptive_maker.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_adaptive_maker_paper_validation.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_clients.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_config.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_distributed_recovery_lock.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_entry.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_exchange_fee_provider.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_exchange_market_resolution_recovery.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_exchange_order_id_normalization.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_exits.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_exposure_reconciliation.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_fee_engine.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_force_clean_orphan.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_force_close.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_indodax_v2.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_mexc_futures_balance.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_order_persistence_guarantees.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_order_recovery.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_pair_registry.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_pnl.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_probe9.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_production_failure_injection.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_quantity.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_rate_limit_resilience.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_recovery.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_risk.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_signal_priority.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_stream_cache.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_supabase_writer.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_trade_fill_reconciliation.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_two_leg_execution_recovery.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |
| `tests/test_venues_modular.py` | KEEP | Inventory / scope only | Existing regression evidence; extend, never replace |

## Mandatory review rule

Before changing any row marked **Inventory / scope only**, inspect the full file and its direct callers/tests at the exact target commit. Update this inventory with the reviewed status and any refined decision in the same PR. For every new source file, add a row. For every deleted or renamed file, retain the old-to-new mapping in the PR description.

## Critical dependency order

1. `executor/exchange_client.py`, `executor/exchange_orders.py`, `executor/exchange_futures.py`, `executor/exchange_trades.py`, `executor/exchange_fee_balance.py`, `executor/exchange_common.py`, `executor/indodax_v2.py` — private exchange boundary.
2. `executor/engine_order_intents.py`, `executor/order_registry.py`, `executor/order_registry_recovery.py`, `executor/order_registry_storage.py` — durable order identity.
3. `executor/engine_recovery.py`, `executor/recovery_discovery.py`, `executor/recovery_orphans.py`, `executor/trade_fill_reconciliation.py`, `executor/exposure_reconciliation.py` — recovery and verification.
4. `executor/engine_runtime.py`, `executor/engine_signal.py`, `executor/entry_monitoring.py`, `executor/entry_orders.py`, `executor/exit_orders.py`, `executor/rollback.py`, `executor/force_close*.py` — order orchestration and emergency handling.
5. `internal/exchange/*.go`, `internal/scanner/*.go`, `cmd/nafaz-scanner/main.go` — public market-data path.
6. `internal/redishandler/client.go`, `internal/telegram/*.go`, `internal/api/*.go` — operator/control plane.
7. `supabase/migrations/*.sql`, `internal/supabase/*.go`, `executor/supabase*.py` — durable ledger and outbox.

## Explicit non-goals

- No strategy rewrite.
- No fee formula rewrite.
- No change to maker/taker selection or adaptive maker algorithm.
- No removal of the current recovery or registry implementation.
- No switch of durable source of truth from Supabase/Postgres to Redis.
- No LIVE enablement or production credential changes.
