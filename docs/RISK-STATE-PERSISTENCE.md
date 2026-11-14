# Durable risk-state recovery

## Inventory and scope

| Runtime state | Durable state type | Scope | Restoration behavior |
|---|---|---|---|
| `_trading_halted`, `_halt_reason` | `trading_halt` | global | Restored before recovery/listeners; manual kill switch is never cleared by UTC rollover |
| Manual resume acknowledgement | `trading_resume` | global | Prevents a previously acknowledged daily-loss halt from reappearing after restart on the same UTC day |
| `_daily_realized_loss_usdt`, `_loss_day` | `daily_loss` + `risk_pnl_events` | global | Rebuilt from immutable verified PnL events for the current UTC date |
| `_exchange_halted` | `exchange_halt` | exchange | Restored per venue; rate-limit cooldowns retain their expiry |
| `_consecutive_order_failures` | `consecutive_order_failures` | exchange | Restored per venue and resolved on clean success/manual venue resume |
| `_recovery_scope_halts` | `reconciliation_halt` | global/exchange/pair | Restored only at the affected exposure scope |
| `_failure_reasons` and reconciliation-required position state | `position_failure` | position | Restored as a position safety state and pair entry gate |
| Unhedged/partially reconciled exposure | `position_failure` + `reconciliation_halt` | position/pair | Remains blocked until the underlying reconciliation is explicitly resolved |

## Storage and ordering

- `risk_state` stores the required `state_id`, `state_type`, `scope`, `exchange`, `pair_key`, `reason`, `value`, `created_at`, `updated_at`, and `resolved_at` fields.
- `risk_pnl_events` is an append-only, unique-event ledger. Only verified realized trade PnL is admitted by the close paths.
- PostgreSQL (via `POSTGRES_DSN` and `executor/postgres_risk.py`) is the primary, direct ACID persistence layer. Local SQLite outbox has been deprecated and eliminated; synchronous transactions commit directly to PostgreSQL on the VPS with zero external network hops.
- If durable risk storage cannot be restored or journaled in LIVE mode, new entries are blocked (`RISK_STATE_PERSISTENCE_UNAVAILABLE`). Position monitoring and de-risking remain available.
- Redis is not used as the sole source of truth for any risk halt.

## UTC loss accounting and duplicate protection

Daily loss is the sum of negative `pnl_usdt` values in `risk_pnl_events` where `loss_day_utc` equals the current UTC date. The immutable event ID is inserted directly to PostgreSQL with unique constraint conflict handling (`ON CONFLICT (event_id) DO NOTHING`); duplicate event IDs do not increment the accumulator. Profits do not offset the gross realized-loss accumulator, preserving existing strategy behavior.

At UTC rollover only the daily-loss halt is eligible to clear. An operator kill switch remains active. A manual resume is durably recorded for the current UTC day; the next new realized PnL event re-arms normal daily-limit evaluation.

## Migration

Apply, in order:

- `20261111000200_add_durable_risk_state.sql`
- `20261111000300_add_idempotent_risk_pnl_events.sql`

Both tables are service-role only and protected by RLS.
