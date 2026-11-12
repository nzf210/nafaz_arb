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
- The SQLite durable outbox is the synchronous local journal. Startup overlays pending local `risk_state` and PnL rows over Supabase reads so a crash before asynchronous delivery does not erase a newly recorded halt.
- LIVE startup requires enabled Supabase service-role access and a writable SQLite outbox on persistent storage. A configured but unavailable outbox is not treated as a successful durable store.
- Order-registry startup reads are paginated and overlay pending local SQLite journal rows. Recent fill-history reads are paginated across both `orders` and `order_registry`; failure of either source blocks startup reconciliation rather than silently omitting executions.
- If durable risk storage cannot be restored or journaled in LIVE mode, new entries are blocked. Position monitoring and de-risking remain available.
- Redis is not used as the sole source of truth for any risk halt.
- Active live positions have a durable `state_snapshot` in Supabase. Startup
  restores missing Redis position keys from these rows before the executor can
  resume management; unresolved/malformed snapshots keep the entry gate closed.

## UTC loss accounting and duplicate protection

Daily loss is the sum of negative `pnl_usdt` values in `risk_pnl_events` where `loss_day_utc` equals the current UTC date. The immutable event ID is inserted once through the durable outbox; duplicate event IDs do not increment the accumulator. Profits do not offset the gross realized-loss accumulator, preserving existing strategy behavior.

At UTC rollover only the daily-loss halt is eligible to clear. An operator kill switch remains active. A manual resume is durably recorded for the current UTC day; the next new realized PnL event re-arms normal daily-limit evaluation.

## Migration

Apply, in order:

- `20261111000200_add_durable_risk_state.sql`
- `20261111000300_add_idempotent_risk_pnl_events.sql`

Both tables are service-role only and protected by RLS.
