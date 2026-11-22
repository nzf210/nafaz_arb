# NAFAZ Executor V3 — Risk Control and Emergency De-risk

## Control semantics

The executor uses three explicit mutation classes:

- `ENTRY`: increases unhedged or directional exposure. It is rejected by global STOP, entry-blocked risk scopes, missing account ownership, unavailable risk metrics required by configured limits, and any exceeded quantitative budget.
- `HEDGE`: opens the protective futures leg for an already admitted spot exposure. It is subject to durable ownership and the `DE_RISK_ALLOWED` control, but is not rejected merely because an entry budget or global STOP is active.
- `DERISK`: closes, unwinds, rolls back, or force-closes exposure. It is controlled by `DE_RISK_ALLOWED` and exact venue/order verification, not by entry budgets.

`ENTRY_BLOCKED` and `DE_RISK_ALLOWED` are independent controls. A scope can reject new entries while continuing to permit protective hedge and close operations.

## Quantitative budget fields

The risk-control plane evaluates an immutable metric snapshot before a scoped entry submit. Limits can be supplied by executor configuration and overridden by the matching durable `executor_risk_controls.limits` row.

Supported limits:

| Limit | Metric |
|---|---|
| `max_gross_exposure_usdt` | `gross_exposure_usdt` |
| `max_net_exposure_usdt` | `net_exposure_usdt` |
| `max_deployed_capital_usdt` | `deployed_capital_usdt` |
| `max_unhedged_notional_usdt` | `unhedged_notional_usdt` |
| `max_unhedged_duration_ms` | `unhedged_duration_ms` |
| `max_open_order_count` | `open_order_count` |
| `max_daily_loss_usdt` | `daily_loss_usdt` |
| `max_drawdown_pct` | `drawdown_pct` |
| `max_slippage_bps` | `slippage_bps` |
| `max_funding_cost_pct` | `funding_cost_pct` |
| `max_market_data_age_ms` | `market_data_age_ms` |
| `min_venue_health_score` | `venue_health_score` |

A configured limit with a missing, non-finite, or unpriced required metric fails closed. IDR pending orders are not interpreted as USDT; an explicit USDT notional or a valid USDT/IDR conversion is required.

Defaults are derived conservatively from existing executor settings: capital allocation, per-pair size, daily loss, entry slippage, market-data staleness, order TTL, and per-venue open-order slots. A zero limit disables that specific budget. Protective `HEDGE` and `DERISK` mutations retain their separate permission path and are not blocked by entry budgets.

The evaluated limits, metrics, action class, and durable control versions are included in the submit attempt's risk-control snapshot.

## Risk-state persistence and startup

Risk-state events are synchronously journaled to the enabled local Supabase outbox before asynchronous PostgreSQL persistence. An in-flight PostgreSQL write sets `_risk_persistence_pending`; readiness and the entry signal gate reject new exposure until the write completes.

A failed write sets the durable risk-persistence failure latch at `data/executor/risk-persistence-failed.json` (overridable with `risk_persistence_latch_path`) and blocks entries. The latch is only cleared after the explicit resume path has durably written its halt/resume records and completed pending writes.

At startup, PostgreSQL risk state is restored first. When PostgreSQL is unavailable but the Supabase durable fallback is available, the engine restores available active risk state and daily loss, then starts in protect-only mode. If neither durable backend is available, it remains entry-blocked. Missing risk history is never treated as a clean risk state.

## Emergency de-risk ownership during PostgreSQL outage

Normal de-risk operations use the PostgreSQL de-risk fence. If PostgreSQL fencing is unavailable, the executor may use a Redis lease only when all of the following hold:

1. `EXECUTOR_INSTANCE_ID` identifies the running executor.
2. `EMERGENCY_DERISK_OWNER_ID` identifies the single designated emergency owner and exactly matches that executor ID.
3. `EMERGENCY_DERISK_ALLOWED=true` explicitly authorizes the emergency fallback while the durable database control plane is unavailable.
4. Redis is available.
5. The mutation supplies a stable client order ID and complete account-scope, venue, market-type, and native-instrument identity.

The lease uses Redis `SET NX PX` with a per-scope key and a bounded TTL (default 120 seconds, configurable through `EMERGENCY_DERISK_LEASE_TTL_MS`, maximum 5 minutes). Release is compare-and-delete; an unconditional delete is never used. Different instruments can be de-risked independently. If designated ownership or Redis is unavailable, the operation fails closed rather than submitting unfenced.

A Redis emergency lease is an ownership fallback, not proof that the venue accepted an order. The mutation path must still perform immediate exact-client-ID reconciliation and retain UNKNOWN state when the venue result cannot be verified. The unified mutation gateway stage owns that common reconciliation contract.

## Operational safety

- No scanner code or scanner behavior is changed by this risk-control stage.
- No entry/exit strategy thresholds are changed.
- No production migration or LIVE deployment is performed by this implementation work.
- The emergency-owner environment variables must be configured on exactly one executor instance before relying on PostgreSQL-outage de-risk fallback.
