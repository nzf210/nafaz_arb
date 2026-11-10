# Live exchange exposure reconciliation

The executor runs a read-only exposure reconciliation worker in LIVE mode. It
compares the quantity of managed spot base assets and futures shorts against
authenticated exchange reads. It never submits, edits, or cancels an order.

## Checks

- Spot: aggregate managed quantity by local exchange and base asset. The
  exchange's total base-asset balance must be at least the managed quantity.
  A larger balance is not treated as a mismatch because the account may hold
  manual or unrelated inventory.
- Perpetual: aggregate managed short quantity by global exchange and contract
  symbol, then compare it with the exchange's actual short quantity. Unexpected
  long, unsigned, malformed, or unavailable position data is never interpreted
  as a flat position.
- Account-wide open orders: each configured spot/futures venue is queried for
  resting orders. Exchange order IDs/client IDs are compared with executor
  pending entry/exit state. An uncorrelated order is exposed in executor status
  and Telegram `/pending`, and new entries on that venue are halted until the
  order is resolved. Unknown orders are never automatically canceled or
  adopted into a strategy.
- Live entry sizing uses the current free Spot and Futures balances. A failed
  balance read rejects new entries instead of falling back to the signal budget.
- Balance snapshots are journaled to the existing Supabase `balance_snapshots`
  table through the executor's durable local outbox.

A confirmed mismatch halts new entries on the affected venue and publishes an
execution-failure event for Telegram/operations. It does not change the active
position state and does not stop existing position monitoring/exits. When the
specific mismatch clears, its reconciliation halt is released unless another
mismatch remains for that venue.

If the exchange mapping for a restored active position is unavailable, the
worker skips that spot/contract mapping rather than guessing a symbol. The
existing startup futures recovery check remains in place.

## Configuration

- `EXPOSURE_OPEN_ORDERS_INTERVAL_SECONDS`: default `15`, minimum `5`, maximum
  `300`; controls account-wide open-order polling independently of balance and
  position reconciliation.
- `EXPOSURE_RECONCILIATION_INTERVAL_SECONDS`: default `300`, minimum `60`.
- `EXPOSURE_RECONCILIATION_REL_TOLERANCE`: default `0.005` (0.5%), capped at
  `0.05` (5%).
- `EXPOSURE_RECONCILIATION_ABS_TOLERANCE`: default `1e-8` base units.

The worker is disabled in MOCK/PAPER mode. It requires the exchange adapters'
read-only `fetch_open_orders`, `fetch_balance_report`, and `fetch_positions`
methods for each configured venue. It reuses the authenticated clients already
registered in `balance_exchanges`; it does not create extra exchange clients. Positions created by this version persist
their base asset and futures symbol in the Redis active-position record. Older
records without that metadata are marked as mapping-unavailable and halt new
entries on their known venues until the mapping is restored or manually
resolved.

It does not reconstruct a complete strategy pair from an unknown order or
reconcile already-filled orphan orders, fees, transfers, deposits, withdrawals,
or funding settlements. Supabase remains an audit/history sink, not the live
source of truth for current open orders; those records require separate
ledger/recovery work.
