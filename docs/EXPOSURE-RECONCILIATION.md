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

- `EXPOSURE_RECONCILIATION_INTERVAL_SECONDS`: default `300`, minimum `60`.
- `EXPOSURE_RECONCILIATION_REL_TOLERANCE`: default `0.005` (0.5%), capped at
  `0.05` (5%).
- `EXPOSURE_RECONCILIATION_ABS_TOLERANCE`: default `1e-8` base units.

The worker is disabled in MOCK/PAPER mode. It requires the exchange adapters'
read-only `fetch_balance_report` and `fetch_positions` methods. The current
executor adapter is configured for one local spot venue and one global
perpetual venue per executor instance; this worker does not create extra
exchange clients for other venues. Positions created by this version persist
their base asset and futures symbol in the Redis active-position record. Older
records without that metadata are marked as mapping-unavailable and halt new
entries on their known venues until the mapping is restored or manually
resolved.

It does not reconcile individual order/fill history, fees, transfers, deposits,
withdrawals, or funding settlements; those require separate ledger
reconciliation.
