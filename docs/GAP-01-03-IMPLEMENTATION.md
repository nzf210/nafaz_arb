# GAP-01 to GAP-03 implementation notes

Branch: `fix/gap-01-03-fx-freshness-fees`

## GAP-01 — IDR FX fail-closed

- The executor now rejects IDR/BIDR entry signals when the scanner payload has no finite, positive USDT/IDR rate, including in mock mode.
- The exact validated rate and scan timestamp are persisted with each pending entry.
- Fill reconciliation no longer substitutes a hardcoded FX rate. A legacy or malformed filled order without its persisted rate is marked `RECONCILIATION_REQUIRED`.
- Pending exposure accounting uses the USDT-equivalent reservation stored when the order was placed, scaled by its remaining quantity.

Operational note: old pending-order records created before this change may not contain the persisted FX rate. If such an order has fills, the executor deliberately requires manual reconciliation instead of guessing.

## GAP-02 — order-book freshness

- Default maximum order-book age is now 5 seconds.
- Default cross-venue receive-time skew is now 3 seconds.
- Indodax and Tokocrypto books explicitly record `ReceivedAt` and `Source=receive_time`.
- OKX and MEXC perpetual books retain exchange timestamps and record local receive time.
- Freshness rejects missing timestamps, stale data, and timestamps more than one second in the future.
- Cross-venue alignment compares local receive times when both are available, avoiding direct comparison of clocks from different exchanges.

Limitation: Indodax/Tokocrypto depth endpoints do not expose a reliable exchange-side book timestamp in the current adapter. Receive-time validation proves the response was recently received, not that the exchange itself did not serve cached depth. This remains a venue/API limitation.

## GAP-03 — fee profiles

- Added optional per-exchange fee overrides under `fee_profiles`.
- Spot maker/taker rates are resolved from the local Spot venue profile; perpetual taker rates are resolved from the Global Perpetual venue profile.
- Explicit zero maker fees are preserved; omitted values fall back to the existing global fee configuration.
- Invalid percentages outside 0–100 are rejected.

Important limitation: these profiles are operator-supplied configuration values, not dynamically fetched account-specific fee tiers. The scanner does not yet call each exchange's authenticated fee endpoint. Before live trading, populate them using the actual account fee schedule. Dynamic fee discovery remains a follow-up gap, not something this change claims to have completed.

Example:

```json
{
  "fee_profiles": {
    "indodax": {
      "spot_maker_fee_pct": 0.0,
      "spot_taker_fee_pct": 0.3
    },
    "tokocrypto": {
      "spot_maker_fee_pct": 0.1,
      "spot_taker_fee_pct": 0.1
    },
    "okx": {
      "perp_taker_fee_pct": 0.05
    },
    "mexc": {
      "perp_taker_fee_pct": 0.0
    }
  }
}
```

The percentages above are illustrative only. Replace them with verified rates from the relevant account; do not copy them as production defaults.
