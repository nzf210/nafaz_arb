# NAFAZ exchange roles and multi-venue routing

NAFAZ uses a deliberately asymmetric spot/perpetual model. These roles are enforced
in scanner discovery, order-book routing, executor client routing, and configuration
validation.

| Role | Allowed exchanges | Market type |
|---|---|---|
| Local Indonesia | Indodax, Tokocrypto | Spot only |
| Foreign | MEXC, OKX, Binance, Bybit | Linear USDT/USDC perpetual only |

## Scanner configuration

`config.json` supports independent venue selection:

```json
{
  "enabled_spot_exchanges": ["indodax", "tokocrypto"],
  "global_futures_exchange": "mexc,okx,binance,bybit",
  "quote_assets": ["USDT", "USDC"],
  "enable_synthetic_idr": false
}
```

Use `global_futures_exchange: "all"` to enable all four foreign perpetual market
feeds. Use a comma-separated subset to disable individual foreign venues. The local
allowlist accepts only `indodax` and `tokocrypto`; the futures allowlist accepts
only `mexc`, `okx`, `binance`, and `bybit`. Invalid role assignments fail
configuration validation.

The scanner discovers each enabled venue independently and creates combinations
only for matching base assets and compatible quote assets. It requires active
contracts and keeps contract size, tick size, lot size and minimum quantity where
provided by the exchange. Order-book and funding requests are routed to the venue
encoded in each market symbol. REST is the fallback for Binance and Bybit; the
existing websocket cache does not yet subscribe to those two venues.

MEXC Spot discovery and Spot order-book access are intentionally disabled. Do not
add a foreign Spot venue to `enabled_spot_exchanges`.

IDR/BIDR local Spot markets are not automatically paired to USDT/USDC perpetuals.
That synthetic FX path remains opt-in through `enable_synthetic_idr`; when enabled,
the scanner requires a live USDT/IDR reference and applies the existing FX sanity
checks. Do not enable it merely to increase the number of displayed pairs.

## Executor configuration and credentials

The Python executor routes a signal using the exchange identity embedded in the
market symbol and the `local_exchange` / `global_exchange` fields in pair metadata.
It loads the CCXT market table and resolves exchange-native market IDs to CCXT
unified symbols before placing orders. Local symbols can route only to Indodax or
Tokocrypto; foreign symbols can route only to MEXC, OKX, Binance or Bybit.

Provide complete API key/secret pairs under the matching `exchanges` entries for
every venue intended for live execution. Additional venues without credentials are
not constructed and fail closed if a signal attempts to use them. Use read/trade
permissions only; withdrawal permissions are not needed and should remain disabled.
The default local and futures venue names must also match configured credentials.
For Binance the CCXT market type is `future`; OKX, MEXC and Bybit use `swap`.

A scanner allowlist and executor credential set must agree. Enabling a venue in the
scanner does not create credentials or grant the executor permission to trade there.

## Notifications

The Redis trade-event channel now carries risk rejection events for insufficient
balances, balance-read failures, unexplained balance mismatches, Spot order API
failures, and failed Futures hedges. Telegram distinguishes a verified Spot rollback
from an unverified/failed rollback; the latter is an urgent manual-reconciliation
condition.

## Safety and rollout

This change does not place live orders or enable LIVE mode. Verify the generated
market intersections, account permissions, quote balances, contract multipliers,
minimum order sizes, precision, funding intervals, and actual order/position
reconciliation in paper/test environments before enabling live execution.

The Supabase schema from the safety branch is a versioned durable-data foundation.
Its runtime persistence adapter is still a separate integration: Redis remains the
live coordination layer, and the migration must be reviewed and applied to a
non-production Supabase project before production use.
