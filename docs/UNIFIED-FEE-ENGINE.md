# Unified Fee Engine

## Fee model

All rates are percentage points: `0.10` means `0.10%`. The scanner and executor use the same four-fill round-trip model:

- Spot entry: maker in `MAKER_TAKER`, taker in `TAKER_TAKER`.
- Spot exit: taker.
- Perpetual entry: taker.
- Perpetual exit: taker.

The estimated fee total is calculated from each executable fill notional, not by multiplying one flat rate by the combined two-leg notional. The configured fee-asset surcharge is applied to all four notionals in both scanner and executor. The executor uses the same formula for floating PnL, the minimum profitable spot exit floor, and close PnL estimates. The spot exit floor algebraically includes its own exit fee.

## Account fee sources

The executor requests authenticated account rates for the exact pair before a LIVE entry:

- Tokocrypto Spot: signed `GET /open/v1/account/spot`; `makerCommission` and `takerCommission` are account commission values in basis points and are converted to percentage points.
- OKX, MEXC, Binance USD-M, and Bybit linear: CCXT `fetch_trading_fee(symbol)`, where supported by the installed CCXT adapter.
- Indodax Spot: the current CCXT adapter does not implement `fetchTradingFee`. Its explicit `fee_profiles.indodax` / global configuration remains the source until a supported account fee endpoint is integrated. This is reported as partial/config fee coverage, never as a verified dynamic rate.

A failed or unsupported endpoint never becomes a zero fee. The scanner keeps the configured rate for that venue/market and marks the profile `CONFIG` or `PARTIAL_EXCHANGE_API`.

## Cache and propagation

- Executor caches per-client, per-symbol fee responses for 5 minutes.
- Fresh account snapshots are published to Redis under symbol-scoped `arb:fee_profile:{exchange}:{market}:{symbol}` keys with a 5-minute TTL.
- Scanner accepts only fresh, verified `EXCHANGE_API` snapshots. Spot and perpetual rates are resolved independently; one dynamic leg plus one configured leg is marked `PARTIAL_EXCHANGE_API`.
- The next scanner cycle uses the cached account rates when available.
- The executor revalidates the scanner's net estimate against the authenticated rates before sending a new order. If fees reduce the estimated net to zero or below, the entry is rejected with `NET_EDGE_ERODED_BY_ACTUAL_FEES`.

## Position snapshots and limitations

Fee rates and their source/timestamp are stored in the active-position Redis record. Existing positions without a fee snapshot use a conservative legacy fallback until closed; newly opened positions retain their entry-time fee snapshot.

This engine calculates fee estimates from account rates. It does not yet reconcile each actual exchange commission/discount asset from every fill into a fill-level fee ledger. Therefore close PnL remains fee-estimated, not final exchange-ledger realized PnL. A separate fill-commission reconciliation change is required before presenting it as exact realized net PnL.

## Regression coverage

`tests/test_fee_engine.py` covers all 8 combinations of Indodax/Tokocrypto Spot with OKX/MEXC/Binance/Bybit Perpetual, both execution modes, explicit zero maker rates, invalid rates, and exit-floor algebra. `tests/test_exchange_fee_provider.py` covers Tokocrypto account commission conversion and the Indodax unsupported-endpoint partial fallback. Go Redis tests cover freshness and verified-source filtering.
