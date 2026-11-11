# Exchange adapter boundaries

The current `LiveAdapter` is a compatibility facade. Venue-specific behavior is
still mixed in the `exchange` package; splitting files by API resource is not
the same as isolating each CEX. Keep existing behavior stable while migrating
toward the following structure:

```text
internal/exchange/
  contracts/       # normalized interfaces and capability types
  registry/        # venue discovery and adapter selection
  indodax/         # Indodax spot markets, books, balances and order rules
  tokocrypto/      # Tokocrypto spot markets, books and order rules
  okx/             # OKX spot/perpetual, funding, books WS and rate limits
  mexc/            # MEXC spot/perpetual, funding, books and rate limits
  facade/          # temporary LiveAdapter compatibility wrapper
```

## Rules

- A venue package owns its REST/WS payloads, symbol translation, precision,
  contract sizing, rate limits, retry policy and health checks.
- Strategies and scanner code consume normalized `domain.Market`,
  `domain.OrderBook`, `domain.FundingRate` and exposure records only.
- Never infer contract size, amount units, tick size or market type from a
  symbol when the venue publishes explicit instrument metadata.
- Capabilities are explicit: spot, margin, perpetual, dated futures, options,
  borrow/repay, funding, websocket books and order execution are independent.
- Keep credentials and signing inside the venue adapter. Do not leak exchange
  payload structs or secrets into domain or strategy packages.
- Preserve fail-closed behavior for unknown position state, stale books,
  missing amount rules, sequence gaps and unverified execution capabilities.
- Migrate one venue at a time behind the existing facade; do not remove the
  facade until parity tests cover markets, books, funding, exposures and errors.

## Current implementation inventory

- Indodax and Tokocrypto: spot market discovery and spot order books.
- OKX and MEXC: perpetual market discovery, funding and perpetual books.
- MEXC: spot market books are also supported.
- OKX: dedicated public books websocket manager and REST fallback.

The next safe migration is to extract OKX and MEXC as independent read-only
venue adapters first, then Indodax and Tokocrypto. Order execution should be a
separate capability and must not be enabled merely because market data works.
