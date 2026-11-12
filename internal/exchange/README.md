# Exchange adapter boundaries

`LiveAdapter` remains the compatibility facade used by the scanner. Venue
HTTP implementations are being moved into independent packages. Each package
owns its payload mapping and returns shared `domain` models.

```text
internal/exchange/
  indodax/       # isolated spot markets, order books, USDT/IDR ticker
  tokocrypto/    # isolated spot markets and order books
  okx/           # next extraction: spot/perpetual, funding, books WS and limiter
  mexc/          # next extraction: spot/perpetual, funding and books
  contracts/     # future normalized interfaces and capability definitions
  registry/      # future venue discovery and adapter selection
  (facade)       # current LiveAdapter compatibility layer
```

## Migration status

### Isolated packages

- `indodax.Client`: pair discovery, quantity increment normalization, spot
  order-book depth and USDT/IDR ticker. Uses the shared HTTP transport from the
  facade and has independent HTTP normalization tests.
- `tokocrypto.Client`: symbol discovery, trading-enabled filtering, LOT_SIZE
  normalization and spot order-book depth. Uses the shared HTTP transport and
  has independent tests.
- `LiveAdapter` delegates Indodax and Tokocrypto spot endpoints to these
  packages. The facade retains health tracking, caching and scanner-facing
  interfaces for backward compatibility.

### Still in the compatibility facade

- OKX: perpetual discovery, funding, REST order books, websocket book manager,
  limiter and contract-size metadata.
- MEXC: spot/perpetual discovery, funding, REST order books and contract-size
  metadata.
- Cross-venue market matching, health aggregation and shared websocket cache.

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
- Keep `LiveAdapter` until parity tests cover markets, books, funding,
  exposures, health and error classification for every migrated venue.

## Next migration order

1. MEXC: spot and perpetual REST market-data client, then funding and depth.
2. OKX: perpetual REST client, then limiter and websocket manager.
3. Move cross-venue aggregation into a registry/composite adapter.
4. Separate authenticated account and execution capabilities from read-only
   market data. Do not enable order execution just because market data works.
