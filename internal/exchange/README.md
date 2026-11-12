# Exchange adapter boundaries

Scanner and strategy code depend on `exchange/contracts`, not on a venue
implementation. `exchange/registry` composes adapters and routes normalized
market requests. The existing `LiveAdapter` remains the compatibility facade
while individual venue capabilities are migrated behind that boundary.

```text
internal/exchange/
  contracts/    # venue-neutral read-only market-data capabilities
  registry/     # registry, composite adapter and optional capability forwarding
  indodax/      # spot REST market data and USDT/IDR ticker
  tokocrypto/   # spot REST market data
  mexc/         # spot/perpetual REST market data and funding
  okx/          # REST limiter/client and sequence-validated books WebSocket
  (facade)      # LiveAdapter compatibility, cache, health and scanner routing
```

## Migration status

- `indodax.Client`: pair discovery, amount-step normalization, spot depth and
  USDT/IDR ticker.
- `tokocrypto.Client`: spot symbol discovery, symbol-type-aware depth, LOT_SIZE normalization; MBX and NextMe depth supervisors.
- `mexc.Client`: perpetual contract metadata, funding, spot depth and
  contract-size-adjusted perpetual depth.
- `okx.Client`: SWAP instrument discovery, funding rate, REST depth recovery,
  per-endpoint token-bucket limiter, bounded concurrency, retry/backoff and
  limiter metrics.
- `okx.BookManager`: persistent public books-channel WebSocket, snapshot plus
  incremental delta application, sequence-chain validation, heartbeat,
  reconnect/resubscribe, subscription batching and stale-book rejection.
- `registry.Composite`: aggregates market discovery and routes normalized
  symbol requests. It forwards optional health, wallet, WS-activity and FX
  capabilities. The LIVE entrypoint currently registers the compatibility
  facade as `default`; venue-specific registrations can replace it as adapters
  reach full interface parity.
- Scanner service accepts `contracts.ReadOnlyAdapter`. The legacy
  `exchange.ReadOnlyAdapter` remains a type alias for source compatibility.

## Scanner market-data transport policy

- Use WebSocket books as the hot path whenever the relevant feed is enabled and
  the book is fresh and sequence-valid.
- Use REST for instrument metadata, periodic funding snapshots, initial
  discovery, bootstrap/recovery and a controlled fallback when a WS book is
  absent, stale or invalid.
- REST calls remain subject to venue-specific rate limits. OKX limiter is
  isolated inside `okx.Client`; the scanner does not own exchange throttling.
- WebSocket does not mean unlimited throughput: exchanges enforce connection,
  subscription, message and private-trading limits. Feed managers must batch
  subscriptions, reconnect with backoff, validate sequence continuity and
  expose staleness/health.
- Do not enable a WS-only fail-closed scanner mode until every configured
  spot/perpetual venue has a tested WS book feed and a safe snapshot/recovery
  path. Tokocrypto spot and MEXC perpetual now have bounded supervisors, but
  REST fallback remains mandatory for unsupported/capped/stale symbols.
- Authenticated account and order execution remain separate capabilities and
  are not implemented by these public market-data clients.

## Rules

- Venue packages own their payload mapping, symbol translation, precision,
  contract sizing, rate limits, retries, WS sequence handling and health.
- Strategies consume `domain.Market`, `domain.OrderBook` and
  `domain.FundingRate` through neutral contracts only.
- Never infer contract size, amount units, tick size or market type when the
  venue publishes explicit instrument metadata.
- Capabilities are explicit and independent: spot, perpetual, funding,
  WebSocket books, account state and order execution.
- Keep fail-closed behavior for unknown position state, stale books, missing
  amount rules, sequence gaps and unverified execution capabilities.
- Retire facade compatibility code only after parity tests cover markets,
  books, funding, exposures, health, error classification and recovery.
\n\n## Tokocrypto and MEXC perpetual WebSocket supervisors\n\nIn LIVE mode, `EnableWebSocketFeed` defaults to enabled (explicit config or
`ENABLE_WEBSOCKET_FEED=false` can opt out). `LiveAdapter` starts per-symbol supervisors for Tokocrypto MBX/NextMe spot and MEXC perpetual depth. Each worker subscribes before snapshot retrieval, buffers at most 512 frames while bootstrapping, applies sequence-validated updates, invalidates and resnapshots after gaps/overflow, reconnects with exponential backoff, and shuts down with the adapter. Connection/freshness health is tracked per symbol. Limits are 100 Tokocrypto and 20 MEXC concurrent connections; uncovered symbols use REST. MEXC uses JSON ping heartbeats and `compress:false`; Tokocrypto relies on WebSocket ping/pong control frames. This does not alter authenticated execution paths.\n