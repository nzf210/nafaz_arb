# Tokocrypto public spot WebSocket

The package now contains a deterministic local-book state manager. A per-symbol connection supervisor is now wired into `LiveAdapter` when `EnableWebSocketFeed` is enabled. It subscribes before fetching the REST snapshot, buffers up to 512 events during bootstrap, replays buffered events through the sequence bridge, reconnects with exponential backoff, and resubscribes active symbols. The supervisor caps concurrent Tokocrypto connections at 100; excess symbols stay on REST.

## Verified MBX protocol

- WebSocket host: `wss://stream-cloud.tokocrypto.site/stream`.
- Raw diff-depth path: `/ws/<lowercase-symbol>@depth`; combined stream is also
  available through `/stream?streams=<symbol>@depth`.
- Dynamic subscription frame on a combined connection:
  `{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":1}`.
- Snapshot: `GET https://www.tokocrypto.site/api/v3/depth?symbol=BTCUSDT&limit=1000`.
- Snapshot fields: `lastUpdateId`, `bids`, `asks`.
- Diff event fields: `s`, `U`, `u`, `b`, `a`, and `E`.
- Event quantities are absolute; zero removes a price level.
- A connection is valid for 24 hours; the server sends ping control frames and
  expects pong responses.

Official reference: <https://www.tokocrypto.com/apidocs/>

## Implemented state manager

`BookManager` provides snapshot bootstrap, first-event bridging
(`U <= lastUpdateId+1 <= u`), strict subsequent continuity
(`U == previous u+1`), absolute quantity updates, zero-level deletion,
sequence-gap invalidation, depth sorting/limiting, and synchronized-book reads.

## Execution and recovery boundary

The public market-data supervisor is integrated with `LiveAdapter` and remains
opt-in through `EnableWebSocketFeed`. REST is still the fallback for stale,
unsynchronized, or connection-capped symbols.

The Python executor's authenticated Tokocrypto spot path uses the official
CCXT adapter and requires the account-issued broker prefix:

- Set `TOKOCRYPTO_BROKER_ID` or `exchanges.tokocrypto.options.broker_id`.
- The prefix is preserved exactly (including separators) and the generated
  suffix is bounded to the venue's 32-character client-ID limit.
- A missing broker ID blocks a new order locally; it is not replaced with a
  guessed prefix.
- Limit orders use CCXT `postOnly=True`, which serializes to Tokocrypto
  `LIMIT_MAKER` (type 7).
- Tokocrypto documents that `clientId` uniqueness is not enforced. It is used
  for correlation and restart lookup only; an ambiguous submission is never
  blindly retried.
- The signed All Orders endpoint requires a symbol. Recovery therefore queries
  symbols from durable order intents and tracked positions rather than calling
  `fetch_open_orders(None)`.

The public depth supervisor supports both match engines. MBX symbol type 1
uses the raw stream URL
`wss://stream-cloud.tokocrypto.site/stream/ws/{symbol}@depth`; NextMe symbol
type 3 connects to `wss://stream-toko.2meta.app` and sends a SUBSCRIBE frame.
Both use their matching REST snapshot host. NextMe events carry `pu` (previous
final update ID), which is enforced for sequence continuity; combined-stream
`data` envelopes and subscription acknowledgements are decoded/ignored safely.
Symbol type 2 is excluded rather than sent to the wrong host. No authenticated
user WebSocket stream is enabled by this package.
