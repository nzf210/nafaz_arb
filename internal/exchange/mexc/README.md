# MEXC public perpetual WebSocket

This package currently owns public REST discovery, funding, and depth
normalization. Perpetual WebSocket books are not yet connected to the scanner.

## Protocol verified against MEXC contract API documentation

- Endpoint: `wss://contract.mexc.com/edge`.
- Subscribe to incremental depth:
  `{"method":"sub.depth","param":{"symbol":"BTC_USDT","compress":false}}`.
- Unsubscribe:
  `{"method":"unsub.depth","param":{"symbol":"BTC_USDT"}}`.
- Keepalive: send `{"method":"ping"}`; expect a `pong` channel response.
- Depth event channel: `push.depth`; event includes `symbol`, `data.bids`,
  `data.asks`, `data.version`, and server timestamp `ts`.
- Each price row contains price and contract volume (additional order-count
  columns may be present). Contract volume must be multiplied by the discovered
  `contractSize` before exposing base-asset quantities.
- REST bootstrap endpoint: `GET /api/v1/contract/depth/{symbol}`.
- Version continuity is strict: each applied event must be exactly previous
  version + 1. A discontinuity requires full reinitialization.

Official reference: https://mexcdevelop.github.io/apidocs/contract_v1_en/

## Required local-book state machine

1. Connect and subscribe with JSON compression disabled; do not attempt to decode
   gzip/protobuf frames as JSON.
2. Buffer updates while fetching the REST depth snapshot and its version.
3. Initialize bid/ask maps from the snapshot, then discard buffered updates whose
   version is not newer than the snapshot.
4. Apply newer updates in increasing version order. Quantities are absolute;
   zero removes a price level and non-zero replaces it.
5. Require exact version continuity. On a gap, stale frame, malformed update, or
   contract-size change, mark the symbol unsynchronized and bootstrap again.
6. Publish only a synchronized two-sided book; preserve exchange timestamp and
   local receive timestamp separately.
7. Batch subscriptions within venue connection/message limits, ping on schedule,
   reconnect with bounded exponential backoff, and resubscribe the active symbol set.

## Integration contract

Implement a package-owned `PerpetualBookManager` with dynamic symbol
subscriptions, bounded buffers, per-symbol sequence state, snapshot recovery,
contract-size normalization, and health metrics. REST remains the recovery path
and fallback until deterministic tests cover snapshot races, duplicate/stale
versions, gaps, reconnect, and contract-size scaling.

Do not treat a connected socket as proof that all symbols are healthy. Expose
per-symbol synchronized/stale status so scanner policy can fail closed only for
the affected market.

No authenticated account or order endpoint belongs in this package.
