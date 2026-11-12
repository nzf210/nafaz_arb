# Tokocrypto public spot WebSocket

This package currently provides REST discovery and REST depth only. The generic
root-package WebSocket stream is **not** a Tokocrypto adapter and must not be
used as one: Tokocrypto requires a symbol-specific stream subscription and a
snapshot/delta synchronization state machine.

## Protocol verified against the official API documentation

- Public stream host for MBX symbols: `wss://stream-cloud.tokocrypto.site/stream`.
- Diff-depth stream: `<lowercase-symbol>@depth` (or `@depth@100ms`).
- Subscribe frame: `{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":1}`.
- Diff event fields: `s` (symbol), `U` (first update ID), `u` (final update ID),
  `b` (bid absolute-quantity changes), `a` (ask absolute-quantity changes),
  and `E` (event time).
- Snapshot: `GET /api/v3/depth?symbol=BTCUSDT&limit=1000`; response carries
  `lastUpdateId`, `bids`, and `asks`.
- Stream connections are rotated by the venue after 24 hours. The server sends
  ping frames and expects pong handling.

Official reference: https://www.tokocrypto.com/apidocs/

## Required local-book state machine

1. Connect and subscribe before requesting a snapshot; buffer incoming events.
2. Fetch the REST snapshot and initialize both sides plus `lastUpdateId`.
3. Discard buffered events with `u <= lastUpdateId`.
4. The first applied event must satisfy
   `U <= lastUpdateId + 1 && u >= lastUpdateId + 1`.
5. Every subsequent event must continue the sequence; a discontinuity invalidates
   the local book and triggers a fresh snapshot/resync.
6. Apply absolute quantities: zero deletes a price, positive quantity replaces it.
7. Publish only a two-sided, synchronized book. Never publish a partial book while
   snapshot bootstrap or resynchronization is in progress.

## Integration contract

Implement a package-owned `BookManager` with dynamic symbol subscriptions,
bounded per-symbol buffers, snapshot singleflight, sequence-gap recovery,
heartbeat/reconnect, and stale timestamps. Keep REST fallback in the facade until
the manager has passed deterministic snapshot/delta/reconnect tests. Do not infer
symbol coverage from a healthy socket: health must be tracked per subscribed symbol.

Tokocrypto symbol type 2/3 hosts and symbol formats are not interchangeable with
the MBX symbol type 1 feed. Initial implementation should explicitly accept only
markets whose metadata confirms MBX symbol type 1; unsupported types stay REST-only.

No authenticated user stream or order endpoint belongs in this package.
