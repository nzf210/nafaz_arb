# MEXC public perpetual WebSocket

This package owns public REST market data and now includes a deterministic
per-contract local-book state manager. A per-contract connection supervisor is wired into `LiveAdapter` when `EnableWebSocketFeed` is enabled. It subscribes before fetching the REST snapshot, buffers up to 512 events during bootstrap, replays contiguous updates, sends JSON ping heartbeats, reconnects with exponential backoff, and resubscribes active contracts. It caps concurrent MEXC connections at 20; excess contracts remain REST-only.

## Verified contract protocol

- Endpoint: `wss://contract.mexc.com/edge`.
- Subscribe: `{"method":"sub.depth","param":{"symbol":"BTC_USDT","compress":false}}`.
- Unsubscribe: `{"method":"unsub.depth","param":{"symbol":"BTC_USDT"}}`.
- Keepalive: send `{"method":"ping"}`; expect `channel:"pong"`.
- Depth channel: `push.depth`; event includes `symbol`, `data.bids`,
  `data.asks`, and server timestamp `ts`.
- Current payloads may provide `begin` and `end` version bounds; older payloads
  expose a single `version`. The manager supports both forms.
- Rows contain price and absolute contract volume. Convert volume to base quantity
  using the discovered `contractSize`; zero removes a price level.
- REST bootstrap: `GET /api/v1/contract/depth/{symbol}`.
- Require exact continuity: the next `begin` must equal previous `end + 1`.
  On a gap, invalidate the local book and rebuild from a fresh full REST depth
  snapshot before publishing again; never stitch a discontinuous book.

Official reference: <https://www.mexc.io/api-docs/futures/update-log> and
<https://mexcdevelop.github.io/apidocs/contract_v1_en/>.

## Implemented state manager

`PerpetualBookManager` provides snapshot bootstrap, begin/end (or legacy version)
validation, absolute contract-volume updates, contract-size normalization,
zero-level deletion, depth sorting/limiting, stale reads, and invalidation on
version gaps or contract-size changes.

## Remaining integration work

The connection supervisor buffers while bootstrapping, replays contiguous
updates, refetches a full REST snapshot after a version gap, pings, reconnects,
and resubscribes. Track health per contract; a connected socket does not mean
every symbol is synchronized.

JSON compression is explicitly disabled in the subscription. Do not decode
compressed frames as JSON. No authenticated account or order endpoint belongs
in this package.
