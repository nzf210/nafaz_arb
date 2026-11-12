# OKX market-data adapter

This package owns OKX public REST and public books-channel WebSocket payloads.
It returns shared `domain` models and contains no authenticated order methods.

- `Client.PerpetualMarkets`: SWAP discovery and explicit contract sizing rules.
- `Client.FundingRate`: current funding rate and exchange settlement time.
- `Client.PerpetualOrderBook`: REST bootstrap/recovery depth, protected by the
  package's endpoint-aware limiter and retry policy.
- `BookManager`: persistent WS books channel with snapshot/delta sequence
  validation, stale rejection, heartbeat and reconnect/resubscribe.
- `Client.Metrics`: REST request, retry, timeout and rate-limit counters.

The scanner should consume fresh, sequence-valid WebSocket books as its hot
path. REST remains a bounded recovery and metadata path; it is not unlimited
and must never be used to bypass OKX's published request limits. Public market
data and authenticated account/trading capabilities remain separate.
