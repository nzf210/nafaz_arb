# Tokocrypto adapter

Owns Tokocrypto-specific public spot endpoints and payload mapping:

- Symbol discovery, trading-enabled filtering and LOT_SIZE normalization.
- Spot order-book depth retrieval and normalized levels.

The adapter exports only shared `domain` models. Authenticated account access
and order execution are intentionally separate future capabilities.
