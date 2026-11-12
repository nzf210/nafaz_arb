# Indodax adapter

This package owns Indodax public market-data HTTP behavior:

- Pair discovery and quantity-increment normalization.
- Spot order-book retrieval and level normalization.
- USDT/IDR ticker conversion.

It returns shared `domain` types and does not expose Indodax wire payloads.
Authentication, order placement and account state are intentionally not
implemented here; they must be added as separate capabilities with independent
tests and explicit execution safeguards.
