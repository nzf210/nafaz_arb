# MEXC public market-data adapter

This package owns MEXC public REST payloads and normalizes them into shared
`domain` models.

Supported read-only capabilities:

- Perpetual contract discovery and contract amount rules.
- Current perpetual funding rate and settlement interval.
- Spot order-book depth.
- Perpetual order-book depth, scaled from contracts into base-asset quantity.

The parent `exchange.LiveAdapter` remains responsible for scanner compatibility,
market health tracking, and the contract-size lookup shared with the scanner.
No authenticated account or order endpoint is implemented here.

All methods accept a caller context and use the shared HTTP client supplied to
`New`, allowing the application transport, timeout, and test server to remain
centrally configured.
