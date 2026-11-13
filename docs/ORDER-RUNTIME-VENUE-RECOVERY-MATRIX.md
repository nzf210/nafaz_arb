# Order runtime, venue and recovery audit matrix

Date: 2026-11-13  
Branch: `feat/v2-bootstrap-config-stop-fencing`

This matrix describes the Python executor's active facade and configured venue modules. It separates source-level/runtime-contract verification from live venue evidence. The modular classes in `executor/venues/` provide venue metadata/fallback helpers; create-order calls in the active executor facade still go through the bound CCXT-compatible clients (with the custom Indodax TAPI v2 adapter for Indodax spot).

## Runtime venue matrix

| Venue | Supported runtime leg | Active create-order adapter | Client-ID/recovery behavior | Current evidence and gaps |
|---|---|---|---|---|
| Indodax | Spot | `IndodaxTAPIv2` via `ExchangeOrderMixin` | TAPI v2 signed requests; exact client ID passed through local order params; status mapping includes NEW/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED/EXPIRED | Unit and schema tests; HTTP 429/503 adapter classification. No live delayed-visibility or pending-order contract test in this CI run. |
| Tokocrypto | Spot | CCXT-compatible local client | Logical client ID is rewritten to venue-safe alphanumeric <=32-char wire ID; acknowledged wire alias is persisted in the order registry; lookup includes direct/open/closed/canceled history and paged private order archive | Unit tests for formatting and generic delayed visibility. No live archive lag, duplicate-ID, or partial-fill contract probe in this CI run. |
| MEXC | Futures/swap (and may be configured in another leg if the CCXT client supports it) | Bound CCXT futures client | Futures market type uses `type=swap`; open-short is SELL with reduceOnly=false; close-short is BUY with reduceOnly=true; exact client-ID recovery uses direct/history/trades where exposed by CCXT | Unit-level generic lookup/pending-close contract. No live MEXC eventual-consistency or pending-close probe in this CI run. |
| OKX | Futures/perpetual (and may be configured in another leg if supported) | Bound CCXT futures client | Short/close params include `posSide=short`; close is BUY reduceOnly=true; generic exact client-ID recovery; CCXT history limit fallback exists for venues that reject large page limits | Unit-level position-side and generic lookup contracts. No live OKX delayed visibility or pending-close probe in this CI run. |
| Binance | Futures in `BALANCE_MARKETS`; can be selected as the configured global futures client | Bound CCXT futures client | Generic CCXT client ID and direct/history/trade lookup; Tokocrypto's Binance Cloud wrapper is separate and must not be conflated with Binance exchange | Added fake delayed-visibility fixture. Runtime pair validation now rejects unsupported/wrong-market assignments. No live Binance futures or order-history lag probe in this CI run. |
| Bybit | Futures in `BALANCE_MARKETS`; can be selected as the configured global futures client | Bound CCXT futures client | Generic CCXT client ID and direct/history/trade lookup; venue-specific category/position-mode settings still need validation | Added fake delayed-visibility fixture. No live Bybit futures or pending-close probe in this CI run. |

Unknown venue names resolve to the generic venue metadata object, but this is not evidence that the venue is safe for LIVE order execution. A configured exchange must have a supported CCXT adapter, amount specification, client-ID semantics and recovery endpoints verified before LIVE use.

## Active order side-effect inventory

| Order operation | Facade call | Risk classification | Retry/recovery contract |
|---|---|---|---|
| Spot entry BUY limit | `place_local_limit_buy` → `local.create_limit_buy_order` | Opens spot exposure | Durable PREPARED/SUBMITTING ledger and shared PostgreSQL fence; ambiguous outcomes remain unresolved and block later entries. |
| Futures hedge SELL market | `open_global_short_market` → `global_fut.create_market_sell_order` | Opens short exposure | Durable ledger/fence; recovery routes to futures exact-client-ID lookup. |
| Spot exit SELL limit | `place_local_limit_sell` → `local.create_limit_sell_order` | Reduces spot exposure | Exact client-ID lookup after ambiguous ACK; if not found/inconclusive, pair becomes RECONCILIATION_REQUIRED and dynamic repricing is blocked. |
| Spot rollback/emergency SELL market | `place_local_market_sell` → `local.create_market_sell_order` | De-risking | Only explicit HTTP 429 rejection may be retried. Timeout, reset, 503 and other ambiguous errors are not blindly retried. |
| Futures close BUY market | `close_global_short_market` → `global_fut.create_market_buy_order` with `reduceOnly=True` | Reduces short exposure | Regular exit path blocks on unverified close. Force-close verifies position flat, but a stale non-zero position response after a prior accepted close can still lead a later invocation to submit a new client ID. This is fixed fail-closed: LIVE force-close uses a deterministic position-scoped client ID, requires a durable order registry and exact-ID lookup, and blocks new submissions when the prior close is pending, partial, not found or inconclusive. It does not automatically submit a residual close. |

Cancel/replace paths are not create-order retries: the existing exit flow waits for a terminal old-order state and applies fills before replacement; inconclusive cancel/status blocks replacement. A cancel acknowledgement alone is not proof that no fill occurred.

## Retry and recovery rules

1. Never interpret a timeout, connection reset, 5xx, malformed response or failed history read as a definitive rejection.
2. Never retry an exposure-opening create-order with a new client ID after an ambiguous result.
3. A negative exact-client-ID lookup is only a point-in-time absence. The attempt remains SUBMITTING/UNKNOWN; periodic reconciliation may find it later.
4. A lookup error or unsupported endpoint is not absence. It remains unresolved and blocks entry admission.
5. A found order must match the exact accepted client ID and resolve to one unique venue order. Multiple matches require manual reconciliation.
6. Spot emergency SELL retry is restricted to explicit HTTP 429 rejection. The same stable client ID is retained across that bounded retry.
7. Futures reduce-only close is not put behind entry Global STOP, because that could prevent de-risking. It still requires its own idempotency/pending-order lifecycle.

## Configuration and adapter review boundaries

- `ExecutorConfig` loads local/global venue names, per-venue credentials, testnet/options, exposure caps, per-venue open-order limits, order-failure breaker and venue rate-limit cooldown.
- `create_exchange_client` binds the configured local spot and global futures clients; `for_venue`/ `for_pair` select the correct client for recovery. LIVE CCXT factory now validates the local/global pair against `BALANCE_MARKETS` and rejects unknown venues or wrong-market assignments before client initialization.
- Indodax uses its custom TAPI v2 client for spot; other configured venues use CCXT clients.
- The modular registry names Indodax, Tokocrypto, MEXC and OKX. The active `BALANCE_MARKETS` configuration additionally supports Binance and Bybit futures clients. The six-name matrix is the repository's explicit runtime set; registry presence alone does not make every market type supported. Indodax's custom TAPI v2 adapter is used on the local spot path only; selecting it as a generic global futures client is not supported.
- No credential values, API keys, signed payloads or full authorization headers belong in logs or audit evidence.
- Go scanner/exchange source has no order-create callsites in the scanned paths; it is not the Python executor's order submission adapter.

## Test coverage added/required

- Added PostgreSQL integration test launches independent executor processes and a parent-controlled fake venue request, kills the executor after SUBMITTING is committed while the venue remains active, and asserts a peer cannot enter its submit fence.
- Added PostgreSQL test for HMAC-authenticated manual resolution, invalid signature rejection, and append-only evidence audit.
- Added delayed visibility fixtures for Indodax, Tokocrypto, MEXC, OKX, Binance and Bybit, plus pending reduce-only close visibility fixtures. These are deterministic fake endpoint tests, not live venue contract evidence.
- Still required before claiming venue contract completion: controlled sandbox/live-paper probes per configured venue for (a) delayed client-ID visibility after ACK loss, (b) open/pending reduce-only close, (c) partial fill followed by cancel/close, (d) duplicate client ID behavior, (e) order-history pagination/retention, and (f) status endpoint staleness.
- Force-close futures now uses deterministic client-ID reuse and exact-ID recovery; its regression test blocks a duplicate when visibility is delayed. Partial/pending outcomes still require reconciliation.

## Release conclusion

The source-level inventory and fake contract tests improve regression coverage, but they do not prove every exchange API's consistency or pending-close semantics. Do not use this matrix as a LIVE certification. The remaining force-close reduce-only retry gap and the absence of real per-venue probes keep this release gate closed.


### R-26 implementation note

The force-close futures path now uses a deterministic client-order ID based on the persisted position open timestamp. A prior order intent must be located in the durable registry and resolved through exact-ID lookup before the path can proceed. Missing visibility, lookup failure, pending status or partial fill blocks another submit. This closes the duplicate-new-client-ID retry path, but does not replace venue-specific pending-close probes.
