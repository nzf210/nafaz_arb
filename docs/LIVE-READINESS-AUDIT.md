# Live-readiness audit — 2026-11-13

## Executive decision

**NOT READY for live trading.** This branch contains defensive implementation changes, but CI has not yet passed and no authenticated exchange smoke test, sandbox execution, paper-trading soak, or recovery drill has been performed. No live orders were sent during this audit.

The audit covers execution submission, private account data, position and order reconciliation, sizing and risk limits, emergency rollback/force-close, restart recovery, CEX API compatibility, and CI coverage.

## Findings and implementation

| Severity | Finding | Change in this branch | Status |
|---|---|---|---|
| P0 | Live sizing could continue after a private balance read failed | Fail closed; halt both venues and do not size/submit without a valid spot + futures balance snapshot | Implemented; CI pending |
| P0 | Final dual-leg funding gate treated missing/invalid balances as fundable | Require finite non-negative available balances and a finite positive per-leg requirement | Implemented; CI pending |
| P0 | Futures leverage was not verified before opening a short | Read leverage for the target contract; require short leverage exactly 1x; halt on missing, malformed, or >1x values | Implemented; authenticated runtime verification pending |
| P0 | Executor assumed account position modes without checking them | Read account position mode before readiness. OKX must be Hedge Mode; MEXC/other supported venues must be One-way Mode | Implemented; authenticated runtime verification pending |
| P0 | Recovery reconciled bot-tracked symbols but could miss manual/orphan futures positions on other symbols | Enumerate all configured futures positions account-wide and compare actual short quantity to durable managed exposure; unknown rows or missing contract size halt the venue | Implemented; exchange smoke test pending |
| P0 | OKX close requests included `reduceOnly` with `posSide=short` | In OKX Hedge Mode close with buy + `posSide=short`; omit `reduceOnly` (OKX documents reduceOnly for net mode) | Implemented |
| P0 | Emergency market-sell retry could duplicate an accepted order after a lost acknowledgement | Submit once; reconcile by client order ID; never blindly retry; unresolved outcome remains reconciliation-required | Implemented; venue-specific history lookup needs smoke test |
| P1 | Missing futures contract size could be silently treated as 1 | Require authoritative positive finite `contractSize` in book normalization, recovery, rollback, and force-close verification | Implemented |
| P1 | Live exposure cap could be disabled with zero | LIVE startup requires a positive cap; runtime rejects invalid cap, planned notional, or non-finite exposure state | Implemented |
| P1 | Live risk could run without durable risk/order journal | LIVE startup requires Supabase enabled and URL/key configured | Implemented; actual service-role permissions/durability pending |
| P1 | MEXC Futures adapter used the legacy API base and old contract metadata endpoint | Switch to `https://api.mexc.com`, `/api/v1/contract/detail/country`; accept object/list payloads and filter API-enabled perpetual contracts | Implemented; live public API smoke test pending |
| P1 | CCXT dependency was floating | Pin CCXT to 4.5.92 for reproducibility | Implemented |
| P1 | CI did not trigger for audit branches/PRs | Add `audit/**` push and PR triggers; update the PR base workflow so PR #30 receives checks | Implemented |
| P2 | Private WebSocket event delivery is not enabled in the deployment | REST polling/reconciliation remains the active mechanism; latency and rate-limit behavior need soak testing | Open |

## Required live account configuration

These are preconditions, not automatic account mutations:

- **OKX:** Hedge Mode; short leverage set to 1x. The adapter uses `posSide=short` and does not send `reduceOnly` on hedge-mode closes.
- **MEXC Futures:** One-way Mode; short leverage set to 1x.
- **Other supported futures venues:** One-way Mode and 1x leverage unless the adapter is explicitly revised and tested for another mode.
- API credentials must have the minimum read permissions for balances, positions, open orders, order history, and trades, plus trade permission for order placement/cancellation. Do not grant withdrawal permission.
- Configure exchange IP allowlists where supported.
- Configure a positive per-exchange exposure cap (zero disables it and is rejected in LIVE). Review/override the current defaults for daily loss (50 USDT), slippage (0.3%), fee reserve (0.1%), and consecutive order failures (50) against account equity, fee tier, and strategy. These defaults are not calibrated to your account.
- Supabase must be reachable and able to read/write the durable risk state and order-intent journal. Keep its server-side key outside the repository.
- Run **one active executor instance only**. Pair execution locks are process-local; the recovery lease protects startup recovery but is not a live-trading leader/fencing lease. Do not horizontally scale the executor until a distributed live-execution lease is implemented and tested.

## CEX documentation notes

- Tokocrypto's current API reference states that `clientId` is not checked for uniqueness. It also has mixed symbol-type documentation: the enum table lists type 2 as NEXT, while the depth and NextMe user-stream mappings explicitly describe type 3. The adapter currently supports type 1 and type 3 and deliberately skips type 2 until the exchange clarifies the mapping. Do not infer that type 2 is safe to route to either depth host.
- Tokocrypto's query-order endpoint requires `orderId`; acknowledgement recovery therefore searches open/terminal order history and trades by exact client ID. If those reads cannot prove the result, the code does not resubmit.
- MEXC's current Futures API documentation announces the `api.mexc.com` base domain and the country contract-detail endpoint. The adapter was updated accordingly.
- CCXT documents `fetchPositionMode` and `fetchLeverage` for both OKX and MEXC; the adapter uses these read-only calls and fails closed if the installed client/API cannot return the expected fields.
- OKX order documentation distinguishes net-mode `reduceOnly` from hedge-mode `posSide`; the close adapter now matches Hedge Mode.

## Validation still required

1. Latest CI run for head 48eb0a1e: Python tests passed, Go unit tests passed, and Supabase schema/migration checks passed. Docker executor image build, Go race tests, and Go vet were still running at the time of this update; do not treat the pipeline as fully green until all jobs finish successfully.
2. Authenticated **read-only** smoke test for each configured CEX: balances, position mode, leverage, all open positions, open orders, order history, and private trade history. No secrets should be placed in source or chat.
3. Paper trading with real market data and account reads, but no order submission.
4. Failure-injection/restart drills: spot fill + futures reject, partial fills, lost create-order acknowledgement, lost cancel acknowledgement, process restart with pending orders, Supabase outage, Redis outage, and stale market data.
5. Controlled canary only after all prior gates pass, with a user-defined minimal notional and operator monitoring.

## Remaining account-source gap: Go scanner

The Python executor now performs authenticated account-wide position/mode/leverage checks, but the Go scanner's `LiveAdapter.OpenExposures()` still deliberately returns an error because the Go adapter has no authenticated account-data implementation. In `config.example.json`, `check_open_positions` defaults to true; with that setting, the scanner marks position state UNKNOWN and suppresses eligible opportunities. Turning that setting off would let the public-data scanner produce candidates, but is safe only if every candidate is consumed by this Python executor, which independently performs the account checks. It is not safe for any alternate execution consumer. Implementing private account APIs in Go is a separate scope requiring an explicit decision about credential ownership and which CEX accounts the scanner must inspect.

## Explicitly not done

- No exchange API keys were requested or used.
- No real balances, positions, or open orders were queried.
- No order was submitted, cancelled, or closed.
- No account leverage or position mode was changed.
- No live-trading readiness claim is made by this audit.
