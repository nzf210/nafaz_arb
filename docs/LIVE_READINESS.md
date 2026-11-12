# V2 live-readiness gate

This checklist is the release gate for unattended live operation. A green unit
test suite or successful Docker build is necessary, but not sufficient. Do not
enable live order submission merely because the scanner reports an opportunity.

## Automated gates

- [x] CI enforces the 500-line limit across all production Go, Python and SQL
      files (tests are excluded from the production-module limit).
- [x] Go unit tests, Python executor tests, Docker image builds and Supabase
      migration/RLS checks are part of the Test workflow.
- [x] Go race tests and `go vet` are part of the Test workflow.
- [x] Redis position and active-order reads fail closed on malformed or
      inconsistent registry state.
- [ ] SonarQube quality gate: requires repository variable
      `SONAR_HOST_URL` and secret `SONAR_TOKEN`; not considered passed until
      the workflow reports a successful scan.

## Exchange-by-exchange acceptance

For each configured venue (Indodax, Tokocrypto, OKX and MEXC):

- [ ] Verify the correct account, API key scope and IP allowlist using the
      read-only live preflight. Keep withdrawal permission disabled.
- [ ] Verify market identity, trading status, tick/lot size, min quantity,
      contract multiplier and quantity unit against the venue.
- [ ] Verify signed balances, open orders, positions, fills and order lookup.
- [ ] Verify rate-limit handling, retry-after behavior, websocket sequence
      continuity, stale-book detection and REST fallback.
- [ ] Verify ambiguous order submission recovery and idempotent client order IDs.
- [ ] Verify reduce-only/close-only semantics for derivatives and reconcile
      exchange-side state after process restart.

## Deployment and operational acceptance

- [ ] Run the full stack in MOCK mode, then venue testnet/demo where supported.
- [ ] Confirm Redis and Supabase durable state, outbox replay, RLS, backups and
      recovery after a forced process/container restart.
- [ ] Confirm API bearer-token authentication, TLS termination, secret
      injection, least-privilege credentials and network restrictions.
- [ ] Confirm Telegram alerts, halt/cancel-all controls, stale-data alarms,
      disk/outbox capacity alarms and operator runbooks.
- [ ] Exercise kill switch, drawdown limits, per-venue exposure limits, daily
      loss limits, order-size caps and reconciliation failure behavior.
- [ ] Complete a manually authorized, minimum-size live order/cancel/close
      exercise for each enabled market type. This is an operator action and
      must never be run automatically by CI or by the assistant.

## Strategy acceptance

The legacy spot-long/perpetual-short scanner and the new research analysis
pipeline are separate. The cross-venue spot dislocation strategy is read-only
and requires fresh verified account fee profiles. It assumes inventory is
already available on both venues and does not model transfer/withdrawal risk.
It is not connected to the executor.

Before any new strategy can trade live, it needs a strategy-specific backtest,
out-of-sample and paper results, fee/funding/borrow model, market-specific
amount rules, liquidity/impact model, portfolio exposure checks, entry/exit
state machine, kill-switch integration and independent risk approval.

## Current limitations

- Each CEX package adapter now owns its market discovery/cache and venue
  capability boundary. The top-level `LiveAdapter` remains the shared book,
  funding, WebSocket/REST-fallback and health transport. Moving those network
  lifecycles fully into each CEX package and completing parity tests is ongoing.
- The read-only analysis endpoint does not authorize execution.
- SonarQube and real account preflight cannot be certified without repository
  credentials and operator-controlled exchange access.
- No live order was submitted as part of this refactor.

The project is **not production-live certified** until every applicable
acceptance item above is checked with evidence and the operator explicitly
authorizes the final live exercise.
