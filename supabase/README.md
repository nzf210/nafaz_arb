# Supabase database

Supabase Postgres is the durable operational history store. Redis remains the
low-latency coordination layer between the Go scanner and Python executor. Supabase
is deliberately not in the critical exchange-order path: a database outage must not
prevent a hedge, rollback, force-close, or Redis signal delivery.

## Schema and migrations

The versioned migrations create:

- exchange_accounts: metadata and encrypted credential slots (ciphertext only).
- strategy_configs, risk_limits, and market_pairs: strategy and market configuration.
- scanner_opportunities: evaluated scanner snapshots, uniquely keyed by
  (scan_id, pair_key) for retry-safe upserts.
- positions and position_legs: current position lifecycle and spot/perpetual legs.
- orders and order_fills: order lifecycle and immutable fills.
- funding_payments: estimated/confirmed funding settlement ledger.
- closed_trades: realized PnL snapshots.
- balance_snapshots: historical exchange balance observations.
- risk_events, executor_events, and executor_status_snapshots: operational audit data.

The initial baseline migration is kept immutable. Later schema changes are added as
new timestamped files under supabase/migrations/. CI applies every migration in
lexicographic order, replays them to verify idempotency, and runs
supabase/tests/schema_smoke.sql against PostgreSQL 16.

All financial quantities use PostgreSQL NUMERIC. Tables have RLS enabled and
anon/authenticated receive no table access. Backend services use only the
Supabase service-role key; never expose it to Vue, Telegram, or any client.

## Runtime persistence currently implemented

- Go scanner: writes evaluated records to scanner_opportunities through the
  Supabase PostgREST endpoint. Retries transient HTTP/network errors and upserts by
  scan_id,pair_key.
- Python executor: writes executor_events, risk_events, positions,
  position_legs, and verified closed_trades. Position rows use a stable UUID
  carried in the Redis active-position snapshot. Closed trades are upserted by
  trade_key; event/risk records are deduplicated by their event keys.
- Redis remains the live source for pub/sub, signal TTLs, active-position recovery,
  locks, balances and executor coordination. Supabase is not used to coordinate
  real-time orders.

Not yet connected by this integration: normalized orders, order_fills,
funding_payments, balance_snapshots, and executor_status_snapshots.
These remain schema-only until their exchange lifecycle data is wired and tested.

Delivery limitation: current writes use bounded in-process retries and fail open.
There is not yet a durable disk/Redis outbox for events after all retries fail. A long
Supabase outage can therefore leave gaps in the audit/history tables even though the
exchange and Redis workflows continue. Monitor the explicit persistence warnings;
do not treat Supabase history as a complete exchange ledger until an outbox and
reconciliation worker are implemented.

## Runtime configuration

Set these variables in the deployment secret manager or process environment and
inject them into both nafaz-scanner and nafaz-executor:

- SUPABASE_ENABLED=true
- SUPABASE_URL=https://<project-ref>.supabase.co
- SUPABASE_SERVICE_ROLE_KEY=<server-only service role key>

The service-role key is environment-only; the executor no longer reads it from
config.json. SUPABASE_KEY remains a backward-compatible Python environment
alias, but new deployments should use SUPABASE_SERVICE_ROLE_KEY. The example
configuration keeps Supabase disabled and contains no credential value. When
SUPABASE_ENABLED is false or credentials are absent, the integration stays off.
If enabled but misconfigured/unreachable, startup logs a warning and trading-risk
management continues on Redis; this is intentional fail-open behavior for the
persistence sink, not permission to ignore missing audit data.

With Compose, export the variables before starting the stack. Do not put the actual
service-role key in a tracked .env, config JSON, image layer, or command history.
Use your host's secret manager and protect the container environment accordingly.

## Apply migrations

1. Install Supabase CLI and authenticate.
2. Link the intended project:
   supabase link --project-ref <project-ref>.
3. Inspect remote migration history:
   supabase migration list --remote.
4. Review the pending SQL and schema state, then apply:
   supabase db push --remote.
5. Verify the remote migration list and inspect RLS/grants before enabling runtime
   writes.

Do not push migrations to production until the existing project schema and migration
history have been reviewed. Avoid ad-hoc remote schema changes in Studio after
adopting versioned migrations.
