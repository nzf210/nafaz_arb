# Supabase database

NAFAZ uses Supabase PostgreSQL as an asynchronous audit and historical-data sink.
Redis remains the low-latency coordination layer between the Go scanner and Python
executor; it is not replaced by Supabase.

## Schema

The versioned migrations create:

- `exchange_accounts`: exchange account metadata and encrypted credential slots.
  Credential columns are ciphertext-only; never store API secrets as plaintext.
- `strategy_configs` and `risk_limits`: versionable strategy parameters and hard risk limits.
- `market_pairs`: normalized local Spot / global Perpetual market mapping.
- `scanner_opportunities`: per-scan opportunity observations, including excluded candidates.
- `positions` and `position_legs`: lifecycle and each exchange leg.
- `orders` and `order_fills`: order lifecycle and fill records.
- `funding_payments`: exchange funding settlements, with estimated and confirmed
  records explicitly distinguished.
- `closed_trades`: realized PnL snapshots.
- `balance_snapshots`: historical balance observations.
- `risk_events` and `executor_events`: operational and audit event journals.
- `executor_status_snapshots`: time-series snapshots for monitoring and incident review.

All financial quantities are PostgreSQL `NUMERIC`, not floating point. Tables have
RLS enabled and no grants for `anon` or `authenticated`; server components use
only the Supabase service-role credential. Never put this credential in Vue, Telegram,
or any other client-side application.

## Runtime integration

The runtime writers are optional and best-effort:

- **Go scanner** posts every evaluated opportunity (eligible and excluded) to
  `scanner_opportunities`. The row ID is deterministic from `scan_id + pair_key`,
  and PostgREST upserts on that ID, so retrying the same scan is idempotent.
- **Python executor** writes `positions`, `orders`, `order_fills`,
  `closed_trades`, and `executor_events` through its background writer.
- **Redis remains authoritative for live coordination**: signals, live prices,
  Pub/Sub events, pending orders, active-position coordination, and locks stay in Redis.
- The configured JSON history file remains in use for opportunity-episode lifecycle
  and scanner metrics; Supabase stores the per-scan candidate audit trail.

The following schema tables are not yet populated by runtime writers:
`exchange_accounts`, `strategy_configs`, `risk_limits`, `market_pairs`,
`position_legs`, `funding_payments`, `balance_snapshots`, `risk_events`,
and `executor_status_snapshots`.

Both writers use bounded in-memory queues and retry transient HTTP failures. They are
not a transactional or disk-backed outbox: a process restart or sustained outage can
lose queued observations after retries are exhausted. This persistence must not gate
a hedge, rollback, force close, scanner signal, or risk decision. The executor must
continue reconciling exposure against the exchanges and Redis; do not treat Supabase
as the live position authority.

## Environment configuration

Set these variables in the server environment or an untracked deployment `.env`
file consumed by Docker Compose / Podman Compose:

- `SUPABASE_URL`: project URL, for example `https://<project-ref>.supabase.co`.
- `SUPABASE_KEY`: server-side **service_role** key. Do not use the publishable
  `anon` key; the schema deliberately denies client roles access.

The same variables are passed only to the scanner and executor containers. They are
not required in `config.json`, are never compiled into either image, and must not be
committed to Git. Keep the deployment `.env` file mode `0600` and rotate the key
if it is exposed. If both variables are absent, persistence is disabled. If only one
is present, the scanner logs a warning and keeps running without Supabase.

## Apply migrations

1. Install the Supabase CLI and authenticate.
2. Link this repository to the intended Supabase project:
   `supabase link --project-ref <project-ref>`.
3. Inspect migration status: `supabase migration list --remote`.
4. Review the SQL, then apply: `supabase db push --remote`.

Do not run migrations against production until the existing schema and migration
history have been checked. Do not make ad-hoc remote schema changes in Studio after
adopting versioned migrations.

CI applies every checked-in migration to PostgreSQL 16, reapplies them to verify
idempotency, and runs `supabase/tests/schema_smoke.sql`. The smoke test also verifies
that the columns consumed by the scanner runtime writer exist in the migration.
