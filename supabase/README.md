# Supabase database

NAFAZ is preparing Supabase as its durable operational database. Redis remains the
low-latency coordination layer between the Go scanner and Python executor. The schema
is versioned here, but application writes are not connected yet; until that integration
is complete, Redis and the configured JSON history file remain the current runtime stores.

## Schema

The initial migration creates:

- `exchange_accounts`: exchange account metadata and encrypted credential slots.
  Credential columns are ciphertext-only; never store API secrets as plaintext.
- `strategy_configs` and `risk_limits`: versionable strategy parameters and hard risk limits.
- `market_pairs`: normalized local spot / global perpetual market mapping.
- `scanner_opportunities`: time-series snapshots of evaluated opportunities.
- `positions` and `position_legs`: lifecycle and each exchange leg.
- `orders` and `order_fills`: order lifecycle and immutable fill records.
- `funding_payments`: exchange funding settlements, with estimated and confirmed
  records explicitly distinguished and exchange payment IDs deduplicated.
- `closed_trades`: realized PnL snapshots.
- `balance_snapshots`: historical balance observations.
- `risk_events` and `executor_events`: operational and audit event journals.
- `executor_status_snapshots`: time-series snapshots for monitoring and incident review.
- Automatic `updated_at` triggers for mutable records; the CI migration check validates RLS, grants, trigger behavior, migration idempotency, and the trade currency/FX columns.

All financial quantities are PostgreSQL `NUMERIC`, not floating point. Tables have
RLS enabled and no grants for `anon` or `authenticated`; server components use
only the Supabase service-role credential. Never put this credential in Vue, Telegram,
or any other client-side application.

## Apply migration

1. Install Supabase CLI and authenticate.
2. Link this repository to the intended Supabase project: `supabase link --project-ref <project-ref>`.
3. Inspect migration status: `supabase migration list --remote`.
4. Review the SQL, then apply: `supabase db push --remote`.

Do not run the migration against a production project until its existing schema and
migration history have been checked. Do not make ad-hoc remote schema changes in
Studio after adopting versioned migrations.

## Runtime persistence status

The runtime uses Supabase as a best-effort audit/history sink. It is not the
authority for live exposure and is not in the exchange-order critical path.

Implemented writes:

- Go scanner: evaluated opportunities to `scanner_opportunities`, using
  `scan_id + pair_key` as a deterministic nullable-unique idempotency key. The pair key includes both the local spot venue/symbol and global perpetual venue/symbol, so parallel global routes do not overwrite each other.
  Transient PostgREST/network errors are retried; permanent schema/auth errors
  are logged and do not suppress Redis signal delivery.
- Python executor: `positions`, `orders`, `order_fills`, `closed_trades`,
  `executor_events`, and `risk_events`. Client-generated/deterministic keys
  make retries idempotent; order-fill rows are derived from cumulative filled
  quantity and only new deltas are emitted.
- Redis remains authoritative for active-position recovery, locks, live prices,
  balances, pub/sub, and fast coordination. Exchange APIs remain authoritative
  for actual balances, order state, and position exposure.

Still schema-only / not yet connected to runtime writes: `position_legs`,
`funding_payments`, `balance_snapshots`, and `executor_status_snapshots`.
Strategy/risk configuration tables are also not read dynamically by this runtime.

Delivery guarantees and remaining limitations:

- Executor writes are first committed to a local SQLite outbox at
  `SUPABASE_OUTBOX_PATH` (default:
  `/app/data/executor/supabase-outbox.sqlite3`) using WAL and
  `synchronous=FULL`. The database must live on a persistent host volume.
- The async worker retries failed PostgREST delivery with bounded exponential
  backoff. Rows are deleted only after a successful idempotent upsert; permanent
  schema/permission failures remain queued for replay after repair.
- Order-fill cumulative watermarks and their corresponding fill rows are
  committed in one SQLite transaction. This prevents a restart between those
  operations from silently advancing the watermark without journaling the fill.
- The in-memory queue is only a wakeup hint. A full wakeup queue does not discard
  the already committed outbox row. The local outbox has a 50,000-row hard cap;
  capacity or disk errors are logged as critical and must be monitored.
- Redis remains the real-time coordination/recovery layer. SQLite is a local
  delivery journal, not a cross-host shared database; run only one executor
  writer against a given outbox file and keep that file on persistent local
  storage. Supabase remains an audit sink, never authority for exchange exposure.
- Scanner persistence still uses bounded in-process retry and does not yet have
  a durable scanner outbox. Scanner history can therefore have gaps during long
  Supabase outages. Do not treat Supabase history as a complete exchange ledger
  or proof that an order/position is flat.

## Runtime configuration

Set these variables in the server environment and inject them into both
`nafaz-scanner` and `nafaz-executor`:

- `SUPABASE_URL=https://<project-ref>.supabase.co`
- `SUPABASE_KEY=<server-only service_role key>`
- `SUPABASE_OUTBOX_PATH=/app/data/executor/supabase-outbox.sqlite3` (executor only)

The existing `SUPABASE_KEY` name is retained for compatibility with the executor;
its value must be the Supabase `service_role` secret, never the anon/publishable
key. The scanner also rejects non-HTTPS remote URLs. HTTP is accepted only for
loopback tests. If both URL/key are absent, persistence stays disabled. If only
one is supplied, scanner startup logs a warning and continues without persistence.
The executor's outbox path must be on persistent local storage and writable by
the container UID; Compose mounts `./data/executor` at `/app/data/executor`.

Do not commit credentials, place them in `config.example.json`, bake them into an
image, or expose them to Vue/Telegram. Use a host secret manager or a protected
systemd/container environment. The repository's `.env` is git-ignored, but a
tracked or shared plaintext secret file is still unsafe.

## Apply migrations

1. Install Supabase CLI and authenticate.
2. Link the intended project: `supabase link --project-ref <project-ref>`.
3. Inspect remote history: `supabase migration list --remote`.
4. Review pending SQL and the remote schema before applying:
   `supabase db push --remote`.

CI applies every versioned migration in lexical order, replays each migration to
check idempotency, and runs `supabase/tests/schema_smoke.sql` against PostgreSQL 16.
The original baseline migration remains immutable. Do not apply migrations to the
production project until its existing migration history and schema have been reviewed.
