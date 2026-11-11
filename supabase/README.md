# Supabase database

Supabase stores durable operational history for NAFAZ. Redis remains the
low-latency coordination layer between the Go scanner and Python executor. Local
persistent outboxes protect scanner and executor audit writes from process restarts
and temporary Supabase outages; they do not replace Redis or exchange-side state.

## Schema

The initial migration creates:

- `exchange_accounts`: exchange account metadata and encrypted credential slots.
  Credential columns are ciphertext-only; never store API secrets as plaintext.
- `strategy_configs` and `risk_limits`: versionable strategy parameters and hard risk limits.
- `market_pairs`: normalized local spot / global perpetual market mapping.
- `scanner_opportunities`: time-series snapshots of evaluated opportunities.
- `positions` and `position_legs`: lifecycle and each exchange leg.
- `orders` and `order_fills`: order lifecycle and immutable fill records.
- `order_registry`: durable universal order registry (one row per submitted order,
  keyed by a deterministic local id derived from the client order id). It is the
  recovery authority after a restart and carries `order_role` (ENTRY / HEDGE /
  EXIT_SPOT / EXIT_FUTURES / EMERGENCY / FORCE_CLOSE / ROLLBACK) and
  `recovery_status` (MANAGED / ADOPTED / UNVERIFIED / ORPHAN_ORDER / PHANTOM).
  `orders` stays the per-leg analytics view.
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
- Scanner writes are first committed to a local append-only JSONL journal at
  `SUPABASE_SCANNER_OUTBOX_PATH` (default:
  `/app/data/scanner/supabase-outbox.jsonl`). Each append is fsynced before
  `SaveOpportunities` returns; a background reconciliation worker replays due
  records to PostgREST using the existing idempotency key.
- Scanner acknowledgements happen only after successful PostgREST upsert.
  Network/HTTP failures remain on disk with persisted exponential backoff;
  malformed committed records fail closed, while an incomplete final append is
  truncated during recovery. The scanner journal has a 128 MiB default hard cap.
- Full disk, permission errors, and capacity exhaustion are logged as CRITICAL.
  Scanner evaluation and Redis signalling remain fail-open, so operators must
  monitor these logs and restore storage before audit history can resume.
- Both journals are local to one process/host and must not be shared by multiple
  replicas. Use persistent local volumes and back up journals before replacing
  hosts or volumes.
- The scanner reconciliation worker is a delivery reconciler only: it replays
  scanner opportunity snapshots and confirms durable delivery. It does not
  inspect private exchange balances/positions, change Redis exposure state, or
  place/cancel orders. Executor restart verification separately checks restored
  futures exposure against exchange positions and marks mismatches
  `RECONCILIATION_REQUIRED`.
- Supabase remains an audit/history sink, never authority for live exposure.
  Scanner opportunities and executor history are not a complete exchange ledger
  unless fills, fees, funding settlements, balances, and position legs are also
  independently reconciled from authenticated exchange records.

## Runtime configuration

Set these variables in the server environment and inject them into both
`nafaz-scanner` and `nafaz-executor`:

- `SUPABASE_URL=https://<project-ref>.supabase.co`
- `SUPABASE_KEY=<server-only service_role key>`
- `SUPABASE_OUTBOX_PATH=/app/data/executor/supabase-outbox.sqlite3` (executor only)
- `SUPABASE_SCANNER_OUTBOX_PATH=/app/data/scanner/supabase-outbox.jsonl` (scanner only)

The existing `SUPABASE_KEY` name is retained for compatibility with the executor;
its value must be the Supabase `service_role` secret, never the anon/publishable
key. The scanner also rejects non-HTTPS remote URLs. HTTP is accepted only for
loopback tests. If both URL/key are absent, persistence stays disabled. If only
one is supplied, scanner startup logs a warning and continues without persistence.
Both outbox paths must be on persistent local storage and writable by
the container UID; Compose mounts `./data` at `/app/data`. The scanner journal
defaults to a 128 MiB hard cap; capacity or disk errors are critical and require
operator intervention. No automatic deletion or truncation of committed records
is performed to make room.

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
