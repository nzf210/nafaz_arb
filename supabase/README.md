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

## Runtime integration status

This migration establishes the durable schema only. The current scanner/executor
still uses Redis for live coordination and the configured JSON opportunity-history
file for scanner history. The Supabase tables are not yet wired into runtime writes.
Do not treat Supabase as the operational source of truth until a persistence adapter
with idempotent writes, retry/outbox handling, and integration tests has been merged.

The intended runtime split is:

- Redis: ephemeral signals, live prices, pub/sub, fast executor coordination.
- Supabase: durable order/fill ledger, position lifecycle, funding settlement ledger,
  closed trades, risk/audit events, scanner opportunity history and status snapshots.

The executor must continue protecting/closing live exposure if Supabase is unavailable.
A Supabase outage must never prevent a required hedge, rollback, or emergency close.
