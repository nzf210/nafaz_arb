# Supabase database

NAFAZ uses Supabase/Postgres for durable operational snapshots and ledgers.
Redis remains the low-latency coordination layer between the Go scanner and
Python executor. The scanner's configured JSON history file remains its local
API/history cache; it is not replaced by Supabase.

## Runtime data flow

| Component | Supabase tables | Write behavior |
|---|---|---|
| Go scanner | `market_pairs`, `scanner_opportunities` | Non-blocking scan queue; deterministic opportunity IDs per scan/pair; PostgREST upserts |
| Python executor | `positions`, `orders`, `order_fills`, `closed_trades`, `funding_payments`, `balance_snapshots`, `risk_events`, `executor_events`, `executor_status_snapshots` | Background batch queue; deterministic IDs for mutable/replayed records; transient HTTP retries |

Redis remains authoritative for live signals, prices, locks, pub/sub, pending
entries and active-position coordination. Supabase is not read by the order,
hedge, rollback, exit or risk-decision path. A Supabase outage must never prevent
a required hedge, rollback, force close or scanner signal.

Order cumulative filled quantity is persisted on `orders`. The adapter only
writes `order_fills` when the exchange response contains an actual trade ID;
it does not manufacture fills from cumulative deltas, because an in-memory
delta cursor is not restart-safe. Funding rows are written only for confirmed
settlements returned by the exchange funding-history endpoint. Balance snapshots
are sourced from balances already fetched for risk sizing or the read-only
Telegram balance report; no extra balance API request is added to the risk path.

The writers are best-effort and intentionally non-blocking. They retry transient
HTTP failures and log permanent errors. If a bounded queue fills, the scan or
executor continues and the dropped persistence batch is logged; this is
operational telemetry, not a transactional trading outbox. Redis remains the
real-time state/coordination layer and should be monitored independently.

## Schema

The initial migration creates:

- `exchange_accounts`: exchange account metadata and encrypted credential slots.
  Credential columns are ciphertext-only; never store API secrets as plaintext.
- `strategy_configs` and `risk_limits`: versionable strategy parameters and hard risk limits.
- `market_pairs`: normalized local spot / global perpetual market mapping.
- `scanner_opportunities`: time-series snapshots of evaluated opportunities.
- `positions` and `position_legs`: lifecycle and each exchange leg.
- `orders` and `order_fills`: order lifecycle and immutable exchange-reported fills.
- `funding_payments`: exchange funding settlements, with exchange payment IDs deduplicated.
- `closed_trades`: realized PnL snapshots.
- `balance_snapshots`: historical balance observations.
- `risk_events` and `executor_events`: operational and audit event journals.
- `executor_status_snapshots`: time-series snapshots for monitoring and incident review.
- Automatic `updated_at` triggers for mutable records.

Current runtime writers do not populate `exchange_accounts`, `strategy_configs`,
`risk_limits`, or `position_legs`. Exchange credentials remain in the existing
server-side environment/configuration; no plaintext API credentials are written
to Supabase.

All financial quantities are PostgreSQL `NUMERIC`, not floating point. Tables
have RLS enabled and no grants for `anon` or `authenticated`; server components
use only the Supabase service-role credential. Never put this credential in Vue,
Telegram, or any other client-side application.

## Environment

Set these variables in the host `.env` used by Docker Compose/Podman Compose:

```dotenv
SUPABASE_ENABLED=true
SUPABASE_URL=https://<project-ref>.supabase.co
SUPABASE_KEY=<server-side-service-role-key>
```

Do not commit the real `.env`. Keep `.env.example` as a placeholder only.
Both scanner and executor enable persistence when `SUPABASE_URL` and
`SUPABASE_KEY` are present. Set `SUPABASE_ENABLED=false` to disable both
without removing credentials from the deployment environment. If the scanner
has `SUPABASE_ENABLED=true` but either credential is missing, startup validation
fails rather than silently pretending persistence is active. The executor logs
a warning and disables the optional writer when credentials are incomplete.

## Apply migration

1. Install Supabase CLI and authenticate.
2. Link this repository to the intended Supabase project:
   `supabase link --project-ref <project-ref>`.
3. Inspect migration status: `supabase migration list --remote`.
4. Review the SQL, then apply: `supabase db push --remote`.

Do not run the migration against a production project until its existing schema
and migration history have been checked. Do not make ad-hoc remote schema changes
in Studio after adopting versioned migrations.

CI applies every migration in `supabase/migrations` twice against PostgreSQL 16,
then runs `supabase/tests/schema_smoke.sql` to verify RLS, grants, triggers and
the expected table/column contract. The application writer tests use local HTTP
test servers and do not require production credentials.

## Deployment checklist

1. Apply and verify the versioned migration before enabling the writers.
2. Add `SUPABASE_ENABLED=true`, `SUPABASE_URL`, and `SUPABASE_KEY` to the host
   environment; never add the service-role key to tracked config files.
3. Recreate both scanner and executor containers so the new environment is loaded.
4. Confirm logs report scanner persistence enabled and that executor writes are
   accepted by PostgREST. Check recent `scanner_opportunities`,
   `balance_snapshots`, `funding_payments`, `risk_events`, and
   `executor_status_snapshots` rows.
5. Keep Redis healthy and retain the existing Redis/JSON recovery paths. Supabase
   being enabled does not switch the scanner/executor to live trading or change
   any strategy, sizing, fee, leverage, or risk threshold.
