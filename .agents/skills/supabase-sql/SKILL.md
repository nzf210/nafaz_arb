---
name: supabase-sql
description: PostgreSQL and Supabase SQL standards for Nafazbot schema, migrations, RLS, grants, and persistence.
---

# Supabase SQL skill — Nafazbot

- Treat PostgreSQL as the source of truth; use explicit types, constraints, foreign keys, and indexes that match query patterns.
- Add schema changes through timestamped, forward-only migrations. Do not rewrite migrations already applied to shared environments.
- Make migrations replay-safe where the existing migration workflow requires it; test both fresh application and replay.
- Enable and verify RLS for user-facing tables. Policies must be least-privilege and must not expose service-role data.
- Review grants separately from RLS. Never grant broad access to anon/authenticated without a documented need.
- Keep service-role credentials server-side only; never put secrets in SQL files, fixtures, logs, or client configuration.
- Use transactions for multi-step data changes and preserve atomicity for order, fill, risk, and reconciliation records.
- Enforce idempotency with unique constraints and conflict-safe writes where duplicate delivery is possible.
- Use timestamptz for instants and define UTC handling explicitly. Avoid implicit timezone assumptions.
- Add indexes for actual lookup/join/filter patterns; avoid speculative indexes and redundant constraints.
- Keep every SQL source file <= 500 lines. Split schema changes into logically ordered migrations, not arbitrary fragments.
- Add schema smoke tests for RLS, grants, constraints, triggers, and migration replay. Never claim a migration is safe without testing it.
