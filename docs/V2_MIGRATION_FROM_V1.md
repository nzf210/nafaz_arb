# Nafazbot V1 → V2 configuration migration runbook

**Status:** Manual, review-gated migration procedure. This document does not authorize a runtime cutover or live order.

## 1. Non-negotiable rules

- Do not replace or rewrite the active \`config.json\` during the foundation stage.
- Preserve the effective V1 \`mode\`. If the deployed intent is \`LIVE\`, the V2 candidate remains \`LIVE\`; do not silently change it to \`MOCK\`.
- Keep \`execution.signals_enabled=false\`, all V2 routes disabled, and the durable operator state at \`STOPPED\` until the runtime cutover and adapter conformance work is approved.
- Never copy \`api_key\`, \`api_secret\`, passphrase, Telegram token, Redis password or database password values into V2 JSON, PostgreSQL JSONB, logs, test fixtures or pull requests. V2 stores reference names only.
- Do not widen risk. A missing, zero/disabled, ambiguous or differently-unitized V1 limit is a migration blocker, not permission to substitute a larger or more permissive value.
- Do not infer account identities or create every possible spot/perpetual Cartesian route. Every route must be explicitly reviewed.
- No migration step below places, cancels or amends an exchange order or changes an exchange account setting.

## 2. Inventory the actual V1 configuration without exposing secrets

The current executor loader is \`ExecutorConfig.load("config.json")\`. It reads the root \`mode\` and \`execution_mode\`, the \`exchanges\` map, the \`executor\` block and Redis settings. The active local exchange can also be overridden by \`LOCAL_EXCHANGE\`; the futures exchange can be overridden by \`GLOBAL_EXCHANGE\`. The scanner config commonly contains \`global_futures_exchange\`, \`quote_assets\`, \`excluded_symbols\`, fee assumptions and synthetic FX settings.

Before creating a V2 candidate, produce a redacted inventory of:

1. The effective mode after environment overrides.
2. The effective local spot exchange and global perpetual exchange.
3. Every exchange/account label that is actually intended to be used, including whether spot and futures share one exchange account or use separate accounts.
4. Existing quote/settle assets, canonical base-symbol allow/deny policy and native market symbols.
5. The effective risk settings and their original units/source (JSON root, \`executor\` block or environment).
6. The names of environment variables or secret-manager references that will resolve credentials inside the Python executor.

Do not dump the \`exchanges\` object or print credential values as part of this inventory. The V1 file can contain inline credentials; treat it as sensitive and keep the inventory redacted.

## 3. Map the V1 structure to V2 explicitly

| V1 source | V2 destination | Migration rule |
|---|---|---|
| Root \`mode\` | Root \`mode\` | Preserve the effective LIVE/MOCK intent. LIVE is not an enable switch. |
| Root \`execution_mode\` | \`execution.execution_mode\` | Preserve only a supported mode; do not change strategy to make validation pass. |
| \`LOCAL_EXCHANGE\` / effective executor local exchange | A spot \`venues[]\` entry | Require operator confirmation; do not assume the code default is the intended account. |
| \`GLOBAL_EXCHANGE\` / \`global_futures_exchange\` | A perpetual \`venues[]\` entry | Require operator confirmation and confirm linear vs inverse product. |
| \`exchanges[exchange]\` credential fields | \`venues[].execution.credentials.*_env\` or \`*_ref\` | Move values to executor-only secret injection first; V2 stores only reference names. |
| Account label / testnet intent | \`accounts[]\` and venue metadata | Use stable account IDs. Do not duplicate a unified account in a way that double-counts exposure. |
| Explicit pair restrictions | \`routes[]\` | Create only routes that are actually approved. Route quote/settle allowlists are not FX conversion instructions. |
| \`excluded_symbols\` | \`routes[].symbols.deny\` | Map only after canonical base-asset normalization is confirmed. |
| Explicitly approved symbols | \`routes[].symbols.allow\` | Supply an explicit allowlist. Do not infer "all symbols" from an empty V1 list. |
| Effective hard risk limits | \`risk_profiles[]\` | Preserve exact semantics and units; require manual review for any missing or ambiguous field. |
| Redis settings | \`redis.url_env\` and transient purpose | Redis remains queue/cache/coordination only, never order or balance authority. |
| Supabase/SQLite persistence | V2 PostgreSQL repositories | No automatic copy, destructive conversion or dual-write cutover is performed by this PR. |

A V1 config with \`global_futures_exchange="all"\` must not become an execution venue. If needed, treat \`all\` as a scanner-only discovery selector outside the V2 route model.

## 4. Risk fields that must not be auto-converted

The V1 runtime has settings with different semantics and units:

- \`executor.max_daily_loss_usdt\` is an absolute USDT loss cap. Carry the effective value only after confirming that it is the intended hard stop.
- \`executor.max_exposure_per_exchange_usdt=0\` means the legacy exposure check is disabled. V2 requires a positive hard exposure limit for an executable risk profile. Do not translate zero into an arbitrary positive number; obtain an explicit reviewed limit.
- \`executor.max_entry_slippage_pct\` is currently treated by the executor as a fraction (for example, \`0.003\` means 0.3%). Scanner \`max_slippage_pct\` is expressed as percentage points (for example, \`0.2\` means 0.2%). V2 \`max_entry_slippage_pct\` is percentage points. Select the effective control and convert it explicitly; never copy one field into the other without unit review.
- \`max_open_spot_orders_per_exchange\` is exchange-wide, while V2 risk profiles are referenced by routes/accounts. Map it per account/route only after reviewing shared-account concurrency.
- \`max_consecutive_order_failures\` must retain the existing threshold; do not reduce it or increase it to make a venue appear healthy.
- \`allocation_per_pair\`, \`min_allocated_capital\`, fee profiles, maker refresh, adaptive maker thresholds and order TTL are strategy/execution policy. They are not silently dropped or guessed into a V2 risk profile. They remain on the V1 runtime until a separately reviewed V2 strategy/policy schema covers them.

The V2 validator requires positive finite daily-loss, account-exposure, route-exposure and slippage limits, positive integer order/failure caps, and mandatory fresh-balance/position-reconciliation flags. If the effective V1 configuration cannot supply these without guessing, do not create or activate an executable V2 route.

## 5. Secret-reference preparation

For each execution-enabled venue, provide exactly one reference for each required credential:

- \`api_key_env\` or \`api_key_ref\`
- \`api_secret_env\` or \`api_secret_ref\`
- optional \`passphrase_env\` or \`passphrase_ref\`

Environment references must be valid environment-variable names. Secret-manager references must use the approved \`secret://\` form. Resolve them only in the Python executor/control-plane process. The Go scanner receives public market configuration only.

Do not put secret values in \`v2_account_secret_refs\`. That table is deliberately limited to references and is not a credential vault. API keys must have withdrawal/transfer permissions disabled at the exchange.

## 6. Build a disabled V2 candidate

1. Preserve \`mode\` from the effective V1 deployment.
2. Set \`execution.engine="python"\`, \`execution.signals_enabled=false\` and \`execution.startup_reconciliation_required=true\`.
3. Set \`storage.driver="postgres"\`, the approved DSN environment variable name, and \`migrations="required"\`.
4. Set Redis only for candidate delivery, transient cache and coordination.
5. Create one venue identity per product family/account identity. Reusing an exchange brand is allowed; reusing a venue ID for different accounts or market types is not.
6. Create matching \`accounts[]\` records and external secret references.
7. Create only reviewed spot/perpetual routes. Keep every route \`enabled=false\` until both adapters have documented and tested required capabilities.
8. Copy reviewed risk values without widening them. If a value is missing or ambiguous, stop and record the blocker instead of inventing a default.
9. Preserve the V1 config and environment files unchanged for rollback/reference.

The current code-level production capability registry is intentionally empty. A route with \`enabled=true\` is rejected unless both adapters are explicitly registered as multi-venue and multi-account capable and prove the required balance/order/position/cancel/reduce-only/client-ID lookup operations. Test fixtures may supply a fake registry; that is not production evidence.

## 7. Validate and stage only after review

The current validation API is \`executor.v2_config.validate_v2_snapshot\`. It returns a typed \`V2ConfigModel\` and rejects unknown keys, duplicate IDs, dangling references, venue/account mismatch, market-type mismatch, malformed secret references, invalid risk profiles and unverified enabled routes.

\`PostgresConfigWriter.create_draft\` validates before persisting an immutable version, audit row and outbox event in one transaction. It does not activate the draft. \`validate_version\` records a validation result; \`activate_version\` uses an expected-current-version compare-and-swap. Do not call activation as part of this migration until runtime mapping, secret resolution, PostgreSQL startup checks, all route capabilities and operator readiness have been independently proven.

Rollback is a new config version cloned from a reviewed source version. Never rewrite or delete immutable history.

## 8. PostgreSQL migration procedure

The V2 schema is additive and versioned through timestamped files in \`supabase/migrations/\`. The new core migration is:

\`20261114000100_v2_multi_venue_execution_core.sql\`

For a disposable local Supabase development stack:

\`\`\`bash
supabase start
supabase db reset
supabase migration list --local
\`\`\`

The repository CI job also applies \`supabase/migrations/*.sql\` in lexical timestamp order to PostgreSQL 16 and replays the same files to verify idempotency, then runs \`supabase/tests/schema_smoke.sql\`. For a standalone local PostgreSQL test database, use the same ordered migration files and \`psql -v ON_ERROR_STOP=1\`; do not run this procedure against production or a VPS as part of this foundation PR.

Do not hand-edit migration history, skip an earlier migration, or apply the V2 migration before the existing core/order-registry and V2 config/control migrations. No remote Supabase migration, VPS deployment or user database change is performed by this repository PR.

## 9. Cutover checklist — not completed by this PR

- [ ] V1 effective configuration inventory reviewed without secrets.
- [ ] V2 candidate has explicit venues, accounts, markets, routes and risk profiles.
- [ ] Secret references resolve only in Python executor; scanner has no private credentials.
- [ ] Every enabled adapter capability has official documentation/payload fixtures and regression evidence.
- [ ] PostgreSQL migration state and backup/restore have been verified on a non-production database.
- [ ] Existing orders, fills, balances and positions have a reviewed, idempotent migration/reconciliation plan.
- [ ] V2 runtime loader is explicitly injected into executor startup; legacy loader is not partially mixed with V2 defaults.
- [ ] Startup recovery, outbox replay, multi-process config convergence and transactional STOP fencing pass integration tests.
- [ ] Operator state remains STOPPED until all readiness checks pass.
- [ ] A separate deployment/release approval authorizes any future live order testing.

**Current result:** schema and validation foundation only. The legacy executor still reads \`config.json\`; V2 is not activated and no exchange order behavior is changed.
