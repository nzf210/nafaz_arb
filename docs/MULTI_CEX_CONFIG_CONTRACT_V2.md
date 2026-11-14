# Nafazbot V2 — Multi-CEX Configuration Contract

**Status:** Schema v2 contract with a strict Python model/validator and additive PostgreSQL foundation. The legacy executor runtime still consumes config.json; V2 is not activated. JSON remains the transport/snapshot format.

## 1. Normative principles

- schema_version is mandatory and must be supported by the binary.
- IDs are stable, unique lowercase identifiers; references use IDs, not display names.
- Exchange, venue/product family, account, market and route are separate concepts.
- There is no global_futures_exchange or local_exchange singleton in v2.
- venues is an array containing any number of spot and futures venue instances.
- routes explicitly declare allowed spot/futures pairings.
- enabled means configured for discovery/eligibility, not automatically authorized to trade.
- Private credential references are resolved only by Python executor. Go scanner ignores private credential blocks and receives no trade-capable secrets.
- Unknown keys, duplicate IDs, dangling references and unsupported capabilities fail closed.

## 2. Illustrative schema v2 configuration

The sample represents two spot venues and one currently active futures venue. More venues can be appended without changing the structure. Values are illustrative, not verified account settings or recommended risk limits.

~~~json
{
  "schema_version": 2,
  "mode": "LIVE",
  "execution": {
    "engine": "python",
    "execution_mode": "MAKER_TAKER",
    "signals_enabled": false,
    "startup_reconciliation_required": true
  },
  "storage": {
    "driver": "postgres",
    "dsn_env": "NAFAZ_DATABASE_URL",
    "migrations": "required",
    "outbox": { "enabled": true, "delivery": "redis" }
  },
  "redis": {
    "enabled": true,
    "url_env": "NAFAZ_REDIS_URL",
    "purpose": ["candidate_queue", "transient_cache", "coordination"]
  },
  "venues": [
    {
      "id": "indodax_spot_primary",
      "exchange": "indodax",
      "market_type": "spot",
      "account_id": "indodax_account_1",
      "enabled": true,
      "market_data": { "enabled": true, "adapter": "indodax" },
      "execution": {
        "enabled": true,
        "adapter": "indodax",
        "credentials": {
          "api_key_env": "INDODAX_API_KEY",
          "api_secret_env": "INDODAX_API_SECRET"
        }
      }
    },
    {
      "id": "tokocrypto_spot_primary",
      "exchange": "tokocrypto",
      "market_type": "spot",
      "account_id": "tokocrypto_account_1",
      "enabled": true,
      "market_data": { "enabled": true, "adapter": "tokocrypto" },
      "execution": {
        "enabled": true,
        "adapter": "tokocrypto",
        "credentials": {
          "api_key_env": "TOKOCRYPTO_API_KEY",
          "api_secret_env": "TOKOCRYPTO_API_SECRET"
        }
      }
    },
    {
      "id": "okx_swap_primary",
      "exchange": "okx",
      "market_type": "linear_perpetual",
      "account_id": "okx_account_1",
      "enabled": true,
      "market_data": { "enabled": true, "adapter": "okx" },
      "execution": {
        "enabled": true,
        "adapter": "okx",
        "credentials": {
          "api_key_env": "OKX_API_KEY",
          "api_secret_env": "OKX_API_SECRET",
          "passphrase_env": "OKX_API_PASSPHRASE"
        },
        "position_mode": "hedge",
        "margin_mode": "isolated",
        "required_leverage": 1
      }
    }
  ],
  "accounts": [
    { "id": "indodax_account_1", "venue_id": "indodax_spot_primary", "label": "Indodax primary", "enabled": true },
    { "id": "tokocrypto_account_1", "venue_id": "tokocrypto_spot_primary", "label": "Tokocrypto primary", "enabled": true },
    { "id": "okx_account_1", "venue_id": "okx_swap_primary", "label": "OKX futures primary", "enabled": true }
  ],
  "routes": [
    {
      "id": "indodax_to_okx",
      "spot": { "venue_id": "indodax_spot_primary", "account_id": "indodax_account_1", "quote_assets": ["IDR", "USDT"] },
      "futures": { "venue_id": "okx_swap_primary", "account_id": "okx_account_1", "settle_assets": ["USDT"] },
      "enabled": false,
      "symbols": { "allow": ["BTC", "ETH"], "deny": [] },
      "risk_profile": "default_live"
    },
    {
      "id": "tokocrypto_to_okx",
      "spot": { "venue_id": "tokocrypto_spot_primary", "account_id": "tokocrypto_account_1", "quote_assets": ["USDT", "IDR"] },
      "futures": { "venue_id": "okx_swap_primary", "account_id": "okx_account_1", "settle_assets": ["USDT"] },
      "enabled": false,
      "symbols": { "allow": ["BTC", "ETH"], "deny": [] },
      "risk_profile": "default_live"
    }
  ],
  "risk_profiles": {
    "default_live": {
      "max_daily_loss_usdt": 5,
      "max_exposure_per_account_usdt": 100,
      "max_exposure_per_route_usdt": 50,
      "max_entry_slippage_pct": 0.2,
      "max_consecutive_order_failures": 5,
      "max_open_spot_orders": 2,
      "max_open_futures_orders": 2,
      "require_fresh_balance": true,
      "require_position_reconciliation": true
    }
  },
  "scanner": {
    "enabled": true,
    "public_market_data_only": true,
    "scan_interval_seconds": 30,
    "quote_assets": ["USDT", "USDC"],
    "synthetic_fx": {
      "enabled": true,
      "source": "configured_live_fx_market",
      "max_age_seconds": 5,
      "static_fallback_allowed_in_live": false
    }
  },
  "observability": { "log_level": "INFO", "metrics_enabled": true }
}
~~~

The sample illustrates shape only. Market type, hedge mode, margin mode, leverage, symbols and quote/settle combinations must be verified against each actual account and instrument. Do not copy the sample risk values as a recommendation. LIVE does not automatically enable signals or make routes ready.

## 3. Field contract

### Root

| Field | Type | Required | Meaning |
|---|---|---:|---|
| schema_version | integer | yes | Must equal supported version 2. |
| mode | MOCK or LIVE | yes | Deployment intent; not blanket order authorization. |
| execution | object | yes | Execution owner, signal gate and startup reconciliation policy. |
| storage | object | yes | Durable database configuration. |
| redis | object | yes | Transient queue/cache configuration. |
| venues | array | yes | Configured venue instances. |
| accounts | array | yes | Logical account identities. |
| routes | array | yes | Explicit spot/futures pairings. |
| risk_profiles | object | yes | Named risk policies. |
| scanner | object | yes | Public discovery settings. |

### Venue

| Field | Contract |
|---|---|
| id | Globally unique stable ID, e.g. okx_swap_primary. |
| exchange | Adapter registry key, e.g. okx, mexc, bybit. |
| market_type | spot, linear_perpetual or inverse_perpetual; only implemented types accepted. |
| account_id | References one account. |
| enabled | Venue participates in validation/discovery when true. |
| market_data | Public adapter/discovery toggle. |
| execution | Executor-only execution adapter and secret references. |

One exchange can have multiple venue IDs and accounts. Do not use exchange brand as a unique key.

### Account and route

Account IDs are unique. Account-to-venue references must be consistent. A unified account spanning products must not be duplicated in a way that double-counts exposure.

Each route has exactly one spot and one futures leg. The spot leg must reference a spot venue; futures leg must reference a supported futures venue. Quote assets and settle assets are allowlists, not FX conversion instructions. Symbol filters apply to canonical base assets. risk_profile must exist. Routes are explicit; adding a venue does not enable all possible pairings automatically.

## 4. Secrets

Fields ending in _env contain environment variable names, never secret values. The executor resolves them from its own environment/secret store. Only executor receives trade-capable credentials. Scanner gets public market configuration only. Missing required secrets make that execution venue not-ready; never fall back silently to another account. Redact values in logs and errors. API keys must not have withdrawal or transfer permissions.

## 5. LIVE readiness

mode LIVE means live clients are intended, not that every route may trade. A route becomes READY only after config validation, PostgreSQL/migrations, secret resolution, fresh unambiguous market metadata, private account reconciliation, position/margin/leverage verification, valid risk profile, no unresolved order/halt, and explicit operator enablement. signals_enabled remains false until Python is verified as the sole consumer capable of causing orders.

## 6. PostgreSQL and Redis

Production storage.driver is postgres; dsn_env names the executor DB connection-string variable. Apply versioned migrations before reconciliation. Use NUMERIC/decimal, unique constraints for venue/account/order IDs and client-order idempotency within documented venue scope, append-only journal and transactional outbox. A PostgreSQL outage blocks new entries.

Redis is non-authoritative. Queue payloads include event_id, schema_version, route_id, canonical symbol, observation timestamp and candidate version. Delivery is at-least-once and executor deduplicates in PostgreSQL. Queue loss must be recoverable from outbox/republication or fresh scanner passes. No private balances, positions, orders, fills or execution authorization may be sourced solely from Redis.

## 7. Validation rules

Reject unsupported schema versions; duplicate IDs; dangling or inconsistent references; wrong route market types; unsupported adapters/capabilities; embedded secret values; invalid/non-finite or negative limits; LIVE without PostgreSQL/migrations/risk profiles/required secrets; static FX fallback in LIVE; enabled execution routes whose legs are disabled/not execution-capable; ambiguous symbols; missing contract size, amount step, price tick or minimums.

Config reload is atomic: validate the entire snapshot, persist version/audit record, then swap active registry. Partial application is forbidden. Disabling a route blocks new entries but does not abandon existing orders/positions.

## 8. Legacy migration

Current baseline uses singleton LOCAL_EXCHANGE/GLOBAL_EXCHANGE, root global_futures_exchange and exchange-keyed credentials. V2 replaces these with venues, accounts and routes.

1. Build a one-time converter from legacy config to a v2 candidate.
2. Never infer unsupported routes or enable all pairings.
3. Move secret values to executor environment variables and emit references only.
4. Map legacy local/global settings to explicit route entries.
5. Preserve existing risk values as migration input; validate and do not widen silently.
6. Reject legacy all as an execution venue; it may only be a scanner discovery selector if separately defined.
7. Keep v1/v2 loaders separate during transition; never partially parse v2 with legacy defaults.
8. Remove legacy singleton environment overrides only after deployments migrate and tests prove no hidden dependency.

## 9. Conformance matrix

| Scenario | Expected result |
|---|---|
| 2 spot + 2 futures venues | Valid; fan-out independent of count. |
| 2 accounts on same exchange | Valid; isolated credentials and state. |
| Enabled route references disabled venue | Not ready / rejected. |
| Spot leg references futures venue | Rejected. |
| Duplicate IDs or dangling account | Rejected. |
| Unsupported capability | Route not ready; no guessed fallback. |
| Missing contract size or amount step | Futures route not ready. |
| Static FX fallback in LIVE | Rejected. |
| Duplicate candidate delivery | One execution intent. |
| PostgreSQL unavailable | No new entries. |
| Redis unavailable | Durable order state retained; queue recovery required. |
| Ambiguous order acknowledgement | UNKNOWN and reconcile; never blind retry. |
| Route disabled with open position | Block new entries; continue managing existing exposure. |
| Scanner process has no private credentials | Verified by deployment test. |

## 10. Implementation boundary

The strict Python model/validator and additive PostgreSQL schema are implemented on the V2 foundation branch. The sample routes are disabled because no production multi-venue capability registry has been approved yet. This is not runtime cutover: executor startup still reads the legacy config.json, signals_enabled must remain false, and no LIVE order is authorized.


## 9. V2 database activation and Telegram control extensions

### Database-backed configuration

PostgreSQL stores immutable config snapshots in `config_versions` and the active pointer in `active_config_version`. A snapshot includes every operational setting listed in this contract, excluding secret values. Secret fields are references such as `secret://nafaz/okx/primary/api-key` or an environment variable name; they are resolved only by the Python executor/control plane. Do not store API keys, API secrets, passphrases, Telegram bot tokens, database passwords or Redis passwords in JSONB snapshots.

A config update is staged and validated before activation. Activation is one transaction with an expected-version compare-and-swap, immutable version insert, actor/time/reason/diff audit and transactional outbox event. The active pointer changes only in that transaction. A rollback is a new version cloned from a previously validated snapshot and passes the same validator; no history is rewritten. Competing writers receive a version conflict and must re-read/rebase; never use last-write-wins.

On every startup/restart, services read the active version and snapshot directly from PostgreSQL, validate schema and hash, then populate Redis with an immutable versioned cache. Redis restart/loss is repaired by reloading PostgreSQL. Config-change notifications are hints only: consumers fetch the authoritative version from PostgreSQL, validate, refresh Redis and atomically swap their in-process snapshot. If PostgreSQL is unavailable or version/hash verification fails, LIVE entry remains disabled. Redis outage also blocks LIVE entry until the required cache/version state is restored and verified. Stale config is never used silently.

### Trading control state and Telegram

The durable control row is initialized to `STOPPED` / `trading_enabled=false`; restart never auto-starts. Process health is a separate liveness signal. Telegram token comes from an external secret reference/environment. Mutating commands require both user-ID and chat-ID allowlists; either empty list denies control. Audit records include actor IDs, reason, update/command ID, config version, UTC timestamps, transition and result. Commands are idempotent by Telegram update ID and explicit command ID. START uses one-time confirmation; STOP is immediately accepted for an authorized operator and does not require confirmation.

Commands: `/trade_status`, `/trade_start <reason>`, `/trade_stop <reason>`, `/trade_confirm <confirmation_id>`. START readiness requires database/migrations, active config version/hash, Redis version, resolved venue credentials, fresh market metadata, live FX with freshness/spread/fee bounds, successful account/order/position reconciliation, valid risk limits and no active kill switch/venue halt. A failed or unknown check leaves STOPPED. STOP enters durable STOPPING before acknowledgement, blocks new entry admission, publishes a versioned event and waits for executor instances to close their entry gates and resolve in-flight submissions before STOPPED. If the barrier is incomplete, status must say so. STOP never closes positions. Default open-order policy is `keep_and_manage`; optional cancellation of unfilled entries must be explicitly configured and reconciled. No automatic position close.

### Open-order policy values

- `keep_and_manage`: do not cancel existing orders; continue monitoring/reconciliation and managing existing positions.
- `cancel_unfilled_entries`: after durable STOPPING, cancel only verified unfilled entry orders, reconcile cancellation/fill races, and keep all position management active.
- No policy may infer that STOP means force-close. Force-close remains a separate, separately authorized emergency operation.

The scanner's Go Telegram component is read-only alerting in V2. Trading control and PostgreSQL authority live in a Python control-plane component with the executor; the scanner receives no private credentials and cannot enable trading.

### Migration and runtime status

Migrations `20261113000100_v2_config_and_operator_control.sql`, `20261113000200_v2_config_consumers.sql`, and `20261114000100_v2_multi_venue_execution_core.sql` establish versioned config/control, executor-consumer and V2 multi-venue persistence schemas. The latest migration uses V2-prefixed tables to coexist with the legacy schema and uses NUMERIC for financial fields. Timestamped files are the migration version identifiers; local Supabase/PostgreSQL CI applies them in order and replays them to verify idempotency. No existing JSON/SQLite/Supabase data or secrets are automatically migrated. Runtime cutover, account/market metadata population, verified adapter capabilities, secret resolution, Telegram command handling and deployed reconciliation remain separate work.


## 12. Implemented validator and capability gate

The Python model is defined in `executor/v2_config_schema.py` and exposed through `validate_v2_snapshot`. It validates strict nested keys and types, stable lowercase IDs, exchange/market types, venue/account ownership, route leg references, risk-profile completeness, positive finite financial limits, asset lists, secret reference formats, LIVE PostgreSQL/Redis/migration requirements, startup reconciliation and public-only scanner policy.

An enabled route is accepted only when a caller supplies a capability registry for both leg adapters. Each entry must explicitly list supported market types and capabilities and set `multi_venue=true` and `multi_account=true`. Spot routes require balance/open-order reads, limit-order creation, cancel and client-ID lookup. Perpetual routes additionally require position reads, market-order creation and reduce-only support. The default registry is empty because the current executor facade still represents a single configured spot/perpetual pair; do not mark a production adapter verified from a config declaration alone. A route without verified evidence must remain disabled.

## 13. PostgreSQL persistence boundary

Migration `20261114000100_v2_multi_venue_execution_core.sql` creates `v2_exchanges`, `v2_venues`, `v2_accounts`, `v2_account_secret_refs`, `v2_venue_capabilities`, `v2_markets`, `v2_risk_profiles`, `v2_trading_routes`, `v2_execution_intents`, `v2_execution_journal`, `v2_balance_snapshots`, `v2_position_snapshots`, `v2_reconciliation_runs`, and `v2_venue_halts`. The existing `outbox_events` table now accepts `aggregate_type=execution`. V2 identity columns on the legacy `order_registry` are nullable and intentionally not backfilled.

`executor/v2_execution_repository.py` writes a durable intent before any external submission and commits each intent transition, append-only journal event and outbox event atomically. It uses Decimal-only financial inputs, unique idempotency keys, compare-and-swap state transitions and rejects secret-like payload fields. The publisher routes execution events to `nafaz:v2:execution:events`, separate from config invalidation events. The repository is not connected to `ArbitrageEngine` yet, so current LIVE order lifecycle still uses the legacy registry/Supabase writer path.

The SQL migration is additive and does not activate a venue, account, route, risk profile, intent or trading state. All V2 venue/account/route rows are disabled by default. No data backfill or credential migration is performed.
