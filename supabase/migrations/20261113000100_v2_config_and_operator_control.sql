-- Nafazbot V2 durable configuration, operator control and transactional outbox.
-- Additive only: existing legacy runtime tables are retained for controlled migration.

create table if not exists public.config_versions (
    version bigint primary key,
    schema_version integer not null,
    snapshot jsonb not null,
    content_sha256 char(64) not null,
    status text not null check (status in ('draft','validated','active','rolled_back','superseded')),
    created_by text not null,
    created_at timestamptz not null default now(),
    validated_by text,
    validated_at timestamptz,
    validation_report jsonb not null default '{}'::jsonb,
    change_reason text not null,
    change_diff jsonb not null default '{}'::jsonb,
    source_version bigint references public.config_versions(version),
    unique (content_sha256, version),
    check (jsonb_typeof(snapshot) = 'object'),
    check (length(trim(created_by)) > 0),
    check (length(trim(change_reason)) > 0)
);

create table if not exists public.active_config_version (
    singleton boolean primary key default true check (singleton),
    version bigint not null references public.config_versions(version),
    updated_at timestamptz not null default now(),
    updated_by text not null,
    reason text not null
);

create table if not exists public.config_audit_log (
    audit_id bigserial primary key,
    version bigint not null references public.config_versions(version),
    previous_version bigint references public.config_versions(version),
    action text not null check (action in ('created','validated','activated','rollback_created','rejected')),
    actor text not null,
    reason text not null,
    diff jsonb not null default '{}'::jsonb,
    occurred_at timestamptz not null default now(),
    request_id text,
    check (length(trim(actor)) > 0),
    check (length(trim(reason)) > 0)
);
create index if not exists idx_config_audit_version_time
    on public.config_audit_log(version, occurred_at desc);

create table if not exists public.operator_control_state (
    singleton boolean primary key default true check (singleton),
    state text not null default 'STOPPED'
        check (state in ('STOPPED','START_PENDING','STARTING','RUNNING','STOPPING','ERROR_HALTED')),
    trading_enabled boolean not null default false,
    state_version bigint not null default 0 check (state_version >= 0),
    config_version bigint references public.config_versions(version),
    open_order_stop_policy text not null default 'keep_and_manage'
        check (open_order_stop_policy in ('keep_and_manage','cancel_unfilled_entries')),
    last_actor_user_id bigint,
    last_actor_chat_id bigint,
    last_reason text not null default 'safe initial state',
    updated_at timestamptz not null default now(),
    check ((state = 'RUNNING') = trading_enabled),
    check (state <> 'STOPPED' or trading_enabled = false)
);
insert into public.operator_control_state(singleton, state, trading_enabled)
values (true, 'STOPPED', false)
on conflict (singleton) do nothing;

create table if not exists public.operator_actions (
    action_id bigserial primary key,
    command_id text not null unique,
    telegram_update_id bigint unique,
    command text not null check (command in ('trade_start','trade_stop','trade_status','trade_confirm')),
    actor_user_id bigint not null,
    actor_chat_id bigint not null,
    reason text not null default '',
    status text not null check (status in ('RECEIVED','PENDING_CONFIRMATION','ACCEPTED','RUNNING','COMPLETED','REJECTED','FAILED')),
    requested_config_version bigint references public.config_versions(version),
    confirmation_hash char(64),
    confirmation_expires_at timestamptz,
    previous_state text,
    resulting_state text,
    result jsonb not null default '{}'::jsonb,
    received_at timestamptz not null default now(),
    completed_at timestamptz,
    check (command in ('trade_status') or length(trim(reason)) > 0 or status = 'REJECTED')
);
create index if not exists idx_operator_actions_actor_time
    on public.operator_actions(actor_user_id, received_at desc);

create table if not exists public.outbox_events (
    event_id bigserial primary key,
    event_key text not null unique,
    aggregate_type text not null check (aggregate_type in ('config','operator_control')),
    aggregate_version bigint not null,
    event_type text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    published_at timestamptz,
    attempts integer not null default 0 check (attempts >= 0),
    next_attempt_at timestamptz not null default now(),
    last_error text,
    locked_at timestamptz,
    locked_by text
);
create index if not exists idx_outbox_pending
    on public.outbox_events(next_attempt_at, event_id)
    where published_at is null;

-- Prevent accidental direct updates to immutable snapshots at the database level.
create or replace function public.reject_config_version_mutation()
returns trigger language plpgsql as $$
begin
    raise exception 'config_versions are immutable; create a new version';
end;
$$;
drop trigger if exists trg_config_versions_immutable on public.config_versions;
create trigger trg_config_versions_immutable
before update or delete on public.config_versions
for each row execute function public.reject_config_version_mutation();

-- This schema deliberately starts the trading gate closed. Runtime services must
-- claim/update it through a transaction that records operator_actions and outbox.
comment on table public.config_versions is
    'Immutable non-secret operational config snapshots; secret values must remain external.';
comment on table public.active_config_version is
    'Single authoritative active config pointer; updates require optimistic version checks and audit/outbox in one transaction.';
comment on table public.operator_control_state is
    'Durable trading gate; initialized STOPPED and never inferred from process liveness or Redis.';
comment on table public.operator_actions is
    'Idempotent Telegram/operator command ledger including actor, reason, status and confirmation.';
comment on table public.outbox_events is
    'Transactional publication queue; Redis notifications are hints, PostgreSQL remains authoritative.';
