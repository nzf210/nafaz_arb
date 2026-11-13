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
returns trigger language plpgsql as $fn$
begin
    if tg_op = 'DELETE' then
        raise exception 'config snapshot is immutable; create a new version';
    end if;
    if new.version is distinct from old.version
       or new.schema_version is distinct from old.schema_version
       or new.snapshot is distinct from old.snapshot
       or new.content_sha256 is distinct from old.content_sha256
       or new.created_by is distinct from old.created_by
       or new.created_at is distinct from old.created_at
       or new.change_reason is distinct from old.change_reason
       or new.change_diff is distinct from old.change_diff
       or new.source_version is distinct from old.source_version then
        raise exception 'config snapshot is immutable; create a new version';
    end if;
    return new;
end;
$fn$;
drop trigger if exists trg_config_versions_immutable on public.config_versions;
create trigger trg_config_versions_immutable
before update or delete on public.config_versions
for each row execute function public.reject_config_version_mutation();

-- Keep these tables private to server-side database roles. On Supabase, the
-- service_role bypasses RLS; on plain PostgreSQL, the migration remains valid
-- even when Supabase-specific roles are absent.
alter table public.config_versions enable row level security;
alter table public.active_config_version enable row level security;
alter table public.config_audit_log enable row level security;
alter table public.operator_control_state enable row level security;
alter table public.operator_actions enable row level security;
alter table public.outbox_events enable row level security;

revoke all on public.config_versions, public.active_config_version,
    public.config_audit_log, public.operator_control_state,
    public.operator_actions, public.outbox_events from public;

do $grant$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        execute 'revoke all on public.config_versions, public.active_config_version, public.config_audit_log, public.operator_control_state, public.operator_actions, public.outbox_events from anon';
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        execute 'revoke all on public.config_versions, public.active_config_version, public.config_audit_log, public.operator_control_state, public.operator_actions, public.outbox_events from authenticated';
    end if;
    if exists (select 1 from pg_roles where rolname = 'service_role') then
        execute 'grant select, insert, update on public.config_versions, public.active_config_version, public.operator_control_state, public.operator_actions, public.outbox_events to service_role';
        execute 'grant select, insert on public.config_audit_log to service_role';
        execute 'grant usage, select on sequence public.config_audit_log_audit_id_seq, public.operator_actions_action_id_seq, public.outbox_events_event_id_seq to service_role';
    end if;
end;
$grant$;

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
