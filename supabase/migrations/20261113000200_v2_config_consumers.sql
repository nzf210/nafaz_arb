-- Durable executor membership and per-instance config convergence acknowledgements.
-- Redis events are hints; PostgreSQL remains the membership and config authority.

create table if not exists public.config_consumers (
    instance_id text primary key,
    process_id text not null,
    status text not null default 'STARTING'
        check (status in ('STARTING','ACTIVE','DEGRADED','STOPPED')),
    acknowledged_version bigint references public.config_versions(version),
    acknowledged_sha256 char(64),
    last_seen_at timestamptz not null default now(),
    last_ack_at timestamptz,
    last_error_code text,
    started_at timestamptz not null default now(),
    stopped_at timestamptz,
    check (length(trim(instance_id)) > 0),
    check (length(trim(process_id)) > 0),
    check (acknowledged_sha256 is null or acknowledged_sha256 ~ '^[0-9a-f]{64}$')
);
create index if not exists idx_config_consumers_live_lease
    on public.config_consumers(status, last_seen_at desc);

alter table public.config_consumers enable row level security;
revoke all on public.config_consumers from public;

do $grant$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        execute 'revoke all on public.config_consumers from anon';
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        execute 'revoke all on public.config_consumers from authenticated';
    end if;
    if exists (select 1 from pg_roles where rolname = 'service_role') then
        execute 'grant select, insert, update on public.config_consumers to service_role';
    end if;
end;
$grant$;

comment on table public.config_consumers is
    'Executor instance leases and verified active-config acknowledgements; Redis delivery alone is never convergence evidence.';
