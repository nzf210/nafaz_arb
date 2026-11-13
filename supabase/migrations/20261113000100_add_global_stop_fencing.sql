-- Cross-instance global STOP state and order-submit serialization.
-- Apply before enabling POSTGRES_DSN on executor instances.
create table if not exists public.executor_global_control (
    singleton_id smallint primary key check (singleton_id = 1),
    is_stopped boolean not null default false,
    epoch bigint not null default 0 check (epoch >= 0),
    reason text,
    source text,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

insert into public.executor_global_control (singleton_id)
values (1)
on conflict (singleton_id) do nothing;

alter table public.executor_global_control enable row level security;
revoke all on table public.executor_global_control from anon, authenticated;
grant select, insert, update, delete on table public.executor_global_control to service_role;

comment on table public.executor_global_control is
'Canonical cross-instance executor STOP state. All order submit and STOP transactions serialize on advisory lock key 731904221.';
