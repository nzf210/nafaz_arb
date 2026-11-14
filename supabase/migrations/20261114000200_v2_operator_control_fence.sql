-- V2 operator START is database-gated; Telegram/worker code is not the only safety boundary.
-- The same advisory key is used by PostgresEntryAdmissionGate and STOP/config activation.
create index if not exists idx_operator_pending_start_actor
    on public.operator_actions(actor_user_id, actor_chat_id, received_at desc)
    where command = 'trade_start' and status = 'PENDING_CONFIRMATION';

create or replace function public.enforce_v2_operator_running_gate()
returns trigger
language plpgsql
as $operator_gate$
declare
    active_version bigint;
    active_hash char(64);
    signals_enabled text;
    active_consumers integer;
    stale_consumers integer;
begin
    if new.state_version < old.state_version then
        raise exception 'operator state_version cannot move backwards';
    end if;

    if new.state = 'RUNNING' or new.trading_enabled then
        select v.version, v.content_sha256, v.snapshot #>> '{execution,signals_enabled}'
          into active_version, active_hash, signals_enabled
          from public.active_config_version p
          join public.config_versions v on v.version = p.version
         where p.singleton = true and v.status = 'active';

        if active_version is null
           or new.config_version is distinct from active_version
           or signals_enabled is distinct from 'true' then
            raise exception 'operator RUNNING requires matching active config with signals_enabled=true';
        end if;

        select count(*) filter (
                   where status in ('STARTING','ACTIVE','DEGRADED')
                     and last_seen_at >= now() - interval '30 seconds'
               ),
               count(*) filter (
                   where status in ('STARTING','ACTIVE','DEGRADED')
                     and last_seen_at >= now() - interval '30 seconds'
                     and (status <> 'ACTIVE'
                       or acknowledged_version is distinct from active_version
                       or acknowledged_sha256 is distinct from active_hash)
               )
          into active_consumers, stale_consumers
          from public.config_consumers;

        if coalesce(active_consumers, 0) < 1 or coalesce(stale_consumers, 0) > 0 then
            raise exception 'operator RUNNING requires every live executor to acknowledge the active config';
        end if;
    end if;

    return new;
end;
$operator_gate$;

drop trigger if exists trg_operator_control_running_gate on public.operator_control_state;
create trigger trg_operator_control_running_gate
before insert or update on public.operator_control_state
for each row execute function public.enforce_v2_operator_running_gate();

comment on function public.enforce_v2_operator_running_gate() is
    'Defense in depth: RUNNING requires signals enabled in the active config and all live executor consumers converged. Application code serializes entry submissions and STOP with advisory lock nafaz:v2:operator_entry_fence.';

comment on column public.operator_control_state.open_order_stop_policy is
    'STOP policy is explicit. V2 Telegram currently supports keep_and_manage only; no automatic position close or order cancellation.';
