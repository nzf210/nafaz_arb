-- Append-only audit for authenticated manual resolution of ambiguous submits.
create table if not exists public.executor_submit_resolution_audit (
    event_id bigserial primary key,
    client_order_id text not null references public.executor_submit_attempts(client_order_id),
    operator_id text not null,
    decision text not null check (
        decision in ('FOUND', 'DEFINITIVE_REJECTION', 'PROVEN_NOT_SUBMITTED')
    ),
    venue_evidence_ref text not null,
    independent_evidence_ref text not null,
    rationale text not null,
    nonce text not null unique,
    signed_payload_sha256 text not null,
    created_at timestamptz not null default now(),
    check (venue_evidence_ref <> independent_evidence_ref)
);

create index if not exists executor_submit_resolution_audit_attempt_idx
    on public.executor_submit_resolution_audit (client_order_id, created_at desc);

alter table public.executor_submit_resolution_audit enable row level security;
revoke all on table public.executor_submit_resolution_audit from anon, authenticated;
grant select, insert on table public.executor_submit_resolution_audit to service_role;

comment on table public.executor_submit_resolution_audit is
'Append-only record of HMAC-authenticated operator decisions resolving ambiguous venue submits. Nonces are globally unique to prevent replay.';
