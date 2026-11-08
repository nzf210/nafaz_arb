-- Add a nullable unique idempotency key without changing or deleting any
-- pre-existing scanner history. Multiple legacy rows may retain NULL; all new
-- runtime writes provide a deterministic key and can be safely retried.
alter table public.scanner_opportunities
    add column if not exists idempotency_key text;

do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conrelid = 'public.scanner_opportunities'::regclass
          and conname = 'uq_scanner_opportunities_idempotency_key'
    ) then
        alter table public.scanner_opportunities
            add constraint uq_scanner_opportunities_idempotency_key
            unique (idempotency_key);
    end if;
end $$;
