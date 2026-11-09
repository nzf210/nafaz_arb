-- Preserve legacy scanner history while making retries safe for new runtime writes.
-- PostgreSQL UNIQUE constraints permit multiple NULL values, so old rows remain valid.
alter table public.scanner_opportunities
    add column if not exists idempotency_key text;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.scanner_opportunities'::regclass
          and conname = 'uq_scanner_opportunities_idempotency_key'
    ) then
        alter table public.scanner_opportunities
            add constraint uq_scanner_opportunities_idempotency_key
            unique (idempotency_key);
    end if;
end $$;
