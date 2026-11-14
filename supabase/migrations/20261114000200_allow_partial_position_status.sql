-- Allow PARTIALLY_FILLED and HEDGE_PENDING in public.positions status check constraint
alter table public.positions drop constraint if exists positions_status_check;
alter table public.positions add constraint positions_status_check check (status in (
    'ENTRY_PENDING',
    'ENTRY_PARTIAL',
    'PARTIALLY_FILLED',
    'HEDGE_PENDING',
    'HEDGED_ACTIVE',
    'EXIT_PENDING',
    'RECONCILIATION_REQUIRED',
    'CLOSED',
    'FAILED'
));

-- Update active position unique index to include PARTIALLY_FILLED and HEDGE_PENDING
drop index if exists public.uq_positions_one_active_pair;
create unique index if not exists uq_positions_one_active_pair
    on public.positions (pair_key, is_mock)
    where status in (
        'ENTRY_PENDING',
        'ENTRY_PARTIAL',
        'PARTIALLY_FILLED',
        'HEDGE_PENDING',
        'HEDGED_ACTIVE',
        'EXIT_PENDING',
        'RECONCILIATION_REQUIRED'
    );
