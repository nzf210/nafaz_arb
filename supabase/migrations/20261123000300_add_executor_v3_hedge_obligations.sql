-- Executor V3 PR-05: durable Spot-fill hedge obligations and in-flight reservations.
-- Scope is one execution session at a time. Cross-session pooled hedge batches are
-- intentionally deferred until batch-level fill identity is introduced.

CREATE TABLE IF NOT EXISTS public.execution_hedge_obligations (
    obligation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    source_fill_id UUID NOT NULL
        REFERENCES public.execution_fills(fill_id) ON DELETE RESTRICT,
    spot_leg_id TEXT NOT NULL
        REFERENCES public.execution_legs(leg_id) ON DELETE RESTRICT,
    futures_leg_id TEXT NOT NULL
        REFERENCES public.execution_legs(leg_id) ON DELETE RESTRICT,
    account_scope_id TEXT NOT NULL,
    pair_key TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    futures_venue TEXT NOT NULL,
    futures_market_type TEXT NOT NULL,
    futures_symbol TEXT NOT NULL,
    contract_type TEXT,
    contract_size NUMERIC(38,18),
    position_side TEXT NOT NULL CHECK (position_side IN ('LONG','SHORT')),
    quantity_unit TEXT NOT NULL,
    base_per_unit NUMERIC(38,18) NOT NULL CHECK (base_per_unit > 0),
    hedge_ratio NUMERIC(24,12) NOT NULL CHECK (hedge_ratio > 0),
    target_base_quantity NUMERIC(38,18) NOT NULL CHECK (target_base_quantity > 0),
    confirmed_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0 CHECK (confirmed_base_quantity >= 0),
    reserved_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0 CHECK (reserved_base_quantity >= 0),
    remaining_base_quantity NUMERIC(38,18) NOT NULL CHECK (remaining_base_quantity >= 0),
    status TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN','PARTIALLY_HEDGED','HEDGED','BLOCKED','RECONCILIATION_REQUIRED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_hedge_obligation_source_fill UNIQUE (source_fill_id),
    CONSTRAINT execution_hedge_obligation_quantities_valid
        CHECK (confirmed_base_quantity + remaining_base_quantity = target_base_quantity)
);

CREATE INDEX IF NOT EXISTS idx_execution_hedge_obligations_available
    ON public.execution_hedge_obligations (
        session_id, futures_venue, futures_market_type, futures_symbol,
        position_side, created_at
    )
    WHERE status IN ('OPEN','PARTIALLY_HEDGED');

CREATE TABLE IF NOT EXISTS public.execution_hedge_reservations (
    reservation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    account_scope_id TEXT NOT NULL,
    futures_venue TEXT NOT NULL,
    futures_market_type TEXT NOT NULL,
    futures_symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    contract_type TEXT,
    contract_size NUMERIC(38,18),
    quantity_unit TEXT NOT NULL,
    base_per_unit NUMERIC(38,18) NOT NULL CHECK (base_per_unit > 0),
    position_side TEXT NOT NULL CHECK (position_side IN ('LONG','SHORT')),
    requested_base_quantity NUMERIC(38,18) NOT NULL CHECK (requested_base_quantity > 0),
    minimum_base_quantity NUMERIC(38,18) NOT NULL CHECK (minimum_base_quantity > 0),
    reserved_base_quantity NUMERIC(38,18) NOT NULL CHECK (reserved_base_quantity > 0),
    allocated_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0 CHECK (allocated_base_quantity >= 0),
    status TEXT NOT NULL DEFAULT 'RESERVED'
        CHECK (status IN ('RESERVED','SUBMITTED','UNKNOWN','PARTIALLY_FILLED','CONSUMED','RELEASED','RECONCILIATION_REQUIRED')),
    client_order_id TEXT,
    idempotency_key TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_hedge_reservation_idempotency
        UNIQUE (session_id, idempotency_key),
    CONSTRAINT execution_hedge_reservation_allocation_valid
        CHECK (allocated_base_quantity <= reserved_base_quantity)
);

CREATE INDEX IF NOT EXISTS idx_execution_hedge_reservations_active
    ON public.execution_hedge_reservations (account_scope_id, futures_venue, futures_market_type, futures_symbol, status)
    WHERE status IN ('RESERVED','SUBMITTED','UNKNOWN','PARTIALLY_FILLED');

CREATE TABLE IF NOT EXISTS public.execution_hedge_reservation_items (
    session_id TEXT NOT NULL,
    reservation_id UUID NOT NULL
        REFERENCES public.execution_hedge_reservations(reservation_id) ON DELETE RESTRICT,
    obligation_id UUID NOT NULL
        REFERENCES public.execution_hedge_obligations(obligation_id) ON DELETE RESTRICT,
    reserved_base_quantity NUMERIC(38,18) NOT NULL CHECK (reserved_base_quantity > 0),
    allocated_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0 CHECK (allocated_base_quantity >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (reservation_id, obligation_id),
    CONSTRAINT execution_hedge_reservation_item_allocation_valid
        CHECK (allocated_base_quantity <= reserved_base_quantity)
);

CREATE TABLE IF NOT EXISTS public.execution_hedge_allocations (
    allocation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    reservation_id UUID NOT NULL
        REFERENCES public.execution_hedge_reservations(reservation_id) ON DELETE RESTRICT,
    futures_fill_id UUID NOT NULL
        REFERENCES public.execution_fills(fill_id) ON DELETE RESTRICT,
    obligation_id UUID NOT NULL
        REFERENCES public.execution_hedge_obligations(obligation_id) ON DELETE RESTRICT,
    allocated_base_quantity NUMERIC(38,18) NOT NULL CHECK (allocated_base_quantity > 0),
    idempotency_key TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_hedge_allocation_fill_obligation
        UNIQUE (futures_fill_id, obligation_id),
    CONSTRAINT uq_execution_hedge_allocation_idempotency
        UNIQUE (session_id, idempotency_key)
);

ALTER TABLE public.execution_hedge_reservation_items
    ADD COLUMN IF NOT EXISTS session_id TEXT;

UPDATE public.execution_hedge_reservation_items i
SET session_id=r.session_id
FROM public.execution_hedge_reservations r
WHERE i.reservation_id=r.reservation_id AND i.session_id IS NULL;

ALTER TABLE public.execution_hedge_reservation_items
    ALTER COLUMN session_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_fills_session_fill
    ON public.execution_fills(session_id, fill_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_hedge_obligations_session_id
    ON public.execution_hedge_obligations(session_id, obligation_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_hedge_reservations_session_id
    ON public.execution_hedge_reservations(session_id, reservation_id);

DO $hedge$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_obligations'::regclass AND conname='execution_hedge_obligations_session_fill_fkey') THEN
        ALTER TABLE public.execution_hedge_obligations ADD CONSTRAINT execution_hedge_obligations_session_fill_fkey
            FOREIGN KEY (session_id, source_fill_id) REFERENCES public.execution_fills(session_id, fill_id) ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_obligations'::regclass AND conname='execution_hedge_obligations_session_spot_leg_fkey') THEN
        ALTER TABLE public.execution_hedge_obligations ADD CONSTRAINT execution_hedge_obligations_session_spot_leg_fkey
            FOREIGN KEY (session_id, spot_leg_id) REFERENCES public.execution_legs(session_id, leg_id) ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_obligations'::regclass AND conname='execution_hedge_obligations_session_futures_leg_fkey') THEN
        ALTER TABLE public.execution_hedge_obligations ADD CONSTRAINT execution_hedge_obligations_session_futures_leg_fkey
            FOREIGN KEY (session_id, futures_leg_id) REFERENCES public.execution_legs(session_id, leg_id) ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_reservation_items'::regclass AND conname='execution_hedge_reservation_items_session_reservation_fkey') THEN
        ALTER TABLE public.execution_hedge_reservation_items ADD CONSTRAINT execution_hedge_reservation_items_session_reservation_fkey
            FOREIGN KEY (session_id, reservation_id) REFERENCES public.execution_hedge_reservations(session_id, reservation_id) ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_reservation_items'::regclass AND conname='execution_hedge_reservation_items_session_obligation_fkey') THEN
        ALTER TABLE public.execution_hedge_reservation_items ADD CONSTRAINT execution_hedge_reservation_items_session_obligation_fkey
            FOREIGN KEY (session_id, obligation_id) REFERENCES public.execution_hedge_obligations(session_id, obligation_id) ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_allocations'::regclass AND conname='execution_hedge_allocations_session_reservation_fkey') THEN
        ALTER TABLE public.execution_hedge_allocations ADD CONSTRAINT execution_hedge_allocations_session_reservation_fkey
            FOREIGN KEY (session_id, reservation_id) REFERENCES public.execution_hedge_reservations(session_id, reservation_id) ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_allocations'::regclass AND conname='execution_hedge_allocations_session_fill_fkey') THEN
        ALTER TABLE public.execution_hedge_allocations ADD CONSTRAINT execution_hedge_allocations_session_fill_fkey
            FOREIGN KEY (session_id, futures_fill_id) REFERENCES public.execution_fills(session_id, fill_id) ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.execution_hedge_allocations'::regclass AND conname='execution_hedge_allocations_session_obligation_fkey') THEN
        ALTER TABLE public.execution_hedge_allocations ADD CONSTRAINT execution_hedge_allocations_session_obligation_fkey
            FOREIGN KEY (session_id, obligation_id) REFERENCES public.execution_hedge_obligations(session_id, obligation_id) ON DELETE RESTRICT NOT VALID;
    END IF;
END $hedge$;

ALTER TABLE public.execution_hedge_obligations VALIDATE CONSTRAINT execution_hedge_obligations_session_fill_fkey;
ALTER TABLE public.execution_hedge_obligations VALIDATE CONSTRAINT execution_hedge_obligations_session_spot_leg_fkey;
ALTER TABLE public.execution_hedge_obligations VALIDATE CONSTRAINT execution_hedge_obligations_session_futures_leg_fkey;
ALTER TABLE public.execution_hedge_reservation_items VALIDATE CONSTRAINT execution_hedge_reservation_items_session_reservation_fkey;
ALTER TABLE public.execution_hedge_reservation_items VALIDATE CONSTRAINT execution_hedge_reservation_items_session_obligation_fkey;
ALTER TABLE public.execution_hedge_allocations VALIDATE CONSTRAINT execution_hedge_allocations_session_reservation_fkey;
ALTER TABLE public.execution_hedge_allocations VALIDATE CONSTRAINT execution_hedge_allocations_session_fill_fkey;
ALTER TABLE public.execution_hedge_allocations VALIDATE CONSTRAINT execution_hedge_allocations_session_obligation_fkey;
CREATE INDEX IF NOT EXISTS idx_execution_hedge_allocations_session
    ON public.execution_hedge_allocations (session_id, created_at);

CREATE OR REPLACE FUNCTION public.reject_execution_hedge_allocation_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution_hedge_allocations is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_execution_hedge_allocations_append_only
    ON public.execution_hedge_allocations;
CREATE TRIGGER trg_execution_hedge_allocations_append_only
    BEFORE UPDATE OR DELETE ON public.execution_hedge_allocations
    FOR EACH ROW EXECUTE FUNCTION public.reject_execution_hedge_allocation_mutation();

ALTER TABLE public.execution_hedge_obligations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_hedge_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_hedge_reservation_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_hedge_allocations ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.execution_hedge_obligations FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_hedge_reservations FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_hedge_reservation_items FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_hedge_allocations FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_hedge_obligations TO service_role;
GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_hedge_reservations TO service_role;
GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_hedge_reservation_items TO service_role;
GRANT SELECT, INSERT ON TABLE public.execution_hedge_allocations TO service_role;

COMMENT ON TABLE public.execution_hedge_obligations IS
    'Durable obligation created once per confirmed Spot entry fill; tracks confirmed and in-flight base-equivalent hedge quantities.';
COMMENT ON TABLE public.execution_hedge_reservations IS
    'Fenced, idempotent reservation of compatible hedge obligations before a Futures order is submitted.';
COMMENT ON TABLE public.execution_hedge_allocations IS
    'Append-only allocation of confirmed Futures fills to source Spot-fill obligations.';
