-- Executor V3 PR-04: durable, session-bound fill facts and private-event inbox.
-- This is additive. The V2 execution_fill_ledger remains untouched and authoritative
-- for existing V2 reporting until an explicit V3 cutover is approved.

ALTER TABLE public.execution_legs
    ADD COLUMN IF NOT EXISTS confirmed_filled_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS confirmed_quote_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS confirmed_price_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS confirmed_exposure_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS average_fill_price NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS last_fill_at TIMESTAMPTZ;

DO $fill$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_fills'::regclass
          AND conname='execution_fills_session_leg_fkey'
    ) THEN
        ALTER TABLE public.execution_fills
            ADD CONSTRAINT execution_fills_session_leg_fkey
            FOREIGN KEY (session_id, leg_id)
            REFERENCES public.execution_legs(session_id, leg_id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END $fill$;
ALTER TABLE public.execution_fills
    VALIDATE CONSTRAINT execution_fills_session_leg_fkey;

CREATE INDEX IF NOT EXISTS idx_execution_fills_session_leg_time
    ON public.execution_fills(session_id, leg_id, filled_at, fill_id);

CREATE INDEX IF NOT EXISTS idx_execution_fills_account_time
    ON public.execution_fills(account_scope_id, filled_at DESC);

CREATE TABLE IF NOT EXISTS public.execution_event_inbox (
    inbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_scope_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    session_id TEXT,
    leg_id TEXT,
    payload JSONB NOT NULL,
    processing_status TEXT NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN ('RECEIVED','PROCESSING','PROCESSED','RETRY','QUARANTINED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    processed_at TIMESTAMPTZ,
    CONSTRAINT uq_execution_event_inbox_source
        UNIQUE (account_scope_id, venue, event_type, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_event_inbox_pending
    ON public.execution_event_inbox(processing_status, received_at)
    WHERE processing_status IN ('RECEIVED','RETRY','PROCESSING');

CREATE OR REPLACE FUNCTION public.reject_execution_fill_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution_fills is append-only; corrections require a separate audited adjustment';
END;
$$;

DROP TRIGGER IF EXISTS trg_execution_fills_append_only ON public.execution_fills;
CREATE TRIGGER trg_execution_fills_append_only
    BEFORE UPDATE OR DELETE ON public.execution_fills
    FOR EACH ROW EXECUTE FUNCTION public.reject_execution_fill_mutation();

ALTER TABLE public.execution_fills ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_event_inbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.execution_fills FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_event_inbox FROM anon, authenticated;
GRANT SELECT, INSERT ON TABLE public.execution_fills TO service_role;
GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_event_inbox TO service_role;

COMMENT ON TABLE public.execution_fills IS
    'V3 immutable confirmed venue trades. Every fill is session/leg/account bound and deduplicated by venue trade identity.';
COMMENT ON TABLE public.execution_event_inbox IS
    'V3 durable private-stream/REST event inbox. Processed state is durable; in-memory deduplication is only an optimization.';
