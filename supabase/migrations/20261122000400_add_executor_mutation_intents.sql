-- Executor V3 PR-03D: durable logical order/mutation intents.
-- This is distinct from executor_submit_attempts (one external create attempt)
-- and order_registry (venue order lifecycle/recovery projection).

CREATE TABLE IF NOT EXISTS public.executor_mutation_intents (
    intent_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('CREATE', 'CANCEL')),
    account_scope_id TEXT NOT NULL,
    session_id TEXT
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    leg_id TEXT
        REFERENCES public.execution_legs(leg_id) ON DELETE RESTRICT,
    venue TEXT NOT NULL,
    market_type TEXT NOT NULL,
    instrument TEXT NOT NULL,
    client_order_id TEXT,
    target_order_id TEXT,
    target_client_order_id TEXT,
    side TEXT,
    order_type TEXT,
    quantity NUMERIC(38,18),
    price NUMERIC(38,18),
    reserved_base_qty NUMERIC(38,18) NOT NULL DEFAULT 0,
    reserved_notional NUMERIC(38,18) NOT NULL DEFAULT 0,
    reservation_asset TEXT,
    request_hash TEXT NOT NULL,
    request_payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PREPARED' CHECK (
        status IN (
            'PREPARED', 'SUBMITTING', 'UNKNOWN', 'ACKNOWLEDGED',
            'REJECTED', 'CANCEL_CONFIRMED', 'ABORTED', 'COMPLETED'
        )
    ),
    state_version BIGINT NOT NULL DEFAULT 1 CHECK (state_version > 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    exchange_order_id TEXT,
    venue_response JSONB,
    owner_scope_key TEXT,
    owner_id TEXT,
    owner_generation BIGINT,
    risk_control_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    CONSTRAINT uq_executor_mutation_intent_idempotency
        UNIQUE (account_scope_id, idempotency_key),
    CONSTRAINT executor_mutation_intent_session_leg_pair
        CHECK (leg_id IS NULL OR session_id IS NOT NULL),
    CONSTRAINT executor_mutation_intent_reservation_nonnegative
        CHECK (reserved_base_qty >= 0 AND reserved_notional >= 0),
    CONSTRAINT executor_mutation_intent_owner_pair
        CHECK (
            (owner_scope_key IS NULL AND owner_id IS NULL AND owner_generation IS NULL)
            OR (
                owner_scope_key IS NOT NULL
                AND owner_id IS NOT NULL
                AND owner_generation IS NOT NULL
                AND owner_generation > 0
            )
        ),
    CONSTRAINT executor_mutation_intent_operation_shape
        CHECK (
            (
                operation='CREATE'
                AND client_order_id IS NOT NULL
                AND side IS NOT NULL
                AND order_type IS NOT NULL
                AND quantity IS NOT NULL AND quantity > 0
                AND price IS NOT NULL AND price >= 0
                AND target_order_id IS NULL
                AND target_client_order_id IS NULL
            )
            OR
            (
                operation='CANCEL'
                AND client_order_id IS NULL
                AND side IS NULL AND order_type IS NULL
                AND quantity IS NULL AND price IS NULL
                AND (target_order_id IS NOT NULL OR target_client_order_id IS NOT NULL)
                AND reserved_base_qty=0 AND reserved_notional=0
            )
        )
);


DO $body$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.executor_mutation_intents'::regclass
          AND conname='executor_mutation_intent_owner_scope_fkey'
    ) THEN
        ALTER TABLE public.executor_mutation_intents
            ADD CONSTRAINT executor_mutation_intent_owner_scope_fkey
            FOREIGN KEY (owner_scope_key)
            REFERENCES public.executor_ownership_leases(scope_key)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END $body$;

ALTER TABLE public.executor_mutation_intents
    VALIDATE CONSTRAINT executor_mutation_intent_owner_scope_fkey;

CREATE UNIQUE INDEX IF NOT EXISTS uq_executor_mutation_intent_client_order
    ON public.executor_mutation_intents (lower(venue), client_order_id)
    WHERE client_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_executor_mutation_intents_session_state
    ON public.executor_mutation_intents (session_id, status, created_at DESC)
    WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_executor_mutation_intents_scope_state
    ON public.executor_mutation_intents (
        account_scope_id, venue, market_type, instrument, status, created_at
    );

CREATE INDEX IF NOT EXISTS idx_executor_mutation_intents_unresolved
    ON public.executor_mutation_intents (updated_at, account_scope_id, venue, instrument)
    WHERE status IN ('SUBMITTING', 'UNKNOWN');

DROP TRIGGER IF EXISTS trg_executor_mutation_intents_updated_at
    ON public.executor_mutation_intents;
CREATE TRIGGER trg_executor_mutation_intents_updated_at
    BEFORE UPDATE ON public.executor_mutation_intents
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.executor_mutation_intent_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    intent_id TEXT NOT NULL
        REFERENCES public.executor_mutation_intents(intent_id) ON DELETE RESTRICT,
    event_version BIGINT NOT NULL CHECK (event_version > 0),
    event_type TEXT NOT NULL,
    event_idempotency_key TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    owner_scope_key TEXT,
    owner_id TEXT,
    owner_generation BIGINT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_executor_mutation_intent_event_version
        UNIQUE (intent_id, event_version),
    CONSTRAINT uq_executor_mutation_intent_event_idempotency
        UNIQUE (intent_id, event_idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_executor_mutation_intent_events_timeline
    ON public.executor_mutation_intent_events (intent_id, event_version DESC);

DROP TRIGGER IF EXISTS trg_executor_mutation_intent_events_append_only
    ON public.executor_mutation_intent_events;
CREATE TRIGGER trg_executor_mutation_intent_events_append_only
    BEFORE UPDATE OR DELETE ON public.executor_mutation_intent_events
    FOR EACH ROW EXECUTE FUNCTION public.reject_executor_audit_mutation();

ALTER TABLE public.executor_mutation_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.executor_mutation_intent_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.executor_mutation_intents FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE public.executor_mutation_intent_events FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT, INSERT, UPDATE ON TABLE public.executor_mutation_intents TO service_role;
GRANT SELECT, INSERT ON TABLE public.executor_mutation_intent_events TO service_role;

COMMENT ON TABLE public.executor_mutation_intents IS
    'Canonical logical create/cancel intent. executor_submit_attempts remains the external create attempt ledger; order_registry remains the venue-order recovery projection.';
COMMENT ON TABLE public.executor_mutation_intent_events IS
    'Append-only mutation intent lifecycle. Projection and event are updated in one PostgreSQL transaction.';
