-- PR-02: additive durable execution-domain schema.
-- Legacy rows retain NULL provenance; no quantity or identity backfill is inferred.
-- PostgreSQL remains the canonical writer. Scanner schema/contracts are untouched.

ALTER TABLE public.execution_sessions
    ADD COLUMN IF NOT EXISTS admission_key TEXT,
    ADD COLUMN IF NOT EXISTS account_scope_id TEXT,
    ADD COLUMN IF NOT EXISTS source_dispatch_id TEXT,
    ADD COLUMN IF NOT EXISTS state_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS aggregate_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fencing_generation BIGINT,
    ADD COLUMN IF NOT EXISTS policy_snapshot JSONB,
    ADD COLUMN IF NOT EXISTS policy_snapshot_hash TEXT,
    ADD COLUMN IF NOT EXISTS state_reason TEXT;

ALTER TABLE public.execution_legs
    ADD COLUMN IF NOT EXISTS account_scope_id TEXT,
    ADD COLUMN IF NOT EXISTS native_symbol TEXT,
    ADD COLUMN IF NOT EXISTS normalized_symbol TEXT,
    ADD COLUMN IF NOT EXISTS base_asset TEXT,
    ADD COLUMN IF NOT EXISTS quote_asset TEXT,
    ADD COLUMN IF NOT EXISTS settle_asset TEXT,
    ADD COLUMN IF NOT EXISTS contract_type TEXT,
    ADD COLUMN IF NOT EXISTS contract_size NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS position_side TEXT,
    ADD COLUMN IF NOT EXISTS quantity_unit TEXT,
    ADD COLUMN IF NOT EXISTS target_order_qty NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS filled_order_qty NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS remaining_order_qty NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS target_base_qty NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS filled_base_qty NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS verified_exposure_base_qty NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS state_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS state_reason TEXT;

-- Do not guess the generated FK name. Verify the actual column-to-column FK,
-- refuse ambiguous/missing definitions, and replace only a cascading FK.
DO $$
DECLARE
    fk RECORD;
    fk_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO fk_count
    FROM pg_constraint c
    WHERE c.conrelid = 'public.execution_legs'::regclass
      AND c.confrelid = 'public.execution_sessions'::regclass
      AND c.contype = 'f'
      AND c.conkey = ARRAY[
          (SELECT attnum FROM pg_attribute
           WHERE attrelid = 'public.execution_legs'::regclass
             AND attname = 'session_id' AND NOT attisdropped)
      ]::smallint[]
      AND c.confkey = ARRAY[
          (SELECT attnum FROM pg_attribute
           WHERE attrelid = 'public.execution_sessions'::regclass
             AND attname = 'session_id' AND NOT attisdropped)
      ]::smallint[];

    IF fk_count <> 1 THEN
        RAISE EXCEPTION
            'Expected exactly one execution_legs(session_id) -> execution_sessions(session_id) FK; found %',
            fk_count;
    END IF;

    SELECT c.conname, c.confdeltype
    INTO fk
    FROM pg_constraint c
    WHERE c.conrelid = 'public.execution_legs'::regclass
      AND c.confrelid = 'public.execution_sessions'::regclass
      AND c.contype = 'f'
      AND c.conkey = ARRAY[
          (SELECT attnum FROM pg_attribute
           WHERE attrelid = 'public.execution_legs'::regclass
             AND attname = 'session_id' AND NOT attisdropped)
      ]::smallint[]
      AND c.confkey = ARRAY[
          (SELECT attnum FROM pg_attribute
           WHERE attrelid = 'public.execution_sessions'::regclass
             AND attname = 'session_id' AND NOT attisdropped)
      ]::smallint[];

    IF fk.confdeltype = 'c' THEN
        EXECUTE format(
            'ALTER TABLE public.execution_legs DROP CONSTRAINT %I',
            fk.conname
        );
        ALTER TABLE public.execution_legs
            ADD CONSTRAINT execution_legs_session_id_v3_fkey
            FOREIGN KEY (session_id)
            REFERENCES public.execution_sessions(session_id)
            ON DELETE RESTRICT;
    ELSIF fk.confdeltype NOT IN ('r', 'a') THEN
        RAISE EXCEPTION 'Unexpected delete action % on execution_legs FK', fk.confdeltype;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_sessions_admission_key
    ON public.execution_sessions (admission_key)
    WHERE admission_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.execution_session_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    entity_type TEXT NOT NULL CHECK (entity_type IN ('SESSION', 'LEG')),
    entity_id TEXT NOT NULL,
    entity_version BIGINT NOT NULL CHECK (entity_version > 0),
    from_state TEXT,
    to_state TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT,
    worker_id TEXT,
    fencing_generation BIGINT,
    correlation_id TEXT,
    idempotency_key TEXT,
    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_execution_session_events_aggregate_version
        UNIQUE (session_id, aggregate_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_session_events_idempotency
    ON public.execution_session_events (session_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_execution_session_events_session_time
    ON public.execution_session_events (session_id, occurred_at DESC);

-- Protect canonical execution rows from client roles. The executor's
-- PostgreSQL service connection remains the writer; REST clients cannot mutate.
ALTER TABLE public.execution_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_legs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_session_events ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.execution_sessions FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_legs FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_session_events FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_sessions TO service_role;
GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_legs TO service_role;
GRANT SELECT, INSERT ON TABLE public.execution_session_events TO service_role;

CREATE TABLE IF NOT EXISTS public.execution_outbox (
    outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL
        REFERENCES public.execution_session_events(event_id) ON DELETE RESTRICT,
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    topic TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    last_error TEXT,
    CONSTRAINT uq_execution_outbox_event_topic UNIQUE (event_id, topic)
);

CREATE INDEX IF NOT EXISTS idx_execution_outbox_pending
    ON public.execution_outbox (available_at, created_at)
    WHERE published_at IS NULL;

ALTER TABLE public.execution_outbox ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.execution_outbox FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_outbox TO service_role;

COMMENT ON TABLE public.execution_session_events IS
    'Append-only execution aggregate transition journal. Application roles receive INSERT/SELECT only; corrections require compensating events.';
COMMENT ON COLUMN public.execution_sessions.fencing_generation IS
    'Nullable during legacy compatibility. NULL means ownership generation is unknown, not generation zero.';
COMMENT ON COLUMN public.execution_sessions.policy_snapshot IS
    'Immutable V3 admission policy. NULL is preserved for legacy sessions with unknown policy provenance.';
COMMENT ON COLUMN public.execution_legs.quantity_unit IS
    'Explicit unit for V3 quantity fields. NULL means legacy unit semantics are unknown.';
