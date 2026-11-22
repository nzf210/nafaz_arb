-- Executor V3 PR-02: scoped ownership leases and independent risk controls.
-- Existing global STOP epoch remains the global kill-switch generation. The new
-- per-scope generation is separate and must be checked at each private mutation.

ALTER TABLE public.executor_submit_attempts
    ADD COLUMN IF NOT EXISTS account_scope_id TEXT,
    ADD COLUMN IF NOT EXISTS owner_scope_key TEXT,
    ADD COLUMN IF NOT EXISTS owner_generation BIGINT;

CREATE INDEX IF NOT EXISTS idx_executor_submit_attempts_owner_scope
    ON public.executor_submit_attempts (owner_scope_key, owner_generation, created_at DESC)
    WHERE owner_scope_key IS NOT NULL;

DO $
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.executor_submit_attempts'::regclass
          AND conname='executor_submit_owner_generation_pair'
    ) THEN
        ALTER TABLE public.executor_submit_attempts
            ADD CONSTRAINT executor_submit_owner_generation_pair
            CHECK (
                (owner_scope_key IS NULL AND owner_generation IS NULL)
                OR (
                    owner_scope_key IS NOT NULL
                    AND owner_generation IS NOT NULL
                    AND owner_generation > 0
                    AND account_scope_id IS NOT NULL
                )
            ) NOT VALID;
    END IF;
END $;

ALTER TABLE public.executor_submit_attempts
    VALIDATE CONSTRAINT executor_submit_owner_generation_pair;

CREATE INDEX IF NOT EXISTS idx_executor_submit_attempts_scoped_unresolved
    ON public.executor_submit_attempts (
        account_scope_id, exchange, market_type, symbol, created_at
    )
    WHERE status IN ('SUBMITTING', 'UNKNOWN');

CREATE TABLE IF NOT EXISTS public.executor_ownership_leases (
    scope_key TEXT PRIMARY KEY,
    account_scope_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    market_type TEXT NOT NULL,
    instrument TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_executor_ownership_scope_identity
        UNIQUE (account_scope_id, venue, market_type, instrument)
);

CREATE INDEX IF NOT EXISTS idx_executor_ownership_leases_expiry
    ON public.executor_ownership_leases (lease_expires_at);

DROP TRIGGER IF EXISTS trg_executor_ownership_leases_updated_at
    ON public.executor_ownership_leases;
CREATE TRIGGER trg_executor_ownership_leases_updated_at
    BEFORE UPDATE ON public.executor_ownership_leases
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.executor_ownership_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_key TEXT NOT NULL
        REFERENCES public.executor_ownership_leases(scope_key) ON DELETE RESTRICT,
    generation BIGINT NOT NULL CHECK (generation > 0),
    owner_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN ('ACQUIRED', 'RENEWED', 'RECLAIMED', 'RELEASED')
    ),
    event_idempotency_key TEXT NOT NULL UNIQUE,
    lease_expires_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_executor_ownership_events_scope_time
    ON public.executor_ownership_events (scope_key, created_at DESC);

CREATE TABLE IF NOT EXISTS public.executor_risk_controls (
    scope_key TEXT PRIMARY KEY,
    scope_type TEXT NOT NULL CHECK (
        scope_type IN ('GLOBAL', 'ACCOUNT', 'VENUE', 'INSTRUMENT', 'PAIR', 'SESSION')
    ),
    account_scope_id TEXT,
    venue TEXT,
    market_type TEXT,
    instrument TEXT,
    pair_key TEXT,
    session_id TEXT,
    entry_blocked BOOLEAN NOT NULL DEFAULT false,
    derisk_allowed BOOLEAN NOT NULL DEFAULT true,
    limits JSONB NOT NULL DEFAULT '{}'::jsonb,
    state JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (scope_type = 'GLOBAL')
        OR (scope_type = 'ACCOUNT' AND account_scope_id IS NOT NULL)
        OR (scope_type = 'VENUE' AND account_scope_id IS NOT NULL AND venue IS NOT NULL)
        OR (scope_type = 'INSTRUMENT' AND account_scope_id IS NOT NULL AND venue IS NOT NULL
            AND market_type IS NOT NULL AND instrument IS NOT NULL)
        OR (scope_type = 'PAIR' AND pair_key IS NOT NULL)
        OR (scope_type = 'SESSION' AND session_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_executor_risk_controls_account
    ON public.executor_risk_controls (account_scope_id, scope_type);
CREATE INDEX IF NOT EXISTS idx_executor_risk_controls_venue
    ON public.executor_risk_controls (venue, market_type, instrument);

DROP TRIGGER IF EXISTS trg_executor_risk_controls_updated_at
    ON public.executor_risk_controls;
CREATE TRIGGER trg_executor_risk_controls_updated_at
    BEFORE UPDATE ON public.executor_risk_controls
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.executor_risk_control_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_key TEXT NOT NULL
        REFERENCES public.executor_risk_controls(scope_key) ON DELETE RESTRICT,
    version BIGINT NOT NULL CHECK (version > 0),
    event_type TEXT NOT NULL,
    event_idempotency_key TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    entry_blocked BOOLEAN NOT NULL,
    derisk_allowed BOOLEAN NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_executor_risk_control_event_version UNIQUE (scope_key, version),
    CONSTRAINT uq_executor_risk_control_event_idempotency UNIQUE (scope_key, event_idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_executor_risk_control_events_scope_time
    ON public.executor_risk_control_events (scope_key, created_at DESC);

CREATE OR REPLACE FUNCTION public.reject_executor_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'executor ownership/risk audit records are append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_executor_ownership_events_append_only
    ON public.executor_ownership_events;
CREATE TRIGGER trg_executor_ownership_events_append_only
    BEFORE UPDATE OR DELETE ON public.executor_ownership_events
    FOR EACH ROW EXECUTE FUNCTION public.reject_executor_audit_mutation();

DROP TRIGGER IF EXISTS trg_executor_risk_control_events_append_only
    ON public.executor_risk_control_events;
CREATE TRIGGER trg_executor_risk_control_events_append_only
    BEFORE UPDATE OR DELETE ON public.executor_risk_control_events
    FOR EACH ROW EXECUTE FUNCTION public.reject_executor_audit_mutation();

ALTER TABLE public.executor_ownership_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.executor_ownership_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.executor_risk_controls ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.executor_risk_control_events ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.executor_ownership_leases FROM anon, authenticated;
REVOKE ALL ON TABLE public.executor_ownership_events FROM anon, authenticated;
REVOKE ALL ON TABLE public.executor_risk_controls FROM anon, authenticated;
REVOKE ALL ON TABLE public.executor_risk_control_events FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.executor_ownership_leases TO service_role;
GRANT SELECT, INSERT ON TABLE public.executor_ownership_events TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.executor_risk_controls TO service_role;
GRANT SELECT, INSERT ON TABLE public.executor_risk_control_events TO service_role;

COMMENT ON TABLE public.executor_ownership_leases IS
    'Credential/account + venue + market + instrument scoped coordinator lease. Generation increases on every ownership transfer or expired reclaim.';
COMMENT ON TABLE public.executor_risk_controls IS
    'Canonical hierarchical risk control state. ENTRY_BLOCKED and DE_RISK_ALLOWED are independent permissions.';
COMMENT ON COLUMN public.executor_submit_attempts.owner_generation IS
    'Per-scope ownership fencing generation observed at the private mutation boundary; separate from global fence_epoch.';
