-- Executor V3 PR-03C: bind the session aggregate to its coordinator lease identity.
-- Legacy sessions remain NULL and are not eligible for automatic coordinator reclaim.

ALTER TABLE public.execution_sessions
    ADD COLUMN IF NOT EXISTS coordinator_scope_key TEXT,
    ADD COLUMN IF NOT EXISTS coordinator_owner_id TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_sessions'::regclass
          AND conname='execution_sessions_coordinator_lease_identity_pair'
    ) THEN
        ALTER TABLE public.execution_sessions
            ADD CONSTRAINT execution_sessions_coordinator_lease_identity_pair
            CHECK (
                (coordinator_scope_key IS NULL AND coordinator_owner_id IS NULL)
                OR (
                    coordinator_scope_key IS NOT NULL
                    AND coordinator_owner_id IS NOT NULL
                    AND fencing_generation IS NOT NULL
                    AND fencing_generation > 0
                )
            ) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_sessions'::regclass
          AND conname='execution_sessions_coordinator_scope_fkey'
    ) THEN
        ALTER TABLE public.execution_sessions
            ADD CONSTRAINT execution_sessions_coordinator_scope_fkey
            FOREIGN KEY (coordinator_scope_key)
            REFERENCES public.executor_ownership_leases(scope_key)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END $$;

ALTER TABLE public.execution_sessions
    VALIDATE CONSTRAINT execution_sessions_coordinator_lease_identity_pair;
ALTER TABLE public.execution_sessions
    VALIDATE CONSTRAINT execution_sessions_coordinator_scope_fkey;

CREATE INDEX IF NOT EXISTS idx_execution_sessions_coordinator_lease
    ON public.execution_sessions (coordinator_scope_key, coordinator_owner_id, fencing_generation)
    WHERE coordinator_scope_key IS NOT NULL;

COMMENT ON COLUMN public.execution_sessions.coordinator_scope_key IS
    'Session-level ownership scope key; distinct from venue/instrument mutation scopes.';
COMMENT ON COLUMN public.execution_sessions.coordinator_owner_id IS
    'Coordinator process owner identity observed when this session fencing generation was committed.';
