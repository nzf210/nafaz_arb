-- Executor V3 PR-03E: connect logical intents to external submit attempts.
-- Existing V2 attempts remain valid with a NULL mutation_intent_id.

ALTER TABLE public.executor_mutation_intents
    ADD COLUMN IF NOT EXISTS pair_key TEXT;

ALTER TABLE public.executor_submit_attempts
    ADD COLUMN IF NOT EXISTS mutation_intent_id TEXT;

DO $body$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.executor_submit_attempts'::regclass
          AND conname='executor_submit_attempt_mutation_intent_fkey'
    ) THEN
        ALTER TABLE public.executor_submit_attempts
            ADD CONSTRAINT executor_submit_attempt_mutation_intent_fkey
            FOREIGN KEY (mutation_intent_id)
            REFERENCES public.executor_mutation_intents(intent_id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END $body$;

ALTER TABLE public.executor_submit_attempts
    VALIDATE CONSTRAINT executor_submit_attempt_mutation_intent_fkey;

CREATE UNIQUE INDEX IF NOT EXISTS uq_executor_submit_attempt_mutation_intent
    ON public.executor_submit_attempts (mutation_intent_id)
    WHERE mutation_intent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_executor_mutation_intents_pair
    ON public.executor_mutation_intents (pair_key, created_at DESC)
    WHERE pair_key IS NOT NULL;

COMMENT ON COLUMN public.executor_submit_attempts.mutation_intent_id IS
    'Logical V3 mutation intent that authorizes this single external create attempt; NULL for legacy V2 attempts.';
COMMENT ON COLUMN public.executor_mutation_intents.pair_key IS
    'Stable route identity used for risk scope, order attempt and session-level reconciliation.';
