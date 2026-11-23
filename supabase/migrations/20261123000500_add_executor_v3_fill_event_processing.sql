-- Executor V3 PR-08: durable event inbox processing lease and retry cadence.

ALTER TABLE public.execution_event_inbox
    ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_execution_event_inbox_due
    ON public.execution_event_inbox(processing_status, next_retry_at, received_at)
    WHERE processing_status IN ('RECEIVED','RETRY');

CREATE INDEX IF NOT EXISTS idx_execution_event_inbox_stale_processing
    ON public.execution_event_inbox(processing_started_at)
    WHERE processing_status='PROCESSING';

COMMENT ON COLUMN public.execution_event_inbox.processing_started_at IS
    'Lease timestamp for one worker claim; stale PROCESSING rows may be safely reclaimed because fill and hedge side effects are idempotent.';
COMMENT ON COLUMN public.execution_event_inbox.next_retry_at IS
    'Bounded retry schedule for transient failures; NULL means immediately eligible.';
