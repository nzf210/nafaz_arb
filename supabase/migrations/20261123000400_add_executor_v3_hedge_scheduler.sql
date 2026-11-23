-- Executor V3 PR-06b: durable bounded hedge liquidity evaluation and wake-up state.

ALTER TABLE public.execution_hedge_obligations
    ADD COLUMN IF NOT EXISTS deferred_reason TEXT,
    ADD COLUMN IF NOT EXISTS deferred_since TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_evaluate_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS max_wait_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_evaluated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_book_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_book_sequence TEXT,
    ADD COLUMN IF NOT EXISTS deferred_count INTEGER NOT NULL DEFAULT 0
        CHECK (deferred_count >= 0),
    ADD COLUMN IF NOT EXISTS de_risk_required BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE public.execution_hedge_obligations
    ALTER COLUMN next_evaluate_at SET DEFAULT clock_timestamp();

UPDATE public.execution_hedge_obligations
SET next_evaluate_at=created_at
WHERE next_evaluate_at IS NULL
  AND status IN ('OPEN','PARTIALLY_HEDGED')
  AND de_risk_required=FALSE;

CREATE INDEX IF NOT EXISTS idx_execution_hedge_obligations_due
    ON public.execution_hedge_obligations(status, next_evaluate_at, created_at)
    WHERE status IN ('OPEN','PARTIALLY_HEDGED')
      AND de_risk_required=FALSE;

CREATE TABLE IF NOT EXISTS public.execution_hedge_scheduler_events (
    scheduler_event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    obligation_id UUID NOT NULL
        REFERENCES public.execution_hedge_obligations(obligation_id) ON DELETE RESTRICT,
    action TEXT NOT NULL CHECK (action IN ('SUBMIT','PARTIAL_SUBMIT','DEFER','DE_RISK_REQUIRED')),
    reason TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    book_sequence TEXT,
    book_observed_at TIMESTAMPTZ,
    book_age_ms BIGINT,
    executable_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0 CHECK (executable_base_quantity >= 0),
    executable_vwap NUMERIC(38,18),
    executable_notional_quote NUMERIC(38,18),
    executable_notional_usd NUMERIC(38,18),
    slippage_bps NUMERIC(24,12),
    estimated_fee_bps NUMERIC(24,12),
    estimated_total_cost_bps NUMERIC(24,12),
    unhedged_notional_usd NUMERIC(38,18),
    wait_elapsed_ms BIGINT,
    wait_remaining_ms BIGINT,
    next_evaluate_at TIMESTAMPTZ,
    decision_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_hedge_scheduler_idempotency
        UNIQUE (obligation_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_execution_hedge_scheduler_events_session
    ON public.execution_hedge_scheduler_events(session_id, created_at);

CREATE OR REPLACE FUNCTION public.reject_execution_hedge_scheduler_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution_hedge_scheduler_events is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_execution_hedge_scheduler_events_append_only
    ON public.execution_hedge_scheduler_events;
CREATE TRIGGER trg_execution_hedge_scheduler_events_append_only
    BEFORE UPDATE OR DELETE ON public.execution_hedge_scheduler_events
    FOR EACH ROW EXECUTE FUNCTION public.reject_execution_hedge_scheduler_event_mutation();

ALTER TABLE public.execution_hedge_scheduler_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.execution_hedge_scheduler_events FROM anon, authenticated;
GRANT SELECT, INSERT ON TABLE public.execution_hedge_scheduler_events TO service_role;

COMMENT ON TABLE public.execution_hedge_scheduler_events IS
    'Append-only V3 hedge liquidity decisions. Book snapshots authorize evaluation only; order submission remains behind the mutation gateway.';
