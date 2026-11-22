-- Executor V3 PR-07: reproducible position accounting and verified funding facts.
-- Reuses pair_pnl_snapshots as the canonical snapshot table; adds completeness
-- metadata rather than creating a competing PnL snapshot store.

ALTER TABLE public.pair_pnl_snapshots
    ADD COLUMN IF NOT EXISTS snapshot_version BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS input_hash TEXT,
    ADD COLUMN IF NOT EXISTS policy_snapshot_hash TEXT,
    ADD COLUMN IF NOT EXISTS realized_pnl_gross NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS realized_pnl_net NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS unrealized_pnl_gross NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS estimated_liquidation_pnl NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS fees_paid NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS estimated_exit_fees NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS exit_slippage_reserve NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS tax_reserve NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS total_deployed_capital NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS strategy_roi_pct NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS return_on_deployed_capital_pct NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS confidence TEXT NOT NULL DEFAULT 'LEGACY_UNKNOWN'
        CHECK (confidence IN ('HIGH','LOW','LEGACY_UNKNOWN')),
    ADD COLUMN IF NOT EXISTS fees_complete BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS funding_complete BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS fx_complete BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS depth_complete BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS marks_fresh BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS missing_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS fill_cursor BIGINT,
    ADD COLUMN IF NOT EXISTS mark_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fx_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS input_payload JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pair_pnl_snapshots_session_input
    ON public.pair_pnl_snapshots(session_id, input_hash)
    WHERE session_id IS NOT NULL AND input_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pair_pnl_snapshots_session_version
    ON public.pair_pnl_snapshots(session_id, snapshot_version DESC, computed_at DESC);

CREATE TABLE IF NOT EXISTS public.execution_funding_payments (
    funding_payment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    leg_id TEXT NOT NULL
        REFERENCES public.execution_legs(leg_id) ON DELETE RESTRICT,
    account_scope_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_payment_id TEXT NOT NULL,
    amount NUMERIC(38,18) NOT NULL,
    asset TEXT NOT NULL,
    paid_at TIMESTAMPTZ NOT NULL,
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    source TEXT NOT NULL DEFAULT 'UNVERIFIED',
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_funding_payment_identity
        UNIQUE (account_scope_id, venue, market_type, symbol, exchange_payment_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_funding_session_leg_time
    ON public.execution_funding_payments(session_id, leg_id, paid_at);

CREATE OR REPLACE FUNCTION public.reject_execution_funding_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution_funding_payments is append-only; corrections require audited adjustment records';
END;
$$;

DROP TRIGGER IF EXISTS trg_execution_funding_payments_append_only
    ON public.execution_funding_payments;
CREATE TRIGGER trg_execution_funding_payments_append_only
    BEFORE UPDATE OR DELETE ON public.execution_funding_payments
    FOR EACH ROW EXECUTE FUNCTION public.reject_execution_funding_mutation();

ALTER TABLE public.execution_funding_payments ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.execution_funding_payments FROM anon, authenticated;
GRANT SELECT, INSERT ON TABLE public.execution_funding_payments TO service_role;

COMMENT ON TABLE public.execution_funding_payments IS
    'Immutable verified/unverified venue funding cash flows used by V3 position PnL. Missing history remains explicitly incomplete.';
