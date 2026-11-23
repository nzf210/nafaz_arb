-- Executor V3 PR-07: entry/exit fill projections, fee/FX valuation evidence,
-- funding settlements and append-only realized PnL/valuation journals.

ALTER TABLE public.execution_fills
    ADD COLUMN IF NOT EXISTS fill_role TEXT NOT NULL DEFAULT 'ENTRY',
    ADD COLUMN IF NOT EXISTS fee_reporting_amount NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS fee_reporting_currency TEXT,
    ADD COLUMN IF NOT EXISTS fee_fx_rate NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS fee_fx_source TEXT,
    ADD COLUMN IF NOT EXISTS fee_fx_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fee_valuation_verified BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS quote_to_reporting_rate NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS quote_fx_source TEXT,
    ADD COLUMN IF NOT EXISTS quote_fx_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS quote_fx_verified BOOLEAN NOT NULL DEFAULT FALSE;

DO $fill_role$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_fills'::regclass
          AND conname='execution_fills_role_valid'
    ) THEN
        ALTER TABLE public.execution_fills
            ADD CONSTRAINT execution_fills_role_valid
            CHECK (fill_role IN ('ENTRY','EXIT')) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_fills'::regclass
          AND conname='execution_fills_fee_valuation_valid'
    ) THEN
        ALTER TABLE public.execution_fills
            ADD CONSTRAINT execution_fills_fee_valuation_valid
            CHECK (
                fee_valuation_verified=FALSE
                OR (
                    fee_reporting_amount IS NOT NULL AND fee_reporting_amount >= 0
                    AND fee_reporting_currency IS NOT NULL
                    AND fee_fx_rate > 0 AND fee_fx_source IS NOT NULL
                    AND fee_fx_observed_at IS NOT NULL
                )
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_fills'::regclass
          AND conname='execution_fills_quote_fx_valid'
    ) THEN
        ALTER TABLE public.execution_fills
            ADD CONSTRAINT execution_fills_quote_fx_valid
            CHECK (
                quote_fx_verified=FALSE
                OR (
                    quote_to_reporting_rate > 0
                    AND quote_fx_source IS NOT NULL
                    AND quote_fx_observed_at IS NOT NULL
                )
            ) NOT VALID;
    END IF;
END $fill_role$;

ALTER TABLE public.execution_fills
    VALIDATE CONSTRAINT execution_fills_role_valid;
ALTER TABLE public.execution_fills
    VALIDATE CONSTRAINT execution_fills_fee_valuation_valid;
ALTER TABLE public.execution_fills
    VALIDATE CONSTRAINT execution_fills_quote_fx_valid;

ALTER TABLE public.execution_legs
    ADD COLUMN IF NOT EXISTS confirmed_exit_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS confirmed_exit_quote_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS confirmed_exit_price_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS confirmed_exit_exposure_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS average_exit_price NUMERIC(38,18);

DO $exit_projection$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_legs'::regclass
          AND conname='execution_legs_exit_projection_nonnegative'
    ) THEN
        ALTER TABLE public.execution_legs
            ADD CONSTRAINT execution_legs_exit_projection_nonnegative
            CHECK (
                confirmed_exit_quantity >= 0
                AND confirmed_exit_quote_quantity >= 0
                AND confirmed_exit_price_quantity >= 0
                AND confirmed_exit_exposure_quantity >= 0
                AND (average_exit_price IS NULL OR average_exit_price >= 0)
            ) NOT VALID;
    END IF;
END $exit_projection$;

ALTER TABLE public.execution_legs
    VALIDATE CONSTRAINT execution_legs_exit_projection_nonnegative;

CREATE TABLE IF NOT EXISTS public.execution_positions (
    position_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL UNIQUE
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    account_scope_id TEXT NOT NULL,
    pair_key TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    reporting_currency TEXT NOT NULL,
    spot_entry_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    spot_exit_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    spot_remaining_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    futures_entry_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    futures_exit_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    futures_remaining_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    hedge_drift_base_quantity NUMERIC(38,18) NOT NULL DEFAULT 0,
    actual_futures_margin_reporting NUMERIC(38,18),
    fee_reserve_reporting NUMERIC(38,18) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN','PARTIALLY_CLOSED','CLOSED','RECONCILIATION_REQUIRED')),
    state_version BIGINT NOT NULL DEFAULT 1 CHECK (state_version > 0),
    last_verified_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT execution_positions_quantities_nonnegative CHECK (
        spot_entry_base_quantity >= 0 AND spot_exit_base_quantity >= 0
        AND spot_remaining_base_quantity >= 0
        AND futures_entry_base_quantity >= 0 AND futures_exit_base_quantity >= 0
        AND futures_remaining_base_quantity >= 0
        AND hedge_drift_base_quantity >= 0 AND fee_reserve_reporting >= 0
        AND (actual_futures_margin_reporting IS NULL OR actual_futures_margin_reporting >= 0)
    ),
    CONSTRAINT execution_positions_spot_conservation CHECK (
        spot_entry_base_quantity >= spot_exit_base_quantity
        AND spot_entry_base_quantity - spot_exit_base_quantity = spot_remaining_base_quantity
    ),
    CONSTRAINT execution_positions_futures_conservation CHECK (
        futures_entry_base_quantity >= futures_exit_base_quantity
        AND futures_entry_base_quantity - futures_exit_base_quantity = futures_remaining_base_quantity
    )
);

CREATE INDEX IF NOT EXISTS idx_execution_positions_account_status
    ON public.execution_positions(account_scope_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS public.execution_funding_events (
    funding_event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    futures_leg_id TEXT NOT NULL,
    account_scope_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_payment_id TEXT NOT NULL,
    amount NUMERIC(38,18) NOT NULL,
    asset TEXT NOT NULL,
    reporting_amount NUMERIC(38,18),
    reporting_currency TEXT,
    fx_rate NUMERIC(38,18),
    fx_source TEXT,
    fx_observed_at TIMESTAMPTZ,
    valuation_verified BOOLEAN NOT NULL DEFAULT FALSE,
    settled_at TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_funding_event
        UNIQUE (account_scope_id, venue, market_type, symbol, exchange_payment_id),
    CONSTRAINT execution_funding_event_valuation_valid
        CHECK (
            valuation_verified=FALSE
            OR (
                reporting_amount IS NOT NULL
                AND reporting_currency IS NOT NULL
                AND fx_rate > 0 AND fx_source IS NOT NULL
                AND fx_observed_at IS NOT NULL
            )
        )
);

CREATE TABLE IF NOT EXISTS public.execution_pnl_ledger (
    pnl_entry_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    source_fill_id UUID REFERENCES public.execution_fills(fill_id) ON DELETE RESTRICT,
    source_funding_event_id UUID REFERENCES public.execution_funding_events(funding_event_id) ON DELETE RESTRICT,
    matched_entry_fill_id UUID REFERENCES public.execution_fills(fill_id) ON DELETE RESTRICT,
    matched_quantity NUMERIC(38,18),
    entry_type TEXT NOT NULL CHECK (entry_type IN (
        'REALIZED_SPOT_PNL','REALIZED_FUTURES_PNL','ENTRY_FEE',
        'EXIT_FEE','FUNDING','TAX','CONVERSION_COST','ADJUSTMENT'
    )),
    amount_reporting NUMERIC(38,18) NOT NULL,
    reporting_currency TEXT NOT NULL,
    evidence_source TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_pnl_ledger_idempotency
        UNIQUE (session_id, idempotency_key),
    CONSTRAINT execution_pnl_ledger_source_valid
        CHECK (num_nonnulls(source_fill_id, source_funding_event_id) = 1),
    CONSTRAINT execution_pnl_ledger_match_valid
        CHECK (
            (
                entry_type IN ('REALIZED_SPOT_PNL','REALIZED_FUTURES_PNL')
                AND source_fill_id IS NOT NULL
                AND matched_entry_fill_id IS NOT NULL
                AND matched_quantity > 0
            )
            OR (
                entry_type NOT IN ('REALIZED_SPOT_PNL','REALIZED_FUTURES_PNL')
                AND matched_entry_fill_id IS NULL
                AND matched_quantity IS NULL
            )
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_funding_events_session_id
    ON public.execution_funding_events(session_id, funding_event_id);

DO $pnl_source_fk$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_funding_events'::regclass
          AND conname='execution_funding_events_session_leg_fkey'
    ) THEN
        ALTER TABLE public.execution_funding_events
            ADD CONSTRAINT execution_funding_events_session_leg_fkey
            FOREIGN KEY (session_id, futures_leg_id)
            REFERENCES public.execution_legs(session_id, leg_id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_pnl_ledger'::regclass
          AND conname='execution_pnl_ledger_session_fill_fkey'
    ) THEN
        ALTER TABLE public.execution_pnl_ledger
            ADD CONSTRAINT execution_pnl_ledger_session_fill_fkey
            FOREIGN KEY (session_id, source_fill_id)
            REFERENCES public.execution_fills(session_id, fill_id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_pnl_ledger'::regclass
          AND conname='execution_pnl_ledger_session_funding_fkey'
    ) THEN
        ALTER TABLE public.execution_pnl_ledger
            ADD CONSTRAINT execution_pnl_ledger_session_funding_fkey
            FOREIGN KEY (session_id, source_funding_event_id)
            REFERENCES public.execution_funding_events(session_id, funding_event_id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END $pnl_source_fk$;

ALTER TABLE public.execution_funding_events
    VALIDATE CONSTRAINT execution_funding_events_session_leg_fkey;
ALTER TABLE public.execution_pnl_ledger
    VALIDATE CONSTRAINT execution_pnl_ledger_session_fill_fkey;
ALTER TABLE public.execution_pnl_ledger
    VALIDATE CONSTRAINT execution_pnl_ledger_session_funding_fkey;

DO $pnl_match_fk$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.execution_pnl_ledger'::regclass
          AND conname='execution_pnl_ledger_session_entry_fill_fkey'
    ) THEN
        ALTER TABLE public.execution_pnl_ledger
            ADD CONSTRAINT execution_pnl_ledger_session_entry_fill_fkey
            FOREIGN KEY (session_id, matched_entry_fill_id)
            REFERENCES public.execution_fills(session_id, fill_id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END $pnl_match_fk$;

ALTER TABLE public.execution_pnl_ledger
    VALIDATE CONSTRAINT execution_pnl_ledger_session_entry_fill_fkey;

CREATE INDEX IF NOT EXISTS idx_execution_pnl_ledger_session_time
    ON public.execution_pnl_ledger(session_id, occurred_at, pnl_entry_id);

CREATE TABLE IF NOT EXISTS public.execution_pnl_snapshots (
    pnl_snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT NOT NULL
        REFERENCES public.execution_sessions(session_id) ON DELETE RESTRICT,
    pair_key TEXT NOT NULL,
    reporting_currency TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK (confidence IN ('VERIFIED','PARTIAL','UNKNOWN')),
    evaluated_at TIMESTAMPTZ NOT NULL,
    spot_estimated_liquidation_pnl NUMERIC(38,18) NOT NULL,
    futures_estimated_liquidation_pnl NUMERIC(38,18) NOT NULL,
    funding_reporting_amount NUMERIC(38,18) NOT NULL,
    entry_fees_reporting NUMERIC(38,18) NOT NULL,
    estimated_exit_fees_reporting NUMERIC(38,18) NOT NULL,
    estimated_tax_reporting NUMERIC(38,18) NOT NULL,
    conversion_cost_reporting NUMERIC(38,18) NOT NULL,
    estimated_net_liquidation_pnl NUMERIC(38,18) NOT NULL,
    strategy_roi_pct NUMERIC(24,12) NOT NULL,
    return_on_deployed_capital_pct NUMERIC(24,12),
    total_deployed_capital_reporting NUMERIC(38,18),
    spot_exit_vwap NUMERIC(38,18) NOT NULL,
    futures_exit_vwap NUMERIC(38,18) NOT NULL,
    spot_slippage_bps NUMERIC(24,12) NOT NULL,
    futures_slippage_bps NUMERIC(24,12) NOT NULL,
    spot_executable_base_quantity NUMERIC(38,18) NOT NULL,
    futures_executable_base_quantity NUMERIC(38,18) NOT NULL,
    hedge_drift_base_quantity NUMERIC(38,18) NOT NULL,
    provenance JSONB NOT NULL CHECK (jsonb_typeof(provenance)='object'),
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_execution_pnl_snapshot_idempotency
        UNIQUE (session_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_execution_pnl_snapshots_session_time
    ON public.execution_pnl_snapshots(session_id, evaluated_at DESC);

CREATE OR REPLACE FUNCTION public.reject_execution_pnl_ledger_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution_pnl_ledger is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_execution_pnl_ledger_append_only
    ON public.execution_pnl_ledger;
CREATE TRIGGER trg_execution_pnl_ledger_append_only
    BEFORE UPDATE OR DELETE ON public.execution_pnl_ledger
    FOR EACH ROW EXECUTE FUNCTION public.reject_execution_pnl_ledger_mutation();

CREATE OR REPLACE FUNCTION public.reject_execution_funding_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'execution_funding_events is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_execution_funding_events_append_only
    ON public.execution_funding_events;
CREATE TRIGGER trg_execution_funding_events_append_only
    BEFORE UPDATE OR DELETE ON public.execution_funding_events
    FOR EACH ROW EXECUTE FUNCTION public.reject_execution_funding_event_mutation();

ALTER TABLE public.execution_positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_funding_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_pnl_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.execution_pnl_snapshots ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.execution_positions FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_funding_events FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_pnl_ledger FROM anon, authenticated;
REVOKE ALL ON TABLE public.execution_pnl_snapshots FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON TABLE public.execution_positions TO service_role;
GRANT SELECT, INSERT ON TABLE public.execution_funding_events TO service_role;
GRANT SELECT, INSERT ON TABLE public.execution_pnl_ledger TO service_role;
GRANT SELECT, INSERT ON TABLE public.execution_pnl_snapshots TO service_role;

COMMENT ON TABLE public.execution_pnl_ledger IS
    'Append-only verified realized PnL, fee, funding, tax, conversion and adjustment facts for V3 sessions.';
COMMENT ON TABLE public.execution_pnl_snapshots IS
    'Append-only estimated pair liquidation PnL snapshots. Never substitute these estimates for realized PnL.';
