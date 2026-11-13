-- Executor V2 Core Schema Migration
-- Implements order slot reservations, per-CEX slot config, execution sessions & legs,
-- pair PnL snapshots, and durable command journal.

-- 1. Per-CEX Spot Order Slot Reservations
CREATE TABLE IF NOT EXISTS public.order_slot_reservations (
    reservation_id TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    account TEXT NOT NULL DEFAULT 'default',
    market_type TEXT NOT NULL DEFAULT 'spot',
    slot_index INT NOT NULL,
    session_id TEXT,
    client_order_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('RESERVED', 'ACTIVE', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_slot_reservation
ON public.order_slot_reservations (venue, account, market_type, slot_index)
WHERE status IN ('RESERVED', 'ACTIVE');

CREATE INDEX IF NOT EXISTS idx_order_slot_reservations_client_order
ON public.order_slot_reservations (client_order_id)
WHERE client_order_id IS NOT NULL;

-- 2. Spot Order Slot Limits per CEX / Account
CREATE TABLE IF NOT EXISTS public.spot_order_slot_config (
    venue TEXT NOT NULL,
    account TEXT NOT NULL DEFAULT 'default',
    max_open_slots INT NOT NULL DEFAULT 2,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (venue, account)
);

-- Seed default 2 slots for active CEXes
INSERT INTO public.spot_order_slot_config (venue, account, max_open_slots)
VALUES
    ('tokocrypto', 'default', 2),
    ('indodax', 'default', 2),
    ('okx', 'default', 2),
    ('mexc', 'default', 2)
ON CONFLICT (venue, account) DO NOTHING;

-- 3. Execution Sessions
CREATE TABLE IF NOT EXISTS public.execution_sessions (
    session_id TEXT PRIMARY KEY,
    opportunity_id TEXT,
    pair_key TEXT NOT NULL,
    strategy_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'CREATED',
    owner_fencing TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_execution_sessions_active
ON public.execution_sessions (status, updated_at DESC)
WHERE status NOT IN ('COMPLETED', 'FAILED', 'REJECTED');

-- 4. Execution Legs (Independent Spot & Futures Leg Tracking)
CREATE TABLE IF NOT EXISTS public.execution_legs (
    leg_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES public.execution_sessions(session_id) ON DELETE CASCADE,
    leg_type TEXT NOT NULL CHECK (leg_type IN ('SPOT', 'FUTURES')),
    venue TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    target_qty NUMERIC NOT NULL DEFAULT 0,
    filled_qty NUMERIC NOT NULL DEFAULT 0,
    exposure_qty NUMERIC NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'CREATED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_session_leg UNIQUE (session_id, leg_type)
);

-- 5. Pair PnL Snapshots (Combined Two-Leg Economics)
CREATE TABLE IF NOT EXISTS public.pair_pnl_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    session_id TEXT,
    pair_key TEXT NOT NULL,
    spot_net_liquidation_pnl NUMERIC NOT NULL DEFAULT 0,
    futures_net_liquidation_pnl NUMERIC NOT NULL DEFAULT 0,
    pair_net_liquidation_pnl NUMERIC NOT NULL DEFAULT 0,
    pair_net_roi_pct NUMERIC NOT NULL DEFAULT 0,
    total_entry_fee NUMERIC NOT NULL DEFAULT 0,
    expected_close_fee NUMERIC NOT NULL DEFAULT 0,
    funding_accrued NUMERIC NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USDT',
    action_recommendation TEXT NOT NULL DEFAULT 'HOLD',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pair_pnl_snapshots_session
ON public.pair_pnl_snapshots (session_id, computed_at DESC);

-- 6. Durable Command Journal (Telegram / Operator / Automation commands)
CREATE TABLE IF NOT EXISTS public.command_journal (
    command_id TEXT PRIMARY KEY,
    command_type TEXT NOT NULL,
    scope TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL DEFAULT 'PENDING',
    attempts INT NOT NULL DEFAULT 0,
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_command_journal_pending
ON public.command_journal (state, created_at ASC)
WHERE state IN ('PENDING', 'PROCESSING');
