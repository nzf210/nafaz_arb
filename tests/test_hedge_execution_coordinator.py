"""Unit tests for obligation-backed V3 hedge execution orchestration."""
from __future__ import annotations

from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace

import pytest

from executor.hedge_execution_coordinator import HedgeExecutionCoordinator
from executor.hedge_liquidity_evaluator import (
    BookLevel, FXSnapshot, HedgeLiquidityPolicy, OrderBookSnapshot,
    evaluate_hedge_liquidity,
)
from executor.mutation_gateway_types import MutationResult, MutationResultType


class _Connection:
    async def fetchrow(self, query, *args):
        assert "execution_hedge_obligations" in query
        return {
            "account_scope_id": "acct-1",
            "pair_key": "indodax:btc_idr->okx:btc-usdt-swap",
            "futures_leg_id": "future-1",
            "futures_venue": "okx",
            "futures_market_type": "swap",
            "futures_symbol": "BTC-USDT-SWAP",
            "base_asset": "BTC",
            "position_side": "SHORT",
            "contract_type": "LINEAR_PERPETUAL",
            "contract_size": Decimal("0.001"),
            "quantity_unit": "CONTRACTS",
            "base_per_unit": Decimal("0.001"),
            "quote_asset": "USDT",
            "settlement_asset": "USDT",
        }


class _Pool:
    @asynccontextmanager
    async def acquire(self):
        yield _Connection()


class _Scheduler:
    def __init__(self):
        self.calls = []

    async def record_decision(self, **kwargs):
        self.calls.append(kwargs)
        return {"action": kwargs["decision"].action, "duplicate": False}


class _Obligations:
    def __init__(self):
        self.calls = []

    async def reserve(self, **kwargs):
        self.calls.append(("reserve", kwargs))
        return {
            "reserved": True, "duplicate": False,
            "reservation_id": "reservation-1",
            "reserved_base_quantity": "0.001",
        }

    async def mark_submitted(self, **kwargs):
        self.calls.append(("mark_submitted", kwargs))
        return {"status": "SUBMITTED", "duplicate": False}

    async def mark_unknown(self, **kwargs):
        self.calls.append(("mark_unknown", kwargs))
        return {"status": "UNKNOWN"}

    async def release(self, **kwargs):
        self.calls.append(("release", kwargs))
        return {"status": "RELEASED"}


class _Intents:
    def __init__(self):
        self.calls = []

    async def prepare_create(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "intent_id": "intent-1",
            "client_order_id": "hedge-client-1",
            "status": "PREPARED",
        }


class _Gateway:
    def __init__(self, status=MutationResultType.CONFIRMED):
        self.calls = []
        self.status = status

    async def execute_create_intent(self, client, *, intent_id, actor_id):
        self.calls.append((client, intent_id, actor_id))
        return MutationResult(
            status=self.status, client_order_id="hedge-client-1",
            exchange_order_id="venue-order-1",
        )


def _decision():
    now = 10_000
    return evaluate_hedge_liquidity(
        required_base_quantity="0.001",
        already_reserved_base_quantity="0",
        base_per_unit="0.001",
        quantity_step="1",
        venue_min_order_quantity="1",
        venue_min_notional_quote="5",
        position_side="SHORT",
        book=OrderBookSnapshot(
            bids=(BookLevel.make("100000", "10"),),
            asks=(BookLevel.make("100001", "10"),),
            observed_at_ms=9_900, sequence="seq-1",
        ),
        fx=FXSnapshot(Decimal("1"), 9_950, "USDT/USD"),
        policy=HedgeLiquidityPolicy(
            internal_min_notional_usd=Decimal("5"),
            max_book_age_ms=500, max_fx_age_ms=1000,
            max_slippage_bps=Decimal("25"),
            max_total_cost_bps=Decimal("50"),
            taker_fee_bps=Decimal("5"),
            safety_buffer_bps=Decimal("5"),
            max_unhedged_notional_usd=Decimal("250"),
            max_unhedged_wait_ms=5000,
            max_adverse_move_bps=Decimal("100"),
        ),
        now_ms=now, unhedged_since_ms=9_000,
        unhedged_notional_usd="100", reference_price="100000",
    )


@pytest.mark.asyncio
async def test_eligible_hedge_is_reserved_and_submitted_only_through_gateway():
    scheduler = _Scheduler()
    obligations = _Obligations()
    intents = _Intents()
    gateway = _Gateway()
    coordinator = HedgeExecutionCoordinator(
        _Pool(), gateway=gateway, order_intents=intents,
        obligations=obligations, scheduler=scheduler,
    )
    client = SimpleNamespace(id="okx")
    result = await coordinator.execute_decision(
        client=client, session_id="session-1", obligation_id="obligation-1",
        decision=_decision(), actor_id="test", idempotency_key="hedge-event-1",
        coordinator_lease=SimpleNamespace(generation=7), fencing_generation=7,
        risk_metrics={"gross_exposure_usdt": 100, "net_exposure_usdt": 0},
        risk_limits={"max_unhedged_notional_usdt": 250},
    )

    assert result["submitted"] is True
    assert [call[0] for call in obligations.calls] == ["reserve", "mark_submitted"]
    assert obligations.calls[0][1]["requested_base_quantity"] == Decimal("0.001")
    assert obligations.calls[1][1]["client_order_id"] == "hedge-client-1"
    intent = intents.calls[0]
    assert intent["side"] == "sell"
    assert intent["order_type"] == "limit"
    assert intent["quantity"] == Decimal("1")
    assert intent["price"] == Decimal("100000")
    assert intent["request_metadata"]["action_class"] == "HEDGE"
    assert intent["request_metadata"]["hedge_reservation_id"] == "reservation-1"
    assert gateway.calls == [(client, "intent-1", "test")]


@pytest.mark.asyncio
async def test_definitive_gateway_rejection_releases_only_the_reserved_remainder():
    obligations = _Obligations()
    coordinator = HedgeExecutionCoordinator(
        _Pool(), gateway=_Gateway(MutationResultType.REJECTED),
        order_intents=_Intents(), obligations=obligations, scheduler=_Scheduler(),
    )
    await coordinator.execute_decision(
        client=SimpleNamespace(id="okx"), session_id="session-1",
        obligation_id="obligation-1", decision=_decision(),
        actor_id="test", idempotency_key="hedge-event-2",
        coordinator_lease=SimpleNamespace(generation=7), fencing_generation=7,
        risk_metrics={"gross_exposure_usdt": 100},
    )
    assert [call[0] for call in obligations.calls] == [
        "reserve", "mark_submitted", "release",
    ]
    assert obligations.calls[-1][1]["terminal_order_verified"] is True
    assert obligations.calls[-1][1]["fills_reconciled"] is True


@pytest.mark.asyncio
async def test_ambiguous_gateway_outcome_keeps_reservation_unknown():
    obligations = _Obligations()
    coordinator = HedgeExecutionCoordinator(
        _Pool(), gateway=_Gateway(MutationResultType.SUBMISSION_UNKNOWN),
        order_intents=_Intents(), obligations=obligations, scheduler=_Scheduler(),
    )
    await coordinator.execute_decision(
        client=SimpleNamespace(id="okx"), session_id="session-1",
        obligation_id="obligation-1", decision=_decision(),
        actor_id="test", idempotency_key="hedge-event-3",
        coordinator_lease=SimpleNamespace(generation=7), fencing_generation=7,
        risk_metrics={"gross_exposure_usdt": 100},
    )
    assert [call[0] for call in obligations.calls] == [
        "reserve", "mark_submitted", "mark_unknown",
    ]
