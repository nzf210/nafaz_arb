"""PostgreSQL integration tests for the V3 session aggregate repository.

The schema workflow runs these against the disposable CI PostgreSQL database.
They are skipped in developer environments without TEST_POSTGRES_DSN.
"""
from __future__ import annotations

import os
import uuid

import pytest

from executor.execution_repository import (
    AdmissionConflict,
    ExecutionSessionRepository,
    InvalidTransition,
    StaleAggregate,
)


pytestmark = pytest.mark.asyncio


def _identity():
    token = uuid.uuid4().hex
    return {
        "session_id": f"v3-session-{token}",
        "opportunity_id": f"v3-opportunity-{token}",
        "source_dispatch_id": f"v3-dispatch-{token}",
        "admission_key": f"v3-admission-{token}",
        "account_scope_id": f"v3-account-{token}",
        "pair_key": "indodax:btc_idr->okx:btc-usdt-swap",
        "actor_id": "pytest:executor-v3",
        "fencing_generation": 71,
        "policy_snapshot": {"strategy": "spot-perp-arbitrage", "version": 3},
        "execution_policy_snapshot": {"mode": "maker-taker"},
        "risk_policy_snapshot": {"max_unhedged_ms": 3000},
        "exit_policy_snapshot": {"policy_version": "existing-exit-policy-v1"},
    }


def _legs(token: str = "a"):
    return [
        {
            "leg_id": f"v3-spot-{token}",
            "leg_type": "SPOT",
            "venue": "indodax",
            "market_type": "spot",
            "symbol": "btc_idr",
            "normalized_symbol": "BTC/IDR",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "quantity_unit": "BASE",
            "base_per_unit": "1",
            "order_quantity": "0.001",
            "exposure_quantity": "0.001",
            "target_qty": "0.001",
        },
        {
            "leg_id": f"v3-futures-{token}",
            "leg_type": "FUTURES",
            "venue": "okx",
            "market_type": "swap",
            "symbol": "BTC-USDT-SWAP",
            "normalized_symbol": "BTC/USDT:USDT",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "settlement_asset": "USDT",
            "contract_type": "LINEAR_PERPETUAL",
            "contract_size": "0.001",
            "position_side": "SHORT",
            "quantity_unit": "CONTRACTS",
            "base_per_unit": "0.001",
            "order_quantity": "1",
            "exposure_quantity": "0.001",
            "target_qty": "1",
        },
    ]


@pytest.fixture
async def repository():
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    try:
        yield ExecutionSessionRepository(pool)
    finally:
        await pool.close()


async def _create(repository, identity=None, legs=None):
    identity = identity or _identity()
    result = await repository.create_session(
        **identity,
        legs=legs or _legs(identity["session_id"]),
    )
    return identity, result


async def test_admission_is_idempotent_and_preserves_native_venue_symbols(repository):
    identity, first = await _create(repository)
    replay = dict(identity)
    replay["session_id"] = f"{identity['session_id']}-retry"
    second = await repository.create_session(
        **replay, legs=_legs(identity["session_id"])
    )

    assert first == {
        "session_id": identity["session_id"],
        "created": True,
        "aggregate_version": 3,
    }
    assert second["session_id"] == identity["session_id"]
    assert second["created"] is False

    async with repository.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT leg_type, venue, symbol, quote_asset, settlement_asset,
                   quantity_unit, order_quantity, exposure_quantity
            FROM public.execution_legs
            WHERE session_id=$1 ORDER BY leg_type
            """,
            identity["session_id"],
        )
    by_type = {row["leg_type"]: dict(row) for row in rows}
    assert by_type["SPOT"]["symbol"] == "btc_idr"
    assert by_type["SPOT"]["quote_asset"] == "IDR"
    assert by_type["FUTURES"]["symbol"] == "BTC-USDT-SWAP"
    assert by_type["FUTURES"]["settlement_asset"] == "USDT"
    from decimal import Decimal
    assert by_type["FUTURES"]["exposure_quantity"] == Decimal("0.001")


async def test_admission_key_cannot_be_reused_for_a_different_route(repository):
    identity, _ = await _create(repository)
    changed_legs = _legs(identity["session_id"])
    changed_legs[0]["symbol"] = "eth_idr"
    with pytest.raises(AdmissionConflict, match="different venue/market legs"):
        await repository.create_session(**identity, legs=changed_legs)


async def test_session_and_leg_events_use_one_monotonic_aggregate_cursor(repository):
    identity, _ = await _create(repository)
    transition = dict(
        session_id=identity["session_id"],
        target_state="VALIDATING",
        expected_state_version=1,
        expected_aggregate_version=3,
        fencing_generation=identity["fencing_generation"],
        event_type="SESSION_VALIDATING",
        event_idempotency_key=f"{identity['admission_key']}:validating",
        actor_id=identity["actor_id"],
        payload={"validation": "started"},
    )
    first = await repository.transition_session(**transition)
    assert first["state_version"] == 2
    assert first["aggregate_version"] == 4

    replay = await repository.transition_session(**transition)
    assert replay["duplicate"] is True
    assert replay["aggregate_version"] == 4

    conflicting_replay = dict(transition)
    conflicting_replay["payload"] = {"validation": "different"}
    with pytest.raises(AdmissionConflict, match="different transition"):
        await repository.transition_session(**conflicting_replay)

    stale = dict(transition)
    stale["event_idempotency_key"] += ":stale"
    with pytest.raises(StaleAggregate):
        await repository.transition_session(**stale)

    leg_result = await repository.transition_leg(
        session_id=identity["session_id"],
        leg_id=f"v3-spot-{identity['session_id']}",
        target_state="PREPARED",
        expected_leg_state_version=1,
        expected_aggregate_version=4,
        fencing_generation=identity["fencing_generation"],
        event_type="SPOT_LEG_PREPARED",
        event_idempotency_key=f"{identity['admission_key']}:spot-prepared",
        actor_id=identity["actor_id"],
    )
    assert leg_result["state_version"] == 2
    assert leg_result["aggregate_version"] == 5

    async with repository.pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT state_version, aggregate_version FROM public.execution_sessions WHERE session_id=$1",
            identity["session_id"],
        )
        events = await conn.fetch(
            "SELECT aggregate_version FROM public.execution_session_events WHERE session_id=$1 ORDER BY aggregate_version",
            identity["session_id"],
        )
    assert (session["state_version"], session["aggregate_version"]) == (2, 5)
    assert [row["aggregate_version"] for row in events] == [1, 2, 3, 4, 5]


async def test_invalid_transition_and_stale_fence_write_nothing(repository):
    identity, _ = await _create(repository)
    with pytest.raises(InvalidTransition):
        await repository.transition_session(
            session_id=identity["session_id"],
            target_state="COMPLETED",
            expected_state_version=1,
            expected_aggregate_version=3,
            fencing_generation=identity["fencing_generation"],
            event_type="INVALID_COMPLETION",
            event_idempotency_key=f"{identity['admission_key']}:bad-completion",
            actor_id=identity["actor_id"],
            completion_evidence={
                "spot_flat": True, "futures_flat": True, "orders_terminal": True,
                "no_unknown_submits": True, "fills_accounted": True,
                "obligations_zero": True, "allocations_zero": True,
                "pnl_reconciled": True, "no_open_incident": True,
            },
        )
    with pytest.raises(StaleAggregate, match="fencing"):
        await repository.transition_session(
            session_id=identity["session_id"],
            target_state="VALIDATING",
            expected_state_version=1,
            expected_aggregate_version=1,
            fencing_generation=identity["fencing_generation"] + 1,
            event_type="SESSION_VALIDATING",
            event_idempotency_key=f"{identity['admission_key']}:stale-fence",
            actor_id=identity["actor_id"],
        )
    async with repository.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state_version, aggregate_version, status FROM public.execution_sessions WHERE session_id=$1",
            identity["session_id"],
        )
        event_count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_session_events WHERE session_id=$1",
            identity["session_id"],
        )
    assert (row["state_version"], row["aggregate_version"], row["status"]) == (1, 3, "CREATED")
    assert event_count == 3


async def test_completed_session_requires_every_flatness_gate(repository):
    identity, _ = await _create(repository)
    with pytest.raises(ValueError, match="flatness evidence"):
        await repository.transition_session(
            session_id=identity["session_id"],
            target_state="COMPLETED",
            expected_state_version=1,
            expected_aggregate_version=1,
            fencing_generation=identity["fencing_generation"],
            event_type="SESSION_COMPLETED",
            event_idempotency_key=f"{identity['admission_key']}:premature-completion",
            actor_id=identity["actor_id"],
            completion_evidence={"spot_flat": True},
        )


def test_admission_rejects_mismatched_spot_and_futures_base_assets():
    legs = _legs()
    legs[1]["base_asset"] = "ETH"
    with pytest.raises(ValueError, match="same base asset"):
        ExecutionSessionRepository._validate_legs(legs)


def test_admission_rejects_futures_misclassified_as_spot_market():
    legs = _legs()
    legs[1]["market_type"] = "spot"
    with pytest.raises(ValueError, match="FUTURES leg market_type"):
        ExecutionSessionRepository._validate_legs(legs)


def test_admission_rejects_missing_futures_position_side():
    legs = _legs()
    legs[1]["position_side"] = ""
    with pytest.raises(ValueError, match="position_side"):
        ExecutionSessionRepository._validate_legs(legs)
