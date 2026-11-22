"""PostgreSQL integration tests for atomic V3 session/leg transitions."""
from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from executor.execution_domain import (
    ExecutionDomainRepository,
    ExecutionIdempotencyConflict,
    StaleExecutionFence,
    StaleExecutionVersion,
)


@pytest.mark.integration
def test_session_admission_and_transitions_commit_with_event_and_outbox_atomically():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    asyncpg = pytest.importorskip("asyncpg")

    async def run():
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
        session_id = "v3-test-" + uuid.uuid4().hex
        spot_leg_id = "v3-spot-" + uuid.uuid4().hex
        futures_leg_id = "v3-futures-" + uuid.uuid4().hex
        admission_key = "v3-admission-" + uuid.uuid4().hex
        repository = ExecutionDomainRepository(pool)
        legs = [
            {
                "leg_id": spot_leg_id, "leg_type": "SPOT",
                "venue": "tokocrypto", "market_type": "spot",
                "symbol": "BTC/IDR", "native_symbol": "BTCIDR",
                "normalized_symbol": "BTC/IDR", "base_asset": "BTC",
                "quote_asset": "IDR", "quantity_unit": "BASE",
                "target_order_qty": "0.001", "target_base_qty": "0.001",
            },
            {
                "leg_id": futures_leg_id, "leg_type": "FUTURES",
                "venue": "okx", "market_type": "swap",
                "symbol": "BTC/USDT:USDT", "native_symbol": "BTC-USDT-SWAP",
                "normalized_symbol": "BTC/USDT:USDT", "base_asset": "BTC",
                "quote_asset": "USDT", "settle_asset": "USDT",
                "contract_type": "linear", "contract_size": "0.001",
                "position_side": "SHORT", "quantity_unit": "CONTRACT",
                "target_order_qty": "1", "target_base_qty": "0.001",
            },
        ]
        admission = dict(
            session_id=session_id, admission_key=admission_key,
            account_scope_id="tokocrypto:default|okx:default",
            source_dispatch_id="dispatch-" + uuid.uuid4().hex,
            pair_key="tokocrypto:BTCIDR|okx:BTC-USDT-SWAP",
            opportunity_id="opportunity-test",
            policy_snapshot={"schema_version": 1, "max_slippage_pct": "0.25"},
            fencing_generation=7, legs=legs, actor_id="test", worker_id="pytest",
        )
        try:
            created = await repository.create_session(**admission)
            assert created.aggregate_version == 3
            assert set(created.leg_ids) == {spot_leg_id, futures_leg_id}
            assert not created.duplicate

            replay = await repository.create_session(**{**admission, "session_id": "ignored-retry-id"})
            assert replay.duplicate
            assert replay.session_id == session_id
            assert replay.policy_snapshot_hash == created.policy_snapshot_hash

            # Force JSON serialization to fail after the projection UPDATE.
            # The transaction must roll that UPDATE back and append no event/outbox.
            with pytest.raises(ValueError):
                await repository.transition(
                    session_id=session_id, entity_type="SESSION", entity_id=session_id,
                    expected_state="CREATED", next_state="VALIDATING",
                    expected_entity_version=1, expected_aggregate_version=3,
                    fencing_generation=7, event_type="SESSION_VALIDATING",
                    idempotency_key="serialization-failure",
                    evidence={"non_finite": float("nan")},
                )
            async with pool.acquire() as conn:
                rolled_back = await conn.fetchrow(
                    """
                    SELECT status, state_version, aggregate_version,
                      (SELECT count(*) FROM public.execution_session_events
                       WHERE session_id = $1) AS events
                    FROM public.execution_sessions WHERE session_id = $1
                    """,
                    session_id,
                )
            assert tuple(rolled_back) == ("CREATED", 1, 3, 3)

            first = await repository.transition(
                session_id=session_id, entity_type="SESSION", entity_id=session_id,
                expected_state="CREATED", next_state="VALIDATING",
                expected_entity_version=1, expected_aggregate_version=3,
                fencing_generation=7, event_type="SESSION_VALIDATING",
                idempotency_key="session-start",
            )
            assert first.entity_version == 2
            assert first.aggregate_version == 4
            assert not first.duplicate

            duplicate = await repository.transition(
                session_id=session_id, entity_type="SESSION", entity_id=session_id,
                expected_state="CREATED", next_state="VALIDATING",
                expected_entity_version=1, expected_aggregate_version=3,
                fencing_generation=7, event_type="SESSION_VALIDATING",
                idempotency_key="session-start",
            )
            assert duplicate.duplicate
            assert duplicate.event_id == first.event_id

            leg = await repository.transition(
                session_id=session_id, entity_type="LEG", entity_id=spot_leg_id,
                expected_state="NOT_STARTED", next_state="PREPARED",
                expected_entity_version=1, expected_aggregate_version=4,
                fencing_generation=7, event_type="SPOT_LEG_PREPARED",
                idempotency_key="spot-prepared",
            )
            assert leg.entity_version == 2
            assert leg.aggregate_version == 5

            with pytest.raises(StaleExecutionVersion):
                await repository.transition(
                    session_id=session_id, entity_type="LEG", entity_id=spot_leg_id,
                    expected_state="NOT_STARTED", next_state="PREPARED",
                    expected_entity_version=1, expected_aggregate_version=5,
                    fencing_generation=7, event_type="SPOT_LEG_PREPARED",
                    idempotency_key="stale-leg-write",
                )

            with pytest.raises(StaleExecutionFence):
                await repository.transition(
                    session_id=session_id, entity_type="SESSION", entity_id=session_id,
                    expected_state="VALIDATING", next_state="ADMITTED",
                    expected_entity_version=2, expected_aggregate_version=5,
                    fencing_generation=6, event_type="SESSION_ADMITTED",
                    idempotency_key="stale-fence",
                )

            with pytest.raises(ExecutionIdempotencyConflict):
                await repository.transition(
                    session_id=session_id, entity_type="SESSION", entity_id=session_id,
                    expected_state="CREATED", next_state="ADMITTED",
                    expected_entity_version=1, expected_aggregate_version=3,
                    fencing_generation=7, event_type="SESSION_ADMITTED",
                    idempotency_key="session-start",
                )

            async with pool.acquire() as conn:
                state = await conn.fetchrow(
                    """
                    SELECT status, state_version, aggregate_version
                    FROM public.execution_sessions WHERE session_id = $1
                    """,
                    session_id,
                )
                leg_state = await conn.fetchrow(
                    """
                    SELECT status, state_version FROM public.execution_legs
                    WHERE leg_id = $1
                    """,
                    spot_leg_id,
                )
                counts = await conn.fetchrow(
                    """
                    SELECT
                      (SELECT count(*) FROM public.execution_session_events
                       WHERE session_id = $1) AS events,
                      (SELECT count(*) FROM public.execution_outbox
                       WHERE session_id = $1) AS outbox
                    """,
                    session_id,
                )
            assert tuple(state) == ("VALIDATING", 2, 5)
            assert tuple(leg_state) == ("PREPARED", 2)
            assert counts["events"] == 5
            assert counts["outbox"] == 5
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM public.execution_outbox WHERE session_id = $1", session_id)
                await conn.execute("DELETE FROM public.execution_session_events WHERE session_id = $1", session_id)
                await conn.execute("DELETE FROM public.execution_legs WHERE session_id = $1", session_id)
                await conn.execute("DELETE FROM public.execution_sessions WHERE session_id = $1", session_id)
            await pool.close()

    asyncio.run(run())



def test_completion_and_flatness_transitions_require_explicit_evidence():
    from executor.execution_domain import (
        InvalidExecutionTransition,
        _validate_transition_evidence,
    )

    with pytest.raises(InvalidExecutionTransition):
        _validate_transition_evidence("SESSION", "COMPLETED", {})
    with pytest.raises(InvalidExecutionTransition):
        _validate_transition_evidence("LEG", "CLOSED", {"verified_flat": True})
    with pytest.raises(InvalidExecutionTransition):
        _validate_transition_evidence("SESSION", "HEDGE_PENDING", {})

    _validate_transition_evidence(
        "SESSION",
        "COMPLETED",
        {
            "spot_flat": True, "futures_flat": True,
            "all_orders_terminal": True, "no_unknown_submits": True,
            "fills_accounted": True, "hedge_obligations_zero": True,
            "allocations_zero": True, "pnl_reconciled": True,
            "risk_clear": True,
        },
    )
