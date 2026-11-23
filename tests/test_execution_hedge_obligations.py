"""PostgreSQL integration tests for durable V3 hedge obligations."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from executor.execution_fill_service import ExecutionFillService, FillConflict
from executor.execution_hedge_obligations import (
    ExecutionHedgeObligationService, HedgeObligationError,
)
from executor.ownership import OwnershipLeaseService
from executor.session_coordinator import SessionCoordinator


def _snapshots():
    now = datetime.now(timezone.utc)
    evidence = {
        name: {
            "status": "VERIFIED" if name != "fx_conversion" else "NOT_REQUIRED",
            "observed_at": now.isoformat(),
            "valid_until": (now + timedelta(minutes=5)).isoformat(),
        }
        for name in ("market_metadata", "fee_schedule", "fx_conversion")
    }
    return {
        "policy_snapshot": {"strategy": "spot-perp", "version": 3, "evidence": evidence},
        "execution_policy_snapshot": {"mode": "maker-taker"},
        "risk_policy_snapshot": {"max_unhedged_ms": 3000},
        "exit_policy_snapshot": {"take_profit": {"net_roi_pct": 0.3}},
    }


def _legs(suffix):
    return [
        {
            "leg_id": f"spot-{suffix}", "leg_type": "SPOT", "venue": "indodax",
            "market_type": "spot", "symbol": "btc_idr", "normalized_symbol": "BTC/IDR",
            "base_asset": "BTC", "quote_asset": "IDR", "quantity_unit": "BASE",
            "base_per_unit": "1", "order_quantity": "0.001",
            "exposure_quantity": "0.001", "target_qty": "0.001",
        },
        {
            "leg_id": f"future-{suffix}", "leg_type": "FUTURES", "venue": "okx",
            "market_type": "swap", "symbol": "BTC-USDT-SWAP",
            "normalized_symbol": "BTC/USDT:USDT", "base_asset": "BTC",
            "quote_asset": "USDT", "settlement_asset": "USDT",
            "contract_type": "LINEAR_PERPETUAL", "contract_size": "0.001",
            "position_side": "SHORT", "quantity_unit": "CONTRACTS",
            "base_per_unit": "0.001", "order_quantity": "1",
            "exposure_quantity": "0.001", "target_qty": "1",
        },
    ]


@pytest.fixture
async def postgres_pool():
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
    try:
        yield pool
    finally:
        await pool.close()


async def _session(postgres_pool, suffix):
    account = "hedge-account-" + suffix
    coordinator = SessionCoordinator(
        pool=postgres_pool,
        ownership=OwnershipLeaseService(owner_id="hedge-worker-" + suffix),
    )
    admitted = await coordinator.admit(
        opportunity_id="hedge-opp-" + suffix,
        source_dispatch_id="hedge-dispatch-" + suffix,
        account_scope_id=account,
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        legs=_legs(suffix), actor_id="hedge-integration-test", **_snapshots(),
    )
    lease = admitted["coordinator_lease"]
    aggregate, state_version = 3, 1
    for state in ("VALIDATING", "ADMITTED", "ENTRY_IN_PROGRESS"):
        result = await coordinator.transition(
            coordinator_lease=lease, session_id=admitted["session_id"],
            target_state=state, expected_state_version=state_version,
            expected_aggregate_version=aggregate,
            fencing_generation=lease.generation, event_type="SESSION_" + state,
            event_idempotency_key=f"{admitted['admission_key']}:{state.lower()}",
            actor_id="hedge-integration-test",
        )
        state_version += 1
        aggregate = result["aggregate_version"]
    leg_versions = {}
    for leg_id, prefix in ((f"spot-{suffix}", "SPOT"), (f"future-{suffix}", "FUTURES")):
        version = 1
        for state in ("PREPARED", "SUBMITTING"):
            result = await coordinator.transition_leg(
                coordinator_lease=lease, session_id=admitted["session_id"],
                leg_id=leg_id, target_state=state,
                expected_leg_state_version=version,
                expected_aggregate_version=aggregate,
                fencing_generation=lease.generation,
                event_type=f"{prefix}_LEG_{state}",
                event_idempotency_key=f"{admitted['admission_key']}:{prefix.lower()}:{state.lower()}",
                actor_id="hedge-integration-test",
            )
            version += 1
            aggregate = result["aggregate_version"]
        leg_versions[leg_id] = version
    return coordinator, admitted, aggregate, leg_versions


async def _spot_fill(pool, admitted, suffix, trade_suffix, leg_version, aggregate):
    result = await ExecutionFillService(pool).record_fill(
        session_id=admitted["session_id"], leg_id=f"spot-{suffix}",
        account_scope_id="hedge-account-" + suffix,
        venue="indodax", market_type="spot", symbol="btc_idr",
        exchange_trade_id="spot-trade-" + trade_suffix, side="buy",
        quantity="0.0002", price="1000000000", quote_asset="IDR",
        quote_quantity="200000", fee_amount="200", fee_asset="IDR",
        fee_verified=True, fee_source="EXCHANGE_TRADE",
        filled_at=datetime.now(timezone.utc), actor_id="hedge-integration-test",
        event_idempotency_key="spot-fill-event-" + trade_suffix,
        expected_leg_state_version=leg_version,
        expected_aggregate_version=aggregate,
        fencing_generation=admitted["coordinator_generation"],
        coordinator_lease=admitted["coordinator_lease"],
    )
    return result


@pytest.mark.asyncio
async def test_postgres_partial_spot_fills_reserve_and_allocate_hedge_exactly_once(postgres_pool):
    suffix = uuid.uuid4().hex
    coordinator, admitted, aggregate, versions = await _session(postgres_pool, suffix)
    service = ExecutionHedgeObligationService(postgres_pool)
    lease = admitted["coordinator_lease"]

    first_fill = await _spot_fill(
        postgres_pool, admitted, suffix, suffix + "-1",
        versions[f"spot-{suffix}"], aggregate,
    )
    aggregate = first_fill["aggregate_version"]
    versions[f"spot-{suffix}"] = first_fill["leg_state_version"]
    first_obligation = await service.create_from_spot_fill(
        session_id=admitted["session_id"], spot_fill_id=first_fill["fill_id"],
        hedge_ratio="1", actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert first_obligation["target_base_quantity"] == "0.0002"
    async with postgres_pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT status FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        ) == "HEDGE_PENDING"

    second_fill = await _spot_fill(
        postgres_pool, admitted, suffix, suffix + "-2",
        versions[f"spot-{suffix}"], first_obligation["aggregate_version"],
    )
    versions[f"spot-{suffix}"] = second_fill["leg_state_version"]
    second_obligation = await service.create_from_spot_fill(
        session_id=admitted["session_id"], spot_fill_id=second_fill["fill_id"],
        hedge_ratio="1", actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert second_obligation["target_base_quantity"] == "0.0002"
    replay = await service.create_from_spot_fill(
        session_id=admitted["session_id"], spot_fill_id=second_fill["fill_id"],
        hedge_ratio="1", actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert replay["duplicate"] is True

    reservation = await service.reserve(
        session_id=admitted["session_id"],
        futures_venue="okx", futures_market_type="swap",
        futures_symbol="BTC-USDT-SWAP", base_asset="BTC",
        position_side="SHORT", contract_type="LINEAR_PERPETUAL",
        contract_size="0.001", quantity_unit="CONTRACTS",
        base_per_unit="0.001", requested_base_quantity="0.0004",
        minimum_base_quantity="0.0004",
        idempotency_key="reserve-" + suffix, actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert reservation["reserved"] is True
    assert reservation["reserved_base_quantity"] == "0.0004"
    assert len(reservation["items"]) == 2
    replay_reservation = await service.reserve(
        session_id=admitted["session_id"],
        futures_venue="okx", futures_market_type="swap",
        futures_symbol="BTC-USDT-SWAP", base_asset="BTC",
        position_side="SHORT", contract_type="LINEAR_PERPETUAL",
        contract_size="0.001", quantity_unit="CONTRACTS",
        base_per_unit="0.001", requested_base_quantity="0.0004",
        minimum_base_quantity="0.0004",
        idempotency_key="reserve-" + suffix, actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert replay_reservation["duplicate"] is True
    with pytest.raises(FillConflict, match="idempotency key was reused"):
        await service.reserve(
            session_id=admitted["session_id"],
            futures_venue="okx", futures_market_type="swap",
            futures_symbol="BTC-USDT-SWAP", base_asset="BTC",
            position_side="SHORT", contract_type="LINEAR_PERPETUAL",
            contract_size="0.001", quantity_unit="CONTRACTS",
            base_per_unit="0.001", requested_base_quantity="0.0004",
            minimum_base_quantity="0.0003",
            idempotency_key="reserve-" + suffix, actor_id="hedge-integration-test",
            coordinator_lease=lease, fencing_generation=lease.generation,
        )

    submitted = await service.mark_submitted(
        session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
        client_order_id="hedge-client-" + suffix,
        actor_id="hedge-integration-test",
        event_idempotency_key="submit-hedge-" + suffix,
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert submitted["status"] == "SUBMITTED"

    futures_fill = await ExecutionFillService(postgres_pool).record_fill(
        session_id=admitted["session_id"], leg_id=f"future-{suffix}",
        account_scope_id="hedge-account-" + suffix,
        venue="okx", market_type="swap", symbol="BTC-USDT-SWAP",
        exchange_trade_id="future-trade-" + suffix, side="sell",
        quantity="0.4", price="100000", quote_asset="USDT",
        quote_quantity="40", fee_amount="0.004", fee_asset="USDT",
        fee_verified=True, fee_source="EXCHANGE_TRADE",
        filled_at=datetime.now(timezone.utc), actor_id="hedge-integration-test",
        event_idempotency_key="future-fill-" + suffix,
        expected_leg_state_version=versions[f"future-{suffix}"],
        expected_aggregate_version=submitted["aggregate_version"],
        fencing_generation=lease.generation, coordinator_lease=lease,
    )
    allocation = await service.allocate_futures_fill(
        session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
        futures_fill_id=futures_fill["fill_id"], actor_id="hedge-integration-test",
        event_idempotency_key="allocate-" + suffix,
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert allocation["allocated_base_quantity"] == "0.0004"
    assert len(allocation["allocations"]) == 2
    duplicate_allocation = await service.allocate_futures_fill(
        session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
        futures_fill_id=futures_fill["fill_id"], actor_id="hedge-integration-test",
        event_idempotency_key="allocate-" + suffix,
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert duplicate_allocation["duplicate"] is True

    async with postgres_pool.acquire() as conn:
        obligations = await conn.fetch(
            """
            SELECT target_base_quantity, confirmed_base_quantity,
                   reserved_base_quantity, remaining_base_quantity, status
            FROM public.execution_hedge_obligations
            WHERE session_id=$1 ORDER BY created_at
            """,
            admitted["session_id"],
        )
        reservation_row = await conn.fetchrow(
            "SELECT status, reserved_base_quantity, allocated_base_quantity FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid",
            reservation["reservation_id"],
        )
        allocation_count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_hedge_allocations WHERE reservation_id=$1::uuid",
            reservation["reservation_id"],
        )
        session_status = await conn.fetchval(
            "SELECT status FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
    assert len(obligations) == 2
    assert all(row["status"] == "HEDGED" for row in obligations)
    assert all(Decimal(str(row["confirmed_base_quantity"])) == Decimal("0.0002") for row in obligations)
    assert all(Decimal(str(row["reserved_base_quantity"])) == 0 for row in obligations)
    assert all(Decimal(str(row["remaining_base_quantity"])) == 0 for row in obligations)
    assert reservation_row["status"] == "CONSUMED"
    assert Decimal(str(reservation_row["allocated_base_quantity"])) == Decimal("0.0004")
    assert allocation_count == 2
    assert session_status == "PARTIALLY_HEDGED"


@pytest.mark.asyncio
async def test_postgres_hedge_reservation_below_minimum_and_unknown_release_fail_closed(postgres_pool):
    suffix = uuid.uuid4().hex
    _, admitted, aggregate, versions = await _session(postgres_pool, suffix)
    lease = admitted["coordinator_lease"]
    fill = await _spot_fill(
        postgres_pool, admitted, suffix, suffix + "-release",
        versions[f"spot-{suffix}"], aggregate,
    )
    service = ExecutionHedgeObligationService(postgres_pool)
    obligation = await service.create_from_spot_fill(
        session_id=admitted["session_id"], spot_fill_id=fill["fill_id"],
        hedge_ratio="1", actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    too_small = await service.reserve(
        session_id=admitted["session_id"], futures_venue="okx",
        futures_market_type="swap", futures_symbol="BTC-USDT-SWAP",
        base_asset="BTC", position_side="SHORT",
        contract_type="LINEAR_PERPETUAL", contract_size="0.001",
        quantity_unit="CONTRACTS", base_per_unit="0.001",
        requested_base_quantity="0.0001", minimum_base_quantity="0.0002",
        idempotency_key="below-min-" + suffix, actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert too_small["reserved"] is False
    assert too_small["reason"] == "BELOW_MINIMUM_HEDGE"

    reservation = await service.reserve(
        session_id=admitted["session_id"], futures_venue="okx",
        futures_market_type="swap", futures_symbol="BTC-USDT-SWAP",
        base_asset="BTC", position_side="SHORT",
        contract_type="LINEAR_PERPETUAL", contract_size="0.001",
        quantity_unit="CONTRACTS", base_per_unit="0.001",
        requested_base_quantity="0.0002", minimum_base_quantity="0.0002",
        idempotency_key="unknown-reserve-" + suffix, actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    await service.mark_submitted(
        session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
        client_order_id="unknown-client-" + suffix, actor_id="hedge-integration-test",
        event_idempotency_key="unknown-submit-" + suffix,
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    await service.mark_unknown(
        session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
        reason="lost submit acknowledgement", actor_id="hedge-integration-test",
        event_idempotency_key="unknown-event-" + suffix,
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    with pytest.raises(HedgeObligationError, match="terminal order and fill proof"):
        await service.release(
            session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
            actor_id="hedge-integration-test", event_idempotency_key="release-unverified-" + suffix,
            coordinator_lease=lease, fencing_generation=lease.generation,
        )
    with pytest.raises(HedgeObligationError, match="terminal_order_evidence"):
        await service.release(
            session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
            actor_id="hedge-integration-test", event_idempotency_key="release-without-evidence-" + suffix,
            terminal_order_verified=True, fills_reconciled=True,
            coordinator_lease=lease, fencing_generation=lease.generation,
        )
    now = datetime.now(timezone.utc).isoformat()
    terminal_evidence = {
        "evidence_id": "terminal-" + suffix, "source": "okx-private-order-stream",
        "verified_at": now, "terminal_status": "canceled",
        "client_order_id": "unknown-client-" + suffix,
    }
    fill_evidence = {
        "evidence_id": "fills-" + suffix, "source": "okx-rest-trades",
        "reconciled_at": now, "fills_checked": True,
        "client_order_id": "unknown-client-" + suffix,
    }
    with pytest.raises(HedgeObligationError, match="does not match reservation"):
        await service.release(
            session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
            actor_id="hedge-integration-test", event_idempotency_key="release-wrong-order-" + suffix,
            terminal_order_verified=True, fills_reconciled=True,
            terminal_order_evidence={**terminal_evidence, "client_order_id": "wrong-order"},
            fill_reconciliation_evidence=fill_evidence,
            coordinator_lease=lease, fencing_generation=lease.generation,
        )
    released = await service.release(
        session_id=admitted["session_id"], reservation_id=reservation["reservation_id"],
        actor_id="hedge-integration-test", event_idempotency_key="release-verified-" + suffix,
        terminal_order_verified=True, fills_reconciled=True,
        terminal_order_evidence=terminal_evidence,
        fill_reconciliation_evidence=fill_evidence,
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    assert released["status"] == "RELEASED"
    async with postgres_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT reserved_base_quantity, remaining_base_quantity FROM public.execution_hedge_obligations WHERE obligation_id=$1::uuid",
            obligation["obligation_id"],
        )
        terminal_evidence_id = await conn.fetchval(
            "SELECT release_evidence #>> '{terminal_order,evidence_id}' FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid",
            reservation["reservation_id"],
        )
        fill_evidence_id = await conn.fetchval(
            "SELECT release_evidence #>> '{fill_reconciliation,evidence_id}' FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid",
            reservation["reservation_id"],
        )
        with pytest.raises(Exception, match="release evidence is immutable"):
            await conn.execute(
                "UPDATE public.execution_hedge_reservations SET release_evidence='{}'::jsonb WHERE reservation_id=$1::uuid",
                reservation["reservation_id"],
            )
    assert terminal_evidence_id == terminal_evidence["evidence_id"]
    assert fill_evidence_id == fill_evidence["evidence_id"]
    assert Decimal(str(row["reserved_base_quantity"])) == 0
    assert Decimal(str(row["remaining_base_quantity"])) == Decimal("0.0002")


def test_hedge_ratio_above_one_requires_explicit_overhedge_policy():
    from types import SimpleNamespace

    service = ExecutionHedgeObligationService(SimpleNamespace(acquire=lambda: None))
    with pytest.raises(HedgeObligationError, match="above 1.0"):
        import asyncio
        asyncio.run(service.create_from_spot_fill(
            session_id="session", spot_fill_id=str(uuid.uuid4()),
            hedge_ratio="1.0001", actor_id="test",
            coordinator_lease=object(), fencing_generation=1,
        ))


def test_reservation_rejects_invalid_position_side_before_database_access():
    from types import SimpleNamespace

    service = ExecutionHedgeObligationService(SimpleNamespace(acquire=lambda: None))
    with pytest.raises(HedgeObligationError, match="position_side"):
        import asyncio
        asyncio.run(service.reserve(
            session_id="session", futures_venue="okx", futures_market_type="swap",
            futures_symbol="BTC-USDT-SWAP", base_asset="BTC",
            position_side="SIDEWAYS", contract_type="LINEAR_PERPETUAL",
            contract_size="0.001", quantity_unit="CONTRACTS",
            base_per_unit="0.001", requested_base_quantity="0.01",
            minimum_base_quantity="0.001", idempotency_key="reserve",
            actor_id="test", coordinator_lease=object(), fencing_generation=1,
        ))


def test_reservation_release_requires_boolean_reconciliation_proofs():
    from types import SimpleNamespace

    service = ExecutionHedgeObligationService(SimpleNamespace(acquire=lambda: None))
    with pytest.raises(HedgeObligationError, match="must be booleans"):
        import asyncio
        asyncio.run(service.release(
            session_id="session", reservation_id=str(uuid.uuid4()),
            actor_id="test", event_idempotency_key="release",
            terminal_order_verified="false", fills_reconciled=True,
            coordinator_lease=object(), fencing_generation=1,
        ))


@pytest.mark.asyncio
async def test_postgres_hedge_reserve_is_blocked_before_hedge_pending(postgres_pool):
    suffix = uuid.uuid4().hex
    _, admitted, _, _ = await _session(postgres_pool, suffix)
    service = ExecutionHedgeObligationService(postgres_pool)
    with pytest.raises(HedgeObligationError, match="cannot reserve hedge obligations"):
        await service.reserve(
            session_id=admitted["session_id"], futures_venue="okx",
            futures_market_type="swap", futures_symbol="BTC-USDT-SWAP",
            base_asset="BTC", position_side="SHORT",
            contract_type="LINEAR_PERPETUAL", contract_size="0.001",
            quantity_unit="CONTRACTS", base_per_unit="0.001",
            requested_base_quantity="0.001", minimum_base_quantity="0.001",
            idempotency_key="premature-reserve-" + suffix,
            actor_id="hedge-integration-test",
            coordinator_lease=admitted["coordinator_lease"],
            fencing_generation=admitted["coordinator_generation"],
        )


@pytest.mark.asyncio
async def test_postgres_hedge_service_rejects_stale_coordinator_generation(postgres_pool):
    suffix = uuid.uuid4().hex
    _, admitted, _, _ = await _session(postgres_pool, suffix)
    service = ExecutionHedgeObligationService(postgres_pool)
    with pytest.raises(HedgeObligationError, match="fencing generation does not match session"):
        await service.create_from_spot_fill(
            session_id=admitted["session_id"], spot_fill_id=str(uuid.uuid4()),
            hedge_ratio="1", actor_id="hedge-integration-test",
            coordinator_lease=admitted["coordinator_lease"],
            fencing_generation=admitted["coordinator_generation"] + 1,
        )


@pytest.mark.asyncio
async def test_postgres_hedge_submit_is_blocked_after_session_enters_reconciliation(postgres_pool):
    suffix = uuid.uuid4().hex
    coordinator, admitted, aggregate, leg_versions = await _session(postgres_pool, suffix)
    fill = await _spot_fill(
        postgres_pool, admitted, suffix, "submit-gate-" + suffix,
        leg_versions[f"spot-{suffix}"], aggregate,
    )
    service = ExecutionHedgeObligationService(postgres_pool)
    obligation = await service.create_from_spot_fill(
        session_id=admitted["session_id"], spot_fill_id=fill["fill_id"],
        hedge_ratio="1", actor_id="hedge-integration-test",
        coordinator_lease=admitted["coordinator_lease"],
        fencing_generation=admitted["coordinator_generation"],
    )
    reservation = await service.reserve(
        session_id=admitted["session_id"], futures_venue="okx",
        futures_market_type="swap", futures_symbol="BTC-USDT-SWAP",
        base_asset="BTC", position_side="SHORT",
        contract_type="LINEAR_PERPETUAL", contract_size="0.001",
        quantity_unit="CONTRACTS", base_per_unit="0.001",
        requested_base_quantity=obligation["target_base_quantity"],
        minimum_base_quantity="0.0001",
        idempotency_key="submit-gate-reserve-" + suffix,
        actor_id="hedge-integration-test",
        coordinator_lease=admitted["coordinator_lease"],
        fencing_generation=admitted["coordinator_generation"],
    )
    async with postgres_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT state_version, aggregate_version FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
    await coordinator.transition(
        coordinator_lease=admitted["coordinator_lease"],
        session_id=admitted["session_id"],
        target_state="RECONCILIATION_REQUIRED",
        expected_state_version=int(session["state_version"]),
        expected_aggregate_version=int(session["aggregate_version"]),
        fencing_generation=admitted["coordinator_generation"],
        event_type="TEST_SESSION_RECONCILIATION_REQUIRED",
        event_idempotency_key="submit-gate-reconcile-" + suffix,
        actor_id="hedge-integration-test",
    )
    with pytest.raises(HedgeObligationError, match="cannot submit hedge reservation"):
        await service.mark_submitted(
            session_id=admitted["session_id"],
            reservation_id=reservation["reservation_id"],
            client_order_id="must-not-submit-" + suffix,
            actor_id="hedge-integration-test",
            event_idempotency_key="submit-gate-mark-" + suffix,
            coordinator_lease=admitted["coordinator_lease"],
            fencing_generation=admitted["coordinator_generation"],
        )
    async with postgres_pool.acquire() as conn:
        current = await conn.fetchrow(
            "SELECT status, client_order_id FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid",
            reservation["reservation_id"],
        )
    assert current["status"] == "RESERVED"
    assert current["client_order_id"] is None


@pytest.mark.asyncio
async def test_postgres_entry_and_exit_fills_keep_separate_leg_projections(postgres_pool):
    suffix = uuid.uuid4().hex
    coordinator, admitted, aggregate, versions = await _session(postgres_pool, suffix)
    lease = admitted["coordinator_lease"]
    service = ExecutionFillService(postgres_pool)

    spot_entry = await _spot_fill(
        postgres_pool, admitted, suffix, "pnl-entry-" + suffix,
        versions[f"spot-{suffix}"], aggregate,
    )
    obligation = await ExecutionHedgeObligationService(postgres_pool).create_from_spot_fill(
        session_id=admitted["session_id"], spot_fill_id=spot_entry["fill_id"],
        hedge_ratio="1", actor_id="hedge-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    futures_entry = await service.record_fill(
        session_id=admitted["session_id"], leg_id=f"future-{suffix}",
        account_scope_id="hedge-account-" + suffix,
        venue="okx", market_type="swap", symbol="BTC-USDT-SWAP",
        exchange_trade_id="futures-entry-" + suffix, side="sell",
        quantity="0.2", price="65000", quote_asset="USDT",
        quote_quantity="13", fee_amount="0.01", fee_asset="USDT",
        fee_verified=True, fee_source="EXCHANGE_TRADE",
        filled_at=datetime.now(timezone.utc), actor_id="hedge-integration-test",
        event_idempotency_key="futures-entry-event-" + suffix,
        expected_leg_state_version=versions[f"future-{suffix}"],
        expected_aggregate_version=obligation["aggregate_version"],
        fencing_generation=lease.generation, coordinator_lease=lease,
    )
    aggregate = futures_entry["aggregate_version"]

    for leg_id, fill_result, prefix in (
        (f"spot-{suffix}", spot_entry, "SPOT"),
        (f"future-{suffix}", futures_entry, "FUTURES"),
    ):
        row_version = fill_result["leg_state_version"]
        transitioned = await coordinator.transition_leg(
            coordinator_lease=lease, session_id=admitted["session_id"],
            leg_id=leg_id, target_state="FILLED",
            expected_leg_state_version=row_version,
            expected_aggregate_version=aggregate,
            fencing_generation=lease.generation,
            event_type=f"{prefix}_ENTRY_FILLED",
            event_idempotency_key=f"{admitted['admission_key']}:{prefix.lower()}:filled",
            actor_id="hedge-integration-test",
        )
        aggregate = transitioned["aggregate_version"]

    async with postgres_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT state_version, aggregate_version FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
    transitioned = await coordinator.transition(
        coordinator_lease=lease, session_id=admitted["session_id"],
        target_state="HEDGED",
        expected_state_version=int(session["state_version"]),
        expected_aggregate_version=int(session["aggregate_version"]),
        fencing_generation=lease.generation,
        event_type="TEST_SESSION_HEDGED",
        event_idempotency_key="session-hedged-" + suffix,
        actor_id="hedge-integration-test",
    )
    transitioned = await coordinator.transition(
        coordinator_lease=lease, session_id=admitted["session_id"],
        target_state="EXIT_IN_PROGRESS",
        expected_state_version=transitioned["state_version"],
        expected_aggregate_version=transitioned["aggregate_version"],
        fencing_generation=lease.generation,
        event_type="TEST_SESSION_EXITING",
        event_idempotency_key="session-exiting-" + suffix,
        actor_id="hedge-integration-test",
    )
    aggregate = transitioned["aggregate_version"]

    leg_versions = {}
    for leg_id, prefix in ((f"spot-{suffix}", "SPOT"), (f"future-{suffix}", "FUTURES")):
        row_version = await postgres_pool.fetchval(
            "SELECT state_version FROM public.execution_legs WHERE session_id=$1 AND leg_id=$2",
            admitted["session_id"], leg_id,
        )
        result = await coordinator.transition_leg(
            coordinator_lease=lease, session_id=admitted["session_id"],
            leg_id=leg_id, target_state="CLOSING",
            expected_leg_state_version=int(row_version),
            expected_aggregate_version=aggregate,
            fencing_generation=lease.generation,
            event_type=f"{prefix}_CLOSING",
            event_idempotency_key=f"{admitted['admission_key']}:{prefix.lower()}:closing",
            actor_id="hedge-integration-test",
        )
        aggregate = result["aggregate_version"]
        leg_versions[leg_id] = result["leg_state_version"]

    spot_exit = await service.record_fill(
        session_id=admitted["session_id"], leg_id=f"spot-{suffix}",
        account_scope_id="hedge-account-" + suffix,
        venue="indodax", market_type="spot", symbol="btc_idr",
        exchange_trade_id="spot-exit-" + suffix, side="sell",
        quantity="0.0002", price="1001000000", quote_asset="IDR",
        quote_quantity="200200", fee_amount="200", fee_asset="IDR",
        fee_verified=True, fee_source="EXCHANGE_TRADE",
        fill_role="EXIT", filled_at=datetime.now(timezone.utc),
        actor_id="hedge-integration-test", event_idempotency_key="spot-exit-event-" + suffix,
        expected_leg_state_version=leg_versions[f"spot-{suffix}"],
        expected_aggregate_version=aggregate,
        fencing_generation=lease.generation, coordinator_lease=lease,
    )
    aggregate = spot_exit["aggregate_version"]
    futures_exit = await service.record_fill(
        session_id=admitted["session_id"], leg_id=f"future-{suffix}",
        account_scope_id="hedge-account-" + suffix,
        venue="okx", market_type="swap", symbol="BTC-USDT-SWAP",
        exchange_trade_id="futures-exit-" + suffix, side="buy",
        quantity="0.2", price="64900", quote_asset="USDT",
        quote_quantity="12.98", fee_amount="0.01", fee_asset="USDT",
        fee_verified=True, fee_source="EXCHANGE_TRADE",
        fill_role="EXIT", filled_at=datetime.now(timezone.utc),
        actor_id="hedge-integration-test", event_idempotency_key="futures-exit-event-" + suffix,
        expected_leg_state_version=leg_versions[f"future-{suffix}"],
        expected_aggregate_version=aggregate,
        fencing_generation=lease.generation, coordinator_lease=lease,
    )
    assert spot_exit["fill_role"] == "EXIT"
    assert futures_exit["fill_role"] == "EXIT"
    async with postgres_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT leg_id, confirmed_filled_quantity, confirmed_exit_quantity,
                   confirmed_exposure_quantity, confirmed_exit_exposure_quantity,
                   status
            FROM public.execution_legs WHERE session_id=$1 ORDER BY leg_id
            """,
            admitted["session_id"],
        )
        fill_roles = await conn.fetch(
            "SELECT fill_role, count(*) AS n FROM public.execution_fills WHERE session_id=$1 GROUP BY fill_role",
            admitted["session_id"],
        )
    by_leg = {row["leg_id"]: row for row in rows}
    assert Decimal(str(by_leg[f"spot-{suffix}"]["confirmed_filled_quantity"])) == Decimal("0.0002")
    assert Decimal(str(by_leg[f"spot-{suffix}"]["confirmed_exit_quantity"])) == Decimal("0.0002")
    assert Decimal(str(by_leg[f"spot-{suffix}"]["confirmed_exit_exposure_quantity"])) == Decimal("0.0002")
    assert Decimal(str(by_leg[f"future-{suffix}"]["confirmed_filled_quantity"])) == Decimal("0.2")
    assert Decimal(str(by_leg[f"future-{suffix}"]["confirmed_exit_quantity"])) == Decimal("0.2")
    assert Decimal(str(by_leg[f"future-{suffix}"]["confirmed_exit_exposure_quantity"])) == Decimal("0.0002")
    assert {row["fill_role"]: row["n"] for row in fill_roles} == {"ENTRY": 2, "EXIT": 2}


@pytest.mark.asyncio
async def test_postgres_exit_fill_overclose_forces_reconciliation(postgres_pool):
    suffix = uuid.uuid4().hex
    coordinator, admitted, aggregate, versions = await _session(postgres_pool, suffix)
    lease = admitted["coordinator_lease"]
    spot_entry = await _spot_fill(
        postgres_pool, admitted, suffix, "overclose-entry-" + suffix,
        versions[f"spot-{suffix}"], aggregate,
    )
    async with postgres_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT state_version, aggregate_version FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
        leg = await conn.fetchrow(
            "SELECT state_version FROM public.execution_legs WHERE session_id=$1 AND leg_id=$2",
            admitted["session_id"], f"spot-{suffix}",
        )
    transition = await coordinator.transition(
        coordinator_lease=lease, session_id=admitted["session_id"],
        target_state="HEDGED",
        expected_state_version=int(session["state_version"]),
        expected_aggregate_version=int(session["aggregate_version"]),
        fencing_generation=lease.generation,
        event_type="TEST_SESSION_HEDGED_OVER_CLOSE",
        event_idempotency_key="overclose-hedged-" + suffix,
        actor_id="hedge-integration-test",
    )
    transition = await coordinator.transition(
        coordinator_lease=lease, session_id=admitted["session_id"],
        target_state="EXIT_IN_PROGRESS",
        expected_state_version=transition["state_version"],
        expected_aggregate_version=transition["aggregate_version"],
        fencing_generation=lease.generation,
        event_type="TEST_SESSION_EXITING_OVER_CLOSE",
        event_idempotency_key="overclose-exiting-" + suffix,
        actor_id="hedge-integration-test",
    )
    leg_transition = await coordinator.transition_leg(
        coordinator_lease=lease, session_id=admitted["session_id"],
        leg_id=f"spot-{suffix}", target_state="CLOSING",
        expected_leg_state_version=int(leg["state_version"]),
        expected_aggregate_version=transition["aggregate_version"],
        fencing_generation=lease.generation,
        event_type="SPOT_CLOSING_OVER_CLOSE",
        event_idempotency_key="overclose-spot-closing-" + suffix,
        actor_id="hedge-integration-test",
    )
    result = await ExecutionFillService(postgres_pool).record_fill(
        session_id=admitted["session_id"], leg_id=f"spot-{suffix}",
        account_scope_id="hedge-account-" + suffix,
        venue="indodax", market_type="spot", symbol="btc_idr",
        exchange_trade_id="spot-overclose-" + suffix, side="sell",
        quantity="0.0003", price="1000000000", quote_asset="IDR",
        quote_quantity="300000", fee_amount="0", fee_asset="IDR",
        fee_verified=True, fee_source="EXCHANGE_TRADE", fill_role="EXIT",
        filled_at=datetime.now(timezone.utc), actor_id="hedge-integration-test",
        event_idempotency_key="spot-overclose-event-" + suffix,
        expected_leg_state_version=leg_transition["leg_state_version"],
        expected_aggregate_version=leg_transition["aggregate_version"],
        fencing_generation=lease.generation, coordinator_lease=lease,
    )
    assert result["session_state"] == "RECONCILIATION_REQUIRED"
