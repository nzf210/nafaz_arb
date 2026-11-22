"""Unit and PostgreSQL integration tests for the V3 Session Coordinator."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from executor.execution_repository import StaleAggregate
from executor.ownership import OwnershipLeaseService
from executor.risk_control import RiskControlPlane, RiskPermissionDenied
from executor.session_coordinator import (
    SessionAdmissionError,
    SessionCoordinator,
    build_admission_identity,
)


def _snapshots():
    now = datetime.now(timezone.utc)
    evidence = {
        name: {
            "status": "VERIFIED" if name != "fx_conversion" else "NOT_REQUIRED",
            "observed_at": now.isoformat(),
            "valid_until": (now + timedelta(minutes=2)).isoformat(),
        }
        for name in ("market_metadata", "fee_schedule", "fx_conversion")
    }
    return {
        "policy_snapshot": {"strategy": "spot-perp", "version": 3, "evidence": evidence},
        "execution_policy_snapshot": {"mode": "maker-taker"},
        "risk_policy_snapshot": {"max_unhedged_ms": 3000},
        "exit_policy_snapshot": {"take_profit": {"net_roi_pct": 0.3}},
    }


def _legs(suffix="unit"):
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


def test_admission_identity_is_stable_and_account_route_scoped():
    first = build_admission_identity(
        account_scope_id="account-a", source_dispatch_id="dispatch-1",
        opportunity_id="opp-1", pair_key="indodax:btc_idr->okx:btc-usdt-swap",
    )
    retry = build_admission_identity(
        account_scope_id="account-a", source_dispatch_id="dispatch-1",
        opportunity_id="opp-1", pair_key="indodax:btc_idr->okx:btc-usdt-swap",
    )
    changed_opportunity = build_admission_identity(
        account_scope_id="account-a", source_dispatch_id="dispatch-1",
        opportunity_id="opp-different", pair_key="indodax:btc_idr->okx:btc-usdt-swap",
    )
    other_account = build_admission_identity(
        account_scope_id="account-b", source_dispatch_id="dispatch-1",
        opportunity_id="opp-1", pair_key="indodax:btc_idr->okx:btc-usdt-swap",
    )
    other_route = build_admission_identity(
        account_scope_id="account-a", source_dispatch_id="dispatch-1",
        opportunity_id="opp-1", pair_key="tokocrypto:btc_idr->mexc:btc_usdt",
    )
    assert first == retry
    assert first == changed_opportunity
    assert first != other_account
    assert first != other_route
    assert first[0].startswith("v3-adm-")
    assert first[1].startswith("v3-session-")


def test_admission_identity_rejects_missing_dispatch_identity():
    with pytest.raises(SessionAdmissionError, match="source_dispatch_id"):
        build_admission_identity(
            account_scope_id="account-a", source_dispatch_id="",
            opportunity_id="opp-1", pair_key="route-a",
        )


def test_policy_snapshot_rejects_stale_critical_evidence():
    from executor.session_coordinator import _validate_policy_evidence

    snapshot = _snapshots()["policy_snapshot"]
    snapshot["evidence"]["fee_schedule"]["valid_until"] = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    ).isoformat()
    with pytest.raises(SessionAdmissionError, match="fee_schedule is stale"):
        _validate_policy_evidence(snapshot)


class _FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_):
        return False


class _FakePool:
    def acquire(self):
        return _FakeAcquire(object())


class _FakeOwnership:
    def __init__(self, *, fail_release=False):
        self.lease = SimpleNamespace(
            scope_key="scope", owner_id="worker-a", generation=9,
            account_scope_id="account-a", venue="coordinator",
            market_type="session", instrument="session-id",
        )
        self.acquired = []
        self.released = []
        self.fail_release = fail_release

    async def acquire(self, conn, **kwargs):
        self.acquired.append(kwargs)
        return self.lease

    async def release(self, conn, lease):
        self.released.append(lease)
        if self.fail_release:
            raise RuntimeError("release failed")
        return True


class _FakeRepository:
    def __init__(self, *, error=None):
        self.calls = []
        self.error = error

    async def create_session(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return {"session_id": kwargs["session_id"], "created": True, "aggregate_version": 3}

    async def transition_session(self, **kwargs):
        self.calls.append(kwargs)
        return {"state": kwargs["target_state"]}

    async def transition_leg(self, **kwargs):
        self.calls.append(kwargs)
        return {"state": kwargs["target_state"]}


@pytest.mark.asyncio
async def test_coordinator_admission_acquires_session_scope_and_passes_fence():
    ownership = _FakeOwnership()
    repository = _FakeRepository()
    coordinator = SessionCoordinator(
        pool=_FakePool(), repository=repository, ownership=ownership,
    )
    result = await coordinator.admit(
        opportunity_id="opp-1", source_dispatch_id="dispatch-1",
        account_scope_id="account-a", pair_key="route-a", legs=_legs(),
        actor_id="unit-test", **_snapshots(),
    )
    assert ownership.acquired == [{
        "account_scope_id": "account-a", "venue": "coordinator",
        "market_type": "session", "instrument": result["session_id"],
    }]
    call = repository.calls[0]
    assert call["fencing_generation"] == 9
    assert call["coordinator_lease"] is ownership.lease
    assert call["session_id"] == result["session_id"]
    assert result["coordinator_generation"] == 9
    assert not ownership.released


@pytest.mark.asyncio
async def test_coordinator_retains_lease_when_admission_outcome_is_ambiguous():
    ownership = _FakeOwnership()
    repository = _FakeRepository(error=RuntimeError("database unavailable"))
    coordinator = SessionCoordinator(
        pool=_FakePool(), repository=repository, ownership=ownership,
    )
    with pytest.raises(RuntimeError, match="database unavailable"):
        await coordinator.admit(
            opportunity_id="opp-1", source_dispatch_id="dispatch-1",
            account_scope_id="account-a", pair_key="route-a", legs=_legs(),
            actor_id="unit-test", **_snapshots(),
        )
    assert ownership.released == []


@pytest.mark.asyncio
async def test_coordinator_never_transitions_without_a_lease():
    coordinator = SessionCoordinator(
        pool=_FakePool(), repository=_FakeRepository(), ownership=_FakeOwnership(),
    )
    with pytest.raises(SessionAdmissionError, match="live coordinator lease"):
        await coordinator.transition(
            coordinator_lease=None, session_id="s", target_state="VALIDATING",
        )


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


@pytest.mark.asyncio
async def test_postgres_coordinator_admission_and_transition_are_fenced(postgres_pool):
    suffix = uuid.uuid4().hex
    account = "coordinator-account-" + suffix
    coordinator = SessionCoordinator(
        pool=postgres_pool,
        ownership=OwnershipLeaseService(owner_id="coordinator-worker-" + suffix),
    )
    result = await coordinator.admit(
        opportunity_id="opp-" + suffix,
        source_dispatch_id="dispatch-" + suffix,
        account_scope_id=account,
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        legs=_legs(suffix),
        actor_id="integration-test",
        **_snapshots(),
    )
    lease = result["coordinator_lease"]
    assert result["created"] is True
    assert result["coordinator_generation"] == lease.generation

    validating = await coordinator.transition(
        coordinator_lease=lease,
        session_id=result["session_id"],
        target_state="VALIDATING",
        expected_state_version=1,
        expected_aggregate_version=3,
        fencing_generation=lease.generation,
        event_type="SESSION_VALIDATING",
        event_idempotency_key=result["admission_key"] + ":validating",
        actor_id="integration-test",
    )
    assert validating["state"] == "VALIDATING"

    # Reclaiming the expired coordinator scope increments the generation and
    # fences the old process even if it still has a cached lease object.
    async with postgres_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE public.executor_ownership_leases
            SET lease_expires_at=clock_timestamp() - interval '1 second'
            WHERE scope_key=$1
            """,
            lease.scope_key,
        )
        replacement = await OwnershipLeaseService(
            owner_id="replacement-worker-" + suffix
        ).acquire(
            conn, account_scope_id=account, venue="coordinator",
            market_type="session", instrument=result["session_id"],
        )
    assert replacement.generation == lease.generation + 1

    recovered = SessionCoordinator(
        pool=postgres_pool,
        ownership=OwnershipLeaseService(owner_id=replacement.owner_id),
    )
    replay = await recovered.recover(
        session_id=result["session_id"],
        account_scope_id=account,
        actor_id="recovery-test",
    )
    assert replay["reclaimed"] is True
    assert replay["session_id"] == result["session_id"]
    assert replay["fencing_generation"] == replacement.generation
    assert replay["state_version"] == 2
    assert replay["aggregate_version"] == 5

    with pytest.raises(StaleAggregate, match="coordinator lease"):
        await coordinator.transition(
            coordinator_lease=lease,
            session_id=result["session_id"],
            target_state="ADMITTED",
            expected_state_version=2,
            expected_aggregate_version=4,
            fencing_generation=lease.generation,
            event_type="SESSION_ADMITTED",
            event_idempotency_key=result["admission_key"] + ":admitted-stale",
            actor_id="stale-worker",
        )
    admitted = await recovered.transition(
        coordinator_lease=replay["coordinator_lease"],
        session_id=result["session_id"],
        target_state="ADMITTED",
        expected_state_version=2,
        expected_aggregate_version=5,
        fencing_generation=replacement.generation,
        event_type="SESSION_ADMITTED",
        event_idempotency_key=result["admission_key"] + ":admitted",
        actor_id="recovery-worker",
    )
    assert admitted["state"] == "ADMITTED"
    async with postgres_pool.acquire() as conn:
        event_types = await conn.fetch(
            "SELECT event_type FROM public.execution_session_events WHERE session_id=$1 ORDER BY aggregate_version",
            result["session_id"],
        )
        binding = await conn.fetchrow(
            """
            SELECT coordinator_scope_key, coordinator_owner_id, fencing_generation
            FROM public.execution_sessions WHERE session_id=$1
            """,
            result["session_id"],
        )
    assert event_types[-2]["event_type"] == "COORDINATOR_LEASE_RECLAIMED"
    assert binding["coordinator_scope_key"] == replay["coordinator_lease"].scope_key
    assert binding["coordinator_owner_id"] == replay["coordinator_lease"].owner_id
    assert binding["fencing_generation"] == replacement.generation
    assert await recovered.release(replay["coordinator_lease"]) is False


@pytest.mark.asyncio
async def test_postgres_concurrent_same_dispatch_creates_one_session(postgres_pool):
    import asyncio

    suffix = uuid.uuid4().hex
    coordinator = SessionCoordinator(
        pool=postgres_pool,
        ownership=OwnershipLeaseService(owner_id="concurrent-worker-" + suffix),
    )
    request = {
        "opportunity_id": "opp-" + suffix,
        "source_dispatch_id": "dispatch-" + suffix,
        "account_scope_id": "concurrent-account-" + suffix,
        "pair_key": "indodax:btc_idr->okx:btc-usdt-swap",
        "legs": _legs(suffix),
        "actor_id": "integration-test",
        **_snapshots(),
    }
    first, second = await asyncio.gather(
        coordinator.admit(**request),
        coordinator.admit(**request),
    )
    assert first["session_id"] == second["session_id"]
    assert {first["created"], second["created"]} == {True, False}
    async with postgres_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_sessions WHERE admission_key=$1",
            first["admission_key"],
        )
        event_count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_session_events WHERE session_id=$1",
            first["session_id"],
        )
    assert count == 1
    assert event_count == 3
    assert await coordinator.release(first["coordinator_lease"]) is False


@pytest.mark.asyncio
async def test_postgres_admission_serializes_against_entry_stop(postgres_pool):
    suffix = uuid.uuid4().hex
    account = "coordinator-stop-account-" + suffix
    controls = RiskControlPlane()
    await controls.set_control(
        postgres_pool, scope_key="global", scope_type="GLOBAL",
        entry_blocked=True, derisk_allowed=True,
        actor_id="integration-test", event_type="ENTRY_BLOCKED",
        event_idempotency_key="coordinator-stop-" + suffix,
    )
    coordinator = SessionCoordinator(
        pool=postgres_pool,
        ownership=OwnershipLeaseService(owner_id="stop-worker-" + suffix),
    )
    with pytest.raises(RiskPermissionDenied, match="ENTRY_BLOCKED"):
        await coordinator.admit(
            opportunity_id="opp-" + suffix,
            source_dispatch_id="dispatch-" + suffix,
            account_scope_id=account, pair_key="route-" + suffix,
            legs=_legs(suffix), actor_id="integration-test",
            **_snapshots(),
        )
    # Reset shared global control and preserve the append-only audit trail.
    async with postgres_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT version FROM public.executor_risk_controls WHERE scope_key='global'"
        )
    await controls.set_control(
        postgres_pool, scope_key="global", scope_type="GLOBAL",
        entry_blocked=False, derisk_allowed=True,
        actor_id="integration-test", event_type="TEST_RESET",
        event_idempotency_key="coordinator-stop-reset-" + suffix,
        expected_version=int(row["version"]),
    )
