from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from executor.derisk_fence import derisk_fence
from executor.emergency_derisk import EmergencyDeriskError, EmergencyDeriskLease
from executor.global_stop import GlobalStopError


class _FakeRedis:
    def __init__(self):
        self.values = {}

    async def set(self, key, value, *, nx=False, px=None):
        if nx and key in self.values:
            return None
        self.values[key] = value
        return "OK"

    async def eval(self, script, numkeys, key, token):
        if self.values.get(key) == token:
            del self.values[key]
            return 1
        return 0


def _engine(redis=None, *, owner="worker-a", designated="worker-a", service=None):
    return SimpleNamespace(
        config=SimpleNamespace(
            mock_mode=False,
            executor_instance_id=owner,
            emergency_derisk_owner_id=designated,
            emergency_derisk_lease_ttl_ms=120000,
        ),
        redis=redis,
        global_stop=service,
    )


@pytest.mark.asyncio
async def test_emergency_lease_serializes_same_scope_and_releases_by_owner_token():
    redis = _FakeRedis()
    lease = await EmergencyDeriskLease.acquire(
        redis,
        owner_id="worker-a",
        designated_owner_id="worker-a",
        client_order_id="close-001",
        account_scope_id="acct-1",
        venue="okx",
        market_type="perpetual",
        instrument="BTC-USDT-SWAP",
    )
    with pytest.raises(EmergencyDeriskError, match="another emergency"):
        await EmergencyDeriskLease.acquire(
            redis,
            owner_id="worker-a",
            designated_owner_id="worker-a",
            client_order_id="close-002",
            account_scope_id="acct-1",
            venue="okx",
            market_type="perpetual",
            instrument="BTC-USDT-SWAP",
        )
    await lease.release()
    assert redis.values == {}


@pytest.mark.asyncio
async def test_emergency_lease_allows_independent_instruments_concurrently():
    redis = _FakeRedis()
    first = await EmergencyDeriskLease.acquire(
        redis, owner_id="worker-a", designated_owner_id="worker-a",
        client_order_id="close-btc", account_scope_id="acct-1",
        venue="okx", market_type="perpetual", instrument="BTC-USDT-SWAP",
    )
    second = await EmergencyDeriskLease.acquire(
        redis, owner_id="worker-a", designated_owner_id="worker-a",
        client_order_id="close-eth", account_scope_id="acct-1",
        venue="okx", market_type="perpetual", instrument="ETH-USDT-SWAP",
    )
    assert first.key != second.key
    await first.release()
    await second.release()


@pytest.mark.asyncio
async def test_emergency_lease_requires_designated_owner_and_stable_scope():
    redis = _FakeRedis()
    with pytest.raises(EmergencyDeriskError, match="not the designated"):
        await EmergencyDeriskLease.acquire(
            redis, owner_id="worker-b", designated_owner_id="worker-a",
            client_order_id="close-001", account_scope_id="acct-1",
            venue="okx", market_type="perpetual", instrument="BTC-USDT-SWAP",
        )
    with pytest.raises(EmergencyDeriskError, match="complete account"):
        await EmergencyDeriskLease.acquire(
            redis, owner_id="worker-a", designated_owner_id="worker-a",
            client_order_id="close-001", account_scope_id="",
            venue="okx", market_type="perpetual", instrument="BTC-USDT-SWAP",
        )


@pytest.mark.asyncio
async def test_derisk_fence_uses_redis_only_when_postgres_fence_is_unavailable():
    redis = _FakeRedis()

    class _UnavailableService:
        @asynccontextmanager
        async def derisk_submit_fence(self, *, client_order_id):
            raise GlobalStopError(
                "PostgreSQL STOP serialization is unavailable; de-risking may proceed unfenced"
            )
            yield None

    engine = _engine(redis, service=_UnavailableService())
    async with derisk_fence(
        engine,
        client_order_id="close-001",
        venue="okx",
        market_type="perpetual",
        instrument="BTC-USDT-SWAP",
        account_scope_id="acct-1",
    ) as lease:
        assert isinstance(lease, EmergencyDeriskLease)
        assert lease.client_order_id == "close-001"
    assert redis.values == {}


@pytest.mark.asyncio
async def test_derisk_fence_does_not_bypass_durable_permission_denial():
    redis = _FakeRedis()

    class _DeniedService:
        @asynccontextmanager
        async def derisk_submit_fence(self, *, client_order_id):
            raise GlobalStopError("DE_RISK_NOT_ALLOWED by durable risk scope")
            yield None

    engine = _engine(redis, service=_DeniedService())
    with pytest.raises(GlobalStopError, match="DE_RISK_NOT_ALLOWED"):
        async with derisk_fence(
            engine,
            client_order_id="close-001",
            venue="okx",
            market_type="perpetual",
            instrument="BTC-USDT-SWAP",
            account_scope_id="acct-1",
        ):
            pytest.fail("permission denial must not reach venue submit")
    assert redis.values == {}


@pytest.mark.asyncio
async def test_live_derisk_without_designated_owner_fails_closed():
    engine = _engine(_FakeRedis(), owner="worker-a", designated="")
    with pytest.raises(GlobalStopError, match="emergency de-risk ownership unavailable"):
        async with derisk_fence(
            engine,
            client_order_id="close-001",
            venue="okx",
            market_type="perpetual",
            instrument="BTC-USDT-SWAP",
            account_scope_id="acct-1",
        ):
            pytest.fail("unowned emergency mutation must not reach venue")
