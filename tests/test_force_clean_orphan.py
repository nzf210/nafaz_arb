"""Tests for force_clean_orphan and Tokocrypto archive type=-1 lookup."""
import json
import pytest
from fakeredis.aioredis import FakeRedis

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine
from executor.exchange_common import _order_archive_page_reader
from executor.models import OrderRecord, OrderRole, PositionState, RecoveryStatus
from executor.supabase_outbox import DurableOutbox


class DummyClient:
    id = "tokocrypto"
    async def fetch_open_orders(self, symbol=None):
        return []
    async def fetch_positions(self):
        return []
    async def close(self):
        pass


class DummySupabase:
    def __init__(self, outbox=None):
        self.enabled = True
        self.url = ""
        self.key = ""
        self.outbox = outbox
        self.deleted_targets = []
        self.deleted_pairs = []

    def order_record(self, row):
        if self.outbox is not None:
            self.outbox.enqueue("order_registry", "local_order_id", row)
        return True

    def event(self, *args, **kwargs):
        pass

    def risk_state(self, *args, **kwargs):
        return True

    def mark_risk_persistence_failure(self):
        pass

    async def fetch_open_order_records(self, limit=500):
        return []

    async def force_delete_order_artifacts(self, target_ids, pair_keys=None):
        self.deleted_targets.extend(target_ids)
        self.deleted_pairs.extend(pair_keys or [])
        outbox_del = 0
        if self.outbox is not None:
            outbox_del = self.outbox.delete_matching(set(target_ids))
        return {
            "deleted_db": len(target_ids),
            "deleted_outbox": outbox_del,
            "pair_keys": list(pair_keys or []),
            "resolved_ids": list(target_ids),
        }


@pytest.mark.asyncio
async def test_force_clean_orphan_clears_redis_memory_outbox_and_halts():
    redis = FakeRedis(decode_responses=True)
    outbox = DurableOutbox(":memory:")
    supa = DummySupabase(outbox=outbox)
    cfg = ExecutorConfig(mock_mode=False)
    engine = ArbitrageEngine(cfg, DummyClient(), redis_client=redis)
    engine.supabase = supa
    engine.order_registry.supabase = supa

    pair_key = "tokocrypto:toko:soon_idr__okx:soon_usdt_swap"
    rec = OrderRecord(
        local_order_id="c7c9c437-00b6-5cf2-8a9a-ef6e6e2cf1c8",
        client_order_id="tokocrypto-9de55875-entry-794488618",
        exchange="tokocrypto",
        market_type="spot",
        symbol="toko:SOON_IDR",
        side="buy",
        order_type="limit",
        order_role=OrderRole.ENTRY.value,
        pair_key=pair_key,
        price=4201.0,
        original_quantity=48.0,
        status="submitting",
        recovery_status=RecoveryStatus.UNVERIFIED.value,
    )
    await engine.order_registry.register(rec)
    await engine.order_registry.mark_pending_recovery(rec.local_order_id, "ambiguous")
    engine._recovered_order_records[rec.local_order_id] = rec
    engine._halt_exposure_scope(pair_key, "order submission acknowledgement is unresolved")
    engine.states[pair_key] = PositionState.RECONCILIATION_REQUIRED
    engine._pause_pair_for_cooldown(pair_key, 86400.0, reason="blacklisted")

    assert outbox.pending_count() == 1
    assert await redis.get(f"arb:order:{rec.local_order_id}") is not None

    res = await engine.force_clean_orphan(rec.local_order_id)

    assert res["cleaned"] is True
    assert res["deleted_redis"] > 0
    assert res["deleted_db"] > 0
    assert pair_key in res["cleared_pairs"]
    assert await redis.get(f"arb:order:{rec.local_order_id}") is None
    assert await redis.hget("arb:order_pending_recovery", rec.local_order_id) is None
    assert await redis.hget("arb:order_recovery_idx", rec.local_order_id) is None
    assert outbox.pending_count() == 0
    assert pair_key not in engine._recovery_scope_halts
    assert pair_key not in engine._pair_halted
    assert engine.states[pair_key] == PositionState.IDLE


@pytest.mark.asyncio
async def test_tokocrypto_order_archive_reader_passes_type_minus_one():
    captured_requests = []

    class FakeTokocrypto:
        id = "tokocrypto"
        def market(self, sym):
            return {"id": "SOON_IDR"}
        async def privateGetOpenV1Orders(self, req):
            captured_requests.append(dict(req))
            return {"code": 0, "data": {"list": []}}

    page = _order_archive_page_reader(FakeTokocrypto(), "SOON/IDR")
    assert page is not None
    rows, done = await page()
    assert rows == []
    assert done is True
    assert len(captured_requests) == 1
    assert captured_requests[0]["type"] == -1
    assert captured_requests[0]["symbol"] == "SOON_IDR"
