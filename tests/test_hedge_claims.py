"""Cross-instance hedge claim tests.

Simulates the PostgreSQL CAS (INSERT .. ON CONFLICT DO NOTHING RETURNING)
with an in-memory table that enforces the same uniqueness semantics, then
proves two executor instances sharing one database can never both claim the
same hedge generation, and that a crashed worker's claim blocks re-claiming.
"""

import pytest

from executor.hedge_claims import (
    HedgeIntentClaimService,
    hedge_claim_key,
    hedge_client_order_id_for_claim,
)


class _FakeConn:
    """Mimics asyncpg connection CAS semantics for the claim insert."""

    def __init__(self, table):
        self.table = table

    def transaction(self):
        return _FakeTransaction()

    async def fetchrow(self, query, *args):
        if "select claim_key, status" in query:
            return next((
                dict(row) for row in self.table.values()
                if row.get("parent_spot_exchange") == args[0]
                and row.get("parent_spot_order_id") == args[1]
                and row.get("status") in ("CLAIMED", "SUBMITTED")
            ), None)
        if "on conflict (claim_key) do nothing" in query:
            key = args[0]
            if key in self.table:
                return None  # conflict: another worker holds the generation
            self.table[key] = {
                "claim_key": key,
                "parent_spot_exchange": args[1],
                "parent_spot_order_id": args[2],
                "hedge_client_order_id": args[10],
                "version": 0,
                "status": "CLAIMED",
                "residual_qty": float(args[6]),
            }
            return self.table[key]
        raise AssertionError(f"unexpected query: {query[:80]}")

    async def execute(self, query, *args):
        if "set status = 'ABANDONED'" in query:
            row = self.table.get(args[0])
            if row is None or row["status"] != "CLAIMED":
                return "UPDATE 0"
            row["status"] = "ABANDONED"
            row["version"] = int(row["version"]) + 1
            return "UPDATE 1"
        if "set status = 'CONFIRMED'" in query:
            row = self.table.get(args[0])
            if row is None or row["status"] not in ("CLAIMED", "SUBMITTED"):
                return "UPDATE 0"
            row["status"] = "CONFIRMED"
            row["confirmed_qty"] = max(
                float(row.get("confirmed_qty") or 0.0), float(args[1])
            )
            row["version"] = int(row["version"]) + 1
            return "UPDATE 1"
        if "set status = $2" in query:
            row = self.table.get(args[0])
            if row is None or row["status"] != "CLAIMED":
                return "UPDATE 0"
            row["status"] = args[1]
            row["version"] = int(row["version"]) + 1
            return "UPDATE 1"
        raise AssertionError(f"unexpected query: {query[:80]}")


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, table):
        self._table = table

    def acquire(self):
        return _FakeAcquire(self._table)


class _FakeAcquire:
    def __init__(self, table):
        self._table = table

    async def __aenter__(self):
        return _FakeConn(self._table)

    async def __aexit__(self, *exc):
        return False


class _FakeStop:
    """Minimal GlobalStopService stand-in with a shared pool."""

    def __init__(self, pool):
        self.pool = pool
        self.enabled = True

    async def ensure_pool(self):
        return True


def _service(table):
    return HedgeIntentClaimService(global_stop=_FakeStop(_FakePool(table)))


def _claim_kwargs(residual=0.004):
    return dict(
        parent_spot_exchange="indodax",
        parent_spot_order_id="889",
        parent_spot_client_order_id="coid-889",
        cumulative_spot_filled=0.004,
        target_hedge_qty=0.004,
        residual_qty=residual,
        hedge_ratio=1.0,
        futures_exchange="binance",
        futures_symbol="BTCUSDT",
        claimed_by="executor:test",
    )


@pytest.mark.asyncio
async def test_only_one_instance_claims_same_generation():
    table = {}
    worker_a = _service(table)
    worker_b = _service(table)

    claim_a = await worker_a.claim_generation(**_claim_kwargs())
    assert claim_a is not None
    claim_b = await worker_b.claim_generation(**_claim_kwargs())
    assert claim_b is None, "second worker must not claim the same generation"

    # Both workers derive the same deterministic client order id for the
    # generation, so even a buggy duplicate submit reuses one venue id.
    key = hedge_claim_key("indodax", "889", int(0.004 * 1e9))
    assert claim_a["hedge_client_order_id"] == hedge_client_order_id_for_claim(key)


@pytest.mark.asyncio
async def test_new_fill_generation_waits_for_previous_claim_resolution():
    table = {}
    worker = _service(table)

    first = await worker.claim_generation(**_claim_kwargs())
    assert first is not None
    # A later cumulative fill cannot claim while the earlier generation is
    # unresolved; otherwise both residuals could be submitted concurrently.
    later_kwargs = {**_claim_kwargs(), "cumulative_spot_filled": 0.006}
    assert await worker.claim_generation(**later_kwargs) is None

    assert await worker.mark_confirmed(first["claim_key"], 0.004)
    second = await worker.claim_generation(**later_kwargs)
    assert second is not None
    assert second["claim_key"] != first["claim_key"]


@pytest.mark.asyncio
async def test_abandoned_pre_submit_claim_allows_later_fill_generation():
    table = {}
    worker = _service(table)

    first = await worker.claim_generation(**_claim_kwargs())
    assert first is not None
    assert await worker.mark_abandoned(first["claim_key"], "minimum notional")

    second = await worker.claim_generation(**{
        **_claim_kwargs(), "cumulative_spot_filled": 0.006,
    })
    assert second is not None
    assert second["claim_key"] != first["claim_key"]
    assert table[first["claim_key"]]["status"] == "ABANDONED"


@pytest.mark.asyncio
async def test_confirmed_claim_cannot_be_readvanced():
    table = {}
    worker = _service(table)
    claim = await worker.claim_generation(**_claim_kwargs())
    assert claim is not None

    assert await worker.mark_confirmed(claim["claim_key"], 0.004)
    # A replayed confirm must not double-count.
    assert not await worker.mark_confirmed(claim["claim_key"], 0.004)
    row = table[claim["claim_key"]]
    assert row["confirmed_qty"] == pytest.approx(0.004)


@pytest.mark.asyncio
async def test_db_unavailable_fails_closed():
    # No pool configured: claim returns None -> caller must not submit.
    service = HedgeIntentClaimService(global_stop=None)
    assert service.enabled is False
    assert await service.claim_generation(**_claim_kwargs()) is None


@pytest.mark.asyncio
async def test_zero_residual_is_never_claimed():
    table = {}
    worker = _service(table)
    assert await worker.claim_generation(**_claim_kwargs(residual=0.0)) is None
    assert table == {}


@pytest.mark.asyncio
async def test_two_full_executors_same_spot_order_single_intent():
    """Two fully independent executor instances (separate service objects,
    shared DB table) processing the SAME spot order: only one hedge intent
    claim wins; the loser must not submit."""
    table = {}
    executor_a = _service(table)
    executor_b = _service(table)

    kwargs = _claim_kwargs()
    claim_a = await executor_a.claim_generation(**kwargs)
    assert claim_a is not None
    # Executor B recomputes the same residual from the same spot fill and
    # attempts its own claim: CAS must reject it.
    claim_b = await executor_b.claim_generation(**kwargs)
    assert claim_b is None

    # Only executor A may advance the claim to SUBMITTED.
    assert await executor_a.mark_submitted(claim_a["claim_key"])
    # B cannot advance a claim it does not hold (already past CLAIMED).
    assert not await executor_b.mark_submitted(claim_a["claim_key"])

    # Exactly one claim row exists for the generation.
    assert len(table) == 1
    row = next(iter(table.values()))
    assert row["status"] == "SUBMITTED"
    assert row["hedge_client_order_id"] == claim_a["hedge_client_order_id"]


@pytest.mark.asyncio
async def test_crashed_worker_claim_blocks_until_reconciled():
    """A worker that crashes after claiming (before CONFIRMED) leaves the
    generation claimed: a restarted worker cannot double-claim it."""
    table = {}
    crashed = _service(table)
    claim = await crashed.claim_generation(**_claim_kwargs())
    assert claim is not None
    # Simulate crash: no mark_submitted / mark_confirmed ever ran.

    restarted = _service(table)
    assert await restarted.claim_generation(**_claim_kwargs()) is None, (
        "restart must not re-claim a generation held by a crashed worker; "
        "reconciliation must resolve it first"
    )
    # The claim stays in CLAIMED state, visible for reconciliation.
    row = table[claim["claim_key"]]
    assert row["status"] == "CLAIMED"
