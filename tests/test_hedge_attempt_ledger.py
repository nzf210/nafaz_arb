from decimal import Decimal

import pytest

from executor.hedge_attempt_ledger import (
    _ACTIVE,
    _client_order_id,
    _decimal,
)


def test_attempt_client_order_ids_are_stable_and_distinct_per_attempt():
    first = _client_order_id("indodax:spot-123:100000000", 1)
    replay = _client_order_id("indodax:spot-123:100000000", 1)
    second = _client_order_id("indodax:spot-123:100000000", 2)

    assert first == replay
    assert first != second
    assert first.startswith("hda")
    assert len(first) <= 32


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "0", None, "not-a-number"])
def test_attempt_quantities_reject_non_positive_or_non_finite_values(value):
    with pytest.raises(ValueError):
        _decimal(value, "quantity")


def test_attempt_fill_fee_allows_zero_but_quantity_does_not():
    assert _decimal("0", "fee", allow_zero=True) == Decimal("0")
    assert _decimal("0.001", "quantity") == Decimal("0.001")


def test_unresolved_attempt_states_block_a_new_attempt():
    assert {
        "PREPARED", "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED", "OPEN",
        "PARTIALLY_FILLED", "RECONCILIATION_REQUIRED",
    } == set(_ACTIVE)
    assert "CANCELED" not in _ACTIVE
    assert "REJECTED" not in _ACTIVE

class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _RecoveryConn:
    def __init__(self, rows):
        self.rows = rows

    def transaction(self):
        return _Transaction()

    async def fetch(self, query, *args):
        return self.rows

    async def fetchrow(self, query, *args):
        if "set status='ACKNOWLEDGED'" in query:
            row = next(r for r in self.rows if r["attempt_id"] == args[0])
            if row["version"] != args[2]:
                return None
            row["status"] = "ACKNOWLEDGED"
            row["venue_order_id"] = args[1]
            row["version"] += 1
            return {"version": row["version"]}
        if "set status=$4" in query:
            row = next(r for r in self.rows if r["attempt_id"] == args[0])
            if row["version"] != args[1] or row["status"] != args[2]:
                return None
            row["status"] = args[3]
            row["version"] += 1
            return {"version": row["version"]}
        raise AssertionError(query)

    async def execute(self, query, *args):
        return "INSERT 0 1"


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class _GlobalStop:
    enabled = True

    def __init__(self, pool):
        self.pool = pool

    async def ensure_pool(self):
        return True


@pytest.mark.asyncio
async def test_startup_recovery_adopts_prepared_attempt_with_exact_venue_order():
    from types import SimpleNamespace

    from executor.hedge_attempt_ledger import HedgeAttemptLedger

    rows = [{
        "attempt_id": "attempt-1",
        "hedge_client_order_id": "hedge-coid-1",
        "futures_exchange": "okx",
        "status": "PREPARED",
        "version": 0,
    }]
    conn = _RecoveryConn(rows)

    class Registry:
        async def find_by_client_id(self, exchange, client_order_id):
            assert exchange == "okx"
            assert client_order_id == "hedge-coid-1"
            return SimpleNamespace(exchange_order_id="venue-order-1")

    ledger = HedgeAttemptLedger(global_stop=_GlobalStop(_Pool(conn)))

    assert await ledger.recover_unresolved_attempts(Registry()) is True
    assert rows[0]["status"] == "ACKNOWLEDGED"
    assert rows[0]["venue_order_id"] == "venue-order-1"


@pytest.mark.asyncio
async def test_startup_recovery_rejects_prepared_attempt_only_after_absence_proof():
    from executor.hedge_attempt_ledger import HedgeAttemptLedger

    rows = [{
        "attempt_id": "attempt-2",
        "hedge_client_order_id": "hedge-coid-2",
        "futures_exchange": "okx",
        "status": "SUBMITTING",
        "version": 1,
    }]
    conn = _RecoveryConn(rows)

    class Registry:
        async def find_by_client_id(self, exchange, client_order_id):
            return None

    ledger = HedgeAttemptLedger(global_stop=_GlobalStop(_Pool(conn)))

    assert await ledger.recover_unresolved_attempts(Registry()) is True
    assert rows[0]["status"] == "REJECTED"
