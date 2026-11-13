import pytest

from executor.engine_entry_submission import EngineEntrySubmissionMixin
from executor.global_stop import FENCE_LOCK_KEY, GlobalStopService
from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self):
        self.executed = []
        self.fetches = []

    def transaction(self):
        return _AsyncContext(self)

    async def execute(self, query, *args):
        self.executed.append((query, args))

    async def fetchrow(self, query, *args):
        self.fetches.append((query, args))
        if "select is_stopped, epoch, reason" in query:
            return {"is_stopped": False, "epoch": 12, "reason": None}
        return {
            "is_stopped": True,
            "epoch": 13,
            "reason": args[0],
            "source": args[1],
            "metadata": args[2],
        }


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AsyncContext(self.conn)


@pytest.mark.asyncio
async def test_order_submit_fence_uses_transaction_advisory_lock_and_reads_stop():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)

    async with service.order_submit_fence(client_order_id="stable-client-order-id") as fenced_conn:
        assert fenced_conn is conn

    assert conn.executed
    assert "pg_advisory_xact_lock" in conn.executed[0][0]
    assert conn.executed[0][1] == (FENCE_LOCK_KEY,)


@pytest.mark.asyncio
async def test_ip_ban_stop_can_be_written_inside_submit_transaction():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")

    state = await service.request_stop_in_transaction(
        conn,
        "CEX_IP_BLOCK",
        source="exchange:binance",
        metadata={"http_status": 418},
    )

    assert state["is_stopped"] is True
    assert state["epoch"] == 13
    query, args = conn.fetches[0]
    assert "epoch = epoch + case when is_stopped then 0 else 1 end" in query
    assert args[0] == "CEX_IP_BLOCK"
    assert args[1] == "exchange:binance"


def test_only_explicit_ip_ban_signatures_escalate_to_global_stop():
    assert EngineEntrySubmissionMixin._is_explicit_ip_ban("ccxt error http_418 banned until")
    assert EngineEntrySubmissionMixin._is_explicit_ip_ban("ip address banned by venue")
    assert not EngineEntrySubmissionMixin._is_explicit_ip_ban("http_418 generic teapot response")
    assert not EngineEntrySubmissionMixin._is_explicit_ip_ban("http_429 too many requests")
    assert not EngineEntrySubmissionMixin._is_explicit_ip_ban("http_503 service unavailable")


@pytest.mark.asyncio
async def test_live_executor_fails_closed_without_postgres_fencing_dsn():
    engine = ArbitrageEngine(config=ExecutorConfig(mock_mode=False), client=object())

    with pytest.raises(RuntimeError, match="requires POSTGRES_DSN"):
        await engine.initialize()
