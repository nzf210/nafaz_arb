import pytest

from executor.engine_entry_submission import EngineEntrySubmissionMixin
from executor.global_stop import FENCE_LOCK_KEY, GlobalStopError, GlobalStopService
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
        self.attempts = {}
        self.control = {"is_stopped": False, "epoch": 12, "reason": None}

    def transaction(self):
        return _AsyncContext(self)

    async def execute(self, query, *args):
        self.executed.append((query, args))
        if "set status='SUBMITTING'" in query:
            client_order_id = args[0]
            row = self.attempts[client_order_id]
            row["status"] = "SUBMITTING"
            row["fence_epoch"] = args[1]
        elif "set status=$2" in query:
            row = self.attempts[args[0]]
            row["status"] = args[1]

    async def fetchrow(self, query, *args):
        self.fetches.append((query, args))
        if "select is_stopped, epoch, reason" in query:
            return dict(self.control)
        if "update public.executor_global_control" in query:
            self.control["is_stopped"] = True
            self.control["epoch"] += 1
            return {
                "is_stopped": True, "epoch": self.control["epoch"],
                "reason": args[0], "source": args[1], "metadata": args[2],
            }
        if "insert into public.executor_submit_attempts" in query:
            client_order_id = args[0]
            self.attempts.setdefault(client_order_id, {
                "client_order_id": client_order_id,
                "exchange": args[1],
                "symbol": args[3],
                "status": "PREPARED",
            })
            return dict(self.attempts[client_order_id])
        if "where status in ('SUBMITTING','UNKNOWN')" in query:
            current_id = args[0]
            return next(
                (dict(row) for key, row in self.attempts.items()
                 if key != current_id and row["status"] in ("SUBMITTING", "UNKNOWN")),
                None,
            )
        if "update public.executor_submit_attempts" in query and "returning client_order_id" in query:
            row = self.attempts.get(args[0])
            if row and row["status"] == "PREPARED":
                row["status"] = "SUBMITTING"
                row["fence_epoch"] = args[1]
                return {"client_order_id": args[0]}
            return None
        if "where client_order_id = $1" in query:
            row = self.attempts.get(args[0])
            return dict(row) if row else None
        return None


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AsyncContext(self.conn)


async def _prepare(service, client_order_id):
    await service.prepare_submit_attempt(
        client_order_id=client_order_id,
        exchange="binance",
        market_type="spot",
        symbol="BTC/USDT",
        side="buy",
        order_type="limit",
        quantity=0.01,
        price=50000,
        pair_key="binance__okx__btc",
        recovery_context={"test": True},
        amount_spec={"unit": "BASE"},
    )


@pytest.mark.asyncio
async def test_submit_fence_commits_durable_submitting_before_http_scope():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)
    await _prepare(service, "stable-client-order-id")

    async with service.order_submit_fence(client_order_id="stable-client-order-id") as fenced_conn:
        assert fenced_conn is conn
        assert conn.attempts["stable-client-order-id"]["status"] == "SUBMITTING"

    statements = [query for query, _ in conn.executed]
    assert any("pg_advisory_lock" in query for query in statements)
    assert any("pg_advisory_unlock" in query for query in statements)
    assert conn.attempts["stable-client-order-id"]["fence_epoch"] == 12


@pytest.mark.asyncio
async def test_lost_submit_connection_leaves_durable_row_blocking_next_entry():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)
    await _prepare(service, "possibly-accepted-order")
    with pytest.raises(ConnectionError):
        async with service.order_submit_fence(client_order_id="possibly-accepted-order"):
            raise ConnectionError("connection dropped after request boundary")

    assert conn.attempts["possibly-accepted-order"]["status"] == "SUBMITTING"
    await _prepare(service, "next-order")
    with pytest.raises(GlobalStopError, match="unresolved in-flight submit"):
        async with service.order_submit_fence(client_order_id="next-order"):
            pytest.fail("second venue request must not be entered")


@pytest.mark.asyncio
async def test_ip_ban_stop_can_be_written_inside_submit_fence():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")

    state = await service.request_stop_in_transaction(
        conn,
        "CEX_IP_BLOCK",
        source="exchange:binance",
        metadata={"http_status": 418},
    )

    # The transaction-level STOP helper is tested via its UPDATE statement.
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
