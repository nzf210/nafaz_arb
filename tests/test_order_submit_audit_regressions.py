from types import SimpleNamespace

import pytest

from executor.derisk_fence import derisk_fence
from executor.entry_hedge import EntryHedgeMixin
from executor.global_stop import GlobalStopError, GlobalStopService
from executor.helpers import is_transient_settlement_error
from executor.rollback import RollbackMixin


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn
        self.closed = False

    def acquire(self):
        return _AsyncContext(self.conn)

    async def close(self):
        self.closed = True


class _SubmitConnection:
    def __init__(self, *, fail_unlock=False):
        self.control = {"is_stopped": False, "epoch": 4, "reason": None}
        self.attempt = None
        self.statements = []
        self.fail_unlock = fail_unlock

    def transaction(self):
        return _AsyncContext(self)

    async def execute(self, query, *args):
        self.statements.append(query)
        if "set status='SUBMITTING'" in query and self.attempt is not None:
            self.attempt["status"] = "SUBMITTING"
        if "pg_advisory_unlock" in query and self.fail_unlock:
            raise ConnectionResetError("postgres session dropped during unlock")
        return "UPDATE 1"

    async def fetchrow(self, query, *args):
        if "insert into public.executor_submit_attempts" in query:
            self.attempt = {
                "client_order_id": args[0],
                "exchange": args[1],
                "market_type": args[2],
                "symbol": args[3],
                "side": args[4],
                "order_type": args[5],
                "quantity": args[6],
                "price": args[7],
                "pair_key": args[8],
                "recovery_context": args[9],
                "amount_spec": args[10],
                "status": "PREPARED",
            }
            return dict(self.attempt)
        if "select is_stopped, epoch, reason" in query:
            return dict(self.control)
        if "where status in ('SUBMITTING','UNKNOWN')" in query:
            return None
        if "returning client_order_id" in query:
            if self.attempt and self.attempt["status"] == "PREPARED":
                self.attempt["status"] = "SUBMITTING"
                return {"client_order_id": self.attempt["client_order_id"]}
        return None


async def _prepare(service, client_order_id):
    await service.prepare_submit_attempt(
        client_order_id=client_order_id,
        exchange="okx",
        market_type="perpetual",
        symbol="BTC/USDT:USDT",
        side="sell",
        order_type="market",
        quantity=0.01,
        price=50000.0,
        pair_key="tokocrypto__okx__btc",
        recovery_context={"parent_spot_client_order_id": "spot-parent-1"},
        amount_spec={"unit": "BASE", "step": 0.001},
    )


def test_settlement_retry_predicate_excludes_ambiguous_transport_and_server_errors():
    assert is_transient_settlement_error(RuntimeError('code: 2206 "insufficient in matching engine"'))
    assert is_transient_settlement_error(RuntimeError('code: 989012 "insufficient available balance"'))

    ambiguous = (
        TimeoutError("venue request timed out"),
        ConnectionResetError("connection reset after POST"),
        RuntimeError("HTTP 503 service unavailable"),
        RuntimeError("internal error"),
        RuntimeError("system busy; please try again later"),
    )
    assert all(not is_transient_settlement_error(exc) for exc in ambiguous)


class _TimeoutSpotVenue:
    def __init__(self):
        self.calls = []

    async def place_local_market_sell(self, symbol, amount, **kwargs):
        self.calls.append((symbol, amount, kwargs.get("client_order_id")))
        raise TimeoutError("venue timed out after receiving POST")


class _RollbackHarness(RollbackMixin):
    def __init__(self, venue):
        self.global_stop = None
        self.config = SimpleNamespace(local_exchange_name="tokocrypto")
        self.client = venue
        self.order_intents = []

    def _next_order_seq(self):
        return 1

    async def _register_order_intent(self, *args, **kwargs):
        self.order_intents.append((args, kwargs))

    async def _sellable_spot_quantity(self, symbol, requested, amount_spec, legacy_amount_step):
        return requested


@pytest.mark.asyncio
async def test_rollback_market_sell_does_not_retry_ambiguous_timeout():
    venue = _TimeoutSpotVenue()
    engine = _RollbackHarness(venue)

    order, error = await engine._liquidate_unhedged_spot(
        "tokocrypto:btc_idr",
        "BTC",
        0.01,
        amount_spec={"unit": "BASE", "step": 0.001, "minimum": 0.001, "base_per_unit": 1},
        local_exchange="tokocrypto",
    )

    assert order is None
    assert isinstance(error, TimeoutError)
    assert len(venue.calls) == 1
    assert venue.calls[0][2]


class _PrepareFailureStop:
    async def prepare_submit_attempt(self, **kwargs):
        raise ConnectionError("PostgreSQL unavailable before futures POST")

    def order_submit_fence(self, *, client_order_id):
        raise AssertionError("submit fence must not be entered after PREPARED failure")


class _FuturesVenue:
    def __init__(self):
        self.submit_calls = 0
        self.lookup_calls = 0

    async def open_global_short_market(self, *args, **kwargs):
        self.submit_calls += 1
        raise AssertionError("futures venue must not be called")

    async def fetch_global_order_by_client_id(self, *args, **kwargs):
        self.lookup_calls += 1
        return None


class _HedgeHarness(EntryHedgeMixin):
    def __init__(self):
        self.global_stop = _PrepareFailureStop()
        self.config = SimpleNamespace(
            mock_mode=False,
            futures_min_notional=0.0,
            partial_fill_safety_policy="scale_to_min",
        )
        self.capabilities = None
        self.client = _FuturesVenue()
        self.intents = []

    async def _register_order_intent(self, *args, **kwargs):
        self.intents.append((args, kwargs))

    def _next_order_seq(self):
        return 9


@pytest.mark.asyncio
async def test_futures_hedge_prepare_failure_is_pre_submit_not_ambiguous():
    engine = _HedgeHarness()
    order_info = {
        "pending_hedge_client_order_id": "hedge-prepared-failure-1",
        "global_exchange": "okx",
        "global_symbol": "BTC/USDT:USDT",
        "global_amount_spec": {
            "unit": "BASE", "step": 0.001, "min": 0.001, "base_per_unit": 1
        },
        "local_exchange": "tokocrypto",
        "order_id": "spot-order-1",
        "client_order_id": "spot-client-1",
    }

    with pytest.raises(ConnectionError, match="before futures POST"):
        await engine._submit_entry_hedge_order(
            "tokocrypto:btc_idr", order_info, "BTC/USDT:USDT", 0.01, 50000.0
        )

    assert order_info["hedge_submit_started"] is False
    assert engine.client.submit_calls == 0
    assert engine.client.lookup_calls == 0


@pytest.mark.asyncio
async def test_derisking_is_allowed_while_global_stop_is_active_and_submit_is_unknown():
    conn = _SubmitConnection()
    conn.control = {"is_stopped": True, "epoch": 8, "reason": "operator stop"}

    class _UnresolvedConnection(_SubmitConnection):
        async def fetchrow(self, query, *args):
            if "where status in ('SUBMITTING','UNKNOWN')" in query:
                return {
                    "client_order_id": "unknown-entry-1",
                    "exchange": "okx",
                    "symbol": "BTC/USDT:USDT",
                    "status": "UNKNOWN",
                }
            return await super().fetchrow(query, *args)

    conn = _UnresolvedConnection()
    conn.control = {"is_stopped": True, "epoch": 8, "reason": "operator stop"}
    service = GlobalStopService("postgresql://unused")
    service.pool = _Pool(conn)

    async with service.derisk_submit_fence(client_order_id="exit-client-1") as fenced:
        assert fenced is conn

    assert any("pg_advisory_lock" in query for query in conn.statements)
    assert any("pg_advisory_unlock" in query for query in conn.statements)


@pytest.mark.asyncio
async def test_derisking_falls_back_unfenced_when_postgres_is_unavailable():
    engine = SimpleNamespace(global_stop=GlobalStopService(""))
    entered = False

    async with derisk_fence(engine, client_order_id="force-close-client-1") as conn:
        entered = True
        assert conn is None

    assert entered


@pytest.mark.asyncio
async def test_venue_timeout_inside_submit_fence_does_not_demote_postgres_pool():
    conn = _SubmitConnection()
    pool = _Pool(conn)
    service = GlobalStopService("postgresql://unused")
    service.pool = pool
    await _prepare(service, "hedge-timeout-1")

    with pytest.raises(TimeoutError, match="venue timeout"):
        async with service.order_submit_fence(client_order_id="hedge-timeout-1"):
            raise TimeoutError("venue timeout after POST")

    assert service.pool is pool
    assert conn.attempt["status"] == "SUBMITTING"


@pytest.mark.asyncio
async def test_postgres_disconnect_at_submit_fence_release_demotes_pool_but_keeps_ledger():
    conn = _SubmitConnection(fail_unlock=True)
    pool = _Pool(conn)
    service = GlobalStopService("postgresql://unused")
    service.pool = pool
    await _prepare(service, "hedge-pg-disconnect-1")

    async with service.order_submit_fence(client_order_id="hedge-pg-disconnect-1"):
        assert conn.attempt["status"] == "SUBMITTING"

    assert service.pool is None
    assert conn.attempt["status"] == "SUBMITTING"
