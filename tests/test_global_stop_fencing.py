import pytest
from types import SimpleNamespace

from executor.engine_entry_submission import EngineEntrySubmissionMixin
from executor.global_stop import FENCE_LOCK_KEY, GlobalStopError, GlobalStopService
from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine
from executor.engine_recovery import EngineRecoveryMixin
from executor.entry_hedge import AmbiguousOrderSubmission, EntryHedgeMixin
from executor.engine_entry_submission import EngineEntrySubmissionMixin


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
            if client_order_id in self.attempts:
                return None
            self.attempts[client_order_id] = {
                "client_order_id": client_order_id,
                "exchange": args[1],
                "market_type": args[2],
                "symbol": args[3],
                "side": args[4],
                "order_type": args[5],
                "quantity": args[6],
                "price": args[7],
                "pair_key": args[8],
                "status": "PREPARED",
                "amount_spec": args[10],
            }
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
async def test_prepared_client_id_reuse_rejects_changed_amount_spec():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)
    await _prepare(service, "stable-intent-id")

    with pytest.raises(GlobalStopError, match="different order intent"):
        await service.prepare_submit_attempt(
            client_order_id="stable-intent-id",
            exchange="binance",
            market_type="spot",
            symbol="BTC/USDT",
            side="buy",
            order_type="limit",
            quantity=0.01,
            price=50000,
            pair_key="binance__okx__btc",
            recovery_context={"test": True},
            amount_spec={"unit": "CONTRACTS", "contract_size": 0.001},
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


class _FakeSpotStop:
    def __init__(self):
        self.prepared = []
        self.unknown = []
        self.stop_attempts = []

    async def prepare_submit_attempt(self, **kwargs):
        self.prepared.append(kwargs)

    def order_submit_fence(self, *, client_order_id):
        return _AsyncContext(object())

    async def request_stop_in_transaction(self, conn, reason, *, source, metadata):
        self.stop_attempts.append((reason, source, metadata))
        raise ConnectionError("postgres disconnected while persisting CEX_IP_BLOCK")

    async def mark_submit_unknown(self, client_order_id, error):
        self.unknown.append((client_order_id, error))


class _SpotIPRestrictedVenue:
    def __init__(self):
        self.lookup_calls = 0

    async def place_local_limit_buy(self, *args, **kwargs):
        raise RuntimeError("IP restricted by venue")

    async def fetch_local_order_by_client_id(self, *args, **kwargs):
        self.lookup_calls += 1
        return {"id": "venue-order-that-must-not-release-fence", "status": "open"}


class _SpotSubmissionHarness(EngineEntrySubmissionMixin):
    def __init__(self, stop, venue):
        self.global_stop = stop
        self.client = venue
        self.halts = []
        self.gate_refreshed = False
        self.ambiguous = []

    def _next_order_seq(self):
        return 17

    def _local_order_limit_price(self, pair_info, snapshot, strategy_price):
        return strategy_price

    def _entry_recovery_context(self, *args):
        return {"test": "spot-ip-ban"}

    async def _register_order_intent(self, *args, **kwargs):
        return None

    def _entry_error_text(self, error):
        value = str(error)
        return value, value.lower()

    def _halt_exposure_scope(self, scope, reason):
        self.halts.append((scope, reason))

    def _refresh_entry_gate(self):
        self.gate_refreshed = True

    async def _mark_order_submission_ambiguous(self, *args):
        self.ambiguous.append(args)


class _FakeAmountSpec:
    def to_dict(self):
        return {"unit": "BASE", "step": 0.001}


@pytest.mark.asyncio
async def test_spot_ip_ban_stop_write_failure_never_uses_lookup_to_acknowledge():
    stop = _FakeSpotStop()
    venue = _SpotIPRestrictedVenue()
    engine = _SpotSubmissionHarness(stop, venue)
    pair_info = SimpleNamespace(
        local_exchange="binance",
        local_symbol="BTC/USDT",
        local_amount_spec=_FakeAmountSpec(),
        global_amount_spec={"unit": "BASE", "step": 0.001},
        quote_asset="USDT",
    )

    await engine._submit_entry_order(
        "pair-spot-1", {}, pair_info, {}, {}, 0.01, 500.0, 50000.0
    )

    assert stop.stop_attempts
    assert stop.unknown
    assert venue.lookup_calls == 0
    assert engine.ambiguous
    assert engine.halts and engine.gate_refreshed


class _FakeHedgeStop:
    def __init__(self, fail_stop=False):
        self.conn = object()
        self.fail_stop = fail_stop
        self.prepared = []
        self.stop_requests = []
        self.unknown = []
        self.rejected = []
        self.notifications = []

    async def prepare_submit_attempt(self, **kwargs):
        self.prepared.append(kwargs)

    def order_submit_fence(self, *, client_order_id):
        return _AsyncContext(self.conn)

    async def request_stop_in_transaction(self, conn, reason, *, source, metadata):
        self.stop_requests.append((conn, reason, source, metadata))
        if self.fail_stop:
            raise ConnectionError("postgres disconnected during STOP persistence")
        return {"is_stopped": True, "epoch": 9, "reason": reason, "source": source}

    async def mark_submit_unknown(self, client_order_id, error):
        self.unknown.append((client_order_id, error))

    async def mark_submit_rejected(self, client_order_id, error):
        self.rejected.append((client_order_id, error))

    async def mark_submit_acknowledged(self, client_order_id, order):
        raise AssertionError("IP-ban fixture must not acknowledge an order")

    async def notify_state(self, state):
        self.notifications.append(state)


class _IPBannedFuturesVenue:
    def __init__(self):
        self.lookup_calls = 0

    async def open_global_short_market(self, *args, **kwargs):
        raise RuntimeError("HTTP_418: IP address banned until reset")

    async def fetch_global_order_by_client_id(self, *args, **kwargs):
        self.lookup_calls += 1
        return None


class _HedgeHarness(EntryHedgeMixin):
    def __init__(self, global_stop):
        self.global_stop = global_stop
        self.client = _IPBannedFuturesVenue()
        self.halts = []
        self.gate_refreshed = False
        self.order_intents = []

    async def _register_order_intent(self, *args, **kwargs):
        self.order_intents.append((args, kwargs))

    def _is_explicit_ip_ban(self, error_text):
        return EngineEntrySubmissionMixin._is_explicit_ip_ban(error_text)

    def _is_definitive_order_rejection(self, error):
        return False

    def _halt_exposure_scope(self, scope, reason):
        self.halts.append((scope, reason))

    def _refresh_entry_gate(self):
        self.gate_refreshed = True


@pytest.mark.asyncio
async def test_futures_hedge_ip_ban_persists_global_stop_inside_submit_fence():
    stop = _FakeHedgeStop()
    engine = _HedgeHarness(stop)
    order_info = {
        "pending_hedge_client_order_id": "hedge-ip-ban-1",
        "global_exchange": "okx",
        "global_symbol": "BTC/USDT:USDT",
        "global_amount_spec": {
            "unit": "BASE", "step": 0.001, "min": 0.001, "base_per_unit": 1
        },
        "local_exchange": "tokocrypto",
        "order_id": "spot-123",
        "client_order_id": "spot-client-123",
    }

    with pytest.raises(AmbiguousOrderSubmission):
        await engine._submit_entry_hedge_order(
            "pair-1", order_info, "BTC/USDT:USDT", 0.01, 50000
        )

    assert stop.stop_requests
    conn, reason, source, metadata = stop.stop_requests[0]
    assert conn is stop.conn
    assert (reason, source) == ("CEX_IP_BLOCK", "exchange:okx")
    assert metadata["exchange"] == "okx"
    assert stop.unknown and not stop.rejected
    assert stop.notifications and stop.notifications[0]["is_stopped"] is True
    assert engine.halts and engine.gate_refreshed


@pytest.mark.asyncio
async def test_futures_hedge_ip_ban_keeps_attempt_unresolved_if_stop_write_fails():
    stop = _FakeHedgeStop(fail_stop=True)
    engine = _HedgeHarness(stop)
    order_info = {
        "pending_hedge_client_order_id": "hedge-ip-ban-db-down",
        "global_exchange": "okx",
        "global_symbol": "BTC/USDT:USDT",
        "global_amount_spec": {
            "unit": "BASE", "step": 0.001, "min": 0.001, "base_per_unit": 1
        },
        "local_exchange": "tokocrypto",
        "order_id": "spot-456",
        "client_order_id": "spot-client-456",
    }

    with pytest.raises(AmbiguousOrderSubmission):
        await engine._submit_entry_hedge_order(
            "pair-2", order_info, "BTC/USDT:USDT", 0.01, 50000
        )

    assert stop.stop_requests
    assert stop.unknown and not stop.rejected
    assert not stop.notifications
    assert engine.client.lookup_calls == 0


class _VenueLookup:
    def __init__(self, result):
        self.result = result
        self.calls = []
        self.futures_calls = []

    async def fetch_local_order_by_client_id(self, client_order_id, symbol, amount_spec):
        self.calls.append((client_order_id, symbol, amount_spec))
        return self.result

    async def fetch_global_order_by_client_id(self, client_order_id, symbol, amount_spec):
        self.futures_calls.append((client_order_id, symbol, amount_spec))
        return self.result


class _BaseClient:
    def __init__(self, venue):
        self.venue = venue
        self.requested_exchange = None

    def for_venue(self, exchange):
        self.requested_exchange = exchange
        return self.venue


class _SubmitLedger:
    def __init__(self, attempts):
        self.attempts = list(attempts)
        self.found = []
        self.not_found = []

    async def list_unresolved_submit_attempts(self, *, limit=100):
        return list(self.attempts)

    async def reconcile_submit_attempt_found(self, client_order_id, order):
        self.found.append((client_order_id, order))
        self.attempts = [row for row in self.attempts if row["client_order_id"] != client_order_id]

    async def reconcile_submit_attempt_not_found(self, client_order_id, *, evidence):
        self.not_found.append((client_order_id, evidence))


class _RecoveryHarness(EngineRecoveryMixin):
    def __init__(self, service, base_client):
        self.global_stop = service
        self._base_client = base_client
        self.halts = []
        self.gate_refreshed = False
        self.failures = []

    def _halt_exposure_scope(self, scope, reason):
        self.halts.append((scope, reason))

    def _refresh_entry_gate(self):
        self.gate_refreshed = True

    async def _publish_execution_failure(self, *args, **kwargs):
        self.failures.append((args, kwargs))


@pytest.mark.asyncio
async def test_startup_reconciliation_adopts_order_found_by_exact_client_id():
    attempt = {
        "client_order_id": "client-id-123",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    order = {"id": "venue-order-99", "status": "open"}
    venue = _VenueLookup(order)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))

    assert await engine._reconcile_durable_submit_attempts() is True
    assert venue.calls == [("client-id-123", "BTC/USDT", {"unit": "BASE"})]
    assert engine._base_client.requested_exchange == "binance"
    assert service.found == [("client-id-123", order)]


@pytest.mark.asyncio
async def test_startup_reconciliation_routes_perpetual_attempt_to_futures_lookup():
    attempt = {
        "client_order_id": "futures-client-id-456",
        "exchange": "okx",
        "market_type": "perpetual",
        "symbol": "BTC/USDT:USDT",
        "amount_spec": {"unit": "CONTRACTS", "contract_size": 0.001},
    }
    order = {"id": "futures-order-77", "status": "closed"}
    venue = _VenueLookup(order)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))

    assert await engine._reconcile_durable_submit_attempts() is True
    assert venue.calls == []
    assert venue.futures_calls == [
        ("futures-client-id-456", "BTC/USDT:USDT", attempt["amount_spec"])
    ]
    assert service.found == [("futures-client-id-456", order)]


@pytest.mark.asyncio
async def test_startup_reconciliation_keeps_not_found_attempt_globally_blocking():
    attempt = {
        "client_order_id": "client-id-unknown",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    venue = _VenueLookup(None)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))

    assert await engine._reconcile_durable_submit_attempts() is False
    assert service.not_found and service.not_found[0][0] == "client-id-unknown"
    assert engine.halts and engine.halts[0][0] == "*"
    assert engine.gate_refreshed is True
    assert engine.failures
