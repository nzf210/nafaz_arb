import pytest
from types import SimpleNamespace

from executor.engine_entry_submission import EngineEntrySubmissionMixin
from executor.global_stop import DERISK_LOCK_KEY, FENCE_LOCK_KEY, GlobalStopError, GlobalStopService
from executor.helpers import RetryExhausted, is_explicit_rate_limit_rejection, retry_with_backoff
from executor.indodax_v2 import IndodaxRateLimitError
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

    async def fetchval(self, query, *args):
        self.executed.append((query, args))
        if "pg_try_advisory_lock" in query:
            return True
        raise AssertionError(f"unexpected fetchval query: {query[:80]}")

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
                "recovery_context": args[9],
                "amount_spec": args[10],
            }
            return dict(self.attempts[client_order_id])
        if "where status in ('SUBMITTING','UNKNOWN')" in query:
            current_id = args[0]
            scope_ex = args[1] if len(args) > 1 else None
            scope_sym = args[2] if len(args) > 2 else None
            return next(
                (dict(row) for key, row in self.attempts.items()
                 if key != current_id
                 and row["status"] in ("SUBMITTING", "UNKNOWN")
                 and (scope_ex is None or str(row.get("exchange", "")).lower() == str(scope_ex).lower())
                 and (scope_sym is None or str(row.get("symbol", "")).lower() == str(scope_sym).lower())),
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
            if row is None:
                return None
            select_clause = query.lower().split("select", 1)[1].split(
                "from public.executor_submit_attempts", 1
            )[0]
            selected_columns = [
                column.strip() for column in select_clause.replace("\\n", " ").split(",")
            ]
            return {column: row.get(column) for column in selected_columns}
        return None


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AsyncContext(self.conn)


async def _prepare(service, client_order_id, recovery_context=None, exchange="binance", symbol="BTC/USDT"):
    await service.prepare_submit_attempt(
        client_order_id=client_order_id,
        exchange=exchange,
        market_type="spot",
        symbol=symbol,
        side="buy",
        order_type="limit",
        quantity=0.01,
        price=50000,
        pair_key=f"{exchange}__okx__btc",
        recovery_context=(
            recovery_context if recovery_context is not None else {"test": True}
        ),
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
async def test_prepared_client_id_reuse_rejects_changed_recovery_context():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)
    await _prepare(
        service, "stable-parent-intent-id",
        {"parent_spot_client_order_id": "spot-parent-a"},
    )

    # The same complete intent is idempotently reusable while still PREPARED.
    await _prepare(
        service, "stable-parent-intent-id",
        {"parent_spot_client_order_id": "spot-parent-a"},
    )

    with pytest.raises(GlobalStopError, match="different order intent"):
        await _prepare(
            service, "stable-parent-intent-id",
            {"parent_spot_client_order_id": "spot-parent-b"},
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
    assert any("pg_try_advisory_lock" in query for query in statements)
    assert any("pg_advisory_unlock" in query for query in statements)
    assert conn.attempts["stable-client-order-id"]["fence_epoch"] == 12


@pytest.mark.asyncio
async def test_pair_admission_guard_runs_under_lock_before_submitting_transition():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)
    await _prepare(service, "pair-guarded-order")
    guard_calls = []

    async def reject_existing_pair():
        assert any("pg_try_advisory_lock" in query for query, _ in conn.executed)
        guard_calls.append(True)
        return False

    with pytest.raises(GlobalStopError, match="pre-submit admission guard"):
        async with service.order_submit_fence(
            client_order_id="pair-guarded-order",
            pre_submit_guard=reject_existing_pair,
        ):
            pytest.fail("venue request must not be entered for an occupied pair")

    assert guard_calls == [True]
    assert conn.attempts["pair-guarded-order"]["status"] == "PREPARED"
    statements = [query for query, _ in conn.executed]
    assert any("pg_advisory_unlock" in query for query in statements)
    assert not any("set status='SUBMITTING'" in query for query in statements)


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
async def test_scoped_submit_fence_allows_different_venue_while_one_unresolved():
    """Unresolved attempt on venue A does not block scoped entry on venue B."""
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)

    # Prepare and leave UNKNOWN attempt on mexc:DODO_USDT
    await _prepare(service, "mexc-dodo-order", exchange="mexc", symbol="DODO_USDT")
    with pytest.raises(ConnectionError):
        async with service.order_submit_fence(
            client_order_id="mexc-dodo-order", scope_exchange="mexc", scope_symbol="DODO_USDT"
        ):
            raise ConnectionError("mexc timeout")
    assert conn.attempts["mexc-dodo-order"]["status"] == "SUBMITTING"

    # Scoped entry on okx:BTC_USDT succeeds despite MEXC unresolved
    await _prepare(service, "okx-btc-order", exchange="okx", symbol="BTC_USDT")
    async with service.order_submit_fence(
        client_order_id="okx-btc-order", scope_exchange="okx", scope_symbol="BTC_USDT"
    ):
        pass
    assert conn.attempts["okx-btc-order"]["status"] == "SUBMITTING"

    # Same venue+symbol (mexc:DODO_USDT) is blocked
    await _prepare(service, "mexc-dodo-retry", exchange="mexc", symbol="DODO_USDT")
    with pytest.raises(GlobalStopError, match="unresolved in-flight submit"):
        async with service.order_submit_fence(
            client_order_id="mexc-dodo-retry", scope_exchange="mexc", scope_symbol="DODO_USDT"
        ):
            pytest.fail("same venue/symbol must be blocked by unresolved attempt")


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
async def test_ambiguous_order_submit_failure_is_not_retried():
    calls = 0

    async def submit():
        nonlocal calls
        calls += 1
        raise TimeoutError("connection reset after request write")

    with pytest.raises(RetryExhausted) as raised:
        await retry_with_backoff(
            submit, max_retries=4, base_delay=0,
            retry_if=is_explicit_rate_limit_rejection,
        )

    assert calls == 1
    assert raised.value.attempts == 1


@pytest.mark.asyncio
async def test_indodax_503_is_not_retried_but_explicit_429_is():
    assert not is_explicit_rate_limit_rejection(
        IndodaxRateLimitError("http_503", "rate limited")
    )
    assert is_explicit_rate_limit_rejection(
        IndodaxRateLimitError("http_429", "rate limited")
    )

    calls = 0

    async def submit_429_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise IndodaxRateLimitError("http_429", "rate limited")
        return {"id": "accepted-after-429"}

    result = await retry_with_backoff(
        submit_429_once, max_retries=2, base_delay=0,
        retry_if=is_explicit_rate_limit_rejection,
    )
    assert result["id"] == "accepted-after-429"
    assert calls == 2


@pytest.mark.asyncio
async def test_ambiguous_503_order_submit_failure_is_not_retried():
    calls = 0

    async def submit():
        nonlocal calls
        calls += 1
        raise IndodaxRateLimitError("http_503", "rate limited")

    with pytest.raises(RetryExhausted) as raised:
        await retry_with_backoff(
            submit, max_retries=4, base_delay=0,
            retry_if=is_explicit_rate_limit_rejection,
        )

    assert calls == 1
    assert raised.value.attempts == 1


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

    def order_submit_fence(self, *, client_order_id, pre_submit_guard=None, **kwargs):
        return _AsyncContext(object())

    async def request_stop_in_transaction(self, conn, reason, *, source, metadata):
        self.stop_attempts.append((reason, source, metadata))
        raise ConnectionError("postgres disconnected while persisting CEX_IP_BLOCK")

    async def mark_submit_unknown(self, client_order_id, error):
        self.unknown.append((client_order_id, error))


class _SpotIPRestrictedVenue:
    def __init__(self):
        self.lookup_calls = 0
        self.submit_calls = 0

    async def place_local_limit_buy(self, *args, **kwargs):
        self.submit_calls += 1
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
        self.rejected = []
        self.failures = []

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

    async def _mark_order_submission_rejected(self, *args):
        self.rejected.append(args)

    async def _publish_execution_failure(self, *args, **kwargs):
        self.failures.append((args, kwargs))


class _FakeAmountSpec:
    def to_dict(self):
        return {"unit": "BASE", "step": 0.001}


@pytest.mark.asyncio
async def test_live_spot_entry_refuses_missing_global_stop_before_venue_call():
    venue = _SpotIPRestrictedVenue()
    engine = _SpotSubmissionHarness(None, venue)
    engine.config = SimpleNamespace(mock_mode=False)

    with pytest.raises(GlobalStopError, match="LIVE spot entry requires"):
        await engine._submit_entry_order(
            "pair-spot-no-stop", {}, None, None, None, 0.01, 500.0, 50000.0
        )

    assert venue.submit_calls == 0
    assert venue.lookup_calls == 0


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

    def order_submit_fence(self, *, client_order_id, **kwargs):
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
        self.submit_calls = 0

    async def open_global_short_market(self, *args, **kwargs):
        self.submit_calls += 1
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
async def test_live_futures_hedge_refuses_missing_global_stop_before_venue_call():
    engine = _HedgeHarness(None)
    engine.config = SimpleNamespace(mock_mode=False)

    with pytest.raises(GlobalStopError, match="LIVE futures hedge requires"):
        await engine._submit_entry_hedge_order(
            "pair-futures-no-stop", {}, "BTC/USDT:USDT", 0.01, 50000.0
        )

    assert engine.client.submit_calls == 0
    assert engine.order_intents == []


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
        self.requested_market = None

    def for_venue(self, exchange, market=None):
        self.requested_exchange = exchange
        self.requested_market = market
        return self.venue


class _MarketAwareBaseClient:
    def __init__(self, venues):
        self.venues = venues
        self.requests = []

    def for_venue(self, exchange, market=None):
        self.requests.append((exchange, market))
        return self.venues.get((exchange, market))


class _SubmitLedger:
    def __init__(self, attempts):
        self.attempts = list(attempts)
        self.found = []
        self.not_found = []
        self.unknown = []

    async def list_unresolved_submit_attempts(self, *, limit=100):
        return list(self.attempts)

    async def reconcile_submit_attempt_found(self, client_order_id, order):
        self.found.append((client_order_id, order))
        self.attempts = [row for row in self.attempts if row["client_order_id"] != client_order_id]

    async def reconcile_submit_attempt_not_found(self, client_order_id, *, evidence):
        self.not_found.append((client_order_id, evidence))

    async def mark_submit_unknown(self, client_order_id, error):
        self.unknown.append((client_order_id, error))


class _FakeIntentRegistry:
    def __init__(
        self, record=None, *, exchange="binance",
        client_order_id="client-id-123",
    ):
        self.record = record
        self.exchange = exchange
        self.client_order_id = client_order_id
        self.registered = []

    async def find_by_client_id(self, exchange, client_order_id):
        assert exchange == self.exchange
        assert client_order_id == self.client_order_id
        return self.record

    async def register(self, record):
        self.registered.append(record)
        return record


class _RecoveryHarness(EngineRecoveryMixin):
    def __init__(self, service, base_client):
        self.global_stop = service
        self._base_client = base_client
        self.order_registry = None
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
    record = SimpleNamespace(
        exchange_order_id="",
        status="submitting",
        filled_quantity=0.0,
        original_quantity=0.01,
        remaining_quantity=0.01,
        average_fill_price=0.0,
        venue_client_order_id="",
    )
    registry = _FakeIntentRegistry(record)
    venue = _VenueLookup(order)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))
    engine.order_registry = registry

    assert await engine._reconcile_durable_submit_attempts() is True
    assert venue.calls == [("client-id-123", "BTC/USDT", {"unit": "BASE"})]
    assert engine._base_client.requested_exchange == "binance"
    assert record.exchange_order_id == "venue-order-99"
    assert record.status == "open"
    assert registry.registered == [record]
    assert service.found == [("client-id-123", order)]


@pytest.mark.asyncio
async def test_submit_reconciliation_missing_registry_keeps_global_fence():
    attempt = {
        "client_order_id": "client-id-123",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    order = {"id": "venue-order-99", "status": "closed"}
    venue = _VenueLookup(order)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))

    assert await engine._reconcile_durable_submit_attempts() is False
    assert service.found == []
    assert service.not_found == []
    assert service.unknown and service.unknown[0][0] == "client-id-123"
    assert engine.halts and engine.halts[0][0] == "*"
    assert engine.gate_refreshed is True


@pytest.mark.asyncio
async def test_submit_reconciliation_binds_found_terminal_order_to_durable_intent():
    attempt = {
        "client_order_id": "client-id-123",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    order = {
        "id": "venue-order-99",
        "status": "closed",
        "amount": 0.01,
        "filled": 0.01,
        "average": 65000.0,
        "clientOrderId": "venue-client-id-99",
    }
    record = SimpleNamespace(
        exchange_order_id="",
        status="submitting",
        filled_quantity=0.0,
        original_quantity=0.01,
        remaining_quantity=0.01,
        average_fill_price=0.0,
        venue_client_order_id="",
    )
    registry = _FakeIntentRegistry(record)
    venue = _VenueLookup(order)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))
    engine.order_registry = registry

    assert await engine._reconcile_durable_submit_attempts() is True
    assert record.exchange_order_id == "venue-order-99"
    # Keep the intent indexed/non-terminal until exact-order recovery fetches
    # the true terminal state and completes its parent Spot/Futures lifecycle.
    assert record.status == "open"
    assert record.filled_quantity == pytest.approx(0.01)
    assert record.remaining_quantity == pytest.approx(0.0)
    assert record.average_fill_price == pytest.approx(65000.0)
    assert record.venue_client_order_id == "venue-client-id-99"
    assert registry.registered == [record]
    assert service.found == [("client-id-123", order)]


@pytest.mark.asyncio
async def test_submit_reconciliation_missing_intent_keeps_global_fence():
    attempt = {
        "client_order_id": "client-id-123",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    order = {"id": "venue-order-99", "status": "closed"}
    venue = _VenueLookup(order)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))
    engine.order_registry = _FakeIntentRegistry(record=None)

    assert await engine._reconcile_durable_submit_attempts() is False
    assert service.found == []
    assert service.not_found == []
    assert service.unknown and service.unknown[0][0] == "client-id-123"
    assert engine.halts and engine.halts[0][0] == "*"
    assert engine.gate_refreshed is True


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
    record = SimpleNamespace(
        exchange_order_id="",
        status="submitting",
        filled_quantity=0.0,
        original_quantity=0.01,
        remaining_quantity=0.01,
        average_fill_price=0.0,
        venue_client_order_id="",
    )
    registry = _FakeIntentRegistry(
        record,
        exchange="okx",
        client_order_id="futures-client-id-456",
    )
    venue = _VenueLookup(order)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(venue))
    engine.order_registry = registry

    assert await engine._reconcile_durable_submit_attempts() is True
    assert venue.calls == []
    assert venue.futures_calls == [
        ("futures-client-id-456", "BTC/USDT:USDT", attempt["amount_spec"])
    ]
    assert record.exchange_order_id == "futures-order-77"
    assert registry.registered == [record]
    assert service.found == [("futures-client-id-456", order)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("market_type", "expected_market", "lookup_side"),
    [
        ("spot", "spot", "spot"),
        ("swap", "futures", "futures"),
    ],
)
async def test_startup_submit_recovery_resolves_exact_mexc_market(
    market_type, expected_market, lookup_side
):
    attempt = {
        "client_order_id": f"mexc-{expected_market}-client-id",
        "exchange": "mexc",
        "market_type": market_type,
        "symbol": "BTC/USDT" if expected_market == "spot" else "BTC/USDT:USDT",
        "amount_spec": {"unit": "BASE" if expected_market == "spot" else "CONTRACTS"},
    }
    order = {"id": f"mexc-{expected_market}-order", "status": "open"}
    spot_venue = _VenueLookup(order)
    futures_venue = _VenueLookup(order)
    base = _MarketAwareBaseClient({
        ("mexc", "spot"): spot_venue,
        ("mexc", "futures"): futures_venue,
    })
    record = SimpleNamespace(
        exchange_order_id="",
        status="submitting",
        filled_quantity=0.0,
        original_quantity=0.01,
        remaining_quantity=0.01,
        average_fill_price=0.0,
        venue_client_order_id="",
    )
    registry = _FakeIntentRegistry(
        record, exchange="mexc", client_order_id=attempt["client_order_id"]
    )
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, base)
    engine.order_registry = registry

    assert await engine._reconcile_durable_submit_attempts() is True
    assert base.requests == [("mexc", expected_market)]
    selected = spot_venue if lookup_side == "spot" else futures_venue
    other = futures_venue if lookup_side == "spot" else spot_venue
    assert len(selected.calls if lookup_side == "spot" else selected.futures_calls) == 1
    assert other.calls == []
    assert other.futures_calls == []
    assert service.found == [(attempt["client_order_id"], order)]


@pytest.mark.asyncio
async def test_submit_recovery_rejects_legacy_venue_resolver_without_market_scope():
    class _LegacyResolver:
        def for_venue(self, exchange):
            raise AssertionError("ambiguous one-argument resolver must not be used")

    attempt = {
        "client_order_id": "mexc-spot-legacy-resolver",
        "exchange": "mexc",
        "market_type": "spot",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _LegacyResolver())

    assert await engine._reconcile_durable_submit_attempts() is False
    assert service.not_found == []
    assert service.unknown and service.unknown[0][0] == attempt["client_order_id"]
    assert engine.halts and engine.halts[0][0] == "*"


class _SpotBalanceRecoveryHarness(EngineRecoveryMixin):
    def __init__(self, positions, balance):
        self.active_positions = positions
        self._pair_infos = {}
        self.client = self
        self.balance = balance
        self.config = SimpleNamespace(local_exchange_name="indodax")

    async def fetch_spot_total_balance(self, asset, *, venue):
        assert asset == "BTC"
        return self.balance

    def _resolve_global_symbol(self, symbol):
        return symbol


@pytest.mark.asyncio
async def test_recovered_spot_balance_is_aggregated_per_venue_and_base_asset():
    positions = {
        "pair-a": SimpleNamespace(
            base_asset="BTC", local_exchange="mexc", qty=1.0,
            global_symbol="BTC/USDT:USDT", symbol="BTC/USDT",
        ),
        "pair-b": SimpleNamespace(
            base_asset="BTC", local_exchange="mexc", qty=1.0,
            global_symbol="BTC/USDC:USDC", symbol="BTC/USDC",
        ),
    }
    engine = _SpotBalanceRecoveryHarness(positions, balance=1.5)

    deficits = await engine._verify_recovered_spot_legs()

    assert set(deficits) == {"BTC/USDT:USDT", "BTC/USDC:USDC"}
    assert all("expected=2.0 BTC" in reason for reason in deficits.values())


@pytest.mark.asyncio
async def test_recovered_spot_balance_is_not_aggregated_across_venues():
    positions = {
        "pair-a": SimpleNamespace(
            base_asset="BTC", local_exchange="mexc", qty=1.0,
            global_symbol="BTC/USDT:USDT", symbol="BTC/USDT",
        ),
        "pair-b": SimpleNamespace(
            base_asset="BTC", local_exchange="tokocrypto", qty=1.0,
            global_symbol="BTC/USDC:USDC", symbol="BTC/USDC",
        ),
    }

    class _PerVenueBalance(_SpotBalanceRecoveryHarness):
        async def fetch_spot_total_balance(self, asset, *, venue):
            return {"mexc": 1.0, "tokocrypto": 1.0}[venue]

    engine = _PerVenueBalance(positions, balance=0.0)

    assert await engine._verify_recovered_spot_legs() == {}


@pytest.mark.asyncio
async def test_submit_reconciliation_lookup_exception_is_unknown_not_not_found():
    class _FailingVenue:
        async def fetch_local_order_by_client_id(self, client_order_id, symbol, amount_spec):
            raise TimeoutError("venue order history temporarily unavailable")

    attempt = {
        "client_order_id": "client-id-timeout",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(_FailingVenue()))

    assert await engine._reconcile_durable_submit_attempts() is False
    assert service.found == []
    assert service.not_found == []
    assert service.unknown and service.unknown[0][0] == "client-id-timeout"
    assert engine.halts and engine.halts[0][0] == "*"


@pytest.mark.asyncio
async def test_submit_reconciliation_registry_write_failure_is_unknown_not_not_found():
    class _FailingRegistry(_FakeIntentRegistry):
        async def register(self, record):
            raise RuntimeError("durable registry writer unavailable")

    attempt = {
        "client_order_id": "client-id-write-failure",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "amount_spec": {"unit": "BASE"},
    }
    order = {"id": "venue-order-101", "status": "open"}
    record = SimpleNamespace(
        exchange_order_id="",
        status="submitting",
        filled_quantity=0.0,
        original_quantity=0.01,
        remaining_quantity=0.01,
        average_fill_price=0.0,
        venue_client_order_id="",
    )
    registry = _FailingRegistry(record)
    service = _SubmitLedger([attempt])
    engine = _RecoveryHarness(service, _BaseClient(_VenueLookup(order)))
    engine.order_registry = registry

    assert await engine._reconcile_durable_submit_attempts() is False
    assert service.found == []
    assert service.not_found == []
    assert service.unknown and service.unknown[0][0] == "client-id-write-failure"
    assert engine.halts and engine.halts[0][0] == "*"


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


def test_spot_slot_account_uses_pair_quote_not_an_idr_fallback():
    assert EngineEntrySubmissionMixin._spot_slot_account(
        SimpleNamespace(quote_asset="USDT")
    ) == "usdt"
    assert EngineEntrySubmissionMixin._spot_slot_account(
        SimpleNamespace(quote_asset="USDC")
    ) == "usdt"
    assert EngineEntrySubmissionMixin._spot_slot_account(
        SimpleNamespace(quote_asset="BIDR")
    ) == "idr"
    assert EngineEntrySubmissionMixin._spot_slot_account(
        SimpleNamespace(quote_asset="IDR")
    ) == "idr"


@pytest.mark.asyncio
async def test_derisk_fence_uses_independent_session_lock_key():
    conn = _FakeConnection()
    service = GlobalStopService("postgresql://unused")
    service.pool = _FakePool(conn)

    async with service.derisk_submit_fence(client_order_id="stable-exit-order") as fenced_conn:
        assert fenced_conn is conn

    lock_calls = [
        args[0]
        for query, args in conn.executed
        if "pg_advisory_lock" in query or "pg_advisory_unlock" in query
    ]
    assert lock_calls == [DERISK_LOCK_KEY, DERISK_LOCK_KEY]
    assert DERISK_LOCK_KEY != FENCE_LOCK_KEY


class _RecordingAsyncContext:
    def __init__(self, events):
        self.events = events

    async def __aenter__(self):
        self.events.append("fence_enter")
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        self.events.append("fence_exit")
        return False


class _RecordingSpotStop(_FakeSpotStop):
    def __init__(self, events):
        super().__init__()
        self.events = events

    async def prepare_submit_attempt(self, **kwargs):
        self.events.append("prepare")
        self.prepared.append(kwargs)

    def order_submit_fence(self, *, client_order_id, pre_submit_guard=None, **kwargs):
        return _RecordingAsyncContext(self.events)

    async def mark_submit_rejected(self, client_order_id, error):
        self.events.append("ledger_rejected")


class _RecordingSlotManager:
    def __init__(self, events):
        self.events = events

    async def reserve_slot(self, *args, **kwargs):
        self.events.append("reserve_slot")

    async def release_slot(self, *args, **kwargs):
        self.events.append("release_slot")


class _DefinitiveRejectSpotVenue:
    def __init__(self, events):
        self.events = events
        self.submit_calls = 0

    async def place_local_limit_buy(self, *args, **kwargs):
        self.submit_calls += 1
        self.events.append("venue_create")
        raise ValueError("definitive invalid order")


class _SlotOrderingHarness(_SpotSubmissionHarness):
    def __init__(self, stop, venue, slot_manager):
        super().__init__(stop, venue)
        self.slot_manager = slot_manager
        self.config = SimpleNamespace(mock_mode=False)

    def _spot_slot_account(self, pair_info):
        return pair_info.quote_asset

    def _is_definitive_order_rejection(self, error):
        return True

    async def _handle_entry_submission_failure(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_spot_entry_persists_prepared_intent_before_reserving_slot():
    events = []
    stop = _RecordingSpotStop(events)
    venue = _DefinitiveRejectSpotVenue(events)
    slots = _RecordingSlotManager(events)
    engine = _SlotOrderingHarness(stop, venue, slots)
    pair_info = SimpleNamespace(
        local_exchange="mexc",
        local_symbol="BTC/USDT",
        local_amount_spec=_FakeAmountSpec(),
        global_amount_spec=_FakeAmountSpec(),
        quote_asset="USDT",
    )

    await engine._submit_entry_order(
        "mexc:BTC/USDT", {}, pair_info, {}, {}, 0.01, 500.0, 50000.0
    )

    assert events.index("prepare") < events.index("reserve_slot")
    assert events.index("reserve_slot") < events.index("fence_enter")
    assert events.index("fence_enter") < events.index("venue_create")
    assert venue.submit_calls == 1
    assert "ledger_rejected" in events
    assert "release_slot" in events
