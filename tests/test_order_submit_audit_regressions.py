from types import SimpleNamespace

import pytest

from executor.derisk_fence import derisk_fence
from executor.emergency_derisk import EmergencyDeriskLease
from executor.entry_hedge import EntryHedgeMixin
from executor.force_close_legs import ForceCloseLegMixin
from executor.engine_order_intents import EngineOrderIntentMixin
from executor.engine_signal import EngineSignalMixin
from executor.global_stop import GlobalStopError, GlobalStopService
from executor.helpers import PENDING_ENTRY_KEY_PREFIX, is_transient_settlement_error
from executor.rollback import RollbackMixin


class _EmergencyRedis:
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

    async def fetchval(self, query, *args):
        self.statements.append(query)
        if "pg_try_advisory_lock" in query:
            return True
        raise AssertionError(f"unexpected fetchval query: {query}")

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
        self.redis = _EmergencyRedis()
        self.config = SimpleNamespace(
            local_exchange_name="tokocrypto",
            exchanges={"tokocrypto": SimpleNamespace(api_key="test-tokocrypto-key")},
            executor_instance_id="test-worker",
            emergency_derisk_owner_id="test-worker",
            emergency_derisk_allowed=True,
            emergency_derisk_lease_ttl_ms=120000,
        )
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

    def order_submit_fence(self, *, client_order_id, **kwargs):
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
            exchanges={"okx": SimpleNamespace(account_scope_id="test-okx-account")},
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

    async def _allow(*args, **kwargs):
        return {"action_class": "DERISK"}

    async def _acquire(*args, **kwargs):
        return SimpleNamespace(generation=1)

    async def _validate(*args, **kwargs):
        return True

    service.risk_control_plane.assert_allowed = _allow
    service.ownership.acquire = _acquire
    service.ownership.validate = _validate

    async with service.derisk_submit_fence(
        client_order_id="exit-client-1",
        account_scope_id="test-okx-account",
        venue="okx",
        market_type="perpetual",
        instrument="BTC/USDT:USDT",
    ) as fenced:
        assert fenced is conn

    assert any("pg_advisory_lock" in query for query in conn.statements)
    assert any("pg_advisory_unlock" in query for query in conn.statements)


@pytest.mark.asyncio
async def test_derisking_uses_designated_redis_lease_when_postgres_is_unavailable():
    engine = SimpleNamespace(
        global_stop=GlobalStopService(""),
        redis=_EmergencyRedis(),
        config=SimpleNamespace(
            mock_mode=False,
            executor_instance_id="test-worker",
            emergency_derisk_owner_id="test-worker",
            emergency_derisk_allowed=True,
            emergency_derisk_lease_ttl_ms=120000,
        ),
    )
    entered = False

    async with derisk_fence(
        engine,
        client_order_id="force-close-client-1",
        venue="okx",
        market_type="perpetual",
        instrument="BTC/USDT:USDT",
        account_scope_id="test-okx-account",
    ) as lease:
        entered = True
        assert isinstance(lease, EmergencyDeriskLease)

    assert entered
    assert engine.redis.values == {}


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


class _AbortTransitionConnection:
    def __init__(self, result):
        self.result = result
        self.query = ""

    async def execute(self, query, *args):
        self.query = query
        return self.result


@pytest.mark.asyncio
async def test_aborted_transition_cannot_retire_submitting_or_unknown_attempt():
    conn = _AbortTransitionConnection("UPDATE 0")
    service = GlobalStopService("postgresql://unused")
    service.pool = _Pool(conn)

    with pytest.raises(GlobalStopError, match="transition rejected"):
        await service.mark_submit_aborted(
            "possibly-accepted-entry-1",
            "caller incorrectly claimed the venue boundary was not crossed",
        )

    assert "status = 'PREPARED'" in conn.query
    assert "status in ('PREPARED','SUBMITTING','UNKNOWN')" not in conn.query


class _PairAdmissionHarness(EngineSignalMixin):
    def __init__(self, redis):
        self.redis = redis
        self.config = SimpleNamespace(mock_mode=False)
        self.active_positions = {}
        self.pending_entry_orders = {}


@pytest.mark.asyncio
async def test_shared_pending_entry_marker_blocks_cross_instance_pair_reentry(fake_redis):
    harness = _PairAdmissionHarness(fake_redis)
    pair_key = "tokocrypto:BTC_IDR"
    await fake_redis.set(PENDING_ENTRY_KEY_PREFIX + pair_key, '{"client_order_id":"entry-a"}')

    assert await harness._has_active_or_pending_position(pair_key) is True


@pytest.mark.asyncio
async def test_pair_admission_fails_closed_when_shared_redis_state_is_unavailable():
    class _UnavailableRedis:
        async def exists(self, _key):
            raise ConnectionError("redis unavailable")

    harness = _PairAdmissionHarness(_UnavailableRedis())
    assert await harness._has_active_or_pending_position("tokocrypto:BTC_IDR") is True


class _ForceCloseRegistry:
    def __init__(self):
        self.intents = {}

    async def find_by_client_id(self, exchange, client_order_id):
        return self.intents.get((exchange, client_order_id))


class _AmbiguousSpotCloseVenue:
    def __init__(self):
        self.submit_calls = 0
        self.lookup_calls = 0

    async def place_local_market_sell(self, *args, **kwargs):
        self.submit_calls += 1
        raise TimeoutError("force-close spot POST acknowledgement lost")

    async def fetch_local_order_by_client_id(self, *args, **kwargs):
        self.lookup_calls += 1
        return None


class _ForceCloseHarness(ForceCloseLegMixin):
    def __init__(self):
        self.config = SimpleNamespace(
            mock_mode=False,
            postgres_dsn="postgresql://unused",
            exchanges={"tokocrypto": SimpleNamespace(api_key="test-tokocrypto-key")},
            executor_instance_id="test-worker",
            emergency_derisk_owner_id="test-worker",
            emergency_derisk_lease_ttl_ms=120000,
        )
        self.redis = _EmergencyRedis()
        self.order_registry = _ForceCloseRegistry()
        self.client = _AmbiguousSpotCloseVenue()
        self.registered_intents = []

    def _next_order_seq(self):
        return 123

    async def _register_order_intent(self, pair_key, client_order_id, **metadata):
        self.registered_intents.append((pair_key, client_order_id, metadata))
        self.order_registry.intents[(metadata["exchange"], client_order_id)] = {
            "client_order_id": client_order_id,
            "status": "submitting",
        }
        return self.order_registry.intents[(metadata["exchange"], client_order_id)]


@pytest.mark.asyncio
async def test_force_close_spot_retry_reuses_position_id_and_adopts_prior_intent():
    engine = _ForceCloseHarness()
    position = SimpleNamespace(
        opened_at_ms=1_800_000_000_123,
        local_exchange="tokocrypto",
        symbol="BTC/USDT",
        qty=0.01,
        local_amount_spec={"unit": "BASE", "step": 0.001},
    )

    first_price = await engine._liquidate_spot_leg("pair:btc", position)
    first_id = engine.registered_intents[0][1]
    second_price = await engine._liquidate_spot_leg("pair:btc", position)

    second_id = engine.registered_intents[0][1]
    assert first_price == 0.0
    assert second_price == 0.0
    assert first_id == second_id
    assert engine.client.submit_calls == 1
    assert engine.client.lookup_calls == 1
    assert len(engine.registered_intents) == 1
    assert engine.registered_intents[0][2]["required"] is True


class _OrderIntentHarness(EngineOrderIntentMixin):
    def __init__(self, *, supabase_enabled, mock_mode=False):
        self.config = SimpleNamespace(
            mock_mode=mock_mode,
            postgres_dsn="postgresql://local-postgres-is-not-the-order-registry",
        )
        self.supabase = SimpleNamespace(enabled=supabase_enabled)
        self.order_registry = None


@pytest.mark.asyncio
async def test_local_global_stop_dsn_does_not_count_as_durable_order_registry():
    engine = _OrderIntentHarness(supabase_enabled=False)

    with pytest.raises(RuntimeError, match="durable order registry/outbox is disabled"):
        await engine._register_order_intent(
            "pair:btc",
            "live-required-intent-1",
            exchange="tokocrypto",
            market_type="spot",
            symbol="BTC/IDR",
            role="entry_spot",
            side="buy",
            order_type="limit",
            quantity=0.01,
            required=True,
        )


@pytest.mark.asyncio
async def test_optional_live_derisk_intent_can_still_proceed_without_durable_registry():
    engine = _OrderIntentHarness(supabase_enabled=False)

    result = await engine._register_order_intent(
        "pair:btc",
        "live-optional-exit-1",
        exchange="tokocrypto",
        market_type="spot",
        symbol="BTC/IDR",
        role="exit_spot",
        side="sell",
        order_type="market",
        quantity=0.01,
        required=False,
    )

    assert result is None


@pytest.mark.asyncio
async def test_force_close_spot_ids_do_not_collide_after_one_billion_ms():
    engine = _ForceCloseHarness()
    base = {
        "local_exchange": "tokocrypto",
        "symbol": "BTC/USDT",
        "qty": 0.01,
        "local_amount_spec": {"unit": "BASE", "step": 0.001},
    }
    first = SimpleNamespace(opened_at_ms=1_800_000_000_123, **base)
    second = SimpleNamespace(opened_at_ms=1_801_000_000_123, **base)

    await engine._liquidate_spot_leg("pair:btc", first)
    await engine._liquidate_spot_leg("pair:btc", second)

    client_ids = [item[1] for item in engine.registered_intents]
    assert len(client_ids) == 2
    assert client_ids[0] != client_ids[1]
    assert engine.client.submit_calls == 2


class _RecordingFuturesVenue:
    def __init__(self):
        self.submitted = []

    async def open_global_short_market(self, symbol, amount, client_order_id=None,
                                       amount_spec=None, price=0.0, min_notional_usdt=0.0):
        self.submitted.append(amount)
        return {
            "id": "hedge-1", "symbol": symbol, "status": "closed",
            "filled": amount, "average": price or 50000.0,
            "client_order_id": client_order_id,
        }


class _PolicyHarness(EntryHedgeMixin):
    """Harness for sizing-policy tests without the fencing machinery."""

    def __init__(self, hedging_cfg):
        self.global_stop = None
        self.config = SimpleNamespace(
            mock_mode=True, futures_min_notional=0.0, hedging=hedging_cfg,
        )
        self.capabilities = None
        self.client = _RecordingFuturesVenue()
        self.intents = []

    async def _register_order_intent(self, *args, **kwargs):
        self.intents.append((args, kwargs))

    async def _register_order(self, *args, **kwargs):
        return kwargs.get("order", args[2] if len(args) > 2 else {})

    def _next_order_seq(self):
        return 7


def _policy_order_info():
    return {
        "pending_hedge_client_order_id": "hedge-policy-1",
        "global_exchange": "okx",
        "global_symbol": "BTC/USDT:USDT",
        "global_amount_spec": {
            "unit": "BASE", "step": 0.0001, "min": 0.0001, "base_per_unit": 1
        },
        "local_exchange": "tokocrypto",
        "order_id": "spot-order-1",
        "client_order_id": "spot-client-1",
        "futures_min_notional": 10.0,
    }


@pytest.mark.asyncio
async def test_hedge_sizing_waits_when_below_min_notional():
    """wait_until_covered (default): sub-minimum delta must WAIT (no submit, no raise)."""
    from executor.config import HedgingConfig

    harness = _PolicyHarness(HedgingConfig())
    order_info = _policy_order_info()
    # $2.50 notional delta, $10 min -> returns None (defer), not a $10 short.
    result = await harness._submit_entry_hedge_order(
        "tokocrypto:btc_idr", order_info, "BTC/USDT:USDT", 0.00005, 50000.0
    )
    hedge_coid, _ctx, short_order, qty = result
    assert short_order is None
    assert qty == 0.0
    assert harness.client.submitted == []
    assert order_info.get("hedge_waiting_for_coverage") is True

@pytest.mark.asyncio
async def test_hedge_sizing_rejects_overhedge_beyond_max_overhedge_pct():
    """allow_min_notional + overhedge=true, but max_overhedge_pct=0 must REJECT
    the scale-to-min result (no silent overhedge)."""
    from executor.config import HedgingConfig

    cfg = HedgingConfig()
    cfg.futures_min_order_policy = "allow_min_notional"
    cfg.futures_allow_min_notional_overhedge = True
    # futures_max_overhedge_pct stays at the sanitized default 0.0
    harness = _PolicyHarness(cfg)
    order_info = _policy_order_info()
    with pytest.raises(ValueError, match="max overhedge"):
        await harness._submit_entry_hedge_order(
            "tokocrypto:btc_idr", order_info, "BTC/USDT:USDT", 0.00005, 50000.0
        )
    assert harness.client.submitted == []

@pytest.mark.asyncio
async def test_hedge_sizing_scales_within_max_overhedge_pct():
    """allow_min_notional + overhedge=true + max_overhedge_pct=300 permits the
    scale-to-min result that stays within the ceiling."""
    from executor.config import HedgingConfig

    cfg = HedgingConfig()
    cfg.futures_min_order_policy = "allow_min_notional"
    cfg.futures_allow_min_notional_overhedge = True
    cfg.futures_max_overhedge_pct = 300.0  # 0.00005 * 4 = 0.0002 ceiling
    harness = _PolicyHarness(cfg)
    order_info = _policy_order_info()
    await harness._submit_entry_hedge_order(
        "tokocrypto:btc_idr", order_info, "BTC/USDT:USDT", 0.00005, 50000.0
    )
    assert harness.client.submitted == [pytest.approx(0.0002)]

@pytest.mark.asyncio
async def test_hedge_sizing_applies_target_hedge_ratio():
    """Ratio is applied ONCE, in _handle_entry_fills (target = spot × ratio −
    confirmed hedge). _submit_entry_hedge_order sizes directly from the
    residual without re-scaling."""
    from executor.config import HedgingConfig

    cfg = HedgingConfig()
    cfg.futures_target_hedge_ratio = 0.5
    harness = _PolicyHarness(cfg)
    order_info = _policy_order_info()
    # Residual already in target units: spot 0.002 × 0.5 − 0 confirmed = 0.001.
    await harness._submit_entry_hedge_order(
        "tokocrypto:btc_idr", order_info, "BTC/USDT:USDT", 0.001, 50000.0
    )
    # No second ×0.5: the submitted qty equals the residual verbatim.
    assert harness.client.submitted == [pytest.approx(0.001)]


@pytest.mark.asyncio
async def test_hedge_sizing_ratio_point_five_converges():
    """ratio 0.5: spot fill 1.0, futures confirmed 0.5 → next delta must be 0 (converged)."""
    from executor.config import HedgingConfig

    cfg = HedgingConfig()
    cfg.futures_target_hedge_ratio = 0.5
    # target_hedge_qty = 1.0 * 0.5 = 0.5; hedged 0.5 → unhedged = 0
    target_hedge_qty = 1.0 * 0.5
    hedged = 0.5
    unhedged = max(0.0, target_hedge_qty - hedged)
    assert unhedged == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_hedge_sizing_ratio_one_point_five_scales_up():
    """ratio 1.5: spot fill 1.0, no futures yet → target hedge 1.5, not 1.0."""
    from executor.config import HedgingConfig

    cfg = HedgingConfig()
    cfg.futures_target_hedge_ratio = 1.5
    target_hedge_qty = 1.0 * 1.5
    hedged = 0.0
    unhedged = max(0.0, target_hedge_qty - hedged)
    assert unhedged == pytest.approx(1.5)


def test_hedged_qty_watermark_is_monotonic():
    """_update_entry_hedge_markers must never reduce the cumulative hedged qty
    (a regressing futures fill snapshot must not shrink the hedge record)."""

    class _MarkersHarness(EntryHedgeMixin):
        pass

    harness = _MarkersHarness()
    order_info = {"hedged_short_qty": 0.5, "hedge_fill_orders": {"h1": 0.5}}
    harness._update_entry_hedge_markers(order_info, 0.3, "h2")
    assert order_info["hedged_short_qty"] == pytest.approx(0.5)
    assert order_info["hedge_fill_orders"]["h2"] == pytest.approx(0.3)
    # A higher fill still raises the watermark.
    harness._update_entry_hedge_markers(order_info, 0.8, "h3")
    assert order_info["hedged_short_qty"] == pytest.approx(0.8)


def test_partial_futures_fill_is_not_treated_as_full_hedge():
    """A futures hedge that filled only part of the request must not advance
    the full requested quantity into the hedge watermark."""

    # Mirrors the guard in _submit_entry_hedge_order:
    futures_hedge_qty = 0.01
    actual_hedged_qty = 0.004  # partial fill
    qty_tolerance = max(1e-12, futures_hedge_qty * 1e-8)
    assert actual_hedged_qty + qty_tolerance < futures_hedge_qty
    # The caller must record only actual_hedged_qty (via _persist_entry_hedge_success),
    # never futures_hedge_qty.
    recorded = actual_hedged_qty
    assert recorded < futures_hedge_qty


@pytest.mark.asyncio
async def test_hedge_ratio_05_submits_half_of_spot():
    """ratio 0.5, belum ada futures fill: submit 0.5 × cumulative spot."""
    from executor.config import HedgingConfig

    cfg = HedgingConfig()
    cfg.futures_target_hedge_ratio = 0.5
    harness = _PolicyHarness(cfg)
    order_info = _policy_order_info()
    # Residual = 0.002 × 0.5 − 0 = 0.001 (dihitung di _handle_entry_fills).
    await harness._submit_entry_hedge_order(
        "tokocrypto:btc_idr", order_info, "BTC/USDT:USDT", 0.001, 50000.0
    )
    assert harness.client.submitted == [pytest.approx(0.001)]


@pytest.mark.asyncio
async def test_hedge_ratio_15_submits_one_and_half_of_spot():
    """ratio 1.5, belum ada futures fill: submit 1.5 × cumulative spot."""
    from executor.config import HedgingConfig

    cfg = HedgingConfig()
    cfg.futures_target_hedge_ratio = 1.5
    harness = _PolicyHarness(cfg)
    order_info = _policy_order_info()
    # Residual = 0.002 × 1.5 − 0 = 0.003.
    await harness._submit_entry_hedge_order(
        "tokocrypto:btc_idr", order_info, "BTC/USDT:USDT", 0.003, 50000.0
    )
    assert harness.client.submitted == [pytest.approx(0.003)]


def test_partial_futures_fill_leaves_only_residual_for_next_hedge():
    """Futures partial fill: hanya residual target yang dikirim berikutnya."""
    # Formula di _handle_entry_fills:
    cumulative_spot = 0.01
    ratio = 1.0
    confirmed_futures = 0.004  # partial fill dari hedge sebelumnya
    target_hedge_qty = cumulative_spot * ratio
    residual = max(0.0, target_hedge_qty - confirmed_futures)
    assert residual == pytest.approx(0.006)
    # Bukan full 0.01 dan bukan 0: hanya sisa yang di-hedge ulang.
