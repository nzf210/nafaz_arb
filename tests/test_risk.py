"""Risk controls: exposure caps, fee buffer, kill switch, balance drift, order-failure halts, status snapshot."""

import asyncio
import json
import time
from unittest.mock import AsyncMock

from executor.models import ActivePosition, PositionState

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine

from tests.conftest import BalancesMockClient, MockExchangeClient, SlowLimitBuyClient, _exposure_engine, _fee_buffer_engine, _fresh_signal_payload, _half_budget_signal, engine, fake_redis, fake_redis_set

@pytest.mark.asyncio
async def test_p2_06_fee_buffer_shrinks_locked_capital(fake_redis):
    """A 1% round-trip fee must reserve 1% of the budget instead of locking it all."""
    engine, client = _fee_buffer_engine(fake_redis, fee_pct=0.01)
    sig = _fresh_signal_payload(int(time.time() * 1000))
    sig["execution_directives"]["max_position_usdt"] = 50.0
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))

    await engine.handle_signal_notification("indodax:btc_idr")

    assert "indodax:btc_idr" in engine.pending_entry_orders
    # 50 * (1 - 0.01) = 49.5, floored to the lot size.
    reserved = engine.pending_entry_orders["indodax:btc_idr"]["spot_cost_usdt"]
    assert reserved <= 49.5 + 1e-9
    assert reserved > 49.0
    assert client.get_balances()["spot_locked"] == reserved

@pytest.mark.asyncio
async def test_p2_06_zero_fee_pct_disables_buffer(fake_redis):
    """estimated_round_trip_fee_pct=0 must lock the full requested budget."""
    engine, _ = _fee_buffer_engine(fake_redis, fee_pct=0.0)
    sig = _fresh_signal_payload(int(time.time() * 1000))
    sig["execution_directives"]["max_position_usdt"] = 50.0
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))

    await engine.handle_signal_notification("indodax:btc_idr")

    reserved = engine.pending_entry_orders["indodax:btc_idr"]["spot_cost_usdt"]
    assert reserved == pytest.approx(50.0, abs=0.01)

@pytest.mark.asyncio
async def test_p2_06_fee_buffer_can_drop_below_min_capital(fake_redis):
    """If the fee buffer pushes the size under the minimum, the entry is rejected."""
    engine, _ = _fee_buffer_engine(fake_redis, fee_pct=0.01)
    sig = _fresh_signal_payload(int(time.time() * 1000))
    sig["execution_directives"]["max_position_usdt"] = 10.0  # min capital is 10.0
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))

    await engine.handle_signal_notification("indodax:btc_idr")

    # 10.0 * 0.99 = 9.9 < 10.0 -> rejected rather than silently undersized
    assert "indodax:btc_idr" not in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_p2_04_exposure_cap_rejects_second_position(fake_redis):
    """Once one $50 position is open, a $50 order against a $60 cap must be rejected."""
    engine, _ = _exposure_engine(fake_redis, cap=60.0)
    now_ms = int(time.time() * 1000)

    sig1 = _half_budget_signal("btc_idr", "BTCUSDT", now_ms)
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig1))
    await engine.handle_signal_notification("indodax:btc_idr")
    assert "indodax:btc_idr" in engine.pending_entry_orders

    sig2 = _half_budget_signal("eth_idr", "ETHUSDT", now_ms)
    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig2))
    await engine.handle_signal_notification("indodax:eth_idr")

    assert "indodax:eth_idr" not in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_p2_04_exposure_cap_counts_active_positions(fake_redis):
    """Open positions (not just pending entries) consume the cap."""
    engine, _ = _exposure_engine(fake_redis, cap=60.0)
    engine.active_positions["indodax:btc_idr"] = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        spot_cost_usdt=50.0,
        futures_margin_usdt=50.0,
    )

    sig = _half_budget_signal("eth_idr", "ETHUSDT", int(time.time() * 1000))
    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig))
    await engine.handle_signal_notification("indodax:eth_idr")

    assert "indodax:eth_idr" not in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_p2_04_exposure_cap_allows_within_budget(fake_redis):
    """A $50 order against a $120 cap must go through."""
    engine, _ = _exposure_engine(fake_redis, cap=120.0)
    engine.active_positions["indodax:btc_idr"] = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        spot_cost_usdt=50.0,
        futures_margin_usdt=50.0,
    )

    sig = _half_budget_signal("eth_idr", "ETHUSDT", int(time.time() * 1000))
    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig))
    await engine.handle_signal_notification("indodax:eth_idr")

    assert "indodax:eth_idr" in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_p2_04_zero_cap_disables_check(fake_redis):
    """max_exposure_per_exchange_usdt=0 disables the exposure cap."""
    engine, _ = _exposure_engine(fake_redis, cap=0.0)
    for tag, sym, gsym in (
        ("indodax:btc_idr", "btc_idr", "BTCUSDT"),
        ("indodax:eth_idr", "eth_idr", "ETHUSDT"),
    ):
        sig = _half_budget_signal(sym, gsym, int(time.time() * 1000))
        await fake_redis.set(f"arb:signal:{tag}", json.dumps(sig))
        await engine.handle_signal_notification(tag)

    assert "indodax:btc_idr" in engine.pending_entry_orders
    assert "indodax:eth_idr" in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_p1_03_daily_loss_limit_halts_new_entries(fake_redis):
    """BUG-08/P1-03: cumulative daily loss beyond the limit stops new entries."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_daily_loss_usdt=10.0),
        client=client,
        redis_client=fake_redis,
    )
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms)))

    # A loss under the limit must not halt anything.
    engine.record_realized_pnl(-4.0)
    assert engine._trading_halted is False

    await engine.handle_signal_notification(pair_tag)
    assert pair_tag in engine.pending_entry_orders
    client.entry_orders = 0

    # Crossing the limit halts entries.
    engine.record_realized_pnl(-7.0)
    assert engine._trading_halted is True
    assert engine.status_snapshot()["trading_halted"] is True
    assert engine.status_snapshot()["daily_realized_loss_usdt"] == pytest.approx(11.0)

    other_pair = "indodax:eth_idr"
    await fake_redis.set(f"arb:signal:{other_pair}", json.dumps(_fresh_signal_payload(now_ms)))
    await engine.handle_signal_notification(other_pair)
    assert other_pair not in engine.pending_entry_orders

    # Manual resume lets trading continue.
    await engine.resume_trading("operator reviewed")
    assert engine._trading_halted is False

@pytest.mark.asyncio
async def test_p1_03_profits_do_not_halt_and_zero_limit_disables(fake_redis):
    """A zero limit disables the breaker; profits never trip it."""
    client = MockExchangeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_daily_loss_usdt=0.0),
        client=client,
        redis_client=fake_redis,
    )
    # A disabled limit never halts, however large the loss.
    engine.record_realized_pnl(-1_000.0)
    assert engine._trading_halted is False
    assert engine._daily_realized_loss_usdt == pytest.approx(1_000.0)

    # With a limit set, a profitable trade must not add to the loss tally.
    engine.config.max_daily_loss_usdt = 5.0
    engine._daily_realized_loss_usdt = 0.0
    engine.record_realized_pnl(100.0)
    assert engine._trading_halted is False
    assert engine._daily_realized_loss_usdt == 0.0

@pytest.mark.asyncio
async def test_p1_04_status_snapshot_reports_failures(fake_redis):
    """P1-04: a failed pair is queryable through the status snapshot."""
    client = MockExchangeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    engine._mark_failed("indodax:btc_idr", "HEDGE_FAILED", "rollback failed", {"qty": 0.005})
    engine.states["indodax:eth_idr"] = PositionState.HEDGED_ACTIVE

    snapshot = engine.status_snapshot()
    assert snapshot["failed_pairs"] == 1
    assert snapshot["positions"]["indodax:btc_idr"]["state"] == "FAILED"
    assert snapshot["failures"]["indodax:btc_idr"]["reason"] == "HEDGE_FAILED"

    await engine.publish_status()
    raw = await fake_redis.get("arb:executor:status")
    assert raw is not None
    assert json.loads(raw)["failed_pairs"] == 1

    engine.clear_failure("indodax:btc_idr")
    assert engine.states["indodax:btc_idr"] == PositionState.IDLE
    assert engine.status_snapshot()["failed_pairs"] == 0

@pytest.mark.asyncio
async def test_p1_01_concurrent_signals_place_only_one_entry_order(fake_redis):
    """BUG-05/P1-01: two simultaneous signals must not double the spot position."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = SlowLimitBuyClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms)))

    await asyncio.gather(
        engine.handle_signal_notification(pair_tag),
        engine.handle_signal_notification(pair_tag),
        engine.handle_signal_notification(pair_tag),
    )

    assert client.entry_orders == 1
    assert len(engine.pending_entry_orders) == 1

@pytest.mark.asyncio
async def test_kill_switch_on_halt_and_off_resume(engine, fake_redis):
    """Kill switch ON halts entries; only an explicit OFF clears it (no UTC auto-reset)."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms)))

    await engine.handle_kill_switch(json.dumps({"action": "on", "reason": "TEST"}))
    assert engine._trading_halted is True
    assert engine._halt_reason == "KILL_SWITCH"

    await engine.handle_signal_notification(pair_tag)
    assert pair_tag not in engine.pending_entry_orders

    # OFF resumes
    await engine.handle_kill_switch(json.dumps({"action": "off"}))
    assert engine._trading_halted is False

    # Plain text forms also work
    await engine.handle_kill_switch("on")
    assert engine._halt_reason == "KILL_SWITCH"
    await engine.handle_kill_switch("off")
    assert engine._trading_halted is False

@pytest.mark.asyncio
async def test_kill_switch_survives_utc_day_rollover(engine):
    """DAILY_LOSS_LIMIT clears on rollover; KILL_SWITCH must not."""
    engine._trading_halted = True
    engine._halt_reason = "KILL_SWITCH"
    engine._loss_day = "2000-01-01"
    engine._roll_loss_day_if_needed()
    assert engine._trading_halted is True
    assert engine._halt_reason == "KILL_SWITCH"

    engine._halt_reason = "DAILY_LOSS_LIMIT"
    engine._loss_day = "2000-01-01"
    engine._roll_loss_day_if_needed()
    assert engine._trading_halted is False

@pytest.mark.asyncio
async def test_balance_mismatch_halts_venue(engine):
    """A large unexplained balance drop halts the venue for new entries."""
    engine.config.balance_mismatch_fraction = 0.20
    client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)
    engine.client = client

    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis_set(engine, f"arb:signal:{pair_tag}", now_ms)

    # Baseline observation: no drop.
    await engine.handle_signal_notification(pair_tag)
    assert engine._exchange_halted == {}

    # Simulate 500 USDT vanishing with no committed capital to explain it.
    client.spot_available = 500.0
    await engine.handle_signal_notification(pair_tag)
    assert "indodax" in engine._exchange_halted
    assert "balance mismatch" in engine._exchange_halted["indodax"]

    # A drop that stays inside the fraction threshold must not halt.
    engine.config.balance_mismatch_fraction = 0.20
    engine2_client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)
    engine.client = engine2_client
    engine._exchange_halted.clear()
    engine._last_balances.clear()
    engine2_client.spot_available = 900.0  # 10% drop < 20%
    await engine.handle_signal_notification(pair_tag)
    assert engine._exchange_halted == {}

@pytest.mark.asyncio
async def test_ambiguous_order_submit_halts_pair_without_retry(engine):
    """An exchange transport failure is ambiguous and must halt before a retry."""
    client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)
    engine.client = client

    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis_set(engine, f"arb:signal:{pair_tag}", now_ms)

    client.fail_next_buy = True
    await engine.handle_signal_notification(pair_tag)

    assert pair_tag in engine._recovery_scope_halts
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert "indodax" not in engine._exchange_halted

    # The same signal cannot trigger another submit while acknowledgement is unresolved.
    client.fail_next_buy = False
    orders_before = len(client.placed_buys)
    await engine.handle_signal_notification(pair_tag)
    assert len(client.placed_buys) == orders_before

def test_resume_exchange_clears_halt_and_failures(engine):
    engine._exchange_halted["okx"] = "test"
    engine._consecutive_order_failures["okx"] = 3
    assert engine.resume_exchange("okx") is True
    assert "okx" not in engine._exchange_halted
    assert "okx" not in engine._consecutive_order_failures
    assert engine.resume_exchange("okx") is False


@pytest.mark.asyncio
async def test_balance_feed_failure_halts_even_when_mismatch_threshold_disabled(engine, fake_redis):
    """Disabling mismatch detection must not disable fail-closed balance availability."""
    engine.config.balance_mismatch_fraction = 0.0
    client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)

    def fail_balances():
        raise TimeoutError("balance endpoint unavailable")

    client.get_balances = fail_balances
    engine.client = client
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(
        f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms))
    )

    await engine.handle_signal_notification(pair_tag)

    assert pair_tag not in engine.pending_entry_orders
    assert "indodax" in engine._exchange_halted
    assert "binance" in engine._exchange_halted


@pytest.mark.asyncio
async def test_balance_feed_failure_halts_new_entries(engine, fake_redis):
    """A failed balance read blocks new entries but does not stop position management."""
    client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)

    def fail_balances():
        raise TimeoutError("exchange balance endpoint unavailable")

    client.get_balances = fail_balances
    engine.client = client
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(
        f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms))
    )

    await engine.handle_signal_notification(pair_tag)

    assert pair_tag not in engine.pending_entry_orders
    assert "indodax" in engine._exchange_halted
    assert "binance" in engine._exchange_halted
    assert "balance feed unavailable" in engine._exchange_halted["indodax"]


@pytest.mark.asyncio
async def test_pair_auto_blacklist_after_two_consecutive_failures(engine, fake_redis):
    """A pair that fails 2x consecutively is blacklisted and rejects subsequent signals."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(
        f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms))
    )

    # 1st failure
    engine._mark_failed(pair_tag, "EXECUTOR_VENUE_MISMATCH", "first failure")
    assert pair_tag not in getattr(engine, "_pair_halted", {})
    assert engine._consecutive_pair_failures[pair_tag] == 1

    # 2nd failure triggers blacklist
    engine._mark_failed(pair_tag, "EXECUTOR_VENUE_MISMATCH", "second failure")
    assert pair_tag in engine._pair_halted
    assert "blacklisted" in engine._pair_halted[pair_tag]

    # Signal for blacklisted pair rejected
    client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)
    engine.client = client
    orders_before = len(client.placed_buys)
    await engine.handle_signal_notification(pair_tag)
    assert len(client.placed_buys) == orders_before
    assert pair_tag not in engine.pending_entry_orders

    # resume_pair clears blacklist and allows entries again
    assert engine.resume_pair(pair_tag) is True
    assert pair_tag not in engine._pair_halted
    assert pair_tag not in engine._consecutive_pair_failures


@pytest.mark.asyncio
async def test_live_entry_does_not_read_balance_before_order(engine, fake_redis):
    """Live order submission, not a balance endpoint, decides whether funds suffice."""
    client = BalancesMockClient()

    def fail_balances():
        raise TimeoutError("balance endpoint rate limited")

    client.get_balances = fail_balances
    engine.config.mock_mode = False
    engine.config.global_exchange_name = "binance"
    engine.client = client
    # Explicitly inject an allow gate: this test exercises the downstream
    # balance/order behavior, not the V2 control-plane denial path.
    engine.v2_admission_gate = AsyncMock()
    # This test isolates balance-probe behavior; the separate adaptive-maker
    # tests cover the fresh Indodax book preflight.
    engine._apply_indodax_adaptive_maker_quote = AsyncMock(return_value=True)
    # This test isolates balance-probe behavior; order-journal durability is
    # covered by the dedicated order-recovery tests.
    engine._register_order_intent = AsyncMock(return_value=object())
    pair_tag = "indodax:btc_idr"
    await fake_redis.set(
        f"arb:signal:{pair_tag}",
        json.dumps(_fresh_signal_payload(int(time.time() * 1000))),
    )

    await engine.handle_signal_notification(pair_tag)

    assert len(client.placed_buys) == 1
    assert engine._exchange_halted == {}


@pytest.mark.asyncio
async def test_live_order_rejection_notifies_exchange_insufficient_balance(
    engine, fake_redis
):
    """An insufficient-funds rejection is reported with the venue's reason."""
    client = BalancesMockClient()

    def fail_balances():
        raise TimeoutError("balance endpoint rate limited")

    async def reject_order(*args, **kwargs):
        raise RuntimeError("InsufficientFunds: insufficient balance")

    client.get_balances = fail_balances
    client.place_local_limit_buy = reject_order
    engine.config.mock_mode = False
    engine.config.global_exchange_name = "binance"
    engine.client = client
    # Explicitly inject an allow gate: this test exercises venue rejection
    # mapping after control-plane admission has succeeded.
    engine.v2_admission_gate = AsyncMock()
    # Keep this test focused on mapping the venue rejection to its public
    # failure reason; adaptive-maker book validation is covered separately.
    engine._apply_indodax_adaptive_maker_quote = AsyncMock(return_value=True)
    # failure reason; durable intent enforcement has dedicated coverage.
    engine._register_order_intent = AsyncMock(return_value=object())
    failures = []

    async def capture_failure(pair_tag, reason, message, **kwargs):
        failures.append((reason, message, kwargs.get("detail", {})))

    engine._publish_execution_failure = capture_failure
    pair_tag = "indodax:btc_idr"
    await fake_redis.set(
        f"arb:signal:{pair_tag}",
        json.dumps(_fresh_signal_payload(int(time.time() * 1000))),
    )

    await engine.handle_signal_notification(pair_tag)

    assert len(failures) == 1
    reason, message, detail = failures[0]
    assert reason == "INSUFFICIENT_BALANCE"
    assert "InsufficientFunds: insufficient balance" in message
    assert detail["exchange_error"] == "InsufficientFunds: insufficient balance"
    assert engine._exchange_halted == {}


def test_business_rejections_do_not_count_toward_exchange_failure_breaker(engine):
    engine.config.max_consecutive_order_failures = 50
    engine._record_order_failure("indodax", "insufficient balance")
    engine._record_order_failure("okx", "MIN_NOTIONAL")
    engine._record_order_failure("mexc", "INVALID_SYMBOL")

    assert engine._consecutive_order_failures == {}
    assert engine._exchange_halted == {}


def test_failure_breaker_isolated_per_exchange_and_trips_at_50(engine):
    engine.config.max_consecutive_order_failures = 50
    for _ in range(49):
        engine._record_order_failure("indodax", "network timeout")
    for _ in range(20):
        engine._record_order_failure("okx", "temporary API error")

    assert engine._consecutive_order_failures["indodax"] == 49
    assert engine._consecutive_order_failures["okx"] == 20
    assert "indodax" not in engine._exchange_halted
    assert "okx" not in engine._exchange_halted

    engine._record_order_failure("indodax", "network timeout")
    assert engine._consecutive_order_failures["indodax"] == 50
    assert "indodax" in engine._exchange_halted
    assert "okx" not in engine._exchange_halted


class _RiskStateMemoryWriter:
    enabled = True

    def __init__(self, rows=None, pnl_results=None, state_ok=True):
        self.rows = list(rows or [])
        self.pnl_results = list(pnl_results or [])
        self.state_ok = state_ok
        self.writes = []

    async def fetch_active_risk_states(self, *, is_mock=True):
        return list(self.rows)

    async def fetch_daily_realized_loss_usdt(self, loss_day, *, is_mock=True):
        for row in self.rows:
            value = row.get("value") or {}
            if row.get("state_type") == "daily_loss" and value.get("loss_day") == loss_day:
                return float(value.get("loss_usdt", 0.0))
        return 0.0

    def risk_state(self, **kwargs):
        self.writes.append(kwargs)
        return self.state_ok

    def clear_risk_persistence_failure(self):
        return None

    def risk_persistence_failed(self):
        return False

    def mark_risk_persistence_failure(self):
        return None

    def risk_pnl_event(self, event_id, **kwargs):
        if self.pnl_results:
            return self.pnl_results.pop(0)
        return "inserted"


@pytest.mark.asyncio
async def test_durable_risk_state_restores_global_exchange_daily_and_position_halts(engine):
    engine.supabase = _RiskStateMemoryWriter([
        {"state_type": "trading_halt", "scope": "global", "reason": "KILL_SWITCH", "value": {"halt_reason": "KILL_SWITCH"}},
        {"state_type": "daily_loss", "scope": "global", "reason": "", "value": {"loss_usdt": 12.5, "loss_day": engine._utc_day()}},
        {"state_type": "exchange_halt", "scope": "exchange", "exchange": "okx", "reason": "venue breaker", "value": {}},
        {"state_type": "consecutive_order_failures", "scope": "exchange", "exchange": "okx", "reason": "", "value": {"count": 7}},
        {"state_type": "reconciliation_halt", "scope": "pair", "pair_key": "indodax:btc_idr", "reason": "unresolved order", "value": {}},
        {"state_type": "position_failure", "scope": "position", "pair_key": "indodax:eth_idr", "reason": "leg mismatch", "value": {"reason": "leg mismatch", "state": "RECONCILIATION_REQUIRED"}},
    ])
    await engine._restore_risk_state()

    assert engine._trading_halted is True
    assert engine._halt_reason == "KILL_SWITCH"
    assert engine._daily_realized_loss_usdt == pytest.approx(12.5)
    assert engine._exchange_halted["okx"] == "venue breaker"
    assert engine._consecutive_order_failures["okx"] == 7
    assert engine._recovery_scope_halts["indodax:btc_idr"] == "unresolved order"
    assert engine.states["indodax:eth_idr"] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_manual_resume_survives_restart_without_daily_halt_reappearing(engine):
    engine.supabase = _RiskStateMemoryWriter()
    engine._daily_realized_loss_usdt = 15.0
    engine._trading_halted = True
    engine._halt_reason = "DAILY_LOSS_LIMIT"

    await engine.resume_trading("operator reviewed")
    assert engine._trading_halted is False
    assert any(w["state_type"] == "trading_resume" for w in engine.supabase.writes)

    restored = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_daily_loss_usdt=10.0),
        client=MockExchangeClient(),
        redis_client=fake_redis,
    )
    restored.supabase = _RiskStateMemoryWriter([
        {"state_type": "daily_loss", "scope": "global", "value": {"loss_usdt": 15.0, "loss_day": restored._utc_day()}},
        {"state_type": "trading_resume", "scope": "global", "value": {"loss_day": restored._utc_day()}},
    ])
    await restored._restore_risk_state()
    assert restored._trading_halted is False


def test_duplicate_realized_pnl_event_is_not_counted_twice(engine):
    engine.supabase = _RiskStateMemoryWriter(pnl_results=["inserted", "duplicate"])
    engine.record_realized_pnl(-4.0, event_id="position-close:btc:1", pair_key="btc")
    engine.record_realized_pnl(-4.0, event_id="position-close:btc:1", pair_key="btc")
    assert engine._daily_realized_loss_usdt == pytest.approx(4.0)


def test_risk_persistence_failure_blocks_live_entries(engine):
    engine.config.mock_mode = False
    engine.supabase = _RiskStateMemoryWriter(state_ok=False)
    assert engine._persist_risk_state("trading_halt", reason="test") is False
    assert engine._risk_persistence_failed is True
    assert engine._entry_blocked is True


def test_position_reconciliation_halt_is_pair_scoped(engine):
    engine._mark_failed(
        "indodax:btc_idr", "LEG_MISMATCH", "spot/futures exposure mismatch",
        state=PositionState.RECONCILIATION_REQUIRED,
    )
    assert engine._venue_halted_reason("indodax:btc_idr")
    assert engine._venue_halted_reason("indodax:eth_idr") == ""


@pytest.mark.asyncio
async def test_operator_kill_switch_overrides_daily_loss_halt(engine):
    engine._trading_halted = True
    engine._halt_reason = "DAILY_LOSS_LIMIT"
    await engine.handle_kill_switch(json.dumps({"action": "on"}))
    assert engine._trading_halted is True
    assert engine._halt_reason == "KILL_SWITCH"


@pytest.mark.asyncio
async def test_venue_cooldown_task_propagates_cancellation(engine):
    """Cancelling a cooldown task must remain observable by its task owner."""
    engine._pause_venue_for_cooldown("indodax", 60.0)
    task = engine._venue_cooldown_tasks["indodax"]

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Cancellation must not accidentally clear the active venue halt.
    assert "indodax" in engine._exchange_halted
