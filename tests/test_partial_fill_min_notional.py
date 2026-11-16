"""Unit tests for partial fills, dynamic futures sizing, and minimum notional constraints."""

import pytest
import time
from decimal import Decimal

from executor.models import PositionState
from executor.quantity import AmountSpec, calculate_order_size, base_to_order_amount
from tests.conftest import MockExchangeClient, _entry_order_info


def test_calculate_order_size_1_to_1_when_above_min_notional():
    """When filled spot notional >= futures min notional, futures size is 1:1 with spot fill."""
    spec = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    filled_spot = 0.001
    price = 60000.0  # notional = $60
    min_notional = 10.0

    target_size, target_notional, delta_imbalance = calculate_order_size(
        filled_spot_amount=filled_spot,
        expected_price_usdt=price,
        futures_amount_spec=spec,
        futures_min_notional=min_notional,
    )

    assert target_size == pytest.approx(0.001)
    assert target_notional == pytest.approx(60.0)
    assert delta_imbalance == pytest.approx(0.0)


def test_calculate_order_size_scales_to_min_notional_with_delta_imbalance():
    """When filled spot notional < futures min notional, scale futures size up and report delta imbalance."""
    spec = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    filled_spot = 0.00005  # notional = 0.00005 * 50000 = $2.50
    price = 50000.0
    min_notional = 10.0  # requires 10 / 50000 = 0.0002 BTC

    target_size, target_notional, delta_imbalance = calculate_order_size(
        filled_spot_amount=filled_spot,
        expected_price_usdt=price,
        futures_amount_spec=spec,
        futures_min_notional=min_notional,
        safety_policy="scale_to_min",
    )

    assert target_size == pytest.approx(0.0002)
    assert target_notional == pytest.approx(10.0)
    assert delta_imbalance == pytest.approx(0.00015)  # 0.0002 - 0.00005


def test_calculate_order_size_scales_to_contract_market_minimum():
    """For contract specs, scale to minimum contract unit when spot fill is smaller."""
    # 1 contract = 0.01 BTC ($600 at $60,000)
    contract_spec = AmountSpec("CONTRACT", 1.0, 1.0, 0.01)
    filled_spot = 0.003  # less than 1 contract
    price = 60000.0

    target_size, target_notional, delta_imbalance = calculate_order_size(
        filled_spot_amount=filled_spot,
        expected_price_usdt=price,
        futures_amount_spec=contract_spec,
        futures_min_notional=5.0,
        safety_policy="scale_to_min",
    )

    # Must scale to 1 contract = 0.01 base
    assert target_size == pytest.approx(0.01)
    assert target_notional == pytest.approx(600.0)
    assert delta_imbalance == pytest.approx(0.007)


def test_calculate_order_size_offset_policy_returns_zero_on_sub_minimum():
    """When safety policy is offset_spot, return size 0 to signal cancellation/offset."""
    spec = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    filled_spot = 0.00005  # $2.50
    price = 50000.0
    min_notional = 10.0

    target_size, target_notional, delta_imbalance = calculate_order_size(
        filled_spot_amount=filled_spot,
        expected_price_usdt=price,
        futures_amount_spec=spec,
        futures_min_notional=min_notional,
        safety_policy="offset_spot",
    )

    assert target_size == 0.0
    assert target_notional == 0.0
    assert delta_imbalance == pytest.approx(-0.00005)


@pytest.mark.asyncio
async def test_order_info_tracks_filled_spot_amount_in_realtime(engine, fake_redis, mock_client):
    """Logika ukuran order memperbarui variabel filled_spot_amount secara real-time setelah respons API spot."""
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["qty"] = 0.05
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.00012,
    }

    await engine._check_entry_orders()

    assert order_info.get("filled_spot_amount") == pytest.approx(0.00012)
    assert order_info.get("filled") == pytest.approx(0.00012)


@pytest.mark.asyncio
async def test_futures_executor_rejects_sub_minimum_fill_under_wait_until_covered(engine, fake_redis, mock_client, caplog):
    """Dengan kebijakan wait_until_covered (default), spot fill di bawah minimum notional
    TIDAK boleh di-scale menjadi short futures (overhedge); harus memicu offset/unwind."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 30
    order_info["qty"] = 0.01
    # Global futures ask: $50,000, price IDR: 800,000,000, USDT/IDR: 16,000 ($50,000)
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    order_info["futures_min_notional"] = 10.0  # 10 USDT min requirement

    engine.config.max_order_ttl_seconds = 10
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    # Spot fully filled 0.01 BTC (past the 20% threshold) so only the
    # min-notional rule applies. Notional is 0.01 * 50000 = $500, well above $10
    # min — sanity check the baseline path hedges normally.
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "closed",
        "filled": 0.01,
    }

    with caplog.at_level("WARNING"):
        await engine._check_entry_orders()

    # Fill above minimum: hedge must go out 1:1.
    assert len(mock_client.global_short_sells) == 1
    assert mock_client.global_short_sells[0]["amount"] == pytest.approx(0.01)


@pytest.mark.asyncio
async def test_futures_executor_does_not_overhedge_below_min_notional(engine, fake_redis, mock_client, caplog):
    """wait_until_covered: fill $2.50 (di bawah min $10) tidak boleh di-scale ke $10 short."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 30
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    order_info["futures_min_notional"] = 10.0

    engine.config.max_order_ttl_seconds = 10
    # Tekan threshold supaya aturan threshold tidak ikut campur; yang diuji
    # di sini murni aturan minimum notional dengan policy wait_until_covered.
    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    # Spot only filled 0.00005 BTC -> Notional is 0.00005 * 50000 = $2.50 (below $10 min)
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "closed",
        "filled": 0.00005,
    }

    with caplog.at_level("WARNING"):
        await engine._check_entry_orders()

    # No futures short may be submitted: scaling up would overhedge.
    assert len(mock_client.global_short_sells) == 0
    # The policy-driven rejection must be surfaced loudly.
    assert any(
        "TERMINAL_ORDER_BELOW_MIN_NOTIONAL" in record.message
        for record in caplog.records
    )
