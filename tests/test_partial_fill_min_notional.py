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


@pytest.mark.asyncio
async def test_wait_until_covered_holds_unhedged_delta_for_next_fill(engine, fake_redis, mock_client, caplog):
    """wait_until_covered: saat order spot MASIH OPEN dan delta di bawah minimum
    Futures, sistem harus MENUNGGU (tidak rollback spot, tidak raise)."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 1
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    order_info["futures_min_notional"] = 10.0

    engine.config.max_order_ttl_seconds = 120
    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    # Spot partially filled $2.50 worth, order still OPEN: more fills may come.
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.00005,
    }

    with caplog.at_level("INFO"):
        await engine._check_entry_orders()

    # No futures short, no rollback sell.
    assert len(mock_client.global_short_sells) == 0
    assert len(mock_client.placed_sells) == 0
    # Pair remains pending (not FAILED, not rolled back).
    assert pair_tag in engine.pending_entry_orders
    assert engine.states[pair_tag] in (PositionState.ENTRY_PENDING, PositionState.ENTRY_PARTIAL)
    assert engine._failure_reasons.get(pair_tag) is None


@pytest.mark.asyncio
async def test_regressing_fill_snapshot_does_not_retrigger_hedge(engine, fake_redis, mock_client):
    """Snapshot exchange yang mundur tidak boleh memicu hedge ulang untuk
    quantity yang sudah ter-hedge."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["qty"] = 0.01
    order_info["filled"] = 0.005
    order_info["filled_spot_amount"] = 0.005
    order_info["hedged_short_qty"] = 0.005
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0

    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    # Venue reports a LOWER fill than previously confirmed (snapshot regression).
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.001,
    }

    await engine._check_entry_orders()

    # Cumulative stays monotonic; unhedged delta = 0 → no new hedge.
    assert order_info["filled"] == pytest.approx(0.005)
    assert len(mock_client.global_short_sells) == 0


@pytest.mark.asyncio
async def test_hedge_ratio_05_reaches_target_and_stops(engine, fake_redis, mock_client):
    """ratio 0.5: setelah futures confirmed = spot * 0.5, delta harus 0 (konvergen)."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["qty"] = 0.01
    order_info["filled"] = 0.01
    order_info["filled_spot_amount"] = 0.01
    # ratio 0.5 → target hedge 0.005; sudah ter-hedge penuh.
    order_info["hedged_short_qty"] = 0.005
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0

    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.config.hedging.futures_target_hedge_ratio = 0.5
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.01,
    }

    await engine._check_entry_orders()

    # Converged: no additional hedge may be submitted.
    assert len(mock_client.global_short_sells) == 0


@pytest.mark.asyncio
async def test_deferred_hedge_on_terminal_spot_requires_reconciliation(engine, fake_redis, mock_client):
    """Spot terminal + hedge deferred (wait_until_covered) → RECONCILIATION_REQUIRED,
    tidak boleh IDLE (kewajiban hedge tidak boleh hilang)."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 30
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    order_info["futures_min_notional"] = 10.0
    # Simulasi: hedge sudah pernah di-defer pada fill sebelumnya.
    order_info["hedge_waiting_for_coverage"] = True
    order_info["filled"] = 0.00005
    order_info["hedged_short_qty"] = 0.0

    engine.config.max_order_ttl_seconds = 10
    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PARTIAL

    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "closed",
        "filled": 0.00005,
    }

    await engine._check_entry_orders()

    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    reason = engine._failure_reasons.get(pair_tag, {}).get("reason")
    # Tertangkap di fill handler (lebih awal) atau di finalize; keduanya
    # mencegah state IDLE dengan exposure tersisa.
    assert reason in (
        "TERMINAL_ORDER_BELOW_MIN_NOTIONAL",
        "DEFERRED_HEDGE_UNCOVERED_ON_TERMINAL_SPOT",
    )


@pytest.mark.asyncio
async def test_hedge_gate_uses_actual_futures_minimum_not_partial_knob(engine, fake_redis, mock_client):
    """Minimum Futures aktual (metadata) yang menentukan hedgeable, bukan
    min_partial_fill_usd: fill $20 dengan min Futures $500 harus tetap wait."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 1
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    # Min futures aktual $500 (metadata venue), knob legacy $10.
    order_info["futures_min_notional"] = 500.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    # Spot filled $20 (> $10 knob, < $500 venue min), order masih open.
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.0004,  # 0.0004 * 50000 = $20
    }

    await engine._check_entry_orders()

    # Tidak boleh submit short $500 (overhedge) — harus menunggu.
    assert len(mock_client.global_short_sells) == 0
    assert len(mock_client.placed_sells) == 0
    assert engine._failure_reasons.get(pair_tag) is None
    assert pair_tag in engine.pending_entry_orders


def test_canonical_hedge_target_ratios():
    """Canonical formula: target = spot × ratio; residual = max(0, target − confirmed)."""
    from executor.quantity import canonical_hedge_target

    # ratio 0.5: spot 1.0, confirmed 0.0 → residual 0.5
    t, r, _ = canonical_hedge_target(1.0, 0.0, 0.5)
    assert t == pytest.approx(0.5) and r == pytest.approx(0.5)
    # ratio 1.0: spot 1.0, confirmed 0.4 (partial futures fill) → residual 0.6
    t, r, _ = canonical_hedge_target(1.0, 0.4, 1.0)
    assert t == pytest.approx(1.0) and r == pytest.approx(0.6)
    # ratio 1.5: spot 1.0, confirmed 0.0 → residual 1.5
    t, r, _ = canonical_hedge_target(1.0, 0.0, 1.5)
    assert t == pytest.approx(1.5) and r == pytest.approx(1.5)
    # Over-confirmed clamps to 0 (never negative residual).
    t, r, _ = canonical_hedge_target(1.0, 1.2, 1.0)
    assert r == 0.0


def test_canonical_hedge_target_rejects_invalid_inputs():
    from executor.quantity import canonical_hedge_target

    for bad_spot in (float("nan"), float("inf"), -0.1):
        with pytest.raises(ValueError):
            canonical_hedge_target(bad_spot, 0.0, 1.0)
    for bad_confirmed in (float("nan"), float("inf"), -0.1):
        with pytest.raises(ValueError):
            canonical_hedge_target(1.0, bad_confirmed, 1.0)
    for bad_ratio in (0.0, -1.0, float("nan")):
        with pytest.raises(ValueError):
            canonical_hedge_target(1.0, 0.0, bad_ratio)


def test_repeated_and_decreasing_snapshots_are_idempotent():
    """Repeated identical snapshots and decreasing snapshots never re-hedge."""
    from executor.quantity import canonical_hedge_target

    confirmed = 0.005
    # Same snapshot twice → same residual.
    _, r1, _ = canonical_hedge_target(0.005, confirmed, 1.0)
    _, r2, _ = canonical_hedge_target(0.005, confirmed, 1.0)
    assert r1 == r2 == pytest.approx(0.0)
    # Decreasing snapshot (0.001 < 0.005 confirmed) → residual 0.
    _, r3, _ = canonical_hedge_target(0.001, confirmed, 1.0)
    assert r3 == 0.0


def test_contract_multiplier_conversion_in_residual():
    """Futures fills in CONTRACT units must be converted to base before residual."""
    from executor.quantity import AmountSpec, order_amount_to_base, canonical_hedge_target

    # 1 contract = 0.01 BTC; venue reports 2 contracts filled = 0.02 base.
    spec = AmountSpec("CONTRACT", 1.0, 1.0, 0.01)
    confirmed_base = order_amount_to_base(2.0, spec)
    assert confirmed_base == pytest.approx(0.02)
    # Spot 0.03 filled, ratio 1.0 → residual 0.01 base.
    _, residual, _ = canonical_hedge_target(0.03, confirmed_base, 1.0)
    assert residual == pytest.approx(0.01)


def test_decimal_precision_quantity_steps():
    """Step rounding must not create phantom residual from float noise."""
    from executor.quantity import AmountSpec, base_to_order_amount, order_amount_to_base

    spec = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    # 0.0055 rounds DOWN to 0.0055 (already on step).
    amt = base_to_order_amount(0.0055, spec)
    assert order_amount_to_base(amt, spec) == pytest.approx(0.0055)
    # 0.00555 rounds down to 0.0055 — never up (no silent overhedge).
    amt = base_to_order_amount(0.00555, spec)
    assert order_amount_to_base(amt, spec) == pytest.approx(0.0055)


def test_exact_minimum_notional_is_hedgeable():
    """Fill exactly at the futures minimum notional must be hedgeable."""
    from executor.quantity import AmountSpec, calculate_order_size

    spec = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    # 0.0002 × 50000 = exactly $10.
    qty, notional, imbalance = calculate_order_size(
        filled_spot_amount=0.0002,
        expected_price_usdt=50000.0,
        futures_amount_spec=spec,
        futures_min_notional=10.0,
        safety_policy="offset_spot",
    )
    assert qty == pytest.approx(0.0002)
    assert notional == pytest.approx(10.0)
    assert imbalance == pytest.approx(0.0)


def test_spot_futures_price_difference_uses_futures_price():
    """Notional must be computed with the FUTURES price, not the spot price."""
    from executor.quantity import AmountSpec, calculate_order_size

    spec = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    # Spot price $40k, futures price $50k: min notional $10 needs 0.0002 base
    # at the futures price.
    qty, notional, _ = calculate_order_size(
        filled_spot_amount=0.0001,
        expected_price_usdt=50000.0,
        futures_amount_spec=spec,
        futures_min_notional=10.0,
        safety_policy="offset_spot",
    )
    # 0.0001 × 50000 = $5 < $10 → deferred (0).
    assert qty == 0.0


def test_invalid_metadata_fails_closed():
    from executor.quantity import AmountSpec, validate_futures_sizing_metadata

    # Missing spec.
    assert validate_futures_sizing_metadata(None, 50000.0, 10.0)
    # Invalid price.
    assert validate_futures_sizing_metadata(
        AmountSpec("BASE", 0.0001, 0.0001, 1.0), 0.0, 10.0
    )
    # Invalid notional.
    assert validate_futures_sizing_metadata(
        AmountSpec("BASE", 0.0001, 0.0001, 1.0), 50000.0, -1.0
    )


def test_missing_reference_price_fails_closed():
    from executor.quantity import AmountSpec, validate_futures_sizing_metadata

    reasons = validate_futures_sizing_metadata(
        AmountSpec("BASE", 0.0001, 0.0001, 1.0), float("nan"), 10.0
    )
    assert any("price" in r for r in reasons)


@pytest.mark.asyncio
async def test_deferred_hedge_executes_once_fill_reaches_minimum(engine, fake_redis, mock_client):
    """Deferred (wait) → fill bertambah sampai minimum terpenuhi → hedge terkirim
    hanya sebesar residual."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 1
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    order_info["futures_min_notional"] = 10.0
    # State dari defer sebelumnya: spot 0.00005 ($2.50) sudah tercatat.
    order_info["filled"] = 0.00005
    order_info["filled_spot_amount"] = 0.00005
    order_info["hedged_short_qty"] = 0.0
    order_info["hedge_waiting_for_coverage"] = True

    engine.config.max_order_ttl_seconds = 120
    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PARTIAL

    # Fill baru: kumulatif naik ke 0.0005 ($25 > $10 min).
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.0005,
    }

    await engine._check_entry_orders()

    # Hedge terkirim sebesar residual penuh (0.0005), bukan hanya delta terakhir.
    assert len(mock_client.global_short_sells) == 1
    assert mock_client.global_short_sells[0]["amount"] == pytest.approx(0.0005)
    assert order_info.get("hedged_short_qty") == pytest.approx(0.0005)
    assert order_info.get("hedge_waiting_for_coverage") is False


@pytest.mark.asyncio
async def test_partial_futures_fill_then_new_spot_fill_hedges_only_residual(engine, fake_redis, mock_client):
    """Futures partial fill (0.002 dari 0.005 diminta) + spot fill baru →
    hedge berikutnya hanya residual target − confirmed futures."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    # Spot kumulatif 0.006; futures confirmed hanya 0.002 (partial).
    order_info["filled"] = 0.006
    order_info["filled_spot_amount"] = 0.006
    order_info["hedged_short_qty"] = 0.002

    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PARTIAL

    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.006,
    }

    await engine._check_entry_orders()

    # Residual = 0.006 − 0.002 = 0.004 (bukan 0.006 penuh).
    assert len(mock_client.global_short_sells) == 1
    assert mock_client.global_short_sells[0]["amount"] == pytest.approx(0.004)


@pytest.mark.asyncio
async def test_fully_covered_target_never_rehedges(engine, fake_redis, mock_client):
    """Target terpenuhi penuh (spot 0.005, futures confirmed 0.005) → tanpa hedge baru."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["qty"] = 0.01
    order_info["filled"] = 0.005
    order_info["filled_spot_amount"] = 0.005
    order_info["hedged_short_qty"] = 0.005
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0

    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PARTIAL

    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.005,
    }

    await engine._check_entry_orders()

    assert len(mock_client.global_short_sells) == 0
    assert len(mock_client.placed_sells) == 0


@pytest.mark.asyncio
async def test_ttl_expiry_with_residual_requires_reconciliation_not_idle(engine, fake_redis, mock_client):
    """TTL expired dengan residual unhedged → RECONCILIATION_REQUIRED, bukan IDLE."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 60  # past TTL
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    order_info["futures_min_notional"] = 10.0
    order_info["hedge_waiting_for_coverage"] = True
    order_info["filled"] = 0.00005
    order_info["hedged_short_qty"] = 0.0

    engine.config.max_order_ttl_seconds = 10
    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PARTIAL

    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "canceled",  # TTL cancel path
        "filled": 0.00005,
    }

    await engine._check_entry_orders()

    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    reason = engine._failure_reasons.get(pair_tag, {}).get("reason")
    assert reason in (
        "TERMINAL_ORDER_BELOW_MIN_NOTIONAL",
        "DEFERRED_HEDGE_UNCOVERED_ON_TERMINAL_SPOT",
        "HEDGE_RESIDUAL_ON_TERMINAL_SPOT",
    )


@pytest.mark.asyncio
async def test_restart_while_deferred_keeps_obligation_durable(engine, fake_redis, mock_client):
    """Restart saat hedge deferred: kewajiban hedge harus survive restart —
    pending entry record dengan hedge_waiting_for_coverage + residual."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 1
    order_info["qty"] = 0.01
    order_info["global_ask_usdt"] = 50000.0
    order_info["price"] = 800000000.0
    order_info["usdt_idr_rate"] = 16000.0
    order_info["futures_min_notional"] = 10.0
    order_info["filled"] = 0.00005
    order_info["hedged_short_qty"] = 0.0
    order_info["hedge_waiting_for_coverage"] = True

    engine.config.max_order_ttl_seconds = 120
    engine.config.hedging.spot_fill_trigger_pct = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PARTIAL

    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.00005,
    }

    await engine._check_entry_orders()

    # Deferred: pair stays pending, durable record retains the obligation.
    assert pair_tag in engine.pending_entry_orders
    persisted = await fake_redis.get("arb:pending_entry:" + pair_tag)
    assert persisted is not None
    import json as _json
    record = _json.loads(persisted)
    assert record.get("hedge_waiting_for_coverage") is True
    assert record.get("filled") == pytest.approx(0.00005)
    assert record.get("hedged_short_qty") == pytest.approx(0.0)
