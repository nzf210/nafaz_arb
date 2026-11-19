"""Tests for per-CEX local market single-flight gating.

Ensures no duplicate positions or pending orders can open for the same
coin (base asset) and quote currency on the same local exchange (e.g. Tokocrypto SPX/IDR),
while allowing different quotes on the same CEX (e.g. SPX/USDT) or different CEXes (Indodax SPX/IDR).
"""
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from executor.engine_signal import EngineSignalMixin
from executor.helpers import parse_pair_tag_local_market, normalize_quote_asset
from executor.models import ActivePosition, ExecutorPayload, PairInfo, PricingSnapshot, ExecutionDirectives


def test_parse_pair_tag_local_market():
    assert parse_pair_tag_local_market("tokocrypto:toko:spx_idr__okx:spx_usdt_swap") == ("tokocrypto", "SPX", "idr")
    assert parse_pair_tag_local_market("indodax:dodoidr__mexc:mexc:dodo_usdt") == ("indodax", "DODO", "idr")
    assert parse_pair_tag_local_market("indodax:btcusdt__okx:btc_usdt_swap") == ("indodax", "BTC", "usdt")
    assert parse_pair_tag_local_market("tokocrypto:toko:ath_usdt__okx:ath_usdt_swap") == ("tokocrypto", "ATH", "usdt")
    assert normalize_quote_asset("BIDR") == "idr"
    assert normalize_quote_asset("IDR") == "idr"
    assert normalize_quote_asset("USDT") == "usdt"
    assert normalize_quote_asset("USDC") == "usdt"


class _DummyEngine(EngineSignalMixin):
    def __init__(self):
        self.active_positions = {}
        self.pending_entry_orders = {}
        self.states = {}
        self._pair_infos = {}
        self.redis = None
        self.config = SimpleNamespace(mock_mode=True, max_stale_latency_ms=1000)


@pytest.mark.asyncio
async def test_duplicate_local_market_blocked_same_quote():
    engine = _DummyEngine()
    # Active SPX/IDR position on Tokocrypto
    engine.active_positions["tokocrypto:toko:spx_idr__okx:spx_usdt_swap"] = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="okx",
        symbol="toko:SPX_IDR",
        entry_price_idr=5800.0,
        short_entry_usdt=0.38,
        qty=100.0,
        base_asset="SPX",
    )

    # 1. Same CEX, same coin, same quote -> BLOCKED
    assert await engine._has_active_or_pending_market("tokocrypto", "SPX", "IDR") is True
    assert await engine._has_active_or_pending_market("tokocrypto", "SPX", "BIDR") is True

    # 2. Same CEX, same coin, DIFFERENT quote (USDT) -> ALLOWED
    assert await engine._has_active_or_pending_market("tokocrypto", "SPX", "USDT") is False

    # 3. DIFFERENT CEX (Indodax), same coin, same quote (IDR) -> ALLOWED
    assert await engine._has_active_or_pending_market("indodax", "SPX", "IDR") is False

    # 4. Same CEX, DIFFERENT coin (BTC) -> ALLOWED
    assert await engine._has_active_or_pending_market("tokocrypto", "BTC", "IDR") is False


@pytest.mark.asyncio
async def test_is_stale_or_duplicate_signal_blocks_same_market():
    engine = _DummyEngine()
    engine.active_positions["tokocrypto:toko:spx_idr__okx:spx_usdt_swap"] = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="okx",
        symbol="toko:SPX_IDR",
        entry_price_idr=5800.0,
        short_entry_usdt=0.38,
        qty=100.0,
        base_asset="SPX",
    )

    # Incoming signal for SPX/IDR paired with Bybit instead of OKX
    pair_info_duplicate = PairInfo(
        local_exchange="tokocrypto",
        local_symbol="toko:SPX_IDR",
        global_exchange="bybit",
        global_symbol="SPXUSDT",
        base_asset="SPX",
        quote_asset="IDR",
        local_price_tick=1.0,
        local_lot_size=1.0,
    )
    payload_duplicate = SimpleNamespace(
        pair_info=pair_info_duplicate,
        pricing_snapshot=SimpleNamespace(is_stale=lambda _: False),
    )

    tag_bybit = "tokocrypto:toko:spx_idr__bybit:spxusdt"
    is_blocked = await engine._is_stale_or_duplicate_signal(tag_bybit, payload_duplicate)
    assert is_blocked is True

    # Incoming signal for SPX/USDT on Tokocrypto -> ALLOWED
    pair_info_usdt = PairInfo(
        local_exchange="tokocrypto",
        local_symbol="toko:SPX_USDT",
        global_exchange="okx",
        global_symbol="SPX-USDT-SWAP",
        base_asset="SPX",
        quote_asset="USDT",
        local_price_tick=0.0001,
        local_lot_size=1.0,
    )
    payload_usdt = SimpleNamespace(
        pair_info=pair_info_usdt,
        pricing_snapshot=SimpleNamespace(is_stale=lambda _: False),
    )
    tag_usdt = "tokocrypto:toko:spx_usdt__okx:spx_usdt_swap"
    is_blocked_usdt = await engine._is_stale_or_duplicate_signal(tag_usdt, payload_usdt)
    assert is_blocked_usdt is False
