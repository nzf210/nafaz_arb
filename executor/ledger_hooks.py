"""Ledger recording helpers for the four arbitrage legs (Tahap 4.1).

Each helper derives the actual execution values from the venue payload, so the
ledger never records a requested price as if it were a fill. The helpers are
best-effort: a missing ledger row must not block an exit or emergency unwind.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from executor import helpers
from executor.engine_common import logger
from executor.fill_ledger import (
    FUTURES_ENTRY,
    FUTURES_EXIT,
    SPOT_ENTRY,
    SPOT_EXIT,
)


def _ledger(engine: Any):
    ledger = getattr(engine, "fill_ledger", None)
    return ledger if ledger is not None and getattr(ledger, "enabled", False) else None


def _fill_from_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """Rebuild a single-fill payload from a cumulative order snapshot.

    Venue order payloads only carry the cumulative average, so this is a
    documented approximation used when per-trade history is not available.

    The synthesised id includes the cumulative filled quantity. A maker order
    that fills in several chunks is polled repeatedly with the same order id but
    a growing cumulative quantity; keying on the order id alone would make the
    ledger treat every later chunk as a replay of the first and drop it. Keying
    on (order id, cumulative filled) keeps a repeated poll of the same watermark
    idempotent while still recording each new increment.
    """
    if not isinstance(order, dict):
        return {}
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    fee = order.get("fee") if isinstance(order.get("fee"), dict) else {}
    trades = order.get("trades") if isinstance(order.get("trades"), list) else []
    if len(trades) == 1 and isinstance(trades[0], dict):
        return trades[0]
    order_id = str(order.get("id") or "")
    cumulative = order.get("filled")
    try:
        watermark = f"{float(cumulative):.18g}"
    except (TypeError, ValueError):
        watermark = "unknown"
    return {
        "id": f"{order_id}:{watermark}" if order_id else "",
        "amount": order.get("filled"),
        "price": order.get("average"),
        "cost": order.get("cost"),
        "timestamp": order.get("timestamp"),
        "fee": fee or None,
        "info": info,
        "fee_source": "ORDER_CUMULATIVE_AVERAGE",
    }


class LedgerMixin:
    """Records actual fills for spot/perp entry and exit legs."""

    async def _record_ledger_fill(
        self,
        *,
        pair_tag: str,
        leg: str,
        exchange: str,
        market_type: str,
        symbol: str,
        side: str,
        qty: float,
        order: Optional[Dict[str, Any]] = None,
        price: Optional[float] = None,
        expected_price: Optional[float] = None,
        fx_rate: Optional[float] = None,
        fx_rate_source: str = "",
        quote_asset: str = "",
        realized_funding_usd: float = 0.0,
        client_order_id: str = "",
    ) -> Optional[str]:
        """Append one leg execution to the immutable ledger."""
        ledger = _ledger(self)
        if ledger is None:
            return None
        order = order if isinstance(order, dict) else {}
        actual_price = float(price or helpers.extract_fill_price(order) or 0.0)
        try:
            filled = float(qty)
        except (TypeError, ValueError):
            filled = 0.0
        if filled <= 0 or actual_price <= 0:
            logger.info(
                "[Fill Ledger] skipping %s %s: no verifiable fill (qty=%s price=%s)",
                pair_tag, leg, filled, actual_price,
            )
            return None
        try:
            return await ledger.record_fill(
                pair_key=pair_tag,
                leg=leg,
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                side=side,
                qty=filled,
                actual_vwap_price=actual_price,
                fill=_fill_from_order(order),
                exchange_order_id=str(order.get("id") or ""),
                client_order_id=client_order_id,
                expected_vwap_price=expected_price,
                quote_asset=quote_asset,
                fx_rate_applied=fx_rate,
                fx_rate_source=fx_rate_source,
                realized_funding_usd=realized_funding_usd,
                is_mock=bool(getattr(self.config, "mock_mode", True)),
            )
        except Exception:
            logger.exception("[Fill Ledger] %s %s append failed", pair_tag, leg)
            return None

    async def _record_spot_entry_fill(
        self, pair_tag: str, order_info: dict, order: dict,
        qty: float, price_idr: float, usdt_rate: float,
    ) -> Optional[str]:
        return await self._record_ledger_fill(
            pair_tag=pair_tag,
            leg=SPOT_ENTRY,
            exchange=str(order_info.get("local_exchange") or ""),
            market_type="spot",
            symbol=str(order_info.get("symbol") or ""),
            side="buy",
            qty=qty,
            order=order,
            price=price_idr,
            expected_price=_finite_or_none(order_info.get("price")),
            fx_rate=usdt_rate,
            fx_rate_source="scanner_payload",
            quote_asset=str(order_info.get("quote_asset") or ""),
            client_order_id=str(order_info.get("client_order_id") or ""),
        )

    async def _record_futures_entry_fill(
        self, pair_tag: str, order_info: dict, short_order: dict,
        qty: float, usdt_rate: float,
    ) -> Optional[str]:
        return await self._record_ledger_fill(
            pair_tag=pair_tag,
            leg=FUTURES_ENTRY,
            exchange=str(order_info.get("global_exchange") or ""),
            market_type="perpetual",
            symbol=str(order_info.get("global_symbol") or ""),
            side="sell",
            qty=qty,
            order=short_order,
            expected_price=_finite_or_none(order_info.get("global_ask_usdt")),
            fx_rate=usdt_rate,
            fx_rate_source="scanner_payload",
            quote_asset="USDT",
            client_order_id=str(order_info.get("hedge_coid") or ""),
        )

    async def _record_spot_exit_fill(
        self, pair_tag: str, pos: Any, order: dict,
        qty: float, exit_price_idr: float, usdt_rate: float,
        realized_funding_usd: float = 0.0,
    ) -> Optional[str]:
        return await self._record_ledger_fill(
            pair_tag=pair_tag,
            leg=SPOT_EXIT,
            exchange=str(getattr(pos, "local_exchange", "") or ""),
            market_type="spot",
            symbol=str(getattr(pos, "symbol", "") or ""),
            side="sell",
            qty=qty,
            order=order,
            price=exit_price_idr,
            expected_price=_finite_or_none(getattr(pos, "entry_price_idr", 0.0)),
            fx_rate=usdt_rate,
            fx_rate_source="exit_snapshot",
            quote_asset=str(getattr(pos, "quote_asset", "") or ""),
            realized_funding_usd=realized_funding_usd,
        )

    async def _record_futures_exit_fill(
        self, pair_tag: str, pos: Any, close_order: dict,
        qty: float, exit_price_usdt: float, usdt_rate: float,
    ) -> Optional[str]:
        return await self._record_ledger_fill(
            pair_tag=pair_tag,
            leg=FUTURES_EXIT,
            exchange=str(getattr(pos, "global_exchange", "") or ""),
            market_type="perpetual",
            symbol=str(getattr(pos, "global_symbol", "") or pos.symbol or ""),
            side="buy",
            qty=qty,
            order=close_order,
            price=exit_price_usdt,
            expected_price=_finite_or_none(getattr(pos, "short_entry_usdt", 0.0)),
            fx_rate=usdt_rate,
            fx_rate_source="exit_snapshot",
            quote_asset="USDT",
        )


    async def _record_expected_vs_realized(
        self,
        pair_tag: str,
        pos: Any,
        closed_trade: dict,
        closed_qty: float,
        *,
        price_idr: float = 0.0,
        usdt_rate: float = 0.0,
    ) -> Optional[str]:
        """Attribute expected vs realized PnL once the position is fully closed."""
        ledger = _ledger(self)
        if ledger is None:
            return None
        try:
            remaining = float(getattr(pos, "qty", 0.0) or 0.0)
        except (TypeError, ValueError):
            remaining = 0.0
        if remaining > 1e-9:
            # Only full closes produce a comparable expected-vs-realized pair;
            # partial closes stay in the fill ledger until the round trip ends.
            return None

        total_qty = float(getattr(pos, "initial_qty", 0.0) or 0.0) or float(closed_qty)
        ratio = min(1.0, closed_qty / total_qty) if total_qty > 0 else 1.0
        expected_net = float(getattr(pos, "expected_net_profit_usdt", 0.0) or 0.0) * ratio
        expected_fees = float(getattr(pos, "expected_fees_usdt", 0.0) or 0.0) * ratio
        realized_net = float(closed_trade.get("net_profit_usdt") or 0.0)
        realized_fees = float(closed_trade.get("fees_usdt") or 0.0)

        opened_at_ms = int(getattr(pos, "opened_at_ms", 0) or 0)
        closed_at_ms = int(closed_trade.get("closed_at_ms") or 0)
        realized_hours = (
            (closed_at_ms - opened_at_ms) / 3_600_000.0
            if opened_at_ms > 0 and closed_at_ms > opened_at_ms
            else None
        )
        expected_hours = float(getattr(pos, "expected_hold_hours", 0.0) or 0.0) or None

        return await ledger.record_deviation(
            pair_key=pair_tag,
            expected_net_pnl_usd=expected_net,
            realized_net_pnl_usd=realized_net,
            expected_fees_usd=expected_fees,
            realized_fees_usd=realized_fees,
            funding_deviation_usd=0.0,
            expected_hold_hours=expected_hours,
            realized_hold_hours=realized_hours,
            trade_key=str(closed_trade.get("trade_id") or "") or None,
            is_mock=bool(getattr(self.config, "mock_mode", True)),
            closed_at=closed_at_ms or None,
            detail={
                "closed_qty": closed_qty,
                "total_qty": total_qty,
                "spot_exit_price_idr": price_idr,
                "usdt_idr_rate": usdt_rate,
                "fee_source": closed_trade.get("fee_source"),
                "fee_verified": closed_trade.get("fee_verified"),
                "spot_fees_usdt": closed_trade.get("spot_fees_usdt"),
                "futures_fees_usdt": closed_trade.get("futures_fees_usdt"),
            },
        )


def _finite_or_none(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")) or number <= 0:
        return None
    return number
