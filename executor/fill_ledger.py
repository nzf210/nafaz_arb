"""Immutable execution fill ledger and expected-vs-realized attribution.

PostgreSQL is the only sink. The Supabase REST writer is disabled on the VPS
(SUPABASE_URL is empty), so a Supabase-only journal silently discards every
fill. This module writes through asyncpg on the pool already owned by
GlobalStopService.

Failures never raise into the trading path: a dropped ledger row is logged and
counted, and the caller keeps managing exposure. Accounting debt is recoverable;
an unmanaged position is not.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger("executor.fill_ledger")

# The four legs of one spot/perp arbitrage round trip.
SPOT_ENTRY = "spot_entry"
FUTURES_ENTRY = "futures_entry"
SPOT_EXIT = "spot_exit"
FUTURES_EXIT = "futures_exit"
LEDGER_LEGS = (SPOT_ENTRY, FUTURES_ENTRY, SPOT_EXIT, FUTURES_EXIT)

_INSERT_SQL = """
    insert into public.execution_fill_ledger (
        pair_key, session_id, leg, exchange_name, market_type, symbol, side,
        exchange_order_id, client_order_id, exchange_fill_id, qty,
        actual_vwap_price, expected_vwap_price, quote_qty, quote_asset,
        actual_fee_paid, fee_currency, fee_verified, fee_source,
        fx_rate_applied, fx_rate_source, realized_funding_usd, is_mock,
        filled_at, adjustment_of, adjustment_reason, raw_payload
    ) values (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
        $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27::jsonb
    )
    on conflict (exchange_name, market_type, exchange_fill_id)
        where exchange_fill_id is not null and adjustment_of is null
    do nothing
    returning fill_id
"""

_DEVIATION_SQL = """
    insert into public.pair_pnl_deviations (
        pair_key, trade_key, expected_net_pnl_usd, realized_net_pnl_usd,
        net_pnl_deviation_usd, expected_fees_usd, realized_fees_usd,
        fee_deviation_usd, slippage_deviation_usd, funding_deviation_usd,
        expected_hold_hours, realized_hold_hours, is_mock, closed_at, detail
    ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
    on conflict (trade_key) where trade_key is not null do nothing
    returning deviation_id
"""


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number and abs(number) != float("inf") else default


def _utc(value: Any) -> datetime:
    """Normalise a venue ms epoch or ISO string into an aware UTC timestamp."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and float(value) > 0:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _fee_details(fill: Dict[str, Any]) -> tuple:
    """Return (amount, currency, verified) from a CCXT-shaped fill."""
    rows = fill.get("fees") if isinstance(fill.get("fees"), list) else []
    fee = fill.get("fee") if isinstance(fill.get("fee"), dict) else {}
    cost = fee.get("cost")
    currency = str(fee.get("currency") or "").strip().upper() or None
    if cost is None and len(rows) == 1 and isinstance(rows[0], dict):
        cost = rows[0].get("cost")
        currency = str(rows[0].get("currency") or "").strip().upper() or None
    amount = _finite(cost, 0.0)
    verified = cost is not None and bool(currency) and len(rows) <= 1
    return max(0.0, amount), currency, verified


class ExecutionFillLedger:
    """Append-only per-execution ledger over the GlobalStopService pool."""

    def __init__(self, pool: Any = None, dsn: str = ""):
        self.pool = pool
        self.dsn = str(dsn or "").strip()
        self._owned_pool = False
        self._dropped = 0

    @property
    def enabled(self) -> bool:
        return self.pool is not None and hasattr(self.pool, "acquire")

    async def start(self) -> None:
        """Reuse the fencing pool. Only open one when none was supplied."""
        if self.pool is not None or not self.dsn:
            return
        try:
            import asyncpg
        except ImportError:
            logger.error("asyncpg missing; execution fill ledger disabled")
            return
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn, min_size=1, max_size=2, command_timeout=10
            )
            self._owned_pool = True
            logger.info("execution fill ledger connected to PostgreSQL")
        except Exception:
            logger.exception("execution fill ledger pool unavailable; ledger disabled")

    async def close(self) -> None:
        if self._owned_pool and self.pool is not None:
            await self.pool.close()
            self.pool = None
            self._owned_pool = False

    async def record_fill(
        self,
        *,
        pair_key: str,
        leg: str,
        exchange: str,
        market_type: str,
        symbol: str,
        side: str,
        qty: float,
        actual_vwap_price: float,
        fill: Optional[Dict[str, Any]] = None,
        order_id: Optional[str] = None,
        exchange_order_id: str = "",
        client_order_id: str = "",
        expected_vwap_price: Optional[float] = None,
        quote_asset: str = "",
        fx_rate_applied: Optional[float] = None,
        fx_rate_source: str = "",
        realized_funding_usd: float = 0.0,
        is_mock: bool = True,
        session_id: Optional[str] = None,
        filled_at: Any = None,
        adjustment_of: Optional[str] = None,
        adjustment_reason: Optional[str] = None,
        position_id: Optional[str] = None,
    ) -> Optional[str]:
        """Append one execution. Returns the fill id, or None when not recorded."""
        if not self.enabled:
            return None
        if leg not in LEDGER_LEGS:
            logger.error("refusing ledger row with unknown leg %r", leg)
            return None
        qty = _finite(qty)
        price = _finite(actual_vwap_price)
        if qty <= 0 or price < 0:
            logger.error(
                "refusing ledger row %s/%s: qty=%s price=%s", pair_key, leg, qty, price
            )
            return None

        payload = fill if isinstance(fill, dict) else {}
        fee_amount, fee_currency, fee_verified = _fee_details(payload)
        fill_id = str(payload.get("id") or payload.get("trade_id") or "").strip() or None
        quote_qty = _finite(payload.get("cost"), qty * price)
        fx = _finite(fx_rate_applied) if fx_rate_applied is not None else None

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    _INSERT_SQL,
                    str(pair_key)[:200],
                    str(session_id)[:200] if session_id else None,
                    leg,
                    str(exchange or "").lower()[:100],
                    str(market_type or "").lower()[:30],
                    str(symbol or "")[:160],
                    str(side or "").lower()[:16],
                    exchange_order_id[:200] if exchange_order_id else None,
                    client_order_id[:200] if client_order_id else None,
                    fill_id,
                    qty,
                    price,
                    _finite(expected_vwap_price) if expected_vwap_price is not None else None,
                    quote_qty,
                    str(quote_asset or "").upper()[:20] or None,
                    fee_amount,
                    fee_currency,
                    fee_verified,
                    str(payload.get("fee_source") or "EXCHANGE_TRADE")[:40],
                    fx,
                    str(fx_rate_source or "")[:60] or None,
                    _finite(realized_funding_usd),
                    bool(is_mock),
                    _utc(filled_at or payload.get("timestamp")),
                    adjustment_of,
                    adjustment_reason,
                    json.dumps(payload, separators=(",", ":"), default=str)[:20000],
                )
            if row is None:
                # Same venue execution replayed: idempotent, not an error.
                return None
            return str(row["fill_id"])
        except Exception:
            self._dropped += 1
            logger.exception(
                "CRITICAL: execution fill ledger append failed for %s/%s", pair_key, leg
            )
            return None

    async def record_round_trip(
        self,
        *,
        pair_key: str,
        fills: Iterable[Dict[str, Any]],
        is_mock: bool = True,
        session_id: Optional[str] = None,
    ) -> List[str]:
        """Append several leg fills in one pass; returns recorded fill ids."""
        recorded = []
        for entry in fills:
            fill_id = await self.record_fill(
                pair_key=pair_key, is_mock=is_mock, session_id=session_id, **entry
            )
            if fill_id:
                recorded.append(fill_id)
        return recorded

    async def record_deviation(
        self,
        *,
        pair_key: str,
        expected_net_pnl_usd: float,
        realized_net_pnl_usd: float,
        expected_fees_usd: float = 0.0,
        realized_fees_usd: float = 0.0,
        funding_deviation_usd: float = 0.0,
        expected_hold_hours: Optional[float] = None,
        realized_hold_hours: Optional[float] = None,
        trade_key: Optional[str] = None,
        is_mock: bool = True,
        closed_at: Any = None,
        detail: Optional[Dict[str, Any]] = None,
        position_id: Optional[str] = None,
    ) -> Optional[str]:
        """Attribute expected vs realized PnL once the position closes fully."""
        if not self.enabled:
            return None
        expected = _finite(expected_net_pnl_usd)
        realized = _finite(realized_net_pnl_usd)
        fee_delta = _finite(expected_fees_usd) - _finite(realized_fees_usd)
        net_delta = realized - expected
        # Split the net delta into fee and price components. Gross PnL moved by
        # (net_delta - fee_delta); whatever fees do not explain is execution
        # slippage. Crossing the signs here would silently invert attribution.
        slippage = net_delta - fee_delta
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    _DEVIATION_SQL,
                    str(pair_key)[:200],
                    trade_key[:200] if trade_key else None,
                    expected,
                    realized,
                    net_delta,
                    _finite(expected_fees_usd),
                    _finite(realized_fees_usd),
                    fee_delta,
                    slippage,
                    _finite(funding_deviation_usd),
                    _finite(expected_hold_hours) if expected_hold_hours is not None else None,
                    _finite(realized_hold_hours) if realized_hold_hours is not None else None,
                    bool(is_mock),
                    _utc(closed_at),
                    json.dumps(detail or {}, separators=(",", ":"), default=str)[:20000],
                )
            return str(row["deviation_id"]) if row is not None else None
        except Exception:
            self._dropped += 1
            logger.exception(
                "CRITICAL: pair PnL deviation append failed for %s", pair_key
            )
            return None

    @property
    def dropped(self) -> int:
        return self._dropped
