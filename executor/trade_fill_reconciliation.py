"""Reconcile immutable exchange trade executions into the durable fill ledger."""

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from executor.models import PositionState

logger = logging.getLogger("executor.trade_fill_reconciliation")


def _timestamp_ms(value: Any, fallback: int = 0) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            try:
                return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
            except ValueError:
                return fallback
    return fallback


class TradeFillReconciliationWorker:
    """Read-only venue fill reader; only appends verified exchange trade IDs."""

    def __init__(self, engine: Any):
        self.engine = engine
        self.interval = self._interval()
        self.relative_tolerance = 0.005
        self.absolute_tolerance = 1e-8
        self._reported: Dict[str, str] = {}
        self._last_indodax_request_at = 0.0

    @staticmethod
    def _interval() -> float:
        import os
        try:
            value = float(os.getenv("TRADE_FILL_RECONCILIATION_INTERVAL_SECONDS", "120"))
            return value if math.isfinite(value) and value >= 30 else 120.0
        except (TypeError, ValueError):
            return 120.0

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            try:
                if not getattr(self.engine.config, "mock_mode", False):
                    await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("periodic trade-fill reconciliation failed")

    @staticmethod
    def _identity(row: Dict[str, Any]) -> str:
        exchange = str(row.get("exchange_name") or row.get("exchange") or "").lower()
        market = str(row.get("market_type") or "").lower()
        order_id = str(row.get("exchange_order_id") or "")
        client_id = str(row.get("client_order_id") or "")
        key = client_id or order_id
        return f"{exchange}:{market}:{key}" if exchange and market and key else ""

    async def _load_candidates(self) -> Dict[str, Dict[str, Any]]:
        candidates: Dict[str, Dict[str, Any]] = {}
        reader = getattr(self.engine.supabase, "fetch_recent_order_fill_candidates", None)
        if callable(reader):
            try:
                rows = await reader(days=90, limit=5000)
                for row in rows or []:
                    if isinstance(row, dict) and row.get("is_mock") is False:
                        identity = self._identity(row)
                        if identity:
                            candidates[identity] = dict(row)
            except Exception:
                logger.exception("could not load durable recent orders for fill reconciliation")

        registry_sources = (
            getattr(self.engine, "_recovered_order_records", {}),
            getattr(getattr(self.engine, "order_registry", None), "records", {}),
        )
        for source in registry_sources:
            values = source.values() if isinstance(source, dict) else ()
            for record in values:
                try:
                    row = {
                        "pair_key": record.pair_key,
                        "exchange_name": record.exchange,
                        "market_type": record.market_type,
                        "leg": {
                            "ENTRY": "spot_entry" if record.market_type == "spot" else "futures_entry",
                            "HEDGE": "futures_entry",
                            "EXIT_SPOT": "spot_exit",
                            "EXIT_FUTURES": "futures_exit",
                            "EMERGENCY": "rollback",
                            "ROLLBACK": "rollback",
                            "FORCE_CLOSE": "futures_exit" if record.market_type == "perpetual" else "force_close",
                        }.get(record.order_role, "spot_entry"),
                        "exchange_order_id": record.exchange_order_id,
                        "client_order_id": record.client_order_id,
                        "symbol": record.symbol,
                        "side": record.side,
                        "status": record.status,
                        "requested_qty": record.original_quantity,
                        "filled_qty": record.filled_quantity,
                        "remaining_qty": record.remaining_quantity,
                        "limit_price": record.price,
                        "average_fill_price": record.average_fill_price,
                        "reduce_only": record.reduce_only,
                        "submitted_at": record.created_at,
                        "completed_at": record.updated_at if record.is_terminal else None,
                        "local_amount_spec": record.local_amount_spec,
                        "global_amount_spec": record.global_amount_spec,
                        "is_mock": record.is_mock,
                    }
                    identity = self._identity(row)
                    if identity and not row["is_mock"]:
                        existing = candidates.get(identity, {})
                        candidates[identity] = {**existing, **row}
                except Exception:
                    logger.exception("invalid order registry record in fill reconciliation")
        return candidates

    async def _halt(self, identity: str, scope: str, message: str, detail: Dict[str, Any]) -> None:
        self._reported[identity] = scope
        halt = getattr(self.engine, "_halt_exposure_scope", None)
        if callable(halt):
            halt(scope, f"trade-fill reconciliation: {message}")
        publish = getattr(self.engine, "_publish_execution_failure", None)
        if callable(publish):
            await publish(
                f"trade-fill-reconciliation:{identity}",
                "TRADE_FILL_RECONCILIATION_REQUIRED",
                message,
                detail=detail,
                state=PositionState.RECONCILIATION_REQUIRED,
            )

    async def _clear(self, identity: str) -> None:
        scope = self._reported.pop(identity, None)
        if not scope:
            return
        # Several filled orders can belong to the same pair. Do not lift that
        # pair's entry gate until every previously mismatched order is verified.
        if any(other_scope == scope for other_scope in self._reported.values()):
            return
        reasons = getattr(self.engine, "_recovery_scope_halts", {})
        reason = str(reasons.get(scope, "") or "")
        if "trade-fill reconciliation:" in reason:
            clear = getattr(self.engine, "_clear_exposure_halt", None)
            if callable(clear):
                await clear(scope, "all exchange trade fills verified")

    async def reconcile_once(self) -> None:
        if getattr(self.engine.config, "mock_mode", False):
            return
        writer = getattr(self.engine, "supabase", None)
        if writer is None or not getattr(writer, "enabled", False):
            logger.warning("trade-fill reconciliation skipped: durable fill writer is disabled")
            return

        candidates = await self._load_candidates()
        for identity, row in candidates.items():
            try:
                filled_qty = float(row.get("filled_qty") or row.get("filled_quantity") or 0.0)
            except (TypeError, ValueError):
                filled_qty = float("nan")
            if not math.isfinite(filled_qty) or filled_qty < 0:
                await self._halt(
                    identity, str(row.get("pair_key") or row.get("exchange_name") or "unknown"),
                    "Order has an invalid cumulative filled quantity; individual fills cannot be reconciled.",
                    {"order": row},
                )
                continue
            if filled_qty <= self.absolute_tolerance:
                await self._clear(identity)
                continue
            watermark_reader = getattr(writer, "fill_sync_watermark", None)
            try:
                previous_verified = watermark_reader(identity) if callable(watermark_reader) else None
            except Exception as exc:
                await self._halt(
                    identity, str(row.get("pair_key") or row.get("exchange_name") or "unknown"),
                    f"Local fill reconciliation journal is unavailable: {exc}",
                    {"order": row, "error": str(exc)},
                )
                continue
            if (
                previous_verified is not None
                and abs(float(previous_verified) - filled_qty)
                <= max(self.absolute_tolerance, abs(filled_qty) * self.relative_tolerance)
            ):
                await self._clear(identity)
                continue

            exchange = str(row.get("exchange_name") or "").lower()
            market_type = str(row.get("market_type") or "").lower()
            pair_key = str(row.get("pair_key") or "")
            symbol = str(row.get("symbol") or "")
            client_order_id = str(row.get("client_order_id") or "")
            exchange_order_id = str(row.get("exchange_order_id") or "")
            scope = pair_key or exchange
            if not exchange or market_type not in ("spot", "perpetual") or not symbol or not (client_order_id or exchange_order_id):
                await self._halt(
                    identity, scope or "unknown",
                    "Filled order is missing exchange, market, symbol, or order identity.",
                    {"order": row},
                )
                continue

            token = None
            try:
                if pair_key and not getattr(self.engine.config, "mock_mode", False):
                    try:
                        token = self.engine._bind_pair_client(self.engine._pair_info_for_tag(pair_key))
                    except (LookupError, KeyError, ValueError):
                        token = None
                client = self.engine.client if token is not None else self.engine._client_for_exchange(exchange)
                fetch = getattr(client, "fetch_order_trades", None)
                if not callable(fetch):
                    raise NotImplementedError(f"no bound trade-history client for {exchange}")
                now_ms = int(time.time() * 1000)
                submitted = max(
                    _timestamp_ms(row.get("submitted_at"), now_ms - 7 * 86400 * 1000),
                    now_ms - 90 * 86400 * 1000,
                )
                amount_spec = (
                    row.get("global_amount_spec") if market_type == "perpetual"
                    else row.get("local_amount_spec")
                )
                if exchange == "indodax":
                    # PairClientRegistry can hold one adapter per pair, while
                    # INDODAX rate limits are shared by account/IP. Throttle
                    # across the worker, not just within one adapter instance.
                    delay = 0.21 - (time.monotonic() - self._last_indodax_request_at)
                    if delay > 0:
                        await asyncio.sleep(delay)
                    self._last_indodax_request_at = time.monotonic()
                trades = await fetch(
                    market_type,
                    symbol,
                    since=submitted,
                    client_order_id=client_order_id or None,
                    order_id=exchange_order_id or None,
                    until=_timestamp_ms(row.get("completed_at")) if row.get("completed_at") else None,
                    amount_spec=amount_spec,
                )
                if not isinstance(trades, list):
                    raise ValueError("venue trade-history response is malformed")
                total_qty = 0.0
                order_key = client_order_id or exchange_order_id
                order_row_id = str(row.get("id") or "")
                if not order_row_id:
                    # Older deployments may have a durable order_registry row
                    # but no matching public.orders analytics parent. Recreate
                    # that parent before inserting fills (the fill FK targets
                    # public.orders, not order_registry).
                    leg = str(row.get("leg") or "")
                    if leg not in (
                        "spot_entry", "futures_entry", "spot_exit",
                        "futures_exit", "rollback", "force_close",
                    ):
                        raise RuntimeError("filled registry order has no valid analytics leg")
                    parent_ok = writer.order(
                        pair_key,
                        leg,
                        exchange,
                        market_type,
                        symbol,
                        {
                            "id": exchange_order_id,
                            "amount": float(row.get("requested_qty") or filled_qty),
                            "filled": filled_qty,
                            "average": row.get("average_fill_price"),
                            "price": row.get("limit_price"),
                            "status": row.get("status") or "closed",
                        },
                        client_order_id=client_order_id or None,
                        reduce_only=bool(row.get("reduce_only", False)),
                        is_mock=False,
                        submitted_at_ms=submitted,
                    )
                    if parent_ok is False:
                        raise RuntimeError("could not durably create parent order row for exchange fills")
                    order_id_fn = getattr(writer, "order_id_for", None)
                    if not callable(order_id_fn):
                        raise RuntimeError("cannot resolve stable analytics order id for exchange fills")
                    order_row_id = order_id_fn(pair_key, exchange, order_key)
                for trade in trades:
                    if not isinstance(trade, dict):
                        raise ValueError("venue returned a malformed individual fill")
                    total_qty += float(trade["amount"])
                    if writer.order_fill(
                        order_id=order_row_id,
                        exchange=exchange,
                        fill=trade,
                        is_mock=False,
                    ) is False:
                        raise RuntimeError(
                            f"exchange fill {trade.get('id')} could not be durably journaled"
                        )
                tolerance = max(self.absolute_tolerance, abs(filled_qty) * self.relative_tolerance)
                if not trades or abs(total_qty - filled_qty) > tolerance:
                    await self._halt(
                        identity, scope,
                        "Individual exchange fills do not match the order's cumulative filled quantity.",
                        {
                            "exchange": exchange,
                            "symbol": symbol,
                            "order_id": exchange_order_id,
                            "client_order_id": client_order_id,
                            "order_filled_qty": filled_qty,
                            "trade_fill_qty": total_qty,
                            "fill_count": len(trades),
                            "tolerance": tolerance,
                        },
                    )
                    continue
                marker = getattr(writer, "mark_fill_sync", None)
                if not callable(marker) or marker(identity, filled_qty) is False:
                    raise RuntimeError("could not persist verified fill reconciliation watermark")
                await self._clear(identity)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._halt(
                    identity, scope,
                    f"Cannot verify individual fills for order {exchange_order_id or client_order_id}: {exc}",
                    {
                        "exchange": exchange, "symbol": symbol,
                        "order_id": exchange_order_id,
                        "client_order_id": client_order_id,
                        "filled_qty": filled_qty,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
            finally:
                if token is not None:
                    self.engine._reset_pair_client(token)
