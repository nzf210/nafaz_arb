"""Reconcile immutable exchange trade executions into the durable fill ledger."""

import asyncio
import logging
import inspect
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
        self._verified_scopes = set()

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

    @staticmethod
    def _registry_record_candidate(record: Any) -> Dict[str, Any]:
        role_legs = {
            "ENTRY": "spot_entry" if record.market_type == "spot" else "futures_entry",
            "HEDGE": "futures_entry",
            "EXIT_SPOT": "spot_exit",
            "EXIT_FUTURES": "futures_exit",
            "EMERGENCY": "rollback",
            "ROLLBACK": "rollback",
            "FORCE_CLOSE": (
                "futures_exit" if record.market_type == "perpetual" else "force_close"
            ),
        }
        return {
            "pair_key": record.pair_key,
            "exchange_name": record.exchange,
            "market_type": record.market_type,
            "leg": role_legs.get(record.order_role, "spot_entry"),
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

    def _merge_registry_candidates(
        self, candidates: Dict[str, Dict[str, Any]], sources: tuple
    ) -> None:
        for source in sources:
            values = source.values() if isinstance(source, dict) else ()
            for record in values:
                try:
                    row = self._registry_record_candidate(record)
                    identity = self._identity(row)
                    if identity and not row["is_mock"]:
                        existing = candidates.get(identity, {})
                        candidates[identity] = {**existing, **row}
                except Exception:
                    logger.exception(
                        "invalid order registry record in fill reconciliation"
                    )

    async def _load_durable_fill_candidates(
        self, candidates: Dict[str, Dict[str, Any]]
    ) -> None:
        reader = getattr(
            self.engine.supabase, "fetch_recent_order_fill_candidates", None
        )
        if not callable(reader):
            raise RuntimeError(
                "LIVE fill reconciliation requires durable recent-order query support"
            )
        rows = await reader(days=90, limit=10000)
        if not isinstance(rows, list):
            raise RuntimeError("durable recent-order query returned malformed rows")
        for row in rows:
            if not isinstance(row, dict):
                raise RuntimeError("durable recent-order query contains a malformed row")
            if row.get("is_mock") is not False:
                continue
            identity = self._identity(row)
            if identity:
                candidates[identity] = dict(row)

    async def _load_candidates(self) -> Dict[str, Dict[str, Any]]:
        candidates: Dict[str, Dict[str, Any]] = {}
        await self._load_durable_fill_candidates(candidates)
        registry_sources = (
            getattr(self.engine, "_recovered_order_records", {}),
            getattr(getattr(self.engine, "order_registry", None), "records", {}),
        )
        self._merge_registry_candidates(candidates, registry_sources)
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

    def _clear(self, identity: str, scope: Optional[str] = None) -> None:
        resolved_scope = self._reported.pop(identity, None) or scope
        if resolved_scope:
            self._verified_scopes.add(resolved_scope)

    async def _flush_clears(self) -> None:
        # Defer clearing until every candidate has been checked. This prevents
        # a good order processed early in the pass from briefly lifting a
        # pair halt while another order for that pair is still mismatched.
        clear = getattr(self.engine, "_clear_exposure_halt", None)
        reasons = getattr(self.engine, "_recovery_scope_halts", {})
        if callable(clear):
            for scope in tuple(self._verified_scopes):
                if any(other_scope == scope for other_scope in self._reported.values()):
                    continue
                reason = str(reasons.get(scope, "") or "")
                if "trade-fill reconciliation:" in reason:
                    result = clear(scope, "all exchange trade fills verified")
                    if inspect.isawaitable(result):
                        await result
        self._verified_scopes.clear()

    @staticmethod
    def _filled_quantity(row: Dict[str, Any]) -> float:
        try:
            return float(row.get("filled_qty") or row.get("filled_quantity") or 0.0)
        except (TypeError, ValueError):
            return float("nan")

    async def _watermark_is_current(
        self, writer, identity: str, scope: str, filled_qty: float
    ) -> Optional[bool]:
        reader = getattr(writer, "fill_sync_watermark", None)
        try:
            previous_verified = reader(identity) if callable(reader) else None
        except Exception as exc:
            await self._halt(
                identity,
                scope,
                f"Local fill reconciliation journal is unavailable: {exc}",
                {"error": str(exc)},
            )
            return None
        if previous_verified is None:
            return False
        tolerance = max(
            self.absolute_tolerance, abs(filled_qty) * self.relative_tolerance
        )
        return abs(float(previous_verified) - filled_qty) <= tolerance

    async def _fetch_candidate_trades(
        self, client, row: Dict[str, Any], exchange: str, market_type: str,
        symbol: str, client_order_id: str, exchange_order_id: str,
    ):
        fetch = getattr(client, "fetch_order_trades", None)
        if not callable(fetch):
            raise NotImplementedError(
                f"no bound trade-history client for {exchange}"
            )
        now_ms = int(time.time() * 1000)
        submitted = max(
            _timestamp_ms(row.get("submitted_at"), now_ms - 7 * 86400 * 1000),
            now_ms - 90 * 86400 * 1000,
        )
        amount_spec = (
            row.get("global_amount_spec")
            if market_type == "perpetual"
            else row.get("local_amount_spec")
        )
        if exchange == "indodax":
            # INDODAX rate limits are shared by account/IP across pair adapters.
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
            until=(
                _timestamp_ms(row.get("completed_at"))
                if row.get("completed_at") else None
            ),
            amount_spec=amount_spec,
        )
        if not isinstance(trades, list):
            raise ValueError("venue trade-history response is malformed")
        return trades, submitted

    def _ensure_fill_order_parent(
        self, writer, row: Dict[str, Any], pair_key: str, exchange: str,
        market_type: str, symbol: str, client_order_id: str,
        exchange_order_id: str, filled_qty: float, submitted: int,
    ) -> str:
        order_row_id = str(row.get("id") or "")
        if order_row_id:
            return order_row_id
        # Registry-only orders need a public.orders parent because the fill FK
        # references that table, not order_registry.
        leg = str(row.get("leg") or "")
        if leg not in (
            "spot_entry", "futures_entry", "spot_exit",
            "futures_exit", "rollback", "force_close",
        ):
            raise RuntimeError("filled registry order has no valid analytics leg")
        order_key = client_order_id or exchange_order_id
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
            raise RuntimeError(
                "could not durably create parent order row for exchange fills"
            )
        order_id_fn = getattr(writer, "order_id_for", None)
        if not callable(order_id_fn):
            raise RuntimeError(
                "cannot resolve stable analytics order id for exchange fills"
            )
        return order_id_fn(pair_key, exchange, order_key)

    @staticmethod
    def _persist_exchange_fills(
        writer, trades: list, order_row_id: str, exchange: str
    ):
        total_qty = 0.0
        unverified_fee_ids = []
        for trade in trades:
            if not isinstance(trade, dict):
                raise ValueError("venue returned a malformed individual fill")
            total_qty += float(trade["amount"])
            fee = trade.get("fee") if isinstance(trade.get("fee"), dict) else {}
            fee_rows = trade.get("fees") if isinstance(trade.get("fees"), list) else []
            if (
                fee.get("cost") is None
                or not str(fee.get("currency") or "").strip()
                or len(fee_rows) > 1
            ):
                unverified_fee_ids.append(str(trade.get("id") or "unknown"))
            if writer.order_fill(
                order_id=order_row_id,
                exchange=exchange,
                fill=trade,
            ) is False:
                raise RuntimeError(
                    f"exchange fill {trade.get('id')} could not be durably journaled"
                )
        return total_qty, unverified_fee_ids

    async def _verify_candidate_fills(
        self, writer, identity: str, scope: str, exchange: str, symbol: str,
        exchange_order_id: str, client_order_id: str, filled_qty: float,
        trades: list, total_qty: float, unverified_fee_ids: list,
    ) -> None:
        tolerance = max(
            self.absolute_tolerance, abs(filled_qty) * self.relative_tolerance
        )
        if not trades or abs(total_qty - filled_qty) > tolerance:
            await self._halt(
                identity,
                scope,
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
            return
        if unverified_fee_ids:
            await self._halt(
                identity,
                scope,
                "One or more exchange fills are missing a verifiable fee amount/currency.",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "order_id": exchange_order_id,
                    "client_order_id": client_order_id,
                    "unverified_fee_trade_ids": unverified_fee_ids,
                },
            )
            return
        marker = getattr(writer, "mark_fill_sync", None)
        if not callable(marker) or marker(identity, filled_qty) is False:
            raise RuntimeError(
                "could not persist verified fill reconciliation watermark"
            )
        self._clear(identity, scope)

    @staticmethod
    def _candidate_context_fields(
        row: Dict[str, Any], filled_qty: float
    ) -> Dict[str, Any]:
        context = {
            "exchange": str(row.get("exchange_name") or "").lower(),
            "market_type": str(row.get("market_type") or "").lower(),
            "pair_key": str(row.get("pair_key") or ""),
            "symbol": str(row.get("symbol") or ""),
            "client_order_id": str(row.get("client_order_id") or ""),
            "exchange_order_id": str(row.get("exchange_order_id") or ""),
            "filled_qty": filled_qty,
        }
        context["scope"] = context["pair_key"] or context["exchange"]
        return context

    @staticmethod
    def _candidate_context_has_identity(context: Dict[str, Any]) -> bool:
        return bool(
            context["exchange"]
            and context["market_type"] in ("spot", "perpetual")
            and context["symbol"]
            and (context["client_order_id"] or context["exchange_order_id"])
        )

    async def _candidate_reconciliation_context(
        self, writer, identity: str, row: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Validate candidate identity and determine whether its fills need replay."""
        scope = str(row.get("pair_key") or row.get("exchange_name") or "unknown")
        filled_qty = self._filled_quantity(row)
        if not math.isfinite(filled_qty) or filled_qty < 0:
            await self._halt(
                identity,
                scope,
                "Order has an invalid cumulative filled quantity; individual fills cannot be reconciled.",
                {"order": row},
            )
            return None
        if filled_qty <= self.absolute_tolerance:
            self._clear(identity, scope)
            return None
        watermark_current = await self._watermark_is_current(
            writer, identity, scope, filled_qty
        )
        if watermark_current is None:
            return None
        if watermark_current:
            self._clear(identity, scope)
            return None
        context = self._candidate_context_fields(row, filled_qty)
        if not self._candidate_context_has_identity(context):
            await self._halt(
                identity,
                context["scope"] or "unknown",
                "Filled order is missing exchange, market, symbol, or order identity.",
                {"order": row},
            )
            return None
        return context

    def _bind_candidate_client(self, pair_key: str, exchange: str):
        token = None
        if pair_key and not getattr(self.engine.config, "mock_mode", False):
            try:
                token = self.engine._bind_pair_client(
                    self.engine._pair_info_for_tag(pair_key)
                )
            except (LookupError, ValueError):
                token = None
        client = (
            self.engine.client
            if token is not None
            else self.engine._client_for_exchange(exchange)
        )
        return token, client

    async def _reconcile_candidate_trades(
        self, writer, identity: str, row: Dict[str, Any], context: Dict[str, Any]
    ) -> None:
        token = None
        exchange = context["exchange"]
        symbol = context["symbol"]
        client_order_id = context["client_order_id"]
        exchange_order_id = context["exchange_order_id"]
        filled_qty = context["filled_qty"]
        scope = context["scope"]
        try:
            token, client = self._bind_candidate_client(context["pair_key"], exchange)
            trades, submitted = await self._fetch_candidate_trades(
                client, row, exchange, context["market_type"], symbol,
                client_order_id, exchange_order_id,
            )
            order_row_id = self._ensure_fill_order_parent(
                writer, row, context["pair_key"], exchange, context["market_type"],
                symbol, client_order_id, exchange_order_id, filled_qty, submitted,
            )
            total_qty, unverified_fee_ids = self._persist_exchange_fills(
                writer, trades, order_row_id, exchange
            )
            await self._verify_candidate_fills(
                writer, identity, scope, exchange, symbol, exchange_order_id,
                client_order_id, filled_qty, trades, total_qty, unverified_fee_ids,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._halt(
                identity,
                scope,
                f"Cannot verify individual fills for order {exchange_order_id or client_order_id}: {exc}",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "order_id": exchange_order_id,
                    "client_order_id": client_order_id,
                    "filled_qty": filled_qty,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
        finally:
            if token is not None:
                self.engine._reset_pair_client(token)

    async def _reconcile_candidate(
        self, writer, identity: str, row: Dict[str, Any]
    ) -> None:
        context = await self._candidate_reconciliation_context(
            writer, identity, row
        )
        if context is None:
            return
        await self._reconcile_candidate_trades(
            writer, identity, row, context
        )

    async def reconcile_once(self) -> None:
        if getattr(self.engine.config, "mock_mode", False):
            return
        writer = getattr(self.engine, "supabase", None)
        if writer is None or not getattr(writer, "enabled", False):
            logger.warning(
                "trade-fill reconciliation skipped: durable fill writer is disabled"
            )
            return
        try:
            candidates = await self._load_candidates()
        except Exception as exc:
            await self._halt(
                "durable-ledger-read",
                "*",
                f"durable fill ledger could not be read: {exc}",
                {"error": str(exc)},
            )
            raise
        self._reported.pop("durable-ledger-read", None)
        self._verified_scopes.clear()
        # A successful complete 90-day read proves the global ledger query is
        # healthy, even when there are currently no fill candidates.
        self._verified_scopes.add("*")
        for identity, row in candidates.items():
            await self._reconcile_candidate(writer, identity, row)
        await self._flush_clears()
