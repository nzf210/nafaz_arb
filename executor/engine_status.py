"""engine status methods for arbitrage engine orchestration."""

import asyncio
import json
import time
from typing import Optional, Dict, Any

from executor.models import PositionState
from executor.engine_common import logger



class EngineStatusMixin:
    """Focused orchestration responsibilities, composed by ArbitrageEngine."""

    def _mark_failed(
        self,
        pair_tag: str,
        reason: str,
        message: str,
        detail: Optional[Dict[str, Any]] = None,
        state: PositionState = PositionState.FAILED,
    ) -> None:
        """Puts a pair into a failure state and records why."""
        self.states[pair_tag] = state
        entry = {
            "reason": reason,
            "message": message,
            "detail": detail or {},
            "timestamp_ms": int(time.time() * 1000),
        }
        self._failure_reasons[pair_tag] = entry
        self._persist_risk_state("position_failure", scope="position", pair_key=pair_tag, reason=reason, value={**entry, "state": state.value})
        if state == PositionState.RECONCILIATION_REQUIRED:
            self._persist_risk_state("reconciliation_halt", scope="pair", pair_key=pair_tag, reason=reason, value={"source": "position_failure"})
            self._recovery_scope_halts[pair_tag] = reason
        logger.critical(f"[{state.value}] {pair_tag}: {reason} - {message}")
        if reason != "HEDGE_FAILED":
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(
                    self._publish_execution_failure(
                        pair_tag, reason, message, detail=detail, state=state
                    )
                )
            except RuntimeError:
                # A synchronous caller without an active event loop still records
                # the failure locally; the next status poll remains available.
                pass

    async def _publish_execution_failure(
        self,
        pair_tag: str,
        reason: str,
        message: str,
        detail: Optional[Dict[str, Any]] = None,
        state: Optional[PositionState] = None,
    ) -> None:
        """Publish actionable execution failures for the Telegram notifier."""
        if self.redis is None:
            return
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        payload = {
            "event": "EXECUTION_FAILURE",
            "pair_tag": pair_tag,
            "local_exchange": pair_info.local_exchange if pair_info else "",
            "global_exchange": pair_info.global_exchange if pair_info else "",
            "symbol": pair_info.local_symbol if pair_info else pair_tag,
            "reason": reason,
            "message": message,
            "detail": detail or {},
            "state": state.value if state else "",
            "is_mock": bool(getattr(self.config, "mock_mode", False)),
            "timestamp_ms": int(time.time() * 1000),
        }
        try:
            await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(payload, default=str))
        except Exception:
            logger.exception("failed to publish execution failure event for %s", pair_tag)
        self.supabase.event(
            "EXECUTION_FAILURE",
            pair_key=pair_tag,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
            occurred_at_ms=payload["timestamp_ms"],
            payload=payload,
        )

    @staticmethod
    def _utc_day() -> str:
        return time.strftime("%Y-%m-%d", time.gmtime())

    def _roll_loss_day_if_needed(self) -> None:
        """Resets the daily loss accumulator when the UTC day changes."""
        today = self._utc_day()
        if today == self._loss_day:
            return
        self._loss_day = today
        self._daily_realized_loss_usdt = 0.0
        self._manual_resume_day = None
        self._persist_risk_state("trading_resume", scope="global", resolved=True)
        self._persist_risk_state("daily_loss", scope="global", value={"loss_usdt": 0.0, "loss_day": today})
        # The operator kill switch does not auto-reset on a new UTC day.
        if (
            self._trading_halted
            and self._halt_reason == "DAILY_LOSS_LIMIT"
        ):
            self._trading_halted = False
            self._halt_reason = ""
            self._persist_risk_state("trading_halt", scope="global", resolved=True)
            logger.warning("[Circuit Breaker] New UTC day - trading resumed")

    def record_realized_pnl(self, net_profit_usdt: float, event_id: Optional[str] = None, pair_key: str = "unknown") -> None:
        """
        Feeds a closed trade's net PnL into the daily loss breaker.

        Losses beyond max_daily_loss_usdt halt *new entries* only: open positions
        keep being managed, because abandoning them unhedged would be worse than
        the loss the breaker is trying to stop.
        """
        self._roll_loss_day_if_needed()
        if event_id and getattr(self.supabase, "enabled", False):
            result = self.supabase.risk_pnl_event(
                event_id, pair_key=pair_key, pnl_usdt=float(net_profit_usdt),
                loss_day=self._loss_day,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            if result == "duplicate":
                logger.info("[Risk] duplicate realized PnL event ignored: %s", event_id)
                return
            if result == "failed":
                self._risk_persistence_failed = True
                self._entry_blocked = True
                logger.critical("[Risk] PnL event durability failed; new entries fail-closed")
        if net_profit_usdt < 0:
            self._daily_realized_loss_usdt += -net_profit_usdt
        self._persist_risk_state("daily_loss", scope="global", value={"loss_usdt": self._daily_realized_loss_usdt, "loss_day": self._loss_day})
        if event_id:
            self._persist_risk_state("trading_resume", scope="global", resolved=True)
            self._manual_resume_day = None

        limit = getattr(self.config, "max_daily_loss_usdt", 0.0)
        if limit <= 0 or self._trading_halted:
            return
        if self._daily_realized_loss_usdt >= limit:
            self._trading_halted = True
            self._halt_reason = "DAILY_LOSS_LIMIT"
            self._persist_risk_state("trading_halt", scope="global", reason="DAILY_LOSS_LIMIT", value={"halt_reason": "DAILY_LOSS_LIMIT", "loss_usdt": self._daily_realized_loss_usdt, "loss_day": self._loss_day})
            logger.critical(
                f"[Circuit Breaker] HALTED new entries: daily realized loss "
                f"{self._daily_realized_loss_usdt:.4f} USDT >= limit {limit:.4f} USDT"
            )

    async def resume_trading(self, reason: str = "MANUAL") -> None:
        """Clears the halt flag; used by the operator once a loss is reviewed."""
        self._trading_halted = False
        self._halt_reason = ""
        halt_saved = self._persist_risk_state("trading_halt", scope="global", resolved=True)
        self._manual_resume_day = self._loss_day
        resume_saved = self._persist_risk_state("trading_resume", scope="global", reason=reason, value={"loss_day": self._loss_day})
        if halt_saved and resume_saved:
            self.supabase.clear_risk_persistence_failure()
            self._risk_persistence_failed = False
        logger.warning(f"[Circuit Breaker] Trading resumed manually ({reason})")
        await self.publish_status()

    def clear_failure(self, pair_tag: str) -> None:
        """Clears a non-reconciliation failure; safety incidents need explicit reconciliation."""
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            logger.warning("[Risk] refusing to clear unresolved reconciliation failure for %s", pair_tag)
            return
        if self._failure_reasons.pop(pair_tag, None) is not None:
            self._persist_risk_state("position_failure", scope="position", pair_key=pair_tag, resolved=True)
            if self.states.get(pair_tag) == PositionState.FAILED:
                self.states[pair_tag] = PositionState.IDLE

    @staticmethod
    def _pending_order_created_at_ms(order: dict) -> int:
        placed_at = order.get("placed_time", 0) or order.get("created_at", 0) or 0
        try:
            placed_value = float(placed_at)
        except (TypeError, ValueError):
            return 0
        return (
            int(placed_value * 1000)
            if placed_value < 100000000000
            else int(placed_value)
        )

    @staticmethod
    def _infer_pending_order_quote_asset(symbol: str) -> str:
        normalized = symbol.replace("_", "/")
        return normalized.split("/")[-1] if "/" in normalized else ""

    def _pending_order_status(self, pair_tag: str, order: dict) -> str:
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            return "RECONCILIATION_REQUIRED"
        filled = max(0.0, float(order.get("filled", 0.0) or 0.0))
        return "PARTIALLY_FILLED" if filled > 0 else "OPEN"

    def _pending_order_snapshot(self, leg: str, pair_tag: str, order: dict) -> dict:
        position = self.active_positions.get(pair_tag)
        status = self._pending_order_status(pair_tag, order)
        quantity = max(0.0, float(order.get("qty", 0.0) or 0.0))
        filled = max(0.0, float(order.get("filled", 0.0) or 0.0))
        symbol = str(order.get("symbol", ""))
        inferred_quote = self._infer_pending_order_quote_asset(symbol)
        created_at_ms = self._pending_order_created_at_ms(order)
        return {
            "pair_tag": pair_tag,
            "leg": leg,
            "order_id": str(order.get("order_id", "")),
            "symbol": symbol,
            "quote_asset": str(order.get("quote_asset") or inferred_quote),
            "local_exchange": str(
                order.get("local_exchange")
                or getattr(position, "local_exchange", "")
                or self.config.local_exchange_name
            ),
            "global_exchange": str(
                order.get("global_exchange")
                or getattr(position, "global_exchange", "")
                or self.config.global_exchange_name
            ),
            "price": float(order.get("price", 0.0) or 0.0),
            "qty": quantity,
            "filled": filled,
            "remaining": max(0.0, quantity - filled),
            "status": status,
            "created_at_ms": created_at_ms,
        }

    def _pending_order_snapshots(self) -> list:
        """Returns read-only metadata for managed spot orders awaiting resolution."""
        orders = []
        for leg, source in (
            ("SPOT_ENTRY", self.pending_entry_orders),
            ("SPOT_EXIT", self.pending_exit_orders),
        ):
            for pair_tag, order in source.items():
                if isinstance(order, dict):
                    orders.append(
                        self._pending_order_snapshot(leg, pair_tag, order)
                    )
        return sorted(
            orders,
            key=lambda item: (item["created_at_ms"], item["pair_tag"]),
        )

    def status_snapshot(self) -> Dict[str, Any]:
        """Point-in-time view of the executor, suitable for a JSON API."""
        return {
            "status": "RUNNING" if self._running else "STOPPED",
            "executor_startup_time_ms": int(getattr(self, "_startup_time_ms", 0)),
            "mock_mode": bool(getattr(self.config, "mock_mode", False)),
            "local_exchange": self.config.local_exchange_name,
            "global_exchange": self.config.global_exchange_name,
            "open_positions": len(self.active_positions),
            "pending_entries": len(self.pending_entry_orders),
            "pending_exits": len(self.pending_exit_orders),
            "spot_order_limits": dict(getattr(self.config, "max_open_spot_orders_per_exchange", {}) or {}),
            "default_max_open_spot_orders_per_exchange": 2,
            "spot_open_order_counts": {
                exchange: self._spot_open_order_count(exchange)
                for exchange in sorted({
                    str(getattr(position, "local_exchange", "")).lower()
                    for position in self.active_positions.values()
                } | {
                    str((order or {}).get("local_exchange", "")).lower()
                    for order in self.pending_entry_orders.values()
                } | set((getattr(self.config, "max_open_spot_orders_per_exchange", {}) or {}).keys()))
                if exchange
            },
            "pending_orders": self._pending_order_snapshots(),
            "failed_pairs": len(self._failure_reasons),
            "trading_halted": self._trading_halted,
            "halt_reason": self._halt_reason,
            "exchange_halted": dict(self._exchange_halted),
            "daily_realized_loss_usdt": round(self._daily_realized_loss_usdt, 6),
            "max_daily_loss_usdt": getattr(self.config, "max_daily_loss_usdt", 0.0),
            "loss_day_utc": self._loss_day,
            "positions": {
                pair: {
                    "state": state.value,
                    "qty": getattr(self.active_positions.get(pair), "qty", 0.0),
                }
                for pair, state in self.states.items()
            },
            "failures": dict(self._failure_reasons),
            "recovery": {
                "complete": bool(getattr(self, "_recovery_complete", False)),
                "ready": not bool(getattr(self, "_entry_blocked", False)),
                "pending_orders": dict(getattr(self, "_recovery_pending_orders", {}) or {}),
                "scope_halts": dict(getattr(self, "_recovery_scope_halts", {}) or {}),
                "tracked_orders": len(getattr(self, "_recovered_order_records", {}) or {}),
                "orphan_orders": list((getattr(self, "_recovered_orphans", {}) or {}).values()),
            },
            "reconciliation": {
                "exposure_mismatches": dict(getattr(getattr(self, "exposure_reconciliation", None), "_reported", {}) or {}),
                "trade_fill_mismatches": dict(getattr(getattr(self, "trade_fill_reconciliation", None), "_reported", {}) or {}),
            },
            "timestamp_ms": int(time.time() * 1000),
        }

    async def publish_status(self) -> None:
        """Makes the latest status readable by the Go API and operators."""
        if self.redis is None:
            return
        payload = json.dumps(self.status_snapshot())
        try:
            await self.redis.set("arb:executor:status", payload, ex=30)
        except Exception:
            logger.exception("failed to publish executor status")

    # -------------------------------------------------------------
    # Emergency Cancel Handling
    # -------------------------------------------------------------

    def _emergency_cancel_targets(self, message_data: str) -> list:
        target_pair_key = None
        try:
            parsed = json.loads(message_data)
            target_pair_key = parsed.get("pair_key")
        except Exception:
            target_pair_key = message_data.strip() if message_data else None
        if not target_pair_key:
            return list(self.pending_entry_orders.keys())
        if target_pair_key in self.pending_entry_orders:
            return [target_pair_key]
        logger.debug(
            "[Emergency Cancel] Ignored for %s: no pending entry order",
            target_pair_key,
        )
        return []

    async def _cancel_emergency_entry_for_pair(self, pair_key: str) -> None:
        async with self._pair_lock(pair_key):
            token = None
            try:
                if not getattr(self.config, "mock_mode", False):
                    token = self._bind_pair_client(self._pair_info_for_tag(pair_key))
                order_info = self.pending_entry_orders.get(pair_key)
                if not order_info:
                    return
                order_id = order_info["order_id"]
                logger.warning(
                    f"[Emergency Cancel] Cancelling pending entry order {order_id} "
                    f"for {pair_key} due to negative funding"
                )
                resolved = await self._cancel_entry_order_and_reconcile(
                    pair_key, order_info, "Emergency Cancel"
                )
                if not resolved:
                    logger.critical(
                        f"[Emergency Cancel] order {order_id} remains unresolved; "
                        "pending state and reserved funds are retained"
                    )
            except Exception:
                logger.exception(
                    f"[Emergency Cancel] pair routing failed for {pair_key}"
                )
            finally:
                if token is not None:
                    self._reset_pair_client(token)

    async def handle_emergency_cancel(self, message_data: str):
        """Cancel matching pending spot entries after a negative-funding signal."""
        for pair_key in self._emergency_cancel_targets(message_data):
            await self._cancel_emergency_entry_for_pair(pair_key)
