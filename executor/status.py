"""StatusMixin responsibilities for ArbitrageEngine."""

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional

from executor.models import PositionState

logger = logging.getLogger("executor.engine")


class StatusMixin:
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
        # The operator kill switch does not auto-reset on a new UTC day.
        if (
            self._trading_halted
            and self._halt_reason == "DAILY_LOSS_LIMIT"
        ):
            self._trading_halted = False
            self._halt_reason = ""
            logger.warning("[Circuit Breaker] New UTC day - trading resumed")

    def record_realized_pnl(self, net_profit_usdt: float) -> None:
        """
        Feeds a closed trade's net PnL into the daily loss breaker.

        Losses beyond max_daily_loss_usdt halt *new entries* only: open positions
        keep being managed, because abandoning them unhedged would be worse than
        the loss the breaker is trying to stop.
        """
        self._roll_loss_day_if_needed()
        if net_profit_usdt < 0:
            self._daily_realized_loss_usdt += -net_profit_usdt

        limit = getattr(self.config, "max_daily_loss_usdt", 0.0)
        if limit <= 0 or self._trading_halted:
            return
        if self._daily_realized_loss_usdt >= limit:
            self._trading_halted = True
            self._halt_reason = "DAILY_LOSS_LIMIT"
            logger.critical(
                f"[Circuit Breaker] HALTED new entries: daily realized loss "
                f"{self._daily_realized_loss_usdt:.4f} USDT >= limit {limit:.4f} USDT"
            )

    async def resume_trading(self, reason: str = "MANUAL") -> None:
        """Clears the halt flag; used by the operator once a loss is reviewed."""
        self._trading_halted = False
        self._halt_reason = ""
        logger.warning(f"[Circuit Breaker] Trading resumed manually ({reason})")
        await self.publish_status()

    def clear_failure(self, pair_tag: str) -> None:
        """Clears a recorded failure so the pair can trade again."""
        if self._failure_reasons.pop(pair_tag, None) is not None:
            if self.states.get(pair_tag) == PositionState.FAILED:
                self.states[pair_tag] = PositionState.IDLE

    def status_snapshot(self) -> Dict[str, Any]:
        """Point-in-time view of the executor, suitable for a JSON API."""
        return {
            "status": "RUNNING" if self._running else "STOPPED",
            "mock_mode": bool(getattr(self.config, "mock_mode", False)),
            "local_exchange": self.config.local_exchange_name,
            "global_exchange": self.config.global_exchange_name,
            "open_positions": len(self.active_positions),
            "pending_entries": len(self.pending_entry_orders),
            "pending_exits": len(self.pending_exit_orders),
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

    async def handle_emergency_cancel(self, message_data: str):
        """
        Handles CANCEL_ALL_NEGATIVE_FUNDING from Scanner:
        Cancels any pending local Limit Buy orders in State 1.
        """
        target_pair_key = None
        try:
            parsed = json.loads(message_data)
            target_pair_key = parsed.get("pair_key")
        except Exception:
            target_pair_key = message_data.strip() if message_data else None

        to_cancel = []
        if target_pair_key:
            if target_pair_key in self.pending_entry_orders:
                to_cancel.append(target_pair_key)
            else:
                # Target pair has negative funding but we have no pending order for it; ignore quietly
                logger.debug(f"[Emergency Cancel] Ignored for {target_pair_key}: no pending entry order")
                return
        else:
            # If no specific key or symbol given, cancel all pending entry orders
            to_cancel = list(self.pending_entry_orders.keys())

        for p_key in to_cancel:
            async with self._pair_lock(p_key):
                order_info = self.pending_entry_orders.get(p_key)
                if not order_info:
                    continue
                if hasattr(self.client, "set_route"):
                    self.client.set_route(
                        order_info.get("local_exchange", ""),
                        order_info.get("global_exchange", ""),
                    )
                order_id = order_info["order_id"]
                logger.warning(
                    f"[Emergency Cancel] Cancelling pending entry order {order_id} for {p_key} due to negative funding"
                )
                resolved = await self._cancel_entry_order_and_reconcile(
                    p_key, order_info, "Emergency Cancel"
                )
                if not resolved:
                    logger.critical(
                        f"[Emergency Cancel] order {order_id} remains unresolved; "
                        "pending state and reserved funds are retained"
                    )
