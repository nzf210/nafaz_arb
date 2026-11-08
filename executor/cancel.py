"""Pending-order cancellation before force close.

Cancels resting orders and reconciles any fills that race the cancellation.
"""

import logging

from executor.models import PositionState

logger = logging.getLogger("executor.engine")


class CancelMixin:
    """Cancels pending entry/exit orders ahead of a force close."""

    async def _cancel_pending_order_for_force_close(
        self, pair_key: str, orders: dict, order_label: str
    ) -> bool:
        """Cancel, verify terminal state, and reconcile any fill racing cancellation."""
        order_info = orders.get(pair_key)
        if not order_info:
            return True
        identity = self._force_close_order_identity(pair_key, order_info, order_label)
        if identity is None:
            return False
        order_id, symbol = identity
        await self._request_force_close_order_cancel(order_id, symbol, order_label)
        order = await self._fetch_force_close_terminal_order(order_id, symbol, order_label)
        if order is None:
            return False
        status = str(order.get("status", "unknown")).lower()
        if not self._is_terminal_force_close_order(status, order_id, order_label):
            return False
        filled = float(order.get("filled", 0.0) or 0.0)
        if order_label == "entry":
            if not await self._reconcile_cancelled_entry(pair_key, order_info, order, status, filled):
                return False
        elif order_label == "exit":
            if not await self._reconcile_cancelled_exit(pair_key, orders, order_info, order_id, symbol, filled):
                return False
        logger.info(
            "[Force Close] Verified terminal %s order %s (status=%s, filled=%s)",
            order_label, order_id, status, filled,
        )
        return True

    def _force_close_order_identity(self, pair_key: str, order_info: dict, order_label: str):
        order_id = str(order_info.get("order_id", ""))
        symbol = str(order_info.get("symbol", ""))
        if order_id and symbol:
            return order_id, symbol
        logger.critical(
            "[Force Close] cannot reconcile malformed %s order record for %s; force close is blocked",
            order_label, pair_key,
        )
        return None

    async def _request_force_close_order_cancel(self, order_id: str, symbol: str, order_label: str) -> None:
        try:
            await self.client.cancel_local_order(order_id, symbol)
        except Exception:
            logger.exception(
                "cancel request failed for %s order %s; checking final venue state",
                order_label, order_id,
            )

    async def _fetch_force_close_terminal_order(self, order_id: str, symbol: str, order_label: str):
        try:
            order = await self.client.fetch_local_order(order_id, symbol)
        except Exception:
            logger.exception(
                "cannot verify %s order %s; force close is blocked", order_label, order_id
            )
            return None
        if isinstance(order, dict):
            return order
        logger.critical(
            "invalid venue response while verifying %s order %s", order_label, order_id
        )
        return None

    @staticmethod
    def _is_terminal_force_close_order(status: str, order_id: str, order_label: str) -> bool:
        terminal = ("closed", "canceled", "cancelled", "rejected", "expired")
        if status in terminal:
            return True
        logger.critical(
            "%s order %s remains unconfirmed (status=%s); force close is blocked",
            order_label, order_id, status,
        )
        return False

    async def _reconcile_cancelled_entry(
        self, pair_key: str, order_info: dict, order: dict, status: str, filled: float
    ) -> bool:
        order_info["filled"] = filled
        await self._handle_entry_fills(pair_key, order_info, order)
        if self.states.get(pair_key) in (
            PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
        ):
            return False
        await self._finalize_entry_order(pair_key, status)
        return True

    async def _reconcile_cancelled_exit(
        self, pair_key: str, orders: dict, order_info: dict,
        order_id: str, symbol: str, filled: float,
    ) -> bool:
        already_closed = float(order_info.get("closed_short_qty", 0.0) or 0.0)
        if filled > already_closed:
            await self._process_pending_exit(pair_key, force_reconcile=True)
            reconciled = float(order_info.get("closed_short_qty", 0.0) or 0.0)
            if reconciled + 1e-12 < filled:
                logger.critical(
                    "exit fill for %s was not fully hedged during cancellation; force close is blocked",
                    order_id,
                )
                return False
        orders.pop(pair_key, None)
        return True

    async def _cancel_pending_exit_for_force_close(self, pair_key: str) -> bool:
        return await self._cancel_pending_order_for_force_close(
            pair_key, self.pending_exit_orders, "exit"
        )

    async def _cancel_pending_entry_for_force_close(self, pair_key: str) -> bool:
        return await self._cancel_pending_order_for_force_close(
            pair_key, self.pending_entry_orders, "entry"
        )

    async def _cancel_pending_orders_for_force_close(self, pair_key: str) -> bool:
        """Force close proceeds only after every resting order is terminal and reconciled."""
        if not await self._cancel_pending_exit_for_force_close(pair_key):
            return False
        if not await self._cancel_pending_entry_for_force_close(pair_key):
            return False
        return True
