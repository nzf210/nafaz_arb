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
        """
        Cancels and then fetches the order from the venue. A cancel acknowledgement
        is not proof that the order is terminal; fills racing cancellation are
        reconciled before force close is allowed to continue.
        """
        order_info = orders.get(pair_key)
        if not order_info:
            return True
        order_id = str(order_info["order_id"])
        symbol = str(order_info["symbol"])
        try:
            await self.client.cancel_local_order(order_id, symbol)
        except Exception:
            logger.exception(
                f"cancel request failed for {order_label} order {order_id}; "
                "checking final venue state"
            )

        try:
            order = await self.client.fetch_local_order(order_id, symbol)
        except Exception:
            logger.exception(
                f"cannot verify {order_label} order {order_id}; force close is blocked"
            )
            return False

        if not isinstance(order, dict):
            logger.critical(
                f"invalid venue response while verifying {order_label} order {order_id}"
            )
            return False

        status = str(order.get("status", "unknown")).lower()
        if status not in ("closed", "canceled", "cancelled", "rejected", "expired"):
            logger.critical(
                f"{order_label} order {order_id} remains unconfirmed (status={status}); "
                "force close is blocked"
            )
            return False

        filled = float(order.get("filled", 0.0) or 0.0)
        if order_label == "entry":
            order_info["filled"] = filled
            await self._handle_entry_fills(pair_key, order_info, order)
            if self.states.get(pair_key) in (
                PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
            ):
                return False
            await self._finalize_entry_order(pair_key, status)
        elif order_label == "exit":
            already_closed = float(order_info.get("closed_short_qty", 0.0) or 0.0)
            if filled > already_closed:
                await self._process_pending_exit(pair_key, force_reconcile=True)
                reconciled = float(order_info.get("closed_short_qty", 0.0) or 0.0)
                if reconciled + 1e-12 < filled:
                    logger.critical(
                        f"exit fill for {order_id} was not fully hedged during cancellation; "
                        "force close is blocked"
                    )
                    return False
            orders.pop(pair_key, None)

        logger.info(
            f"[Force Close] Verified terminal {order_label} order {order_id} "
            f"(status={status}, filled={filled})"
        )
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
