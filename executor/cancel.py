"""Pending-order cancellation before force close.

Cancels the resting exit and entry orders tracked for a pair so a force close
never races an in-flight fill. Extracted from executor/force_close.py; mixed
into ArbitrageEngine so the self.* state stays on one object.
"""

import json
import logging

from executor.helpers import BALANCES_KEY

logger = logging.getLogger("executor.engine")


class CancelMixin:
    """Cancels pending entry/exit orders ahead of a force close."""

    async def _cancel_pending_order_for_force_close(
        self, pair_key: str, orders: dict, order_label: str
    ) -> bool:
        """Cancels a pending order (exit/entry) tracked in `orders`. Returns True ONLY if cancellation succeeded on the exchange."""
        order_info = orders.get(pair_key)
        if not order_info:
            return False
        try:
            res = await self.client.cancel_local_order(order_info["order_id"], order_info["symbol"])
            # If exchange client returns status, ensure it's not an explicit failure
            if isinstance(res, dict) and res.get("status") in ("failed", "rejected"):
                logger.error(
                    f"[Force Close] Exchange rejected cancellation of {order_label} order {order_info['order_id']}: {res}"
                )
                return False
            logger.info(
                f"[Force Close] Canceled pending {order_label} order {order_info['order_id']}"
            )
            return True
        except Exception:
            logger.exception(f"failed to cancel {order_label} order {order_info.get('order_id')} on force close")
            return False

    async def _cancel_pending_exit_for_force_close(self, pair_key: str):
        if await self._cancel_pending_order_for_force_close(
            pair_key, self.pending_exit_orders, "exit"
        ):
            self.pending_exit_orders.pop(pair_key, None)

    async def _cancel_pending_entry_for_force_close(self, pair_key: str):
        order_info = self.pending_entry_orders.get(pair_key)
        if not await self._cancel_pending_order_for_force_close(
            pair_key, self.pending_entry_orders, "entry"
        ):
            return
        if hasattr(self.client, "unlock_funds"):
            self.client.unlock_funds(
                float(order_info.get("spot_cost_usdt", 0.0)),
                float(order_info.get("futures_margin_usdt", 0.0)),
            )
            bals = self.client.get_balances()
            await self.redis.set(BALANCES_KEY, json.dumps(bals))
        self.pending_entry_orders.pop(pair_key, None)
        await self._clear_pending_entry(pair_key)

    async def _cancel_pending_orders_for_force_close(self, pair_key: str) -> None:
        """Cancels pending exit and entry orders before closing position legs."""
        await self._cancel_pending_exit_for_force_close(pair_key)
        await self._cancel_pending_entry_for_force_close(pair_key)
