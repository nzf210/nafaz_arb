"""Force-close request orchestration and failure/success handling."""

"""Force-close handling for ArbitrageEngine.

Extracted from executor/engine.py; mixed into ArbitrageEngine so the
self.* state and helper methods stay on one object.
"""

import json
import logging
import math
import time
from typing import Tuple

from executor.models import PositionState
from executor import helpers
from executor.helpers import (
    ARB_TRADE_HISTORY_KEY,
    ARB_TRADE_HISTORY_MAX_LEN,
    BALANCES_KEY,
    RetryExhausted,
    build_client_order_id,
)

logger = logging.getLogger("executor.engine")


class ForceCloseHandlerMixin:
    async def handle_force_close(self, message_data: str):
        """
        Handles FORCE_CLOSE directive from Telegram or API:
        1. Cancels pending exit/entry orders on local exchange.
        2. Closes open Short position on Global Futures via Market Buy (reduceOnly=True).
        3. Liquidates local spot coins if necessary.
        4. Removes active position from Redis.
        """
        logger.warning(f"Received FORCE_CLOSE directive: {message_data}")
        pair_key = self._parse_force_close_pair_key(message_data)
        if not pair_key:
            return

        # Serialize against the entry/exit paths for this pair: cancelling
        # pending orders and liquidating legs must not interleave with a
        # hedge or exit fill processed on another task.
        async with self._pair_lock(pair_key):
            await self._force_close_locked(pair_key)


    async def _execute_force_close_legs(
        self, pair_key: str, pos, allow_already_flat: bool = False
    ) -> Tuple[bool, bool, dict]:
        """
        Executes short closure on futures and spot liquidation.
        Returns (futures_closed, spot_sell_failed, pnl).
        """
        if not pos:
            return True, False, {"net_profit_usdt": 0.0, "net_profit_idr": 0.0}
        if pos.qty <= 0:
            logger.critical(
                f"[Force Close] refusing to clear {pair_key}: persisted position qty "
                f"is invalid ({pos.qty}); manual reconciliation required"
            )
            return False, True, {"net_profit_usdt": 0.0, "net_profit_idr": 0.0}

        futures_closed, short_exit_price_usdt = await self._close_global_short_leg(
            pair_key, pos, allow_already_flat=allow_already_flat
        )
        spot_sell_failed = True
        spot_exit_price_idr = 0.0

        # Even when the futures close fill price is unavailable, a verified-flat
        # futures leg must not leave the spot leg exposed. Liquidate spot, but
        # mark the realized PnL as unverified instead of substituting a quote.
        if futures_closed:
            spot_exit_price_idr = await self._liquidate_spot_leg(pair_key, pos)
            spot_sell_failed = spot_exit_price_idr <= 0

        current_ask, usdt_rate, current_spot_bid = await self._resolve_force_close_prices(
            pair_key, pos, spot_exit_price_idr if futures_closed else 0.0
        )
        if futures_closed and short_exit_price_usdt > 0:
            current_ask = short_exit_price_usdt
            pnl = self._compute_force_close_pnl(
                pos, current_ask, usdt_rate, current_spot_bid
            )
        else:
            pnl = {
                "net_profit_usdt": 0.0,
                "net_profit_idr": 0.0,
                "pnl_unverified": True,
            }

        if futures_closed and not spot_sell_failed and not pnl.get("pnl_unverified"):
            await self._record_force_close_trade(
                pair_key,
                pos,
                pnl,
                current_ask,
                current_spot_bid,
                spot_sell_failed,
                usdt_rate,
            )
        elif futures_closed and spot_sell_failed and not pnl.get("pnl_unverified"):
            # Futures leg closed but spot liquidation failed — record partial close so
            # the futures PnL is not lost from history, and flag it clearly.
            logger.error(
                f"[Force Close] Recording partial force-close for {pair_key}: "
                f"futures closed, spot liquidation FAILED. Manual spot sale required."
            )
            await self._record_force_close_trade(
                pair_key,
                pos,
                pnl,
                current_ask,
                current_spot_bid,
                spot_sell_failed,
                usdt_rate,
            )

        return futures_closed, spot_sell_failed, pnl

    async def _handle_force_close_failure(
        self, pair_key: str, pos, futures_closed: bool, spot_sell_failed: bool
    ) -> None:
        """P0: Retains persistent state with RECONCILIATION_REQUIRED on partial or failed force close."""
        fail_reason = "FORCE_CLOSE_PARTIAL_OR_FAILED"
        fail_msg = (
            f"futures_closed={futures_closed}, spot_liquidated={not spot_sell_failed}. "
            "Manual reconciliation required; position retained in state."
        )
        if pos:
            pos.state = "RECONCILIATION_REQUIRED"
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(f"arb:active_pos:{pair_key}", json.dumps(pos.to_dict()))

        self._mark_failed(
            pair_key,
            fail_reason,
            fail_msg,
            detail={
                "qty": pos.qty if pos else 0.0,
                "symbol": pos.symbol if pos else "",
                "futures_closed": futures_closed,
                "spot_sell_failed": spot_sell_failed,
            },
            state=PositionState.RECONCILIATION_REQUIRED,
        )
        logger.critical(
            f"[Force Close Alert] {pair_key} incomplete! Retaining active position in Redis: {fail_msg}"
        )

    async def _handle_force_close_success(self, pair_key: str, pos, pnl: dict) -> None:
        """Cleans up active state in Redis and memory when force close fully succeeds."""
        await self.redis.delete(f"arb:active_pos:{pair_key}")
        self.active_positions.pop(pair_key, None)
        self.last_pegged_global_ask.pop(pair_key, None)
        if pos and pos.qty > 0 and not pnl.get("pnl_unverified"):
            self.record_realized_pnl(pnl["net_profit_usdt"])
        self.states[pair_key] = PositionState.CLOSED
        if pnl.get("pnl_unverified"):
            logger.critical(
                f"[Force Close Complete] Exposure for {pair_key} is flat, but realized "
                "PnL could not be verified from exchange fills; review manually."
            )
        else:
            logger.info(f"[Force Close Complete] Position for {pair_key} successfully closed.")

    async def _force_close_locked(self, pair_key: str) -> None:
        # A corrupted persisted position has no trustworthy quantity/symbol to
        # submit against the venue. Never treat it as an already-flat position.
        failure = self._failure_reasons.get(pair_key, {})
        if failure.get("reason") == "ACTIVE_POSITION_RECORD_INVALID":
            logger.critical(
                f"[Force Close] cannot execute for {pair_key}: persisted position "
                "record is invalid; manual exchange-side reconciliation required"
            )
            await self.publish_status()
            return

        # Select the position's original venues BEFORE cancelling any pending
        # orders; otherwise a retry could cancel an order on the default venue.
        pos = self.active_positions.get(pair_key)
        pending = self.pending_entry_orders.get(pair_key) or self.pending_exit_orders.get(pair_key) or {}
        if hasattr(self.client, "set_route"):
            if pos:
                self.client.set_route(pos.local_exchange, pos.global_exchange)
            elif pending:
                self.client.set_route(
                    pending.get("local_exchange", ""),
                    pending.get("global_exchange", ""),
                )
        # Remember whether this is a retry of an incomplete force close.
        # The retry path checks actual futures exposure first so a previously
        # closed futures leg is not submitted a second time.
        allow_already_flat = (
            self.states.get(pair_key) == PositionState.RECONCILIATION_REQUIRED
        )
        # Guard concurrent exit-fill processing for this pair while force close runs.
        self.states[pair_key] = PositionState.RECONCILIATION_REQUIRED
        orders_reconciled = await self._cancel_pending_orders_for_force_close(pair_key)
        if not orders_reconciled:
            await self._handle_force_close_failure(
                pair_key, pos, False, True
            )
            await self.publish_status()
            return

        futures_closed, spot_sell_failed, pnl = await self._execute_force_close_legs(
            pair_key, pos, allow_already_flat=allow_already_flat
        )

        if not futures_closed or spot_sell_failed:
            await self._handle_force_close_failure(pair_key, pos, futures_closed, spot_sell_failed)
        else:
            await self._handle_force_close_success(pair_key, pos, pnl)

        await self.publish_status()
