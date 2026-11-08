"""Exit fill settlement, paired PnL accounting, and position finalization."""

"""Dynamic exit pegging and exit execution for ArbitrageEngine.

Covers State 3 (dynamic limit sell + funding decay re-pegging) and
State 4 (local limit sell fill -> global market buy reduceOnly).
Extracted from executor/engine.py as a mixin.
"""

import json
import logging
import math
import time
from typing import Optional

from executor.models import ActivePosition, PositionState
from executor.helpers import (
    ceil_to_tick,
)

logger = logging.getLogger("executor.engine")


class ExitFillMixin:
    async def _execute_exit_fill(
        self,
        pair_tag: str,
        order_info: dict,
        pos: ActivePosition,
        unclosed_delta: float,
        spot_order: Optional[dict] = None,
    ) -> None:
        exit_price_idr, cumulative_spot_cost = self._resolve_spot_exit_fill_price(
            order_info, spot_order, unclosed_delta
        )
        entry_price_idr = float(pos.entry_price_idr)
        short_entry_usdt = float(pos.short_entry_usdt)
        usdt_rate, _ = await self._load_exit_market_snapshot(pair_tag, short_entry_usdt)
        logger.info(
            f"[State 4 Exit Execution] Local Limit Sell fill detected: sold_delta={unclosed_delta} "
            f"on {pair_tag}. Immediately closing Short via Market Buy (reduceOnly=True)!"
        )
        try:
            close_order, global_symbol, client_order_id = await self._close_futures_short_for_exit(
                pair_tag, pos, unclosed_delta
            )
            settlement = await self._settle_exit_fill(
                pair_tag, order_info, pos, unclosed_delta, close_order,
                exit_price_idr, cumulative_spot_cost, entry_price_idr,
                short_entry_usdt, usdt_rate,
            )
            await self._persist_exit_settlement(pair_tag, pos, settlement)
            await self._finalize_exit_position(pair_tag, pos)
        except Exception as exc:
            await self._mark_exit_reconciliation_required(pair_tag, order_info, pos, unclosed_delta, exc)

    def _resolve_global_symbol(self, symbol: str) -> str:
        """Convert local/pos symbol into global futures symbol format."""
        global_sym = symbol.upper()
        if "/" not in global_sym and "_" in global_sym:
            global_sym = global_sym.replace("_", "/")
        if "IDR" in global_sym:
            base = global_sym.split("/")[0] if "/" in global_sym else global_sym.replace("IDR", "")
            return f"{base}/USDT:USDT"
        return global_sym

    async def _finalize_exit_position(
        self, pair_tag: str, pos: ActivePosition
    ) -> None:
        """Update Redis active position or delete if fully closed."""
        active_key = f"arb:active_pos:{pair_tag}"
        if pos.qty <= 0.00001:  # dust threshold / fully exited
            logger.info(
                f"[State 4 Complete] Arbitrage position for {pair_tag} fully exited! Deleting active pos."
            )
            await self.redis.delete(active_key)
            del self.active_positions[pair_tag]
            del self.pending_exit_orders[pair_tag]
            if pair_tag in self.last_pegged_global_ask:
                del self.last_pegged_global_ask[pair_tag]
            self.states[pair_tag] = PositionState.CLOSED
        else:
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(active_key, json.dumps(pos.to_dict()))
