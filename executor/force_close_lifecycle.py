"""force close lifecycle responsibilities for emergency position closure."""

import json
import logging
import math
import time
from typing import Tuple

from executor.models import PositionState
from executor.helpers import ARB_TRADE_HISTORY_KEY, ARB_TRADE_HISTORY_MAX_LEN, BALANCES_KEY

logger = logging.getLogger("executor.engine")
ARB_STATS_KEY = "arb:stats"



def _parse_force_close_directive_pair_key(message_data: str):
    """Extract the pair key from a FORCE_CLOSE directive."""
    if not message_data:
        return None
    try:
        parsed = json.loads(message_data)
    except Exception:
        return message_data.strip() or None
    if isinstance(parsed, dict):
        return parsed.get("pair_key")
    return None


class ForceCloseLifecycleMixin:
    """Focused emergency close leg or lifecycle operations."""

    @staticmethod
    def _parse_force_close_directive_pair_key(message_data: str):
        """Compatibility alias for the module-level directive parser."""
        return _parse_force_close_directive_pair_key(message_data)


    async def _record_force_close_trade(
        self,
        pair_key: str,
        pos,
        pnl: dict,
        current_ask: float,
        current_spot_bid: float,
        spot_sell_failed: bool,
        usdt_rate: float,
    ):
        """Persists the closed trade, settles balances, and updates stats/events."""
        closed_trade = {
            "trade_id": f"force-{int(time.time()*1000)}",
            "local_exchange": pos.local_exchange,
            "global_exchange": pos.global_exchange,
            "symbol": pos.symbol,
            "qty": pos.qty,
            "entry_price_idr": pos.entry_price_idr,
            "exit_price_idr": current_spot_bid,
            "short_entry_usdt": pos.short_entry_usdt,
            "short_exit_usdt": current_ask,
            "pnl_complete": pnl.get("pnl_complete", True),
            "spot_pnl_idr": pnl["spot_pnl_idr"],
            "futures_pnl_usdt": pnl["futures_pnl_usdt"],
            "accumulated_funding_usdt": pnl["funding_usdt"],
            "spot_fees_usdt": pnl["spot_fees_usdt"],
            "futures_fees_usdt": pnl["futures_fees_usdt"],
            "fees_usdt": pnl["fees_usdt"],
            "fee_source": pnl["fee_source"],
            "fee_verified": pnl["fee_verified"],
            "net_profit_usdt": pnl["net_profit_usdt"],
            "net_profit_idr": pnl["net_profit_idr"],
            "net_roi_pct": pnl["roi_pct"],
            "is_mock": getattr(self.config, "mock_mode", False),
            "spot_liquidation_failed": spot_sell_failed,
            "closed_at_ms": int(time.time() * 1000),
        }
        await self.redis.lpush(ARB_TRADE_HISTORY_KEY, json.dumps(closed_trade))
        await self.redis.ltrim(
            ARB_TRADE_HISTORY_KEY, 0, ARB_TRADE_HISTORY_MAX_LEN
        )
        self.supabase.trade_closed(pair_key, closed_trade, pos)
        pnl["risk_event_id"] = closed_trade["trade_id"]

        # Update exchange account balances with realized PnL & release locked funds
        spot_cost = float(getattr(pos, "spot_cost_usdt", 0.0) or 0.0)
        if spot_cost <= 0 and usdt_rate > 0 and math.isfinite(usdt_rate):
            spot_cost = pos.qty * pos.entry_price_idr / usdt_rate
        if spot_cost <= 0:
            logger.warning(
                f"[Force Close] {pair_key}: live FX unavailable and no stored "
                "USDT spot-cost basis; balance accounting is incomplete"
            )
        fut_margin = float(
            getattr(pos, "futures_margin_usdt", 0.0)
            or (pos.qty * pos.short_entry_usdt)
        )
        if hasattr(self.client, "apply_realized_trade"):
            self.client.apply_realized_trade(
                spot_cost_usdt=spot_cost,
                spot_pnl_usdt=pnl["spot_pnl_usdt"],
                spot_fees_usdt=pnl["spot_fees_usdt"],
                futures_margin_usdt=fut_margin,
                futures_pnl_usdt=pnl["futures_pnl_usdt"],
                funding_usdt=pnl["funding_usdt"],
                futures_fees_usdt=pnl["futures_fees_usdt"],
            )
            if usdt_rate > 0 and math.isfinite(usdt_rate):
                bals = await self._get_balance_snapshot(
                    pair_tag=pair_key,
                    symbol=pos.symbol,
                    usdt_idr_rate=usdt_rate,
                )
                if bals is not None:
                    await self.redis.set(BALANCES_KEY, json.dumps(bals))

        pipe = self.redis.pipeline()
        pipe.hincrby(ARB_STATS_KEY, "total_closed", 1)
        if pnl["net_profit_usdt"] > 0:
            pipe.hincrby(ARB_STATS_KEY, "win_count", 1)
        pipe.hincrbyfloat(ARB_STATS_KEY, "total_pnl_usdt", pnl["net_profit_usdt"])
        pipe.hincrbyfloat(ARB_STATS_KEY, "total_pnl_idr", pnl["net_profit_idr"])
        event_msg = {
            "event": "POSITION_CLOSED",
            **closed_trade,
        }
        pipe.publish(self.ARB_TRADE_EVENTS, json.dumps(event_msg))
        await pipe.execute()

    async def handle_force_close(self, message_data: str):
        """
        Handles FORCE_CLOSE directive from Telegram or API:
        1. Cancels pending exit/entry orders on local exchange.
        2. Closes open Short position on Global Futures via Market Buy (reduceOnly=True).
        3. Liquidates local spot coins if necessary.
        4. Removes active position from Redis.
        """
        logger.warning(f"Received FORCE_CLOSE directive: {message_data}")
        pair_key = _parse_force_close_directive_pair_key(message_data)
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
            self.record_realized_pnl(pnl["net_profit_usdt"], event_id=pnl.get("risk_event_id") or f"force-close:{pair_key}:{getattr(pos, 'opened_at_ms', 0)}", pair_key=pair_key)
        self.states[pair_key] = PositionState.CLOSED
        self._failure_reasons.pop(pair_key, None)
        self._persist_risk_state("position_failure", scope="position", pair_key=pair_key, resolved=True)
        self._clear_exposure_halt(pair_key, "verified force close completed")
        if pnl.get("pnl_unverified"):
            logger.critical(
                f"[Force Close Complete] Exposure for {pair_key} is flat, but realized "
                "PnL could not be verified from exchange fills; review manually."
            )
        else:
            logger.info(f"[Force Close Complete] Position for {pair_key} successfully closed.")

    async def _force_close_locked(self, pair_key: str) -> None:
        """Bind emergency order routing to this position's exact venues."""
        await self._run_pair_bound(
            pair_key, self._force_close_locked_bound, pair_key
        )

    async def _force_close_locked_bound(self, pair_key: str) -> None:
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

        # Remember whether this is a retry of an incomplete force close.
        # The retry path checks actual futures exposure first so a previously
        # closed futures leg is not submitted a second time.
        allow_already_flat = (
            self.states.get(pair_key) == PositionState.RECONCILIATION_REQUIRED
        )
        # Guard concurrent exit-fill processing for this pair while force close runs.
        self.states[pair_key] = PositionState.RECONCILIATION_REQUIRED
        orders_reconciled = await self._cancel_pending_orders_for_force_close(pair_key)
        pos = self.active_positions.get(pair_key)
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
