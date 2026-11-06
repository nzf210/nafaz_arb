"""Hedge-failure rollback: spot liquidate, ambiguity guard, reserve release.

Extracted from executor/entry.py; mixed into ArbitrageEngine so the self.*
state and helper methods stay on one object.
"""

import json
import logging
import time
from typing import Any, Dict, Optional

from executor import helpers
from executor.helpers import (
    RetryExhausted,
    build_client_order_id,
    retry_with_backoff,
)
from executor.models import PositionState

logger = logging.getLogger("executor.engine")


class RollbackMixin:
    """Emergency unwind when the global futures short cannot be opened."""

    async def _liquidate_unhedged_spot(
        self, pair_tag: str, symbol: str, unhedged_delta: float
    ) -> tuple[Optional[Dict[str, Any]], Optional[Exception]]:
        """
        Market-sells the unhedged spot quantity back to quote.

        Returns ``(order, error)``: the order is None when every attempt failed,
        and ``error`` then carries the last failure so the operator event can say
        what went wrong.
        """
        max_retries = 4  # 5 total attempts
        # One id for the whole retry sequence: every attempt is the same logical
        # order, so the venue can reject duplicates rather than sell twice.
        seq = self._next_order_seq()
        try:
            emergency_order = await retry_with_backoff(
                lambda: self.client.place_local_market_sell(
                    symbol,
                    unhedged_delta,
                    client_order_id=build_client_order_id(pair_tag, "rollback", seq),
                ),
                max_retries=max_retries,
                base_delay=0.5,
                label=f"hedge rollback sell {symbol} for {pair_tag}",
            )
        except RetryExhausted as exhausted:
            logger.critical(
                f"[Hedge Rollback] FAILED to liquidate spot leg for {pair_tag} after "
                f"{exhausted.attempts} attempts: {exhausted.last_error}. "
                f"MANUAL INTERVENTION REQUIRED: {unhedged_delta} {symbol} is UNHEDGED."
            )
            return None, exhausted.last_error
        return emergency_order, None

    async def _hedge_may_be_open(
        self, pair_tag: str, global_symbol: str, error: Exception
    ) -> bool:
        """
        Returns True when a futures short MIGHT be open after a hedge error.

        Only a venue-confirmed zero position is safe to roll back against. Any
        query failure or unexpected payload blocks the rollback (fail-safe):
        a wrongly-cancelled rollback leaves unhedged spot (recoverable by an
        operator), whereas a wrong rollback on an open short inverts exposure.
        """
        if not global_symbol:
            return True
        try:
            positions = await self.client.fetch_positions(global_symbol)
        except AttributeError:
            # Client without fetch_positions (e.g. some mocks): cannot confirm.
            logger.warning(
                f"[Hedge Ambiguity] {pair_tag}: client lacks fetch_positions; treating hedge state as unknown"
            )
            return True
        except Exception as exc:
            logger.warning(
                f"[Hedge Ambiguity] {pair_tag}: position query failed ({exc}); treating hedge state as unknown"
            )
            return True
        if not positions:
            return False
        total = 0.0
        for p in positions:
            side = str(p.get("side", "")).lower()
            qty = abs(float(p.get("contracts", 0.0) or 0.0))
            total += qty if side != "long" else -qty
        if abs(total) > 1e-12:
            logger.critical(
                f"[Hedge Ambiguity] {pair_tag}: venue reports net short {total} on "
                f"{global_symbol} despite hedge error ({error}) - spot NOT rolled back"
            )
            return True
        return False

    async def _release_unhedged_reserve(
        self, unhedged_delta: float, price_idr: float, usdt_rate: float
    ) -> None:
        """Releases the capital reserved for the spot leg that never got hedged."""
        if not hasattr(self.client, "unlock_funds"):
            return
        spot_cost = (unhedged_delta * price_idr) / usdt_rate if usdt_rate > 0 else 0.0
        self.client.unlock_funds(spot_cost, 0.0)
        if hasattr(self.client, "get_balances"):
            try:
                await self.redis.set(BALANCES_KEY, json.dumps(self.client.get_balances()))
            except Exception:
                logger.exception("failed to persist balances after hedge rollback")

    async def _rollback_hedge_failure(
        self,
        pair_tag: str,
        order_info: dict,
        unhedged_delta: float,
        price_idr: float,
        usdt_rate: float,
        error: Exception,
    ) -> None:
        """
        Emergency rollback when the global futures short cannot be opened.

        Without this the local spot coins bought by the maker order stay exposed
        to full directional risk. We market-sell the unhedged spot quantity back
        to quote and surface a CRITICAL event so the operator sees the loss.
        """
        symbol = order_info.get("symbol", "")
        logger.critical(
            f"[Hedge Rollback] Selling {unhedged_delta} {symbol} spot back to quote "
            f"after hedge failure on {pair_tag}"
        )

        # Timeout ambiguity guard: a hedge "failure" may be a timeout AFTER the
        # venue accepted the market sell. Rolling back the spot while the short
        # is actually open would leave the book net-short in the wrong direction.
        # Only proceed with the rollback when the venue confirms no open short.
        global_symbol = str(order_info.get("global_symbol", ""))
        hedge_error_is_ambiguous = await self._hedge_may_be_open(
            pair_tag, global_symbol, error
        )
        if hedge_error_is_ambiguous:
            self._mark_failed(
                pair_tag,
                "HEDGE_STATE_UNKNOWN",
                "hedge error is ambiguous and a futures short may be open; spot rollback blocked pending manual check",
                detail={
                    "unhedged_qty": unhedged_delta,
                    "symbol": symbol,
                    "global_symbol": global_symbol,
                    "hedge_error": str(error),
                },
            )
            event = {
                "event": "HEDGE_FAILED",
                "pair_key": pair_tag,
                "local_exchange": order_info.get("local_exchange"),
                "global_exchange": order_info.get("global_exchange"),
                "symbol": symbol,
                "qty": unhedged_delta,
                "entry_price_idr": price_idr,
                "exit_price_idr": 0.0,
                "rollback_succeeded": False,
                "reason": f"hedge state unknown, spot NOT rolled back: {error}",
                "rollback_error": None,
                "is_mock": getattr(self.config, "mock_mode", False),
                "timestamp_ms": int(time.time() * 1000),
            }
            await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(event))
            await self.publish_status()
            return

        emergency_order, emergency_error = await self._liquidate_unhedged_spot(
            pair_tag, symbol, unhedged_delta
        )

        exit_price_idr = 0.0
        if emergency_order:
            exit_price_idr = helpers.extract_fill_price(emergency_order, price_idr)
            if exit_price_idr <= 0:
                exit_price_idr = price_idr

        # Release the reserved capital for the leg that never got hedged.
        await self._release_unhedged_reserve(unhedged_delta, price_idr, usdt_rate)

        event = {
            "event": "HEDGE_FAILED",
            "pair_key": pair_tag,
            "local_exchange": order_info.get("local_exchange"),
            "global_exchange": order_info.get("global_exchange"),
            "symbol": symbol,
            "qty": unhedged_delta,
            "entry_price_idr": price_idr,
            "exit_price_idr": exit_price_idr,
            "rollback_succeeded": emergency_error is None,
            "reason": str(error),
            "rollback_error": str(emergency_error) if emergency_error else None,
            "is_mock": getattr(self.config, "mock_mode", False),
            "timestamp_ms": int(time.time() * 1000),
        }
        await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(event))

        if emergency_order is None:
            self._mark_failed(
                pair_tag,
                "HEDGE_FAILED",
                f"hedge failed and spot rollback failed: {emergency_error}",
                detail={
                    "unhedged_qty": unhedged_delta,
                    "symbol": symbol,
                    "entry_price_idr": price_idr,
                    "hedge_error": str(error),
                },
            )
        else:
            self.states[pair_tag] = PositionState.CLOSED
        await self.publish_status()
