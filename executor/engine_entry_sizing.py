"""Entry sizing and dual-leg funding checks."""

from decimal import Decimal

from executor.models import PositionState
from executor.quantity import quantize_base_quantity
from executor.helpers import BALANCES_KEY
from executor.engine_common import logger


class EngineEntrySizingMixin:
    """Prepares a quantity only after both spot and futures legs are fundable."""

    async def _prepare_entry_sizing(self, pair_tag, payload, pair_info, directives):
        snapshot = payload.pricing_snapshot
        # Target Limit Buy price in IDR
        buy_price_idr = directives.recommended_limit_buy_idr
        if buy_price_idr <= 0:
            return None
        sizing = await self._compute_entry_sizing(pair_tag, payload)
        if sizing is None:
            return None
        max_pos_usdt, _ = sizing

        capital_idr = Decimal(str(max_pos_usdt)) * Decimal(str(snapshot.usdt_idr_rate))
        desired_qty = float(capital_idr / Decimal(str(buy_price_idr)))
        try:
            buy_qty = quantize_base_quantity(
                desired_qty,
                pair_info.local_amount_spec,
                pair_info.global_amount_spec,
            )
        except ValueError as exc:
            self._mark_failed(
                pair_tag,
                "MARKET_AMOUNT_RULES_REJECT_SIZE",
                "Requested position cannot satisfy both market amount rules.",
                detail={"requested_base_qty": desired_qty, "error": str(exc)},
            )
            await self.publish_status()
            return None
        if buy_qty <= 0:
            return None
        actual_cost_usdt = (buy_qty * buy_price_idr) / snapshot.usdt_idr_rate

        # Both legs are committed at entry: the spot buy locks the local quote
        # currency and the 1x futures short locks margin on the global venue.
        # Verify BOTH are funded before submitting, so a live run cannot end up
        # with a filled spot leg and an unhedged (or margin-rejected) short leg.
        if not getattr(self.config, "mock_mode", False):
            funded = await self._verify_dual_leg_funding(
                pair_tag, pair_info, actual_cost_usdt, snapshot.usdt_idr_rate
            )
            if not funded:
                return None
        # Paper trading reserves simulated funds locally. Live orders are sent
        # directly to the venue so its response is authoritative for availability.
        if getattr(self.config, "mock_mode", False) and hasattr(self.client, "lock_funds"):
            if not self.client.lock_funds(actual_cost_usdt, actual_cost_usdt):
                logger.warning(
                    f"[State 1 Balance Check] Failed to lock funds for {actual_cost_usdt:.2f} USDT on {pair_tag}"
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "BALANCE_LOCK_FAILED",
                    "Exchange client gagal mengunci saldo untuk entry.",
                    detail={"required_usdt_per_leg": actual_cost_usdt},
                )
                return None
            # Sync balances to Redis
            bals = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                pair_info=pair_info,
                usdt_idr_rate=snapshot.usdt_idr_rate,
            )
            if bals is not None:
                await self.redis.set(BALANCES_KEY, json.dumps(bals))
        return buy_qty, actual_cost_usdt, buy_price_idr
