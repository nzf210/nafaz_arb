"""exit pricing methods for exit lifecycle management."""

import logging
import json
import math
import time
from typing import Optional

from executor.models import ActivePosition
from executor import helpers
from executor.helpers import ceil_to_tick
from executor.fee_engine import rates_for_position, required_spot_exit_usdt as required_spot_exit_usdt_fn

logger = logging.getLogger("executor.engine")



class ExitPricingMixin:
    """Focused exit pricing, order management, or execution responsibilities."""

    async def _fetch_live_price(
        self, pair_tag: str
    ) -> Optional[tuple[float, float]]:
        """Return fresh (global ask, USDT/IDR rate), or None when unavailable."""
        live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if live_price_raw:
            try:
                live_snapshot = json.loads(live_price_raw)
                global_ask_usdt = float(
                    live_snapshot.get("global_best_ask_usdt", 0.0)
                )
                usdt_idr_rate = float(
                    live_snapshot.get("usdt_idr_rate", 0.0) or 0.0
                )
                if (
                    global_ask_usdt > 0
                    and math.isfinite(usdt_idr_rate)
                    and usdt_idr_rate > 0
                    and not helpers.snapshot_is_stale(live_snapshot)
                ):
                    return global_ask_usdt, usdt_idr_rate
            except Exception:
                pass
        return None

    def _collect_settled_funding_payments(
        self, pair_tag: str, pos: ActivePosition, payments: list, now_ms: int
    ) -> list:
        settled = []
        for payment in payments:
            if not isinstance(payment, dict):
                continue
            timestamp = int(payment.get("timestamp") or 0)
            if timestamp <= pos.last_funding_time_ms or timestamp > now_ms:
                continue
            asset = str(
                payment.get("code") or payment.get("currency") or "USDT"
            ).upper()
            if asset != "USDT":
                logger.error(
                    f"[Funding Reconciliation] unsupported settlement asset {asset} "
                    f"for {pair_tag}; payment is not added to USDT PnL"
                )
                continue
            try:
                amount = float(payment.get("amount"))
            except (TypeError, ValueError):
                logger.error(
                    f"[Funding Reconciliation] invalid payment amount for {pair_tag}: {payment}"
                )
                continue
            settled.append((timestamp, amount, payment))
        return settled

    async def _refresh_funding_schedule(
        self, pair_tag: str, symbol: str, current_rate: float
    ) -> tuple[int, float]:
        next_funding_ms = 0
        try:
            rate_data = await self.client.fetch_funding_rate(symbol)
            if isinstance(rate_data, dict):
                next_funding_ms = int(
                    rate_data.get("nextFundingTimestamp")
                    or rate_data.get("nextFundingTime")
                    or 0
                )
                if rate_data.get("fundingRate") is not None:
                    current_rate = float(rate_data["fundingRate"])

                # Prefer an explicit nextFundingRate. Otherwise use CCXT's
                # unified fundingRate as the venue's current/predicted rate;
                # never interpret a historical lastFundingRate as a forecast.
                info = rate_data.get("info")
                info = info if isinstance(info, dict) else {}
                next_rate_raw = (
                    rate_data.get("nextFundingRate")
                    if rate_data.get("nextFundingRate") is not None
                    else info.get("nextFundingRate")
                )
                if next_rate_raw is None:
                    next_rate_raw = rate_data.get("fundingRate")
                if next_rate_raw is not None:
                    next_rate = float(next_rate_raw)
                    if math.isfinite(next_rate):
                        rates = getattr(self, "_next_funding_rate_by_pair", {})
                        rates[pair_tag] = next_rate
                        self._next_funding_rate_by_pair = rates
        except Exception:
            logger.exception(
                f"[Funding Reconciliation] cannot refresh next settlement schedule "
                f"for {pair_tag}; automatic funding rollover is paused"
            )
        return next_funding_ms, current_rate

    def _funding_decay_pct(
        self, pair_tag: str, pos: ActivePosition, position_value_usdt: float
    ) -> float:
        """Return only the permitted funding contribution to spread decay.

        Actual accumulated funding remains untouched for the PnL/profit-floor
        calculation. A predicted positive funding rate at or above +0.03%
        disables spread decay for this quote.
        """
        if position_value_usdt <= 0 or not math.isfinite(position_value_usdt):
            return 0.0
        next_rates = getattr(self, "_next_funding_rate_by_pair", {})
        next_rate = next_rates.get(pair_tag)
        if next_rate is not None:
            try:
                next_rate = float(next_rate)
            except (TypeError, ValueError):
                next_rate = None
        if next_rate is not None and math.isfinite(next_rate) and next_rate >= 0.0003:
            return 0.0

        retention = float(
            getattr(self.config, "funding_decay_retention_ratio", 0.60)
        )
        if not math.isfinite(retention):
            retention = 0.60
        retention = min(1.0, max(0.0, retention))
        decay_eligible_funding = pos.accumulated_funding_usdt * (1.0 - retention)
        return (decay_eligible_funding / position_value_usdt) * 100.0

    async def _refresh_predicted_funding_rate(
        self, pair_tag: str, pos: ActivePosition, now_ms: int
    ) -> None:
        """Refresh the upcoming rate periodically, before the funding boundary."""
        retry_after = getattr(self, "_funding_rate_refresh_retry_after_ms", {})
        if now_ms < retry_after.get(pair_tag, 0):
            return
        retry_after[pair_tag] = now_ms + 30_000
        self._funding_rate_refresh_retry_after_ms = retry_after
        symbol = self._resolve_global_symbol(pos.symbol)
        await self._refresh_funding_schedule(
            pair_tag, symbol, pos.current_funding_rate
        )

    async def _persist_next_funding_schedule(
        self, pair_tag: str, pos: ActivePosition, now_ms: int,
        next_funding_ms: int, current_rate: float,
    ) -> None:
        if next_funding_ms <= now_ms:
            return
        pos.next_funding_time_ms = next_funding_ms
        pos.current_funding_rate = current_rate
        pos.updated_at = now_ms
        await self.redis.set(
            f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
        )

    async def _apply_settled_funding(
        self, pair_tag: str, pos: ActivePosition, settled: list,
        next_funding_ms: int, current_rate: float, now_ms: int,
    ) -> None:
        settled.sort(key=lambda item: item[0])
        is_mock = bool(getattr(self.config, "mock_mode", False))
        for _timestamp, _amount, payment in settled:
            if hasattr(self.supabase, "funding_payment"):
                self.supabase.funding_payment(
                    pair_tag, pos, payment, is_mock=is_mock
                )
        actual_amount = sum(item[1] for item in settled)
        pos.accumulated_funding_usdt += actual_amount
        pos.last_funding_time_ms = settled[-1][0]
        pos.current_funding_rate = current_rate
        pos.next_funding_time_ms = (
            next_funding_ms if next_funding_ms > pos.last_funding_time_ms else 0
        )
        pos.updated_at = now_ms
        await self.redis.set(
            f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
        )
        logger.info(
            f"[Funding Reconciliation] {pair_tag}: applied {len(settled)} actual "
            f"settlement(s), net {actual_amount:+.8f} USDT; "
            f"cumulative={pos.accumulated_funding_usdt:+.8f} USDT"
        )
        if pos.next_funding_time_ms <= 0:
            logger.critical(
                f"[Funding Reconciliation] next settlement time unavailable for "
                f"{pair_tag}; funding accrual paused pending a valid exchange schedule"
            )

    async def _handle_funding_rollover(
        self, pair_tag: str, pos: ActivePosition, now_ms: int
    ) -> bool:
        """Apply only exchange-confirmed funding payments and refresh settlement schedule."""
        if pos.next_funding_time_ms <= 0 or now_ms < pos.next_funding_time_ms:
            return False
        retry_after = getattr(self, "_funding_reconcile_retry_after_ms", {})
        if now_ms < retry_after.get(pair_tag, 0):
            return False
        retry_after[pair_tag] = now_ms + 30_000
        self._funding_reconcile_retry_after_ms = retry_after

        symbol = self._resolve_global_symbol(pos.symbol)
        since_ms = (
            pos.last_funding_time_ms
            if pos.last_funding_time_ms > 0
            else max(0, pos.next_funding_time_ms - 1)
        )
        try:
            payments = await self.client.fetch_funding_history(
                symbol, since=since_ms, limit=100
            )
        except Exception:
            logger.exception(
                f"[Funding Reconciliation] cannot fetch actual settlement history "
                f"for {pair_tag}; estimated funding will not be credited"
            )
            return False
        if not isinstance(payments, list):
            logger.error(
                f"[Funding Reconciliation] invalid funding history response for {pair_tag}"
            )
            return False

        settled = self._collect_settled_funding_payments(
            pair_tag, pos, payments, now_ms
        )
        next_funding_ms, current_rate = await self._refresh_funding_schedule(
            pair_tag, symbol, pos.current_funding_rate
        )
        if not settled:
            await self._persist_next_funding_schedule(
                pair_tag, pos, now_ms, next_funding_ms, current_rate
            )
            return False
        await self._apply_settled_funding(
            pair_tag, pos, settled, next_funding_ms, current_rate, now_ms
        )
        return True

    def _should_repeg_exit(
        self, pair_tag: str, global_ask_usdt: float, funding_passed: bool
    ) -> bool:
        if pair_tag not in self.pending_exit_orders:
            return True

        last_ask = self.last_pegged_global_ask.get(pair_tag, 0.0)
        ask_movement = 0.0
        if last_ask > 0:
            ask_movement = abs(global_ask_usdt - last_ask) / last_ask

        return (
            (last_ask <= 0)
            or (ask_movement >= self.config.exit_price_move_threshold)
            or funding_passed
        )

    def _calculate_limit_sell_idr(
        self,
        pair_tag: str,
        pos: ActivePosition,
        global_ask_usdt: float,
        usdt_idr_rate: float,
    ) -> tuple[float, float]:
        # 1. Base pegged exit price using spread & funding decay
        pos_value_usdt = pos.qty * global_ask_usdt
        funding_decay_pct = self._funding_decay_pct(
            pair_tag, pos, pos_value_usdt
        )

        effective_exit_spread_pct = pos.base_target_exit_spread_pct - funding_decay_pct
        pegged_limit_sell_idr = (global_ask_usdt * usdt_idr_rate) * (
            1.0 + (effective_exit_spread_pct / 100.0)
        )

        # 2. Strict Mathematical Break-Even & Profit Floor Protection:
        # We enforce: Realized Net Profit >= min_profit_target (> 0)
        # Net Profit = Spot_PnL + Futures_PnL + Funding - Fees
        # Futures PnL = Qty * (short_entry_usdt - global_ask_usdt)
        # Fees = Qty * (short_entry_usdt + global_ask_usdt) * fee_rate (covers both legs)
        rates = rates_for_position(pos)

        # Buffer min ROI: at least +0.05% net ROI or 25% of base target exit spread
        min_roi_pct = max(0.05, pos.base_target_exit_spread_pct * 0.25)
        capital_usdt = pos.qty * pos.short_entry_usdt
        min_profit_target_usdt = capital_usdt * (min_roi_pct / 100.0)

        # Solve the spot exit floor including all four entry/exit fees.
        spot_entry_usdt = pos.entry_price_idr / usdt_idr_rate if usdt_idr_rate > 0 else 0.0
        required_spot_exit_usdt_value = required_spot_exit_usdt_fn(
            pos.qty, spot_entry_usdt, pos.short_entry_usdt, global_ask_usdt,
            pos.accumulated_funding_usdt, min_profit_target_usdt, rates,
        )
        min_profitable_sell_idr = required_spot_exit_usdt_value * usdt_idr_rate

        limit_sell_idr = max(pegged_limit_sell_idr, min_profitable_sell_idr)

        tick_size = getattr(pos, "local_price_tick", 1.0)
        if tick_size <= 0:
            if pair_tag in self.pending_exit_orders:
                tick_size = self.pending_exit_orders[pair_tag].get("tick_size", 1.0)
            else:
                tick_size = 1.0

        # CEIL to tick size so rounding NEVER rounds down below profitable floor
        limit_sell_idr = ceil_to_tick(limit_sell_idr, tick_size)

        logger.info(
            f"[State 3 Dynamic Exit Pegging] Re-pegging Limit Sell for {pair_tag}: "
            f"global_ask={global_ask_usdt}, spread={effective_exit_spread_pct:.3f}%, "
            f"floor_idr={min_profitable_sell_idr:.2f}, limit_sell_idr={limit_sell_idr:.2f}"
        )
        return limit_sell_idr, tick_size
