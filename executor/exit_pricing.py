"""Exit pricing, funding rollover, and adaptive slippage calculations."""

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
from executor import helpers
from executor.helpers import (
    ARB_TRADE_HISTORY_KEY,
    ARB_TRADE_HISTORY_MAX_LEN,
    BALANCES_KEY,
    build_client_order_id,
    ceil_to_tick,
)
from executor.private_stream import fetch_order_preferring_stream

logger = logging.getLogger("executor.engine")


class ExitPricingMixin:
    async def _fetch_live_price(
        self, pair_tag: str, pos: Optional[ActivePosition] = None
    ) -> Optional[tuple[float, float]]:
        """Returns (global_ask_usdt, usdt_idr_rate) from a fresh live-price
        snapshot, or None when no fresh data exists. A stale snapshot is NOT
        substituted with the entry price: doing so fabricates a zero futures
        PnL and makes floating PnL display unreliable."""
        live_price_key = f"arb:live_price:{pair_tag}"
        live_price_raw = await self.redis.get(live_price_key)
        if live_price_raw:
            try:
                live_snapshot = json.loads(live_price_raw)
                global_ask_usdt = float(live_snapshot.get("global_best_ask_usdt", 0.0))
                usdt_idr_rate = float(live_snapshot.get("usdt_idr_rate", 0.0) or 0.0)
                if global_ask_usdt > 0 and math.isfinite(usdt_idr_rate) and usdt_idr_rate > 0 and not helpers.snapshot_is_stale(live_snapshot):
                    return global_ask_usdt, usdt_idr_rate
            except Exception:
                pass

        return None

    async def _handle_funding_rollover(
        self, pair_tag: str, pos: ActivePosition, global_ask_usdt: float, now_ms: int
    ) -> bool:
        if pos.next_funding_time_ms <= 0 or now_ms < pos.next_funding_time_ms:
            return False
        if not self._funding_retry_allowed(pair_tag, now_ms):
            return False
        symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
        payments = await self._fetch_actual_funding_payments(pair_tag, symbol, pos, now_ms)
        if payments is None:
            return False
        next_funding_ms, current_rate = await self._refresh_funding_schedule(
            pair_tag, symbol, pos.current_funding_rate
        )
        return await self._apply_funding_settlements(
            pair_tag, pos, payments, next_funding_ms, current_rate, now_ms
        )

    def _funding_retry_allowed(self, pair_tag: str, now_ms: int) -> bool:
        retry_after = getattr(self, "_funding_reconcile_retry_after_ms", {})
        if now_ms < retry_after.get(pair_tag, 0):
            return False
        retry_after[pair_tag] = now_ms + 30_000
        self._funding_reconcile_retry_after_ms = retry_after
        return True

    async def _fetch_actual_funding_payments(self, pair_tag: str, symbol: str, pos: ActivePosition, now_ms: int):
        since_ms = pos.last_funding_time_ms if pos.last_funding_time_ms > 0 else max(0, pos.next_funding_time_ms - 1)
        try:
            payments = await self.client.fetch_funding_history(symbol, since=since_ms, limit=100)
        except Exception:
            logger.exception(
                "[Funding Reconciliation] cannot fetch actual settlement history for %s; estimated funding will not be credited",
                pair_tag,
            )
            return None
        if not isinstance(payments, list):
            logger.error("[Funding Reconciliation] invalid funding history response for %s", pair_tag)
            return None

        settled = []
        for payment in payments:
            if not isinstance(payment, dict):
                continue
            timestamp = int(payment.get("timestamp") or 0)
            if timestamp <= pos.last_funding_time_ms or timestamp > now_ms:
                continue
            if not self._is_supported_funding_asset(payment, pair_tag):
                continue
            amount = self._parse_funding_payment_amount(payment, pair_tag)
            if amount is None:
                continue
            settled.append((timestamp, amount, payment))
        return settled

    @staticmethod
    def _is_supported_funding_asset(payment: dict, pair_tag: str) -> bool:
        asset = str(payment.get("code") or payment.get("currency") or "USDT").upper()
        if asset == "USDT":
            return True
        logger.error("[Funding Reconciliation] unsupported settlement asset %s for %s", asset, pair_tag)
        return False

    @staticmethod
    def _parse_funding_payment_amount(payment: dict, pair_tag: str):
        try:
            return float(payment.get("amount"))
        except (TypeError, ValueError):
            logger.error("[Funding Reconciliation] invalid payment amount for %s: %s", pair_tag, payment)
            return None

    async def _refresh_funding_schedule(self, pair_tag: str, symbol: str, current_rate: float) -> tuple[int, float]:
        next_funding_ms = 0
        try:
            rate_data = await self.client.fetch_funding_rate(symbol)
            if isinstance(rate_data, dict):
                next_funding_ms = int(rate_data.get("nextFundingTimestamp") or rate_data.get("nextFundingTime") or 0)
                if rate_data.get("fundingRate") is not None:
                    current_rate = float(rate_data["fundingRate"])
        except Exception:
            logger.exception(
                "[Funding Reconciliation] cannot refresh next settlement schedule for %s; automatic funding rollover is paused",
                pair_tag,
            )
        return next_funding_ms, current_rate

    async def _apply_funding_settlements(
        self, pair_tag: str, pos: ActivePosition, settled: list,
        next_funding_ms: int, current_rate: float, now_ms: int,
    ) -> bool:
        if not settled:
            if next_funding_ms > now_ms:
                pos.next_funding_time_ms = next_funding_ms
                pos.current_funding_rate = current_rate
                pos.updated_at = now_ms
                await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))
            return False
        settled.sort(key=lambda item: item[0])
        actual_amount = sum(item[1] for item in settled)
        pos.accumulated_funding_usdt += actual_amount
        pos.last_funding_time_ms = settled[-1][0]
        pos.current_funding_rate = current_rate
        pos.next_funding_time_ms = next_funding_ms if next_funding_ms > pos.last_funding_time_ms else 0
        pos.updated_at = now_ms
        await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))
        logger.info(
            "[Funding Reconciliation] %s: applied %d actual settlement(s), net %+.8f USDT; cumulative=%+.8f USDT",
            pair_tag, len(settled), actual_amount, pos.accumulated_funding_usdt,
        )
        if pos.next_funding_time_ms <= 0:
            logger.critical(
                "[Funding Reconciliation] next settlement time unavailable for %s; funding accrual paused pending a valid exchange schedule",
                pair_tag,
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

    async def _calculate_dynamic_exit_slippage(self, pair_tag: str, snapshot: dict, now_ms: int) -> float:
        """Adaptive reserve from distinct, timestamped market snapshots."""
        minimum, maximum, fallback = self._exit_slippage_limits()
        parsed = self._parse_exit_slippage_snapshot(snapshot, now_ms)
        if parsed is None:
            return fallback
        ask, bid, snapshot_ts = parsed
        try:
            samples = await self._load_exit_slippage_samples(pair_tag, snapshot_ts, ask, now_ms)
        except Exception:
            logger.warning("[Dynamic Exit Buffer] history unavailable for %s; using fallback", pair_tag)
            return fallback
        samples = sorted({ts: price for ts, price in samples}.items())
        if not self._has_sufficient_exit_samples(samples):
            return fallback
        volatility = self._max_adverse_ask_move(samples)
        spread = (ask - bid) / ((ask + bid) / 2.0)
        adaptive = spread * float(getattr(self.config, "exit_spread_multiplier", 0.5))
        adaptive += volatility * float(getattr(self.config, "exit_volatility_multiplier", 2.0))
        result = min(maximum, max(minimum, adaptive))
        self._log_dynamic_exit_buffer(pair_tag, samples, spread, volatility, result)
        return result

    def _exit_slippage_limits(self) -> tuple[float, float, float]:
        base = float(getattr(self.config, "max_exit_slippage_pct", 0.003))
        minimum = float(getattr(self.config, "min_exit_slippage_pct", 0.001))
        maximum = float(getattr(self.config, "max_dynamic_exit_slippage_pct", 0.02))
        return minimum, maximum, min(maximum, max(minimum, base))

    @staticmethod
    def _parse_exit_slippage_snapshot(snapshot: dict, now_ms: int):
        try:
            ask = float(snapshot.get("global_best_ask_usdt", 0.0) or 0.0)
            bid = float(snapshot.get("global_best_bid_usdt", 0.0) or 0.0)
            snapshot_ts = int(snapshot.get("timestamp_ms", 0) or 0)
        except (TypeError, ValueError):
            return None
        if not ExitPricingMixin._valid_exit_book(ask, bid):
            return None
        if not ExitPricingMixin._valid_exit_snapshot_time(snapshot_ts, now_ms):
            return None
        return ask, bid, snapshot_ts

    @staticmethod
    def _valid_exit_book(ask: float, bid: float) -> bool:
        return math.isfinite(ask) and math.isfinite(bid) and ask > 0 and bid > 0 and bid <= ask

    @staticmethod
    def _valid_exit_snapshot_time(snapshot_ts: int, now_ms: int) -> bool:
        return (
            snapshot_ts > 0
            and snapshot_ts <= now_ms + 5_000
            and now_ms - snapshot_ts <= helpers.LIVE_PRICE_STALE_MS
        )

    async def _load_exit_slippage_samples(self, pair_tag: str, snapshot_ts: int, ask: float, now_ms: int) -> list:
        window_ms = 900_000
        key = f"arb:exit_slippage_samples:v2:{pair_tag}"
        existing = self._parse_exit_slippage_samples(await self.redis.lrange(key, 0, 19), now_ms, window_ms)
        newest_ts = max((ts for ts, _ in existing), default=0)
        if snapshot_ts > newest_ts:
            await self.redis.lpush(key, json.dumps({"ts": snapshot_ts, "ask": ask}))
            await self.redis.ltrim(key, 0, 19)
        await self.redis.expire(key, 1800)
        raw = await self.redis.lrange(key, 0, 19)
        return self._parse_exit_slippage_samples(raw, now_ms, window_ms)

    @staticmethod
    def _parse_exit_slippage_samples(raw_samples, now_ms: int, window_ms: int) -> list:
        samples = []
        for item_raw in raw_samples or []:
            try:
                item = json.loads(item_raw)
                timestamp, price = int(item["ts"]), float(item["ask"])
            except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                continue
            if timestamp <= 0 or timestamp > now_ms + 5_000:
                continue
            if now_ms - timestamp > window_ms or not math.isfinite(price) or price <= 0:
                continue
            samples.append((timestamp, price))
        return samples

    @staticmethod
    def _has_sufficient_exit_samples(samples: list) -> bool:
        return len(samples) >= 3 and samples[-1][0] - samples[0][0] >= 30_000

    @staticmethod
    def _max_adverse_ask_move(samples: list) -> float:
        prices = [price for _, price in samples]
        return max(
            (max(0.0, prices[i] / prices[i - 1] - 1.0) for i in range(1, len(prices))),
            default=0.0,
        )

    @staticmethod
    def _log_dynamic_exit_buffer(pair_tag: str, samples: list, spread: float, volatility: float, result: float) -> None:
        logger.info(
            "[Dynamic Exit Buffer] %s samples=%d span=%.1fs spread=%.4f%% adverse_move=%.4f%% buffer=%.4f%%",
            pair_tag, len(samples), (samples[-1][0] - samples[0][0]) / 1000.0,
            spread * 100, volatility * 100, result * 100,
        )

    def _calculate_limit_sell_idr(
        self,
        pair_tag: str,
        pos: ActivePosition,
        global_ask_usdt: float,
        usdt_idr_rate: float,
        exit_slippage_pct: Optional[float] = None,
    ) -> tuple[float, float]:
        # 1. Base pegged exit price using spread & funding decay
        pos_value_usdt = pos.qty * global_ask_usdt
        funding_decay_pct = 0.0
        if pos_value_usdt > 0:
            funding_decay_pct = (pos.accumulated_funding_usdt / pos_value_usdt) * 100.0

        effective_exit_spread_pct = pos.base_target_exit_spread_pct - funding_decay_pct
        pegged_limit_sell_idr = (global_ask_usdt * usdt_idr_rate) * (
            1.0 + (effective_exit_spread_pct / 100.0)
        )

        # 2. Combined two-leg profit floor. Use one fee convention everywhere:
        # estimated_round_trip_fee_pct is the fee reserve for both legs and
        # the complete round trip, expressed as a fraction of position notional.
        capital_usdt = pos.qty * pos.short_entry_usdt
        fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.001) or 0.0)
        est_fees_usdt = capital_usdt * fee_pct

        # Closing the short is a market BUY at the futures ask. Add an adverse
        # movement allowance so the spot limit is not priced against a perfect
        # top-of-book fill.
        if exit_slippage_pct is None:
            exit_slippage_pct = float(getattr(self.config, "max_exit_slippage_pct", 0.003) or 0.0)
        risk_adjusted_global_ask = global_ask_usdt * (1.0 + exit_slippage_pct)
        futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - risk_adjusted_global_ask)

        # Buffer min ROI: at least +0.05% net ROI or 25% of base target exit spread
        min_roi_pct = max(0.05, pos.base_target_exit_spread_pct * 0.25)
        min_profit_target_usdt = capital_usdt * (min_roi_pct / 100.0)

        # Required spot PnL covers the conservative futures buyback, fees and funding.
        required_spot_pnl_usdt = (
            min_profit_target_usdt
            + est_fees_usdt
            - futures_pnl_usdt
            - pos.accumulated_funding_usdt
        )

        min_profitable_sell_idr = pos.entry_price_idr
        if pos.qty > 0 and usdt_idr_rate > 0:
            min_profitable_sell_idr += (required_spot_pnl_usdt * usdt_idr_rate) / pos.qty

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
