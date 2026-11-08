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
            """
            Applies only funding payments returned by the exchange settlement ledger.
            The market's settlement interval is never assumed to be eight hours.
            """
            if pos.next_funding_time_ms <= 0 or now_ms < pos.next_funding_time_ms:
                return False

            retry_after = getattr(self, "_funding_reconcile_retry_after_ms", {})
            if now_ms < retry_after.get(pair_tag, 0):
                return False
            retry_after[pair_tag] = now_ms + 30_000
            self._funding_reconcile_retry_after_ms = retry_after

            symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
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

            settled = []
            for payment in payments:
                if not isinstance(payment, dict):
                    continue
                timestamp = int(payment.get("timestamp") or 0)
                if timestamp <= pos.last_funding_time_ms or timestamp > now_ms:
                    continue
                asset = str(payment.get("code") or payment.get("currency") or "USDT").upper()
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

            # Refresh the venue's actual next settlement schedule. Different
            # contracts/venues can use 1h, 2h, 4h, 8h or custom intervals.
            next_funding_ms = 0
            current_rate = pos.current_funding_rate
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
            except Exception:
                logger.exception(
                    f"[Funding Reconciliation] cannot refresh next settlement schedule "
                    f"for {pair_tag}; automatic funding rollover is paused"
                )

            if not settled:
                if next_funding_ms > now_ms:
                    pos.next_funding_time_ms = next_funding_ms
                    pos.current_funding_rate = current_rate
                    pos.updated_at = now_ms
                    await self.redis.set(
                        f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
                    )
                return False

            settled.sort(key=lambda item: item[0])
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
            """Adaptive reserve from distinct, timestamped market snapshots.

            Sampling is intentionally independent of order repricing. The market
            timestamp, not the executor polling time, identifies an observation.
            """
            base = float(getattr(self.config, "max_exit_slippage_pct", 0.003))
            minimum = float(getattr(self.config, "min_exit_slippage_pct", 0.001))
            maximum = float(getattr(self.config, "max_dynamic_exit_slippage_pct", 0.02))
            fallback = min(maximum, max(minimum, base))

            try:
                ask = float(snapshot.get("global_best_ask_usdt", 0.0) or 0.0)
                bid = float(snapshot.get("global_best_bid_usdt", 0.0) or 0.0)
                snapshot_ts = int(snapshot.get("timestamp_ms", 0) or 0)
            except (TypeError, ValueError):
                return fallback

            if not (
                math.isfinite(ask) and math.isfinite(bid)
                and ask > 0 and bid > 0 and bid <= ask
                and snapshot_ts > 0
                and snapshot_ts <= now_ms + 5_000
                and now_ms - snapshot_ts <= helpers.LIVE_PRICE_STALE_MS
            ):
                return fallback

            # Keep enough history for low-frequency scanner cycles (e.g. 180s),
            # while requiring three real observations spanning at least 30s.
            window_ms = 900_000
            min_samples = 3
            min_span_ms = 30_000
            key = f"arb:exit_slippage_samples:v2:{pair_tag}"
            try:
                raw = await self.redis.lrange(key, 0, 19)
                existing = []
                for item_raw in raw or []:
                    try:
                        item = json.loads(item_raw)
                        ts, price = int(item["ts"]), float(item["ask"])
                        if (
                            0 < ts <= now_ms + 5_000
                            and now_ms - ts <= window_ms
                            and math.isfinite(price) and price > 0
                        ):
                            existing.append((ts, price))
                    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                        continue

                # Repeated executor polls see the same scanner snapshot. Do not
                # turn one unchanged quote into many artificial volatility samples.
                known_timestamps = {ts for ts, _ in existing}
                newest_ts = max(known_timestamps, default=0)
                if snapshot_ts not in known_timestamps and snapshot_ts > newest_ts:
                    await self.redis.lpush(
                        key, json.dumps({"ts": snapshot_ts, "ask": ask})
                    )
                    await self.redis.ltrim(key, 0, 19)
                await self.redis.expire(key, 1800)
                raw = await self.redis.lrange(key, 0, 19)
                samples = []
                for item_raw in raw or []:
                    try:
                        item = json.loads(item_raw)
                        ts, price = int(item["ts"]), float(item["ask"])
                        if (
                            0 < ts <= now_ms + 5_000
                            and now_ms - ts <= window_ms
                            and math.isfinite(price) and price > 0
                        ):
                            samples.append((ts, price))
                    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                        continue
            except Exception:
                logger.warning(
                    "[Dynamic Exit Buffer] history unavailable for %s; using fallback",
                    pair_tag,
                )
                return fallback

            # One observation per market timestamp; sort chronologically.
            samples = sorted({ts: price for ts, price in samples}.items())
            if (
                len(samples) < min_samples
                or samples[-1][0] - samples[0][0] < min_span_ms
            ):
                return fallback

            prices = [price for _, price in samples]
            # Conservative adverse-move proxy: largest positive consecutive ask
            # return. Downward ask moves help the short buyback and are ignored.
            volatility = max(
                (
                    max(0.0, prices[i] / prices[i - 1] - 1.0)
                    for i in range(1, len(prices))
                ),
                default=0.0,
            )
            spread = (ask - bid) / ((ask + bid) / 2.0)
            adaptive = spread * float(
                getattr(self.config, "exit_spread_multiplier", 0.5)
            )
            adaptive += volatility * float(
                getattr(self.config, "exit_volatility_multiplier", 2.0)
            )
            result = min(maximum, max(minimum, adaptive))
            logger.info(
                "[Dynamic Exit Buffer] %s samples=%d span=%.1fs spread=%.4f%% "
                "adverse_move=%.4f%% buffer=%.4f%%",
                pair_tag,
                len(samples),
                (samples[-1][0] - samples[0][0]) / 1000.0,
                spread * 100,
                volatility * 100,
                result * 100,
            )
            return result

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
