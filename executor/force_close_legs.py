"""Exchange-leg closure and force-close PnL calculation."""

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


class ForceCloseLegsMixin:

    @staticmethod
    def _parse_force_close_pair_key(message_data: str):
        """Extracts the pair_key from a FORCE_CLOSE directive (JSON or plain text)."""
        if not message_data:
            return None
        try:
            parsed = json.loads(message_data)
        except Exception:
            return message_data.strip() or None
        if isinstance(parsed, dict):
            return parsed.get("pair_key")
        return None


    async def _close_global_short_leg(
        self, pair_key: str, pos, allow_already_flat: bool = False
    ) -> Tuple[bool, float]:
        global_symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
        if allow_already_flat:
            retry_safe, already_flat = await self._check_retry_short_state(pair_key, pos, global_symbol)
            if not retry_safe:
                return False, 0.0
            if already_flat:
                return True, 0.0

        await self._sync_force_close_quote(pair_key, global_symbol)
        try:
            client_order_id = build_client_order_id(pair_key, "force-close", self._next_order_seq())
            order = await self.client.close_global_short_market(
                global_symbol, pos.qty, client_order_id=client_order_id
            )
            if not self._validate_force_close_order(pair_key, order, pos.qty):
                return False, 0.0
            self.supabase.order(
                pair_key, "futures_exit", pos.global_exchange, "perpetual",
                global_symbol, order, client_order_id=client_order_id,
                reduce_only=True, is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            if not await self._verify_short_flat(pair_key, global_symbol):
                return False, 0.0
            fill_price = helpers.extract_fill_price(order)
            if fill_price <= 0:
                logger.critical(
                    "[Force Close] futures position is flat but actual close fill price is unavailable for %s; "
                    "spot liquidation will continue, but realized PnL will be marked unverified",
                    pair_key,
                )
            logger.info("[Force Close] Futures position verified flat for %s after close response %r", pair_key, order)
            return True, fill_price
        except Exception:
            logger.exception("failed to close or verify global short on force close for %s", pair_key)
            return False, 0.0

    async def _check_retry_short_state(self, pair_key: str, pos, global_symbol: str) -> tuple[bool, bool]:
        if not hasattr(self.client, "fetch_positions"):
            logger.critical("[Force Close] cannot retry %s: futures position API unavailable", pair_key)
            return False, False
        try:
            positions = await self.client.fetch_positions(global_symbol)
        except Exception:
            logger.exception("[Force Close] cannot verify existing futures exposure for retry %s", pair_key)
            return False, False
        if positions is None:
            logger.critical("[Force Close] retry position lookup returned None for %s", pair_key)
            return False, False
        if not positions:
            logger.warning("[Force Close] futures leg for %s is already flat; skipping duplicate close", pair_key)
            return True, True

        actual_short_qty = 0.0
        for position in positions:
            valid, quantity = self._parse_retry_short_position(position, pair_key)
            if not valid:
                return False, False
            actual_short_qty += quantity
        tolerance = max(1e-10, pos.qty * 1e-4)
        if actual_short_qty <= tolerance:
            logger.warning("[Force Close] futures leg for %s is already flat; skipping duplicate close", pair_key)
            return True, True
        logger.critical(
            "[Force Close] retry for %s found residual short %s (expected %s); refusing duplicate close",
            pair_key, actual_short_qty, pos.qty,
        )
        return False, False

    def _parse_retry_short_position(self, position: dict, pair_key: str) -> tuple[bool, float]:
        details = self._retry_position_details(position)
        if details is None:
            return False, 0.0
        side, raw_size, uses_contracts, contract_size = details
        if raw_size is None:
            logger.critical("[Force Close] retry position size unavailable for %s", pair_key)
            return False, 0.0
        signed_size = self._parse_retry_position_size(raw_size, contract_size)
        if signed_size is None:
            return False, 0.0
        size = abs(signed_size)
        if size <= 1e-12:
            return True, 0.0
        if not self._is_verified_short_side(side, uses_contracts, signed_size, pair_key):
            return False, 0.0
        multiplier = contract_size if uses_contracts else 1.0
        return True, size * multiplier

    @staticmethod
    def _retry_position_details(position: dict):
        if not isinstance(position, dict):
            return None
        info = position.get("info") or {}
        side = str(
            position.get("side") or position.get("positionSide")
            or info.get("posSide") or info.get("positionSide") or ""
        ).lower()
        contracts = position.get("contracts")
        raw_size = contracts if contracts is not None else position.get("positionAmt", info.get("positionAmt"))
        try:
            contract_size = float(position.get("contractSize") or 1.0)
        except (TypeError, ValueError):
            return None
        return side, raw_size, contracts is not None, contract_size

    @staticmethod
    def _parse_retry_position_size(raw_size, contract_size: float):
        try:
            size = float(raw_size)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(size) or not math.isfinite(contract_size) or contract_size <= 0:
            return None
        return size

    def _is_verified_short_side(self, side: str, uses_contracts: bool, signed_size: float, pair_key: str) -> bool:
        if side and side != "short":
            logger.critical("[Force Close] refusing retry for %s: unexpected futures side=%s", pair_key, side)
            return False
        if not side and (uses_contracts or signed_size >= 0):
            logger.critical("[Force Close] refusing retry for %s: position side cannot be verified", pair_key)
            return False
        return True

    async def _sync_force_close_quote(self, pair_key: str, global_symbol: str) -> None:
        if not hasattr(self.client, "update_market_price"):
            return
        try:
            raw = await self.redis.get(f"arb:live_price:{pair_key}")
            if not raw:
                return
            snapshot = json.loads(raw)
            if helpers.snapshot_is_stale(snapshot):
                return
            self.client.update_market_price(
                symbol=global_symbol,
                best_bid=float(snapshot.get("global_best_bid_usdt", 0.0)),
                best_ask=float(snapshot.get("global_best_ask_usdt", 0.0)),
            )
        except Exception:
            logger.exception("[Force Close] failed to sync futures quote for %s", pair_key)

    def _validate_force_close_order(self, pair_key: str, order: dict, requested_qty: float) -> bool:
        if not isinstance(order, dict):
            logger.critical("[Force Close] futures close returned no verifiable order for %s", pair_key)
            return False
        status = str(order.get("status", "")).lower()
        if status not in ("closed", "filled"):
            logger.error("[Force Close] futures close is not terminal (status=%s): %s", status, order)
            return False
        try:
            filled = float(order.get("filled") or 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        tolerance = max(1e-12, requested_qty * 1e-8)
        if filled + tolerance < requested_qty:
            logger.critical(
                "[Force Close] futures close fill incomplete for %s: requested=%s, filled=%s",
                pair_key, requested_qty, filled,
            )
            return False
        return True

    async def _verify_short_flat(self, pair_key: str, global_symbol: str) -> bool:
        if not hasattr(self.client, "fetch_positions"):
            logger.critical("[Force Close] cannot verify futures position for %s: fetch_positions unavailable", pair_key)
            return False
        positions = await self.client.fetch_positions(global_symbol)
        if positions is None:
            logger.critical("[Force Close] futures position lookup returned None for %s", pair_key)
            return False
        for position in positions:
            if not isinstance(position, dict):
                logger.critical("[Force Close] invalid position payload for %s: %r", pair_key, position)
                return False
            info = position.get("info") or {}
            raw_size = position.get("contracts")
            if raw_size is None:
                raw_size = position.get("positionAmt", info.get("positionAmt"))
            if raw_size is None:
                logger.critical("[Force Close] position size unavailable for %s: %s", pair_key, position)
                return False
            try:
                remaining = abs(float(raw_size))
            except (TypeError, ValueError):
                return False
            if remaining > 1e-12:
                logger.critical("[Force Close] residual futures exposure for %s: %s remains", pair_key, remaining)
                return False
        return True

    async def _liquidate_spot_leg(self, pair_key: str, pos) -> float:
        """
        Liquidates the local spot leg. Returns the realized exit price in IDR
        only if the order actually filled. Never falls back to entry price.
        """
        logger.warning(f"[Force Close] Selling {pos.qty} {pos.symbol} spot leg...")
        try:
            seq = self._next_order_seq()
            spot_order = await helpers.retry_with_backoff(
                lambda: self.client.place_local_market_sell(
                    pos.symbol,
                    pos.qty,
                    client_order_id=build_client_order_id(pair_key, "force-liq", seq),
                ),
                max_retries=4,  # 5 total attempts
                base_delay=0.5,
                label=f"force-close spot liquidation {pos.symbol} for {pair_key}",
            )
            # Require the full requested quantity to be filled. A partial
            # market sell leaves residual spot exposure and must not be reported
            # as a successful force close.
            if isinstance(spot_order, dict):
                self.supabase.order(
                    pair_key,
                    "force_close",
                    pos.local_exchange,
                    "spot",
                    pos.symbol,
                    spot_order,
                    client_order_id=build_client_order_id(pair_key, "force-liq", seq),
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                )
                filled_qty = float(spot_order.get("filled", 0.0) or 0.0)
                spot_exit_price_idr = helpers.extract_fill_price(spot_order)
                if filled_qty + max(1e-12, pos.qty * 1e-8) < pos.qty:
                    order_id = str(spot_order.get("id", "") or "")
                    if order_id:
                        try:
                            verified = await self.client.fetch_local_order(
                                order_id, pos.symbol
                            )
                            if isinstance(verified, dict):
                                filled_qty = float(verified.get("filled", 0.0) or 0.0)
                                spot_exit_price_idr = (
                                    helpers.extract_fill_price(verified)
                                    or spot_exit_price_idr
                                )
                        except Exception:
                            logger.exception(
                                f"[Force Close] cannot verify spot sell {order_id} for {pair_key}"
                            )
                if (
                    filled_qty + max(1e-12, pos.qty * 1e-8) >= pos.qty
                    and spot_exit_price_idr > 0
                ):
                    logger.info(
                        f"[Force Close] Spot leg fully liquidated: {pos.qty} {pos.symbol} "
                        f"@ {spot_exit_price_idr} (filled={filled_qty})"
                    )
                    return spot_exit_price_idr
                logger.critical(
                    f"[Force Close] partial/unverified spot liquidation for {pair_key}: "
                    f"requested={pos.qty}, filled={filled_qty}; manual reconciliation required"
                )

            logger.critical(
                f"[Force Close] FAILED to liquidate spot leg for {pair_key}. "
                f"MANUAL INTERVENTION REQUIRED: {pos.qty} {pos.symbol} is UNHEDGED. Response: {spot_order}"
            )
        except RetryExhausted:
            logger.exception(
                f"[Force Close] spot liquidation exhausted retries for {pair_key}"
            )
        return 0.0

    async def _resolve_force_close_prices(self, pair_key: str, pos, spot_exit_price_idr: float):
        """Returns (current_ask_usdt, usdt_rate, current_spot_bid_idr) for PnL accounting."""
        current_ask = pos.short_entry_usdt
        usdt_rate = 0.0
        current_spot_bid = pos.entry_price_idr

        price_data = await self._fetch_live_price(pair_key, pos)
        if price_data:
            current_ask, usdt_rate = price_data

        live_price_raw = await self.redis.get(f"arb:live_price:{pair_key}")
        if live_price_raw:
            try:
                s_snap = json.loads(live_price_raw)
                if s_snap.get("local_best_bid"):
                    current_spot_bid = float(s_snap["local_best_bid"])
            except Exception:
                pass

        if spot_exit_price_idr > 0:
            current_spot_bid = spot_exit_price_idr
        return current_ask, usdt_rate, current_spot_bid

    @staticmethod
    def _compute_force_close_pnl(pos, current_ask: float, usdt_rate: float, current_spot_bid: float) -> dict:
        """Computes realized PnL components for a force-closed position."""
        spot_pnl_idr = pos.qty * (current_spot_bid - pos.entry_price_idr)
        spot_pnl_usdt = spot_pnl_idr / usdt_rate if usdt_rate > 0 else 0.0
        futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - current_ask)
        funding_usdt = float(pos.accumulated_funding_usdt)
        fees_usdt = (pos.qty * (pos.short_entry_usdt + current_ask)) * 0.0005
        net_profit_usdt = spot_pnl_usdt + futures_pnl_usdt + funding_usdt - fees_usdt
        capital_usdt = pos.qty * pos.short_entry_usdt
        return {
            "pnl_complete": usdt_rate > 0 and math.isfinite(usdt_rate),
            # A force close can flatten both legs while FX is unavailable.
            # Do not publish or persist partial USDT PnL as fully realized.
            "pnl_unverified": not (usdt_rate > 0 and math.isfinite(usdt_rate)),
            "spot_pnl_idr": spot_pnl_idr,
            "spot_pnl_usdt": spot_pnl_usdt,
            "futures_pnl_usdt": futures_pnl_usdt,
            "funding_usdt": funding_usdt,
            "fees_usdt": fees_usdt,
            "net_profit_usdt": net_profit_usdt,
            "net_profit_idr": net_profit_usdt * usdt_rate,
            "roi_pct": (net_profit_usdt / capital_usdt * 100.0) if capital_usdt > 0 else 0.0,
        }

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
                spot_fees_usdt=0.0,
                futures_margin_usdt=fut_margin,
                futures_pnl_usdt=pnl["futures_pnl_usdt"],
                funding_usdt=pnl["funding_usdt"],
                futures_fees_usdt=pnl["fees_usdt"],
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
        pipe.hincrby("arb:stats", "total_closed", 1)
        if pnl["net_profit_usdt"] > 0:
            pipe.hincrby("arb:stats", "win_count", 1)
        pipe.hincrbyfloat("arb:stats", "total_pnl_usdt", pnl["net_profit_usdt"])
        pipe.hincrbyfloat("arb:stats", "total_pnl_idr", pnl["net_profit_idr"])
        event_msg = {
            "event": "POSITION_CLOSED",
            **closed_trade,
        }
        pipe.publish(self.ARB_TRADE_EVENTS, json.dumps(event_msg))
        await pipe.execute()
