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
            """Closes the futures short; returns its actual fill price only after flat verification.

            When retrying a previously reconciliation-required force close, first
            inspect the exchange. If futures are already flat, skip a duplicate
            reduce-only close and continue with the remaining spot liquidation.
            """
            global_sym = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            if allow_already_flat:
                if not hasattr(self.client, "fetch_positions"):
                    logger.critical(
                        f"[Force Close] cannot retry {pair_key}: futures position API unavailable"
                    )
                    return False, 0.0
                try:
                    current_positions = await self.client.fetch_positions(global_sym)
                except Exception:
                    logger.exception(
                        f"[Force Close] cannot verify existing futures exposure for retry {pair_key}"
                    )
                    return False, 0.0
                if current_positions is None:
                    logger.critical(
                        f"[Force Close] retry position lookup returned None for {pair_key}"
                    )
                    return False, 0.0
                if not current_positions:
                    logger.warning(
                        f"[Force Close] futures leg for {pair_key} is already flat; "
                        "skipping duplicate futures close and retrying spot liquidation"
                    )
                    return True, 0.0

                actual_short_qty = 0.0
                for position in current_positions:
                    if not isinstance(position, dict):
                        return False, 0.0
                    info = position.get("info") or {}
                    side = str(
                        position.get("side")
                        or position.get("positionSide")
                        or info.get("posSide")
                        or info.get("positionSide")
                        or ""
                    ).lower()
                    contracts = position.get("contracts")
                    raw_size = contracts
                    if raw_size is None:
                        raw_size = position.get("positionAmt", info.get("positionAmt"))
                    if raw_size is None:
                        logger.critical(
                            f"[Force Close] retry position size unavailable for {pair_key}"
                        )
                        return False, 0.0
                    try:
                        signed_size = float(raw_size)
                        contract_size = float(position.get("contractSize") or 1.0)
                    except (TypeError, ValueError):
                        return False, 0.0
                    if not math.isfinite(signed_size) or not math.isfinite(contract_size) or contract_size <= 0:
                        return False, 0.0
                    if abs(signed_size) <= 1e-12:
                        continue
                    if side:
                        if side != "short":
                            logger.critical(
                                f"[Force Close] refusing retry for {pair_key}: "
                                f"unexpected futures side={side}"
                            )
                            return False, 0.0
                    elif contracts is not None or signed_size >= 0:
                        logger.critical(
                            f"[Force Close] refusing retry for {pair_key}: "
                            "position side cannot be verified"
                        )
                        return False, 0.0
                    actual_short_qty += abs(signed_size) * (contract_size if contracts is not None else 1.0)

                tolerance = max(1e-10, pos.qty * 1e-4)
                if actual_short_qty <= tolerance:
                    logger.warning(
                        f"[Force Close] futures leg for {pair_key} is already flat; "
                        "skipping duplicate futures close and retrying spot liquidation"
                    )
                    return True, 0.0
                if actual_short_qty > tolerance:
                    # The previous close attempt may still be pending or its
                    # exchange outcome may be unknown. Do not submit another
                    # futures close with a new client order id; manual order
                    # reconciliation is required before retrying that leg.
                    logger.critical(
                        f"[Force Close] retry for {pair_key} found residual short "
                        f"{actual_short_qty} (expected {pos.qty}); refusing duplicate "
                        "futures close submission"
                    )
                    return False, 0.0

            logger.warning(f"[Force Close] Closing global short {pos.qty} on {global_sym}...")
            # Keep paper-mode market fills tied to the latest observed futures book.
            if hasattr(self.client, "update_market_price"):
                try:
                    raw = await self.redis.get(f"arb:live_price:{pair_key}")
                    if raw:
                        snapshot = json.loads(raw)
                        if not helpers.snapshot_is_stale(snapshot):
                            self.client.update_market_price(
                                symbol=global_sym,
                                best_bid=float(snapshot.get("global_best_bid_usdt", 0.0)),
                                best_ask=float(snapshot.get("global_best_ask_usdt", 0.0)),
                            )
                except Exception:
                    logger.exception(
                        f"[Force Close] failed to sync futures quote for {pair_key}"
                    )
            try:
                force_coid = build_client_order_id(
                    pair_key, "force-close", self._next_order_seq()
                )
                res = await self.client.close_global_short_market(
                    global_sym,
                    pos.qty,
                    client_order_id=force_coid,
                )
                if not isinstance(res, dict):
                    logger.critical(
                        f"[Force Close] futures close returned no verifiable order for {pair_key}"
                    )
                    return False, 0.0
                status = str(res.get("status", "")).lower()
                if status not in ("closed", "filled"):
                    logger.error(
                        f"[Force Close] futures close is not terminal (status={status}): {res}"
                    )
                    return False, 0.0
                try:
                    filled_qty = float(res.get("filled") or 0.0)
                except (TypeError, ValueError):
                    filled_qty = 0.0
                qty_tolerance = max(1e-12, pos.qty * 1e-8)
                if filled_qty + qty_tolerance < pos.qty:
                    logger.critical(
                        f"[Force Close] futures close fill incomplete for {pair_key}: "
                        f"requested={pos.qty}, filled={filled_qty}"
                    )
                    return False, 0.0

                self.supabase.order(
                    pair_key,
                    "futures_exit",
                    pos.global_exchange,
                    "perpetual",
                    global_sym,
                    res,
                    client_order_id=force_coid,
                    reduce_only=True,
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                )

                # Order acknowledgement is not proof that the short is flat.
                # The authoritative position endpoint must confirm zero exposure.
                if not hasattr(self.client, "fetch_positions"):
                    logger.critical(
                        f"[Force Close] cannot verify futures position for {pair_key}: "
                        "client has no fetch_positions method"
                    )
                    return False, 0.0
                positions = await self.client.fetch_positions(global_sym)
                if positions is None:
                    logger.critical(
                        f"[Force Close] futures position lookup returned None for {pair_key}"
                    )
                    return False, 0.0
                for position in positions:
                    if not isinstance(position, dict):
                        logger.critical(
                            f"[Force Close] invalid position payload for {pair_key}: {position!r}"
                        )
                        return False, 0.0
                    info = position.get("info") or {}
                    raw_size = position.get("contracts")
                    if raw_size is None:
                        raw_size = position.get("positionAmt", info.get("positionAmt"))
                    if raw_size is None:
                        logger.critical(
                            f"[Force Close] position size unavailable for {pair_key}: {position}"
                        )
                        return False, 0.0
                    try:
                        remaining = abs(float(raw_size))
                    except (TypeError, ValueError):
                        return False, 0.0
                    if remaining > 1e-12:
                        logger.critical(
                            f"[Force Close] residual futures exposure for {pair_key}: "
                            f"{remaining} contracts/units remain"
                        )
                        return False, 0.0

                fill_price = helpers.extract_fill_price(res)
                if fill_price <= 0:
                    logger.critical(
                        f"[Force Close] futures position is flat but actual close fill price "
                        f"is unavailable for {pair_key}; spot liquidation will continue, "
                        "but realized PnL will be marked unverified"
                    )
                logger.info(
                    f"[Force Close] Futures position verified flat for {pair_key} "
                    f"after close response {res!r}"
                )
                return True, fill_price
            except Exception:
                logger.exception(
                    f"failed to close or verify global short on force close for {pair_key}"
                )
                return False, 0.0

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
