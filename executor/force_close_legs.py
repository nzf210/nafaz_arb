"""force close leg responsibilities for emergency position closure."""

import logging
import json
import math
from typing import Optional, Tuple

from executor.models import OrderRole
from executor.fee_engine import rates_for_position, round_trip_fee_breakdown_usdt
from executor import helpers
from executor.helpers import build_client_order_id

logger = logging.getLogger("executor.engine")



class ForceCloseLegMixin:
    """Focused emergency close leg or lifecycle operations."""

    @staticmethod
    def _retry_position_details(
        pair_key: str, position: dict
    ) -> Optional[Tuple[str, Optional[float], float, float]]:
        """Normalize a venue position row before deciding whether it is a short."""
        if not isinstance(position, dict):
            return None
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
            return None
        try:
            signed_size = float(raw_size)
            contract_size = float(position.get("contractSize") or 1.0)
        except (TypeError, ValueError):
            return None
        if (
            not math.isfinite(signed_size)
            or not math.isfinite(contract_size)
            or contract_size <= 0
        ):
            return None
        return side, contracts, signed_size, contract_size

    @staticmethod
    def _retry_position_side_is_valid(
        pair_key: str, side: str, contracts: Optional[float], signed_size: float
    ) -> bool:
        if side and side != "short":
            logger.critical(
                f"[Force Close] refusing retry for {pair_key}: "
                f"unexpected futures side={side}"
            )
            return False
        if not side and (contracts is not None or signed_size >= 0):
            logger.critical(
                f"[Force Close] refusing retry for {pair_key}: "
                "position side cannot be verified"
            )
            return False
        return True

    @staticmethod
    def _retry_position_short_quantity(
        pair_key: str, position: dict
    ) -> Optional[float]:
        details = ForceCloseLegMixin._retry_position_details(pair_key, position)
        if details is None:
            return None
        side, contracts, signed_size, contract_size = details
        if abs(signed_size) <= 1e-12:
            return 0.0
        if not ForceCloseLegMixin._retry_position_side_is_valid(
            pair_key, side, contracts, signed_size
        ):
            return None
        multiplier = contract_size if contracts is not None else 1.0
        return abs(signed_size) * multiplier

    def _retry_short_quantity(self, pair_key: str, positions) -> Optional[float]:
        """Return verified short quantity, or None for ambiguous venue data."""
        actual_short_qty = 0.0
        for position in positions:
            quantity = self._retry_position_short_quantity(pair_key, position)
            if quantity is None:
                return None
            actual_short_qty += quantity
        return actual_short_qty

    async def _retry_short_is_flat(self, pair_key: str, pos, global_sym: str) -> bool:
        """Require authoritative proof of flat exposure before retrying spot liquidation."""
        if not hasattr(self.client, "fetch_positions"):
            logger.critical(
                f"[Force Close] cannot retry {pair_key}: futures position API unavailable"
            )
            return False
        try:
            current_positions = await self.client.fetch_positions(global_sym)
        except Exception:
            logger.exception(
                f"[Force Close] cannot verify existing futures exposure for retry {pair_key}"
            )
            return False
        if current_positions is None:
            logger.critical(
                f"[Force Close] retry position lookup returned None for {pair_key}"
            )
            return False
        if not current_positions:
            logger.warning(
                f"[Force Close] futures leg for {pair_key} is already flat; "
                "skipping duplicate futures close and retrying spot liquidation"
            )
            return True

        actual_short_qty = self._retry_short_quantity(pair_key, current_positions)
        if actual_short_qty is None:
            return False
        tolerance = max(1e-10, pos.qty * 1e-4)
        if actual_short_qty <= tolerance:
            logger.warning(
                f"[Force Close] futures leg for {pair_key} is already flat; "
                "skipping duplicate futures close and retrying spot liquidation"
            )
            return True
        logger.critical(
            f"[Force Close] retry for {pair_key} found residual short "
            f"{actual_short_qty} (expected {pos.qty}); refusing duplicate "
            "futures close submission"
        )
        return False

    async def _sync_force_close_market_price(self, pair_key: str, global_sym: str) -> None:
        """Refresh paper-mode futures pricing from the latest non-stale market snapshot."""
        if not hasattr(self.client, "update_market_price"):
            return
        try:
            raw = await self.redis.get(f"arb:live_price:{pair_key}")
            if not raw:
                return
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

    async def _verify_force_close_futures_flat(self, pair_key: str, global_sym: str) -> bool:
        """Verify that the authoritative futures position endpoint reports zero exposure."""
        if not hasattr(self.client, "fetch_positions"):
            logger.critical(
                f"[Force Close] cannot verify futures position for {pair_key}: "
                "client has no fetch_positions method"
            )
            return False
        positions = await self.client.fetch_positions(global_sym)
        if positions is None:
            logger.critical(
                f"[Force Close] futures position lookup returned None for {pair_key}"
            )
            return False
        for position in positions:
            if not isinstance(position, dict):
                logger.critical(
                    f"[Force Close] invalid position payload for {pair_key}: {position!r}"
                )
                return False
            info = position.get("info") or {}
            raw_size = position.get("contracts")
            if raw_size is None:
                raw_size = position.get("positionAmt", info.get("positionAmt"))
            if raw_size is None:
                logger.critical(
                    f"[Force Close] position size unavailable for {pair_key}: {position}"
                )
                return False
            try:
                remaining = abs(float(raw_size))
            except (TypeError, ValueError):
                return False
            if remaining > 1e-12:
                logger.critical(
                    f"[Force Close] residual futures exposure for {pair_key}: "
                    f"{remaining} contracts/units remain"
                )
                return False
        return True

    async def _close_global_short_leg(
        self, pair_key: str, pos, allow_already_flat: bool = False
    ) -> Tuple[bool, float]:
        """Close the futures short and return its fill price only after flat verification."""
        global_sym = self._resolve_global_symbol(pos.symbol)
        if allow_already_flat:
            already_flat = await self._retry_short_is_flat(pair_key, pos, global_sym)
            return (True, 0.0) if already_flat else (False, 0.0)

        logger.warning(f"[Force Close] Closing global short {pos.qty} on {global_sym}...")
        await self._sync_force_close_market_price(pair_key, global_sym)
        try:
            force_coid = build_client_order_id(
                pair_key, "force-close", self._next_order_seq()
            )
            await self._register_order_intent(
                pair_key, force_coid,
                exchange=pos.global_exchange, market_type="perpetual",
                symbol=global_sym, role=OrderRole.FORCE_CLOSE.value,
                side="buy", order_type="market", price=0.0,
                quantity=pos.qty, reduce_only=True,
                global_amount_spec=pos.global_amount_spec
            )
            res = await self.client.close_global_short_market(
                global_sym,
                pos.qty,
                client_order_id=force_coid,
                amount_spec=pos.global_amount_spec,
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
            await self._register_order(
                pair_key,
                force_coid,
                res,
                exchange=pos.global_exchange,
                market_type="perpetual",
                symbol=global_sym,
                role=OrderRole.FORCE_CLOSE.value,
                side="buy",
                order_type="market",
                price=helpers.extract_fill_price(res),
                quantity=pos.qty,
                reduce_only=True,
            )
            if not await self._verify_force_close_futures_flat(pair_key, global_sym):
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

    async def _record_force_close_spot_order(
        self, pair_key: str, pos, order: dict, client_order_id: str
    ) -> None:
        self.supabase.order(
            pair_key,
            "force_close",
            pos.local_exchange,
            "spot",
            pos.symbol,
            order,
            client_order_id=client_order_id,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        await self._register_order(
            pair_key,
            client_order_id,
            order,
            exchange=pos.local_exchange,
            market_type="spot",
            symbol=pos.symbol,
            role=OrderRole.FORCE_CLOSE.value,
            side="sell",
            order_type="market",
            price=helpers.extract_fill_price(order),
            quantity=pos.qty,
        )

    async def _verified_force_close_spot_fill(
        self, pair_key: str, pos, order: dict
    ) -> Tuple[float, float]:
        filled_qty = float(order.get("filled", 0.0) or 0.0)
        exit_price = helpers.extract_fill_price(order)
        tolerance = max(1e-12, pos.qty * 1e-8)
        if filled_qty + tolerance >= pos.qty:
            return filled_qty, exit_price
        order_id = str(order.get("id", "") or "")
        if not order_id:
            return filled_qty, exit_price
        try:
            verified = await self.client.fetch_local_order(order_id, pos.symbol)
            if isinstance(verified, dict):
                filled_qty = float(verified.get("filled", 0.0) or 0.0)
                exit_price = helpers.extract_fill_price(verified) or exit_price
        except Exception:
            logger.exception(
                "[Force Close] cannot verify spot sell %s for %s",
                order_id, pair_key,
            )
        return filled_qty, exit_price

    async def _liquidate_spot_leg(self, pair_key: str, pos) -> float:
        """
        Liquidates the local spot leg. Returns the realized exit price in IDR
        only if the order actually filled. Never falls back to entry price.
        """
        logger.warning(f"[Force Close] Selling {pos.qty} {pos.symbol} spot leg...")
        try:
            seq = self._next_order_seq()
            client_order_id = build_client_order_id(pair_key, "force-liq", seq)
            await self._register_order_intent(
                pair_key, client_order_id,
                exchange=pos.local_exchange, market_type="spot",
                symbol=pos.symbol, role=OrderRole.FORCE_CLOSE.value,
                side="sell", order_type="market", price=0.0,
                quantity=pos.qty, local_amount_spec=pos.local_amount_spec
            )
            spot_order = await self._submit_local_order_once_with_recovery(
                pair_key,
                pos.local_exchange,
                pos.symbol,
                client_order_id,
                lambda: self.client.place_local_market_sell(
                    pos.symbol,
                    pos.qty,
                    client_order_id=client_order_id,
                    amount_spec=pos.local_amount_spec,
                ),
                amount_spec=pos.local_amount_spec,
            )
            if isinstance(spot_order, dict):
                await self._record_force_close_spot_order(
                    pair_key, pos, spot_order, client_order_id
                )
                filled_qty, exit_price = await self._verified_force_close_spot_fill(
                    pair_key, pos, spot_order
                )
                if (
                    filled_qty + max(1e-12, pos.qty * 1e-8) >= pos.qty
                    and exit_price > 0
                ):
                    logger.info(
                        f"[Force Close] Spot leg fully liquidated: {pos.qty} {pos.symbol} "
                        f"@ {exit_price} (filled={filled_qty})"
                    )
                    return exit_price
                logger.critical(
                    f"[Force Close] partial/unverified spot liquidation for {pair_key}: "
                    f"requested={pos.qty}, filled={filled_qty}; manual reconciliation required"
                )

            logger.critical(
                f"[Force Close] FAILED to liquidate spot leg for {pair_key}. "
                f"MANUAL INTERVENTION REQUIRED: {pos.qty} {pos.symbol} is UNHEDGED. Response: {spot_order}"
            )
        except Exception:
            logger.exception(
                "[Force Close] spot liquidation was not verified after one submit for %s",
                pair_key,
            )
        return 0.0

    async def _resolve_force_close_prices(self, pair_key: str, pos, spot_exit_price_idr: float):
        """Returns (current_ask_usdt, usdt_rate, current_spot_bid_idr) for PnL accounting."""
        current_ask = pos.short_entry_usdt
        usdt_rate = 0.0
        current_spot_bid = pos.entry_price_idr

        price_data = await self._fetch_live_price(pair_key)
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
        rates = rates_for_position(pos)
        spot_entry_usdt = (
            pos.entry_price_idr / usdt_rate
            if usdt_rate > 0 and math.isfinite(usdt_rate)
            else 0.0
        )
        spot_exit_usdt = (
            current_spot_bid / usdt_rate
            if usdt_rate > 0 and math.isfinite(usdt_rate)
            else 0.0
        )
        fee_breakdown = round_trip_fee_breakdown_usdt(
            pos.qty,
            spot_entry_usdt,
            spot_exit_usdt,
            pos.short_entry_usdt,
            current_ask,
            rates,
        )
        spot_fees_usdt = fee_breakdown["spot_fees_usdt"]
        futures_fees_usdt = fee_breakdown["futures_fees_usdt"]
        fees_usdt = fee_breakdown["total_fees_usdt"]
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
            "spot_fees_usdt": spot_fees_usdt,
            "futures_fees_usdt": futures_fees_usdt,
            "fees_usdt": fees_usdt,
            "fee_source": rates.source,
            "fee_verified": rates.verified,
            "net_profit_usdt": net_profit_usdt,
            "net_profit_idr": net_profit_usdt * usdt_rate,
            "roi_pct": (net_profit_usdt / capital_usdt * 100.0) if capital_usdt > 0 else 0.0,
        }
