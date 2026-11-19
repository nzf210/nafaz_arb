"""force close leg responsibilities for emergency position closure."""

import logging
import json
import math
from typing import Optional, Tuple

from executor.models import OrderRole
from executor.fee_engine import rates_for_position, round_trip_fee_breakdown_usdt
from executor import helpers
from executor.helpers import RetryExhausted, build_client_order_id
from executor.derisk_fence import derisk_fence

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

    async def _adopt_existing_force_close(
        self, pair_key: str, pos, global_sym: str, client_order_id: str
    ) -> Tuple[bool, float]:
        """Adopt a prior reduceOnly close; never resubmit while its outcome is uncertain."""
        try:
            order = await self.client.fetch_global_order_by_client_id(
                client_order_id, global_sym, pos.global_amount_spec
            )
        except Exception:
            logger.exception(
                "[Force Close] exact client-id lookup failed for prior futures close %s",
                client_order_id,
            )
            return False, 0.0
        if not isinstance(order, dict):
            logger.critical(
                "[Force Close] prior reduceOnly close %s is not yet visible; "
                "refusing a new client ID while venue history may be eventually consistent",
                client_order_id,
            )
            return False, 0.0
        status = str(order.get("status", "")).lower()
        if status not in ("closed", "filled"):
            logger.critical(
                "[Force Close] prior reduceOnly close %s remains non-terminal (%s); "
                "refusing duplicate close",
                client_order_id, status or "unknown",
            )
            return False, 0.0
        try:
            raw_filled = float(order.get("filled") or 0.0)
            contract_size = float(
                order.get("contractSize")
                or (pos.global_amount_spec.get("base_per_unit") if isinstance(pos.global_amount_spec, dict) else getattr(pos.global_amount_spec, "base_per_unit", 1.0))
                or 1.0
            )
            unit = str(
                pos.global_amount_spec.get("unit") if isinstance(pos.global_amount_spec, dict) else getattr(pos.global_amount_spec, "unit", "")
            ).upper()
            filled_qty = raw_filled * contract_size if unit == "CONTRACT" and raw_filled <= pos.qty / contract_size + 1e-4 else raw_filled
        except (TypeError, ValueError):
            return False, 0.0
        if filled_qty + max(1e-12, pos.qty * 1e-8) < pos.qty:
            logger.critical(
                "[Force Close] prior reduceOnly close %s is terminal but partial "
                "(requested=%s filled=%s); manual reconciliation required",
                client_order_id, pos.qty, filled_qty,
            )
            return False, 0.0

        self.supabase.order(
            pair_key, "futures_exit", pos.global_exchange, "perpetual",
            global_sym, order, client_order_id=client_order_id,
            reduce_only=True, is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        await self._register_order(
            pair_key, client_order_id, order,
            exchange=pos.global_exchange, market_type="perpetual",
            symbol=global_sym, role=OrderRole.FORCE_CLOSE.value,
            side="buy", order_type="market",
            price=helpers.extract_fill_price(order), quantity=pos.qty,
            reduce_only=True,
        )
        if not await self._verify_force_close_futures_flat(pair_key, global_sym):
            return False, 0.0
        return True, helpers.extract_fill_price(order)

    async def _close_global_short_leg(
        self, pair_key: str, pos, allow_already_flat: bool = False
    ) -> Tuple[bool, float]:
        """Close the futures short and return its fill price only after flat verification."""
        global_sym = self._resolve_global_symbol(pos.symbol)
        if allow_already_flat:
            already_flat = await self._retry_short_is_flat(pair_key, pos, global_sym)
            return (True, 0.0) if already_flat else (False, 0.0)

        logger.warning(f"[Force Close] Closing global short {pos.qty} on {global_sym}...")
        is_mock = bool(getattr(self.config, "mock_mode", False))
        opened_at_ms = int(getattr(pos, "opened_at_ms", 0) or 0)
        if opened_at_ms <= 0:
            opened_at_ms = int(getattr(pos, "updated_at", 0) or 0)
            if opened_at_ms > 0:
                pos.opened_at_ms = opened_at_ms
        if not is_mock and opened_at_ms <= 0:
            logger.critical(
                "[Force Close] refusing a LIVE futures close without a stable position opened_at_ms "
                "for deterministic client-order-id recovery (%s)",
                pair_key,
            )
            return False, 0.0
        # Preserve the full position timestamp: modulo truncation collides
        # for the same pair every 1e9 milliseconds (~11.6 days).
        force_seq = max(1, opened_at_ms) if not is_mock else self._next_order_seq()
        force_coid = build_client_order_id(pair_key, "force-close", force_seq)

        # The shared de-risk fence now covers prior-intent lookup, intent
        # persistence, venue submit, and ACK registration. Otherwise two
        # replicas can both observe "no prior intent" before either reaches
        # the lock and submit the same deterministic reduceOnly client ID.
        registry = None
        if not is_mock:
            registry = getattr(self, "order_registry", None)
            if registry is None:
                logger.critical(
                    "[Force Close] LIVE reduceOnly retry protection requires the durable order registry (%s)",
                    pair_key,
                )
                return False, 0.0

        await self._sync_force_close_market_price(pair_key, global_sym)
        try:
            async with derisk_fence(self, client_order_id=force_coid):
                if not is_mock:
                    try:
                        prior_intent = await registry.find_by_client_id(
                            pos.global_exchange, force_coid
                        )
                    except Exception:
                        logger.exception(
                            "[Force Close] cannot inspect prior close intent %s; refusing resubmit",
                            force_coid,
                        )
                        return False, 0.0
                    if prior_intent is not None:
                        return await self._adopt_existing_force_close(
                            pair_key, pos, global_sym, force_coid
                        )

                await self._register_order_intent(
                    pair_key, force_coid,
                    exchange=pos.global_exchange, market_type="perpetual",
                    symbol=global_sym, role=OrderRole.FORCE_CLOSE.value,
                    side="buy", order_type="market", price=0.0,
                    quantity=pos.qty, reduce_only=True,
                    global_amount_spec=pos.global_amount_spec,
                    required=not is_mock,
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

    async def _adopt_existing_force_close_spot(
        self, pair_key: str, pos, client_order_id: str
    ) -> float:
        """Adopt an earlier spot liquidation; never mint a second ID while uncertain."""
        lookup = getattr(self.client, "fetch_local_order_by_client_id", None)
        if not callable(lookup):
            logger.critical(
                "[Force Close] exact client-ID lookup is unavailable for prior spot liquidation %s",
                client_order_id,
            )
            return 0.0
        try:
            order = await lookup(
                client_order_id, pos.symbol, pos.local_amount_spec
            )
        except Exception:
            logger.exception(
                "[Force Close] exact client-ID lookup failed for prior spot liquidation %s",
                client_order_id,
            )
            return 0.0
        if not isinstance(order, dict) or not order.get("id"):
            logger.critical(
                "[Force Close] prior spot liquidation %s is not visible yet; "
                "refusing a new client ID while venue history may be eventually consistent",
                client_order_id,
            )
            return 0.0

        await self._record_force_close_spot_order(
            pair_key, pos, order, client_order_id
        )
        status = str(order.get("status") or "").lower()
        terminal = status in {
            "closed", "filled", "canceled", "cancelled", "expired", "rejected",
        }
        if not terminal:
            logger.critical(
                "[Force Close] prior spot liquidation %s remains non-terminal (%s); "
                "refusing duplicate SELL",
                client_order_id, status or "unknown",
            )
            return 0.0
        filled_qty, exit_price = await self._verified_force_close_spot_fill(
            pair_key, pos, order
        )
        if (
            filled_qty + max(1e-12, pos.qty * 1e-8) < pos.qty
            or exit_price <= 0
        ):
            logger.critical(
                "[Force Close] prior spot liquidation %s is terminal but not a "
                "verified full fill (requested=%s filled=%s); manual reconciliation required",
                client_order_id, pos.qty, filled_qty,
            )
            return 0.0
        return exit_price

    async def _check_retry_spot_balance(
        self, pair_key: str, pos
    ) -> Tuple[bool, Optional[float]]:
        """Check local spot balance before retrying liquidation of a reconciled position.

        Returns (already_flat, sell_qty_override).
        """
        balance_of = getattr(self.client, "fetch_spot_total_balance", None) or getattr(
            self.client, "fetch_spot_free_balance", None
        )
        if not callable(balance_of):
            return False, None
        base_asset = str(getattr(pos, "base_asset", "") or "").strip().upper()
        if not base_asset:
            raw_sym = str(getattr(pos, "symbol", "") or "").upper()
            for sep in ("/", "_", "-"):
                if sep in raw_sym:
                    base_asset = raw_sym.split(sep)[0].strip()
                    break
            if not base_asset:
                for quote in ("IDR", "USDT", "USDC"):
                    if raw_sym.endswith(quote) and len(raw_sym) > len(quote):
                        base_asset = raw_sym[: -len(quote)].strip()
                        break
        if not base_asset:
            return False, None
        try:
            actual = float(await balance_of(base_asset))
        except NotImplementedError:
            return False, None
        except Exception:
            logger.exception(
                "[Force Close] cannot verify local spot %s balance for retry %s",
                base_asset, pair_key,
            )
            return False, 0.0
        if not math.isfinite(actual) or actual < 0:
            return False, 0.0
        spec = getattr(pos, "local_amount_spec", None)
        if isinstance(spec, dict):
            step = float(spec.get("step", 0.0) or 0.0)
            min_qty = float(spec.get("min", spec.get("minimum", 0.0)) or 0.0)
        else:
            step = float(getattr(spec, "step", 0.0) or 0.0)
            min_qty = float(getattr(spec, "minimum", 0.0) or 0.0)
        flat_threshold = max(1e-10, float(pos.qty or 0.0) * 1e-4, step, min_qty)
        if actual < flat_threshold:
            logger.warning(
                "[Force Close] spot leg for %s is already flat (balance=%s %s); "
                "skipping duplicate spot market sell",
                pair_key, actual, base_asset,
            )
            return True, 0.0
        if actual + 1e-12 < float(pos.qty or 0.0):
            sell_qty = (
                math.floor((actual + 1e-12) / step) * step
                if step > 0
                else actual
            )
            logger.warning(
                "[Force Close] partial spot balance for %s: expected=%s, available=%s %s; "
                "liquidating available %s",
                pair_key, pos.qty, actual, base_asset, sell_qty,
            )
            return False, sell_qty
        return False, None

    async def _liquidate_spot_leg(
        self, pair_key: str, pos, allow_already_flat: bool = False
    ) -> float:
        """
        Liquidates the local spot leg. Returns the realized exit price only after
        a verified full fill, or -1.0 when allow_already_flat confirms zero balance.
        LIVE retries reuse a position-bound client ID and adopt any prior intent
        before another create-order call.
        """
        sell_qty = float(pos.qty)
        already_flat, qty_override = await self._check_retry_spot_balance(
            pair_key, pos
        )
        if allow_already_flat and already_flat:
            return -1.0
        if qty_override is not None:
            if qty_override <= 0:
                return 0.0
            sell_qty = qty_override

        logger.warning(f"[Force Close] Selling {sell_qty} {pos.symbol} spot leg...")
        is_mock = bool(getattr(self.config, "mock_mode", False))
        opened_at_ms = int(getattr(pos, "opened_at_ms", 0) or 0)
        if opened_at_ms <= 0:
            opened_at_ms = int(getattr(pos, "updated_at", 0) or 0)
            if opened_at_ms > 0:
                pos.opened_at_ms = opened_at_ms
        if not is_mock and opened_at_ms <= 0:
            logger.critical(
                "[Force Close] refusing LIVE spot liquidation without stable "
                "position opened_at_ms for deterministic client-ID recovery (%s)",
                pair_key,
            )
            return 0.0

        if not is_mock:
            registry = getattr(self, "order_registry", None)
            if registry is None:
                logger.critical(
                    "[Force Close] LIVE spot liquidation requires the durable order registry (%s)",
                    pair_key,
                )
                return 0.0
            # Same full timestamp as the futures leg; never reuse an ID
            # for a later position opened more than 1e9 ms afterward.
            force_seq = max(1, opened_at_ms)
        else:
            registry = None
            force_seq = self._next_order_seq()

        client_order_id = build_client_order_id(
            pair_key, "force-liq", force_seq
        )
        try:
            async with derisk_fence(self, client_order_id=client_order_id):
                # Lookup and intent registration must be inside the same shared
                # lock as the POST. A pre-lock lookup is a TOCTOU race across
                # executor replicas even when the client ID is deterministic.
                if not is_mock:
                    try:
                        prior_intent = await registry.find_by_client_id(
                            pos.local_exchange, client_order_id
                        )
                    except Exception:
                        logger.exception(
                            "[Force Close] cannot inspect prior spot liquidation %s; refusing resubmit",
                            client_order_id,
                        )
                        return 0.0
                    if prior_intent is not None:
                        return await self._adopt_existing_force_close_spot(
                            pair_key, pos, client_order_id
                        )

                await self._register_order_intent(
                    pair_key, client_order_id,
                    exchange=pos.local_exchange, market_type="spot",
                    symbol=pos.symbol, role=OrderRole.FORCE_CLOSE.value,
                    side="sell", order_type="market", price=0.0,
                    quantity=sell_qty, local_amount_spec=pos.local_amount_spec,
                    recovery_context={
                        "position_opened_at_ms": opened_at_ms,
                        "force_close_leg": "spot",
                    },
                    required=not is_mock,
                )

                async def _spot_sell():
                    return await self.client.place_local_market_sell(
                        pos.symbol,
                        sell_qty,
                        client_order_id=client_order_id,
                        amount_spec=pos.local_amount_spec,
                    )

                # Keep the shared lock through all explicit-429 retries and the
                # durable ACK registration. Ambiguous errors are never retried.
                spot_order = await helpers.retry_with_backoff(
                    _spot_sell,
                    max_retries=4,
                    base_delay=0.5,
                    label=f"force-close spot liquidation {pos.symbol} for {pair_key}",
                    retry_if=helpers.is_explicit_rate_limit_rejection,
                )
                if isinstance(spot_order, dict):
                    await self._record_force_close_spot_order(
                        pair_key, pos, spot_order, client_order_id
                    )
                    status = str(spot_order.get("status") or "").lower()
                    filled_qty, exit_price = await self._verified_force_close_spot_fill(
                        pair_key, pos, spot_order
                    )
                    if (
                        status in ("closed", "filled")
                        and filled_qty + max(1e-12, sell_qty * 1e-8) >= sell_qty
                        and exit_price > 0
                    ):
                        logger.info(
                            f"[Force Close] Spot leg fully liquidated: {sell_qty} {pos.symbol} "
                            f"@ {exit_price} (filled={filled_qty})"
                        )
                        return exit_price
                    logger.critical(
                        f"[Force Close] partial/unverified spot liquidation for {pair_key}: "
                        f"requested={sell_qty}, filled={filled_qty}; manual reconciliation required"
                    )

            logger.critical(
                f"[Force Close] FAILED to liquidate spot leg for {pair_key}. "
                f"MANUAL INTERVENTION REQUIRED: {pos.qty} {pos.symbol} is UNHEDGED. Response: {spot_order}"
            )
        except RetryExhausted as exhausted:
            rejection_check = getattr(self, "_is_definitive_order_rejection", None)
            if callable(rejection_check) and rejection_check(exhausted.last_error):
                await self._mark_order_submission_rejected(pos.local_exchange, client_order_id)
            logger.exception(
                f"[Force Close] spot liquidation exhausted retries for {pair_key}"
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
