"""recovery adoption helpers for the universal order lifecycle."""

import logging
import time
from typing import Any, Dict, Optional

from executor.models import OrderRecord, OrderRole, OrderTerminal, PositionState, RecoveryStatus
from executor.recovery_utils import ACKNOWLEDGED_EXIT_ROLES, _spec_dict

logger = logging.getLogger("executor.order_recovery")



class RecoveryAdoptionMixin:
    """Focused order-recovery responsibilities."""

    async def _adopt_resting_entry_order(
        self, record: OrderRecord, order: Dict[str, Any],
        pair_tag: str, amount_spec: Any,
    ) -> bool:
        if pair_tag in self.pending_entry_orders:
            record.recovery_status = RecoveryStatus.ADOPTED.value
            await self.order_registry.register(record)
            await self.order_registry.clear_pending_recovery(record.local_order_id)
            return True
        info = self._rebuild_entry_order_info(record, order, amount_spec)
        if info is None:
            record.recovery_status = RecoveryStatus.UNVERIFIED.value
            await self.order_registry.register(record)
            await self.order_registry.mark_pending_recovery(
                record.local_order_id,
                "resting entry could not be rebuilt from bot state; manual reconciliation required",
            )
            return False
        self.pending_entry_orders[pair_tag] = info
        self.states[pair_tag] = (
            PositionState.ENTRY_PARTIAL
            if record.filled_quantity > 0 else PositionState.ENTRY_PENDING
        )
        await self._persist_pending_entry(pair_tag, info)
        record.recovery_status = RecoveryStatus.ADOPTED.value
        await self.order_registry.register(record)
        return True

    async def _adopt_resting_exit_order(
        self, record: OrderRecord, pair_tag: str, amount_spec: Any,
    ) -> bool:
        pos = self.active_positions.get(pair_tag)
        if pos is None:
            await self.order_registry.mark_pending_recovery(
                record.local_order_id, "resting exit order has no tracked position"
            )
            return False
        previous = self.pending_exit_orders.get(pair_tag, {})
        self.pending_exit_orders[pair_tag] = {
            "order_id": record.exchange_order_id,
            "client_order_id": record.client_order_id,
            "symbol": record.symbol,
            "price": record.price,
            "qty": record.original_quantity or pos.qty,
            "filled": record.filled_quantity,
            "closed_short_qty": float(previous.get("closed_short_qty", 0.0) or 0.0),
            "tick_size": getattr(pos, "local_price_tick", 1.0),
            "local_exchange": pos.local_exchange,
            "local_amount_spec": _spec_dict(amount_spec),
        }
        if self.states.get(pair_tag) != PositionState.RECONCILIATION_REQUIRED:
            self.states[pair_tag] = PositionState.EXIT_PENDING
        record.recovery_status = RecoveryStatus.ADOPTED.value
        await self.order_registry.register(record)
        return True

    async def _reject_unexpected_resting_market_order(
        self, record: OrderRecord
    ) -> bool:
        record.recovery_status = RecoveryStatus.UNVERIFIED.value
        await self.order_registry.register(record)
        await self.order_registry.mark_pending_recovery(
            record.local_order_id,
            f"unexpected resting {record.order_role} order; manual reconciliation required",
        )
        return False

    async def _apply_adopted_order(
        self,
        record: OrderRecord,
        order: Dict[str, Any],
        pair_tag: str,
        amount_spec: Any,
    ) -> bool:
        """Binds an adopted order back into the live lifecycle."""
        if record.local_amount_spec is None and amount_spec is not None:
            record.local_amount_spec = (
                amount_spec.to_dict() if hasattr(amount_spec, "to_dict") else amount_spec
            )
        self._apply_venue_state(record, order)
        live = self.pending_entry_orders.get(pair_tag)
        if isinstance(live, dict):
            record.hedged_quantity = max(
                float(record.hedged_quantity or 0.0),
                float(live.get("hedged_short_qty", 0.0) or 0.0),
            )
        status = record.classify_status()
        if status == OrderTerminal.UNKNOWN:
            record.recovery_status = RecoveryStatus.UNVERIFIED.value
            await self.order_registry.register(record)
            await self.order_registry.mark_pending_recovery(
                record.local_order_id, f"unknown venue status '{record.status}'"
            )
            return False
        if status in (OrderTerminal.FILLED, OrderTerminal.CANCELED):
            return await self._finalize_adopted_terminal(
                record, order, pair_tag, amount_spec
            )
        if record.order_role == OrderRole.ENTRY.value:
            return await self._adopt_resting_entry_order(
                record, order, pair_tag, amount_spec
            )
        if record.order_role == OrderRole.EXIT_SPOT.value:
            return await self._adopt_resting_exit_order(
                record, pair_tag, amount_spec
            )
        return await self._reject_unexpected_resting_market_order(record)

    async def _finalize_terminal_entry(
        self, record: OrderRecord, order: Dict[str, Any], pair_tag: str,
        amount_spec: Any,
    ) -> bool:
        info = self.pending_entry_orders.get(pair_tag) or (
            self._rebuild_entry_order_info(record, order, amount_spec)
        )
        if info is None:
            await self.order_registry.mark_pending_recovery(
                record.local_order_id,
                "terminal entry cannot be reconciled without its pair context",
            )
            return False
        info["filled"] = record.filled_quantity
        self.pending_entry_orders[pair_tag] = info
        self.states[pair_tag] = PositionState.ENTRY_PENDING
        await self._handle_entry_fills(pair_tag, info, order)
        if self.states.get(pair_tag) in (
            PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
        ):
            logger.critical(
                "[Recovery] fill reconciliation failed for %s; pending record retained",
                pair_tag,
            )
            return False
        await self._finalize_entry_order(pair_tag, record.status)
        return True

    async def _finalize_terminal_spot_exit(
        self, record: OrderRecord, pair_tag: str, amount_spec: Any,
    ) -> bool:
        pos = self.active_positions.get(pair_tag)
        if pos is None:
            return True
        self.pending_exit_orders[pair_tag] = {
            "order_id": record.exchange_order_id,
            "client_order_id": record.client_order_id,
            "symbol": record.symbol,
            "price": record.price,
            "qty": record.original_quantity,
            "filled": record.filled_quantity,
            "closed_short_qty": 0.0,
            "tick_size": getattr(pos, "local_price_tick", 1.0),
            "local_exchange": getattr(pos, "local_exchange", ""),
            "local_amount_spec": _spec_dict(amount_spec),
        }
        await self._process_pending_exit(pair_tag, force_reconcile=True)
        pos = self.active_positions.get(pair_tag)
        if pos is not None:
            await self._finalize_exit_position(pair_tag, pos)
        return True

    async def _finalize_adopted_terminal(
        self,
        record: OrderRecord,
        order: Dict[str, Any],
        pair_tag: str,
        amount_spec: Any,
    ) -> bool:
        """Reconciles an already-terminal order discovered during the sweep."""
        await self.order_registry.register(record)
        await self.order_registry.retire(record)
        if record.order_role == OrderRole.ENTRY.value:
            return await self._finalize_terminal_entry(
                record, order, pair_tag, amount_spec
            )
        if record.order_role == OrderRole.EXIT_SPOT.value:
            return await self._finalize_terminal_spot_exit(
                record, pair_tag, amount_spec
            )
        if record.order_role == OrderRole.HEDGE.value:
            return await self._reconcile_recovered_entry_hedge(
                record, order, pair_tag
            )
        if record.order_role in ACKNOWLEDGED_EXIT_ROLES:
            logger.info(
                "[Recovery] terminal %s order %s for %s acknowledged",
                record.order_role, record.exchange_order_id, pair_tag,
            )
        return True

    async def _reconcile_recovered_entry_hedge(
        self, hedge_record: OrderRecord, venue_order: Dict[str, Any], pair_tag: str
    ) -> bool:
        """Reconciles a futures fill independently against its originating spot order.

        The hedge intent is journaled as non-terminal until the spot watermark is
        durable. If the process dies after the futures fill, this path fetches the
        exact futures order and advances the parent spot watermark exactly once.
        """
        context = hedge_record.recovery_context or {}
        parent_exchange = str(context.get("parent_spot_exchange") or "").lower()
        parent_order_id = str(context.get("parent_spot_order_id") or "")
        parent_client_id = str(context.get("parent_spot_client_order_id") or "")
        if not parent_exchange or not (parent_order_id or parent_client_id):
            await self.order_registry.mark_pending_recovery(
                hedge_record.local_order_id,
                "futures hedge has no durable parent spot-order identity",
            )
            return False

        parent = None
        if parent_order_id:
            parent = await self.order_registry.find_by_exchange_id(parent_exchange, parent_order_id)
        if parent is None and parent_client_id:
            parent = await self.order_registry.find_by_client_id(parent_exchange, parent_client_id)
        if parent is None:
            await self.order_registry.mark_pending_recovery(
                hedge_record.local_order_id,
                "originating spot order is missing; cannot reconcile the futures leg safely",
            )
            self._halt_exposure_scope(pair_tag or parent_exchange, "futures hedge parent missing")
            return False

        try:
            filled = float(venue_order.get("filled", 0.0) or 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        if filled <= 0:
            await self.order_registry.mark_pending_recovery(
                hedge_record.local_order_id,
                "terminal futures hedge has no verifiable positive filled quantity",
            )
            return False

        # Marker and cumulative watermark are committed in one parent record;
        # replaying startup recovery cannot count the same futures fill twice.
        parent_context = dict(parent.recovery_context or {})
        hedge_fills = parent_context.get("hedge_fill_orders")
        if not isinstance(hedge_fills, dict):
            hedge_fills = {}
        hedge_key = str(hedge_record.client_order_id or hedge_record.exchange_order_id)
        if hedge_key not in hedge_fills:
            parent.hedged_quantity = max(0.0, float(parent.hedged_quantity or 0.0)) + filled
            hedge_fills[hedge_key] = filled
        parent_context["hedge_fill_orders"] = hedge_fills
        parent.recovery_context = parent_context
        await self.order_registry.register(parent)

        live = self.pending_entry_orders.get(pair_tag)
        if isinstance(live, dict):
            live["hedged_short_qty"] = max(
                float(live.get("hedged_short_qty", 0.0) or 0.0),
                float(parent.hedged_quantity or 0.0),
            )
            await self._persist_pending_entry(pair_tag, live)

        hedge_record.status = str(venue_order.get("status") or "closed").lower()
        hedge_record.filled_quantity = max(float(hedge_record.filled_quantity or 0.0), filled)
        hedge_record.remaining_quantity = max(
            0.0, hedge_record.original_quantity - hedge_record.filled_quantity
        )
        hedge_record.recovery_status = RecoveryStatus.ADOPTED.value
        await self.order_registry.register(hedge_record)
        await self.order_registry.retire(hedge_record)
        await self.order_registry.clear_pending_recovery(hedge_record.local_order_id)
        logger.warning(
            "[Recovery] reconciled futures hedge %s independently: filled=%s, spot watermark=%s (%s)",
            hedge_key, filled, parent.hedged_quantity, pair_tag,
        )
        return True


    @staticmethod
    def _hedge_matches_parent(
        hedge: OrderRecord, parent_exchange: str,
        parent_order_id: str, parent_client_id: str,
    ) -> bool:
        if hedge.order_role != OrderRole.HEDGE.value:
            return False
        context = hedge.recovery_context or {}
        if str(context.get("parent_spot_exchange") or "").lower() != parent_exchange:
            return False
        if (
            parent_order_id
            and str(context.get("parent_spot_order_id") or "") != parent_order_id
        ):
            return False
        if (
            parent_client_id
            and str(context.get("parent_spot_client_order_id") or "") != parent_client_id
        ):
            return False
        return True

    async def _reconcile_one_pending_entry_hedge(
        self, hedge: OrderRecord, pair_tag: str
    ) -> bool:
        if not hedge.exchange_order_id:
            await self.order_registry.mark_pending_recovery(
                hedge.local_order_id,
                "futures hedge submit outcome is ambiguous after restart",
            )
            return False
        try:
            venue_order = await self._fetch_order_by_role(
                hedge, hedge.exchange_order_id, hedge.symbol
            )
        except Exception as exc:
            await self.order_registry.mark_pending_recovery(
                hedge.local_order_id,
                f"futures hedge fetch failed: {type(exc).__name__}: {exc}",
            )
            return False
        if not isinstance(venue_order, dict):
            await self.order_registry.mark_pending_recovery(
                hedge.local_order_id,
                "futures hedge returned malformed venue state",
            )
            return False
        self._apply_venue_state(hedge, venue_order)
        classification = hedge.classify_status()
        if classification in (OrderTerminal.OPEN, OrderTerminal.UNKNOWN):
            await self.order_registry.register(hedge)
            await self.order_registry.mark_pending_recovery(
                hedge.local_order_id, "futures hedge is not confirmed terminal"
            )
            return False
        if hedge.filled_quantity > 0:
            return await self._reconcile_recovered_entry_hedge(
                hedge, venue_order, pair_tag
            )
        # A confirmed zero-fill terminal hedge is safe to retire. The spot
        # fill handler below may then place the hedge once.
        await self.order_registry.register(hedge)
        await self.order_registry.retire(hedge)
        await self.order_registry.clear_pending_recovery(hedge.local_order_id)
        return True

    async def _find_parent_spot_order(
        self, parent_exchange: str, parent_order_id: str,
        parent_client_id: str,
    ) -> Optional[OrderRecord]:
        parent = None
        if parent_exchange and parent_order_id:
            parent = await self.order_registry.find_by_exchange_id(
                parent_exchange, parent_order_id
            )
        if parent is None and parent_exchange and parent_client_id:
            parent = await self.order_registry.find_by_client_id(
                parent_exchange, parent_client_id
            )
        return parent

    async def _reconcile_pending_entry_hedges(
        self, pair_tag: str, spot_order_info: Dict[str, Any]
    ) -> bool:
        """Resolve journaled futures hedges before a terminal spot fill can retry one."""
        records = await self.order_registry.active_records()
        parent_exchange = str(
            spot_order_info.get("local_exchange") or ""
        ).lower()
        parent_order_id = str(spot_order_info.get("order_id") or "")
        parent_client_id = str(spot_order_info.get("client_order_id") or "")
        for hedge in records.values():
            if not self._hedge_matches_parent(
                hedge, parent_exchange, parent_order_id, parent_client_id
            ):
                continue
            if not await self._reconcile_one_pending_entry_hedge(
                hedge, pair_tag
            ):
                return False

        parent = await self._find_parent_spot_order(
            parent_exchange, parent_order_id, parent_client_id
        )
        if parent is not None:
            spot_order_info["hedged_short_qty"] = max(
                float(spot_order_info.get("hedged_short_qty", 0.0) or 0.0),
                float(parent.hedged_quantity or 0.0),
            )
        return True

    def _rebuild_entry_order_info(
        self, record: OrderRecord, order: Dict[str, Any], amount_spec: Any
    ) -> Optional[dict]:
        """Rebuilds the in-memory pending-entry shape from a durable record.

        Requires the persisted ``pair_key`` *and* the FX snapshot. A filled entry
        cannot be hedged without a real USDT/IDR rate, and inventing one would
        book a position on a guessed basis, so a record missing it stays pending
        for manual reconciliation instead.
        """
        pair_tag = record.pair_key
        if not pair_tag or not record.symbol:
            return None
        context = record.recovery_context or {}
        try:
            usdt_rate = float(context.get("usdt_idr_rate", 0.0) or 0.0)
        except (TypeError, ValueError):
            usdt_rate = 0.0
        if usdt_rate <= 0:
            return None
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        local_exchange = record.exchange or (
            getattr(pair_info, "local_exchange", "") if pair_info else self.config.local_exchange_name
        )
        global_exchange = (
            getattr(pair_info, "global_exchange", "") if pair_info else self.config.global_exchange_name
        )
        global_symbol = (
            context.get("global_symbol")
            or (getattr(pair_info, "global_symbol", "") if pair_info else "")
        )
        base_asset = context.get("base_asset") or (
            getattr(pair_info, "base_asset", "") if pair_info else ""
        )
        quote_asset = context.get("quote_asset") or (
            getattr(pair_info, "quote_asset", "") if pair_info else ""
        )
        if not global_symbol:
            base, _ = self._symbol_parts(record.symbol)
            if not base:
                return None
            global_symbol = f"{base.upper()}USDT"
        if not base_asset:
            base_asset, _ = self._symbol_parts(record.symbol)
        if not quote_asset:
            _, quote_asset = self._symbol_parts(record.symbol)
        return {
            "order_id": record.exchange_order_id,
            "client_order_id": record.client_order_id,
            "symbol": record.symbol,
            "global_symbol": global_symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "local_exchange": local_exchange,
            "global_exchange": global_exchange,
            # Persisted venue rules first: they are the only source that makes the
            # rebuilt order hedgeable at the correct quantity.
            "local_amount_spec": (
                record.local_amount_spec
                or amount_spec
                or self._default_amount_spec_for(local_exchange, "spot")
            ),
            "global_amount_spec": record.global_amount_spec,
            "price": record.price,
            "qty": record.original_quantity,
            "filled": record.filled_quantity,
            # Restored, not zeroed: a fill already hedged before the crash must
            # not be hedged a second time by the resumed monitoring loop.
            "hedged_short_qty": float(record.hedged_quantity or 0.0),
            "tick_size": getattr(
                self.active_positions.get(pair_tag), "local_price_tick", 1.0
            ),
            "placed_time": (record.created_at or int(time.time() * 1000)) / 1000.0,
            "recovered_from_registry": True,
            "is_mock": record.is_mock,
            # Restored verbatim so a post-restart hedge uses the same basis,
            # fees and funding window the order was sized against.
            "usdt_idr_rate": usdt_rate,
            "fx_rate_timestamp_ms": context.get("fx_rate_timestamp_ms", 0),
            "global_ask_usdt": float(context.get("global_ask_usdt", 0.0) or 0.0),
            "cancel_threshold_usdt": float(context.get("cancel_threshold_usdt", 0.0) or 0.0),
            "min_entry_spread_pct": float(context.get("min_entry_spread_pct", 0.0) or 0.0),
            "base_target_exit_spread_pct": float(
                context.get("base_target_exit_spread_pct", 0.3) or 0.3
            ),
            "current_funding_rate": float(context.get("current_funding_rate", 0.0) or 0.0),
            "next_funding_time_ms": int(context.get("next_funding_time_ms", 0) or 0),
            "spot_entry_fee_pct": float(context.get("spot_entry_fee_pct", 0.0) or 0.0),
            "spot_exit_fee_pct": float(context.get("spot_exit_fee_pct", 0.0) or 0.0),
            "perp_entry_fee_pct": float(context.get("perp_entry_fee_pct", 0.0) or 0.0),
            "perp_exit_fee_pct": float(context.get("perp_exit_fee_pct", 0.0) or 0.0),
            "fee_source": str(context.get("fee_source", "LEGACY_UNVERIFIED")),
            "fee_verified": bool(context.get("fee_verified", False)),
            "fee_fetched_at_ms": int(context.get("fee_fetched_at_ms", 0) or 0),
            "fee_asset_surcharge_pct": float(
                context.get("fee_asset_surcharge_pct", 0.0) or 0.0
            ),
            "spot_cost_usdt": float(context.get("spot_cost_usdt", 0.0) or 0.0),
            "futures_margin_usdt": float(context.get("futures_margin_usdt", 0.0) or 0.0),
        }
