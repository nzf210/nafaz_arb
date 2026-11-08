"""Capital-exposure, venue-halt and balance-mismatch risk controls.

Extracted from executor/entry.py; mixed into ArbitrageEngine so the self.*
state and helper methods stay on one object.
"""

import asyncio
import inspect
import json
import logging
import math
from typing import Dict, Optional

from executor.models import ActivePosition, ExecutorPayload, PairInfo
from executor.helpers import PENDING_ENTRY_KEY_PREFIX

logger = logging.getLogger("executor.engine")


class ExposureRiskMixin:
    def _open_exposure_by_venue(self) -> Dict[str, float]:
        """Capital currently committed to open positions, keyed by exchange name."""
        by_venue: Dict[str, float] = {}
        for pos in self.active_positions.values():
            if pos.local_exchange:
                by_venue[pos.local_exchange] = by_venue.get(pos.local_exchange, 0.0) + pos.spot_cost_usdt
            if pos.global_exchange:
                by_venue[pos.global_exchange] = by_venue.get(pos.global_exchange, 0.0) + pos.futures_margin_usdt
        return by_venue

    def _pending_entry_exposure_all(self) -> tuple[float, float]:
        """
        Capital reserved by resting entry orders across ALL pairs.
        Mirrors _pending_entry_exposure but venue-agnostic, for the global
        balance-mismatch sweep which runs outside any single pair's context.
        """
        total_spot = 0.0
        total_futures = 0.0
        for order_info in self.pending_entry_orders.values():
            remaining_qty = float(order_info.get("qty", 0.0)) - float(order_info.get("filled", 0.0))
            remaining_qty = max(0.0, remaining_qty)
            original_qty = float(order_info.get("qty", 0.0) or 0.0)
            if original_qty <= 0:
                continue
            remaining_fraction = min(1.0, remaining_qty / original_qty)
            # The pending order already stores the USDT-equivalent reservation
            # calculated from the validated live FX snapshot at placement time.
            # Scale that persisted amount by the unfilled fraction; never
            # recalculate with a hardcoded FX fallback.
            total_spot += float(order_info.get("spot_cost_usdt", 0.0) or 0.0) * remaining_fraction
            total_futures += float(order_info.get("futures_margin_usdt", 0.0) or 0.0) * remaining_fraction
        return total_spot, total_futures

    def _exposure_allows(
        self, pair_tag: str, pair_info: PairInfo, planned_usdt: float
    ) -> bool:
        """
        Checks the planned order against ``max_exposure_per_exchange_usdt``.

        Exposure is the capital already committed to open positions plus any
        entry orders still resting, counted separately per exchange so a single
        venue cannot be concentrated beyond the configured cap.
        """
        cap = float(getattr(self.config, "max_exposure_per_exchange_usdt", 0.0) or 0.0)
        if cap <= 0:
            return True

        local_exposure = 0.0
        global_exposure = 0.0
        for tag, pos in self.active_positions.items():
            if tag == pair_tag:
                continue
            if pos.local_exchange == pair_info.local_exchange:
                local_exposure += pos.spot_cost_usdt
            if pos.global_exchange == pair_info.global_exchange:
                global_exposure += pos.futures_margin_usdt

        pending_local, pending_global = self._pending_entry_exposure(
            pair_tag, pair_info
        )
        local_exposure += pending_local
        global_exposure += pending_global
        # The spot leg and the futures margin each consume their own venue's cap.
        blocked = (
            local_exposure + planned_usdt > cap
            or global_exposure + planned_usdt > cap
        )
        if blocked:
            logger.warning(
                f"[State 1 Exposure Cap] Rejecting {pair_tag}: planned ${planned_usdt:.2f} would "
                f"push {pair_info.local_exchange} to ${local_exposure + planned_usdt:.2f} "
                f"and {pair_info.global_exchange} to ${global_exposure + planned_usdt:.2f} "
                f"(cap ${cap:.2f} each)"
            )
            return False
        return True

    def _pending_entry_exposure(
        self, pair_tag: str, pair_info: PairInfo
    ) -> tuple[float, float]:
        """
        Capital already reserved by resting entry orders, per exchange.

        An order that was accepted but has not filled yet still consumes the cap:
        ignoring it lets a burst of signals oversubscribe the same venue before
        any of them turns into a position.
        """
        local_exposure = 0.0
        global_exposure = 0.0
        for tag, info in self.pending_entry_orders.items():
            if tag == pair_tag:
                continue
            if info.get("local_exchange") == pair_info.local_exchange:
                local_exposure += float(info.get("spot_cost_usdt", 0.0))
            if info.get("global_exchange") == pair_info.global_exchange:
                global_exposure += float(info.get("futures_margin_usdt", 0.0))
        return local_exposure, global_exposure

    def _venue_halted_reason(self, pair_tag: str) -> str:
        """Returns the venue pair halted, or '' when clear."""
        local_ex, global_ex = self._pair_venues(pair_tag)
        if local_ex and local_ex in self._exchange_halted:
            return f"{local_ex}: {self._exchange_halted[local_ex]}"
        if global_ex and global_ex in self._exchange_halted:
            return f"{global_ex}: {self._exchange_halted[global_ex]}"
        return ""

    async def _compute_entry_sizing(
        self, pair_tag: str, payload: ExecutorPayload
    ) -> Optional[tuple[float, float]]:
        max_position, minimum_capital = self._entry_sizing_limits(payload)
        balances = await self._get_entry_sizing_balances(pair_tag, payload)
        local_exchange, global_exchange = self._pair_venues(pair_tag)
        if self._has_halted_entry_venue(pair_tag, local_exchange, global_exchange):
            return None
        if not await self._validate_entry_balance_snapshot(
            pair_tag, balances, local_exchange, global_exchange
        ):
            return None
        if balances is None:
            return max_position, minimum_capital
        return await self._size_entry_from_balances(
            pair_tag, payload, balances, max_position, minimum_capital
        )

    def _entry_sizing_limits(self, payload: ExecutorPayload) -> tuple[float, float]:
        max_position = float(payload.execution_directives.max_position_usdt or 0.0)
        if max_position <= 0:
            max_position = 100.0
        minimum = float(getattr(self.config, "min_allocated_capital", 10.0))
        return max_position, minimum

    async def _get_entry_sizing_balances(self, pair_tag: str, payload: ExecutorPayload) -> Optional[dict]:
        balances = self._balance_snapshots.pop(pair_tag, None)
        if balances is not None:
            return balances
        return await self._check_balance_mismatch(
            payload.pair_info, payload.pricing_snapshot.usdt_idr_rate, pair_tag
        )

    def _has_halted_entry_venue(self, pair_tag: str, local_exchange: str, global_exchange: str) -> bool:
        for venue in (local_exchange, global_exchange):
            if venue and venue in self._exchange_halted:
                logger.warning(
                    "[Risk Control] rejecting entry for %s: %s halted (%s)",
                    pair_tag, venue, self._exchange_halted[venue],
                )
                return True
        return False

    async def _validate_entry_balance_snapshot(
        self, pair_tag: str, balances: Optional[dict], local_exchange: str, global_exchange: str
    ) -> bool:
        if balances is not None or getattr(self.config, "mock_mode", False):
            return True
        await self._publish_execution_failure(
            pair_tag, "BALANCE_SNAPSHOT_UNAVAILABLE",
            "Snapshot saldo live tidak tersedia; entry ditolak.",
            detail={"local_exchange": local_exchange, "global_exchange": global_exchange},
        )
        return False

    async def _size_entry_from_balances(
        self, pair_tag: str, pair_info: PairInfo, balances: dict,
        max_position: float, minimum_capital: float,
    ) -> Optional[tuple[float, float]]:
        spot_available = float(balances["spot_available"])
        futures_available = float(balances["futures_available"])
        usable_capital = min(spot_available, futures_available)
        if usable_capital < minimum_capital:
            await self._report_insufficient_entry_balance(
                pair_tag, spot_available, futures_available, minimum_capital
            )
            return None
        max_position = min(max_position, usable_capital)
        if not self._exposure_allows(pair_tag, pair_info, max_position):
            await self._publish_execution_failure(
                pair_tag, "EXPOSURE_LIMIT_REACHED",
                "Entry ditolak karena batas exposure exchange akan terlampaui.",
                detail={
                    "planned_usdt": max_position,
                    "limit_usdt": getattr(self.config, "max_exposure_per_exchange_usdt", 0.0),
                },
            )
            return None
        max_position = self._reserve_entry_fee_buffer(pair_tag, max_position)
        if max_position < minimum_capital:
            await self._report_below_minimum_after_fees(pair_tag, max_position, minimum_capital)
            return None
        return max_position, minimum_capital

    async def _report_insufficient_entry_balance(
        self, pair_tag: str, spot_available: float, futures_available: float, minimum: float
    ) -> None:
        logger.warning(
            "[State 1 Balance Check] Insufficient balance for %s: spot=$%.2f futures=$%.2f min=$%.2f",
            pair_tag, spot_available, futures_available, minimum,
        )
        await self._publish_execution_failure(
            pair_tag, "INSUFFICIENT_OR_UNBALANCED_BALANCE",
            "Saldo Spot/Futures tidak mencukupi atau tidak seimbang untuk membuka posisi.",
            detail={
                "spot_available_usdt": spot_available,
                "futures_available_usdt": futures_available,
                "minimum_required_usdt": minimum,
            },
        )

    def _reserve_entry_fee_buffer(self, pair_tag: str, max_position: float) -> float:
        fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.0) or 0.0)
        if fee_pct <= 0:
            return max_position
        fee_buffer = max_position * fee_pct
        logger.debug(
            "[State 1 Fee Buffer] %s reserved $%.4f (%.3f%%) for fees",
            pair_tag, fee_buffer, fee_pct * 100,
        )
        return max_position - fee_buffer

    async def _report_below_minimum_after_fees(
        self, pair_tag: str, position_usdt: float, minimum: float
    ) -> None:
        logger.warning(
            "[State 1 Balance Check] Position size $%.2f < minimum $%.2f for %s",
            position_usdt, minimum, pair_tag,
        )
        await self._publish_execution_failure(
            pair_tag, "INSUFFICIENT_AFTER_FEE_RESERVE",
            "Ukuran posisi setelah cadangan fee berada di bawah minimum exchange.",
            detail={"position_usdt": position_usdt, "minimum_required_usdt": minimum},
        )
