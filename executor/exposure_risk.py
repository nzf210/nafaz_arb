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
        """
        Sizes an entry from the signal budget, available balance, exposure cap and
        the fee reserve. Returns ``(max_pos_usdt, min_cap)``, or None when the
        trade cannot be funded.
        """
        directives = payload.execution_directives
        pair_info = payload.pair_info

        max_pos_usdt = directives.max_position_usdt
        if max_pos_usdt <= 0:
            max_pos_usdt = 100.0  # fallback budget

        # Check exchange balance limits ($100 balance per exchange in simulation)
        min_cap = getattr(self.config, "min_allocated_capital", 10.0)

        balances = self._balance_snapshots.pop(pair_tag, None)
        if balances is None:
            balances = await self._check_balance_mismatch(
                pair_info,
                payload.pricing_snapshot.usdt_idr_rate,
                pair_tag,
            )
        local_ex, global_ex = self._pair_venues(pair_tag)
        for venue in (local_ex, global_ex):
            if venue and venue in self._exchange_halted:
                logger.warning(
                    f"[Risk Control] rejecting entry for {pair_tag}: "
                    f"{venue} halted ({self._exchange_halted[venue]})"
                )
                return None

        if balances is None and not getattr(self.config, "mock_mode", False):
            await self._publish_execution_failure(
                pair_tag,
                "BALANCE_SNAPSHOT_UNAVAILABLE",
                "Snapshot saldo live tidak tersedia; entry ditolak.",
                detail={"local_exchange": local_ex, "global_exchange": global_ex},
            )
            return None

        if balances is not None:
            avail_spot = float(balances["spot_available"])
            avail_fut = float(balances["futures_available"])
            usable_capital = min(avail_spot, avail_fut)

            if usable_capital < min_cap:
                logger.warning(
                    f"[State 1 Balance Check] Insufficient available balance for {pair_tag}: "
                    f"Spot Avail=${avail_spot:.2f}, Futures Avail=${avail_fut:.2f}, "
                    f"Min Required=${min_cap:.2f}. Rejecting entry signal."
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "INSUFFICIENT_OR_UNBALANCED_BALANCE",
                    "Saldo Spot/Futures tidak mencukupi atau tidak seimbang untuk membuka posisi.",
                    detail={"spot_available_usdt": avail_spot, "futures_available_usdt": avail_fut, "minimum_required_usdt": min_cap},
                )
                return None

            max_pos_usdt = min(max_pos_usdt, usable_capital)
            if not self._exposure_allows(pair_tag, pair_info, max_pos_usdt):
                await self._publish_execution_failure(
                    pair_tag,
                    "EXPOSURE_LIMIT_REACHED",
                    "Entry ditolak karena batas exposure exchange akan terlampaui.",
                    detail={"planned_usdt": max_pos_usdt, "limit_usdt": getattr(self.config, "max_exposure_per_exchange_usdt", 0.0)},
                )
                return None

            fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.0) or 0.0)
            if fee_pct > 0:
                fee_buffer_usdt = max_pos_usdt * fee_pct
                max_pos_usdt -= fee_buffer_usdt
                logger.debug(
                    f"[State 1 Fee Buffer] {pair_tag} reserved ${fee_buffer_usdt:.4f} "
                    f"({fee_pct * 100:.3f}%) for fees"
                )

            if max_pos_usdt < min_cap:
                logger.warning(
                    f"[State 1 Balance Check] Position size ${max_pos_usdt:.2f} "
                    f"< min required ${min_cap:.2f} for {pair_tag}"
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "INSUFFICIENT_AFTER_FEE_RESERVE",
                    "Ukuran posisi setelah cadangan fee berada di bawah minimum exchange.",
                    detail={"position_usdt": max_pos_usdt, "minimum_required_usdt": min_cap},
                )
                return None

        return max_pos_usdt, min_cap
