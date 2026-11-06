"""Capital-exposure, venue-halt and balance-mismatch risk controls.

Extracted from executor/entry.py; mixed into ArbitrageEngine so the self.*
state and helper methods stay on one object.
"""

import asyncio
import logging
from typing import Dict, Optional

from executor.models import ExecutorPayload, PairInfo
from executor import helpers

logger = logging.getLogger("executor.engine")


class RiskMixin:
    """Exposure caps, per-venue halts and balance-drift detection."""

    def _record_order_success(self, exchange: str) -> None:
        """Clears the consecutive-failure counter for an exchange after a clean order."""
        if exchange:
            self._consecutive_order_failures.pop(exchange, None)

    def _record_order_failure(self, exchange: str, reason: str) -> None:
        """
        Counts a placement failure per exchange and halts the venue once the
        consecutive-failure threshold is crossed. Halting prevents the engine
        from repeatedly hitting a broken venue while open positions stay managed.
        """
        if not exchange:
            return
        threshold = int(getattr(self.config, "max_consecutive_order_failures", 0) or 0)
        if threshold <= 0:
            return
        count = self._consecutive_order_failures.get(exchange, 0) + 1
        self._consecutive_order_failures[exchange] = count
        if count >= threshold:
            self._exchange_halted[exchange] = f"{count} consecutive order failures ({reason})"
            logger.critical(
                f"[Risk Control] Exchange {exchange} HALTED for new entries: "
                f"{count} consecutive order failures ({reason})"
            )
            # Fire-and-forget status refresh; caller context may not be a coroutine.
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._maybe_publish_status())
            except RuntimeError:
                pass

    def resume_exchange(self, exchange: str, reason: str = "MANUAL") -> bool:
        """Clears a venue halt; returns True when a halt was actually removed."""
        if exchange in self._exchange_halted:
            del self._exchange_halted[exchange]
            self._consecutive_order_failures.pop(exchange, None)
            logger.warning(f"[Risk Control] Exchange {exchange} resumed ({reason})")
            return True
        return False

    def _check_balance_mismatch(self) -> None:
        """
        Detects when an exchange's available balance drops far below what the
        engine last observed - e.g. a withdrawal, liquidation, or another
        process consuming the capital. Halts that venue for new entries.

        Fail-open on read errors: a broken balance feed must not be conflated
        with actual missing funds, and refusing to hedge on a stale read leaves
        the spot leg naked.
        """
        fraction = float(getattr(self.config, "balance_mismatch_fraction", 0.0) or 0.0)
        if fraction <= 0 or not hasattr(self.client, "get_balances"):
            return

        try:
            balances = self.client.get_balances()
        except Exception:
            logger.exception("[Risk Control] failed to read balances; skipping mismatch check")
            return

        venue_available = {}
        venue_name = self.config.local_exchange_name
        venue_available[venue_name] = float(balances.get("spot_available", 0.0) or 0.0)
        global_venue = self.config.global_exchange_name
        venue_available[global_venue] = float(balances.get("futures_available", 0.0) or 0.0)

        # A venue with open positions naturally shows less available balance; only
        # flag when the drop cannot be explained by any known committed capital.
        pending_local, pending_global = self._pending_entry_exposure_all()
        committed = self._open_exposure_by_venue()
        committed[venue_name] = committed.get(venue_name, 0.0) + pending_local
        committed[global_venue] = committed.get(global_venue, 0.0) + pending_global

        for venue, avail in venue_available.items():
            last = self._last_balances.get(venue)
            self._last_balances[venue] = avail
            if last is None or last <= 0:
                continue
            drop = last - avail
            if drop <= 0 or drop <= last * fraction:
                continue
            explained = committed.get(venue, 0.0)
            if drop <= explained * (1.0 + 0.01):
                continue
            self._exchange_halted[venue] = (
                f"balance mismatch: available dropped {last:.2f} -> {avail:.2f} "
                f"(-{drop:.2f}) with only {explained:.2f} committed"
            )
            logger.critical(
                f"[Risk Control] Exchange {venue} HALTED: {self._exchange_halted[venue]}"
            )

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
            usdt_rate = float(order_info.get("usdt_idr_rate", self.FALLBACK_USDT_IDR_RATE) or self.FALLBACK_USDT_IDR_RATE)
            value = remaining_qty * float(order_info.get("price", 0.0))
            total_spot += value / usdt_rate if usdt_rate > 0 else 0.0
            total_futures += value / usdt_rate if usdt_rate > 0 else 0.0
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

        self._check_balance_mismatch()
        local_ex, global_ex = self._pair_venues(pair_tag)
        for venue in (local_ex, global_ex):
            if venue and venue in self._exchange_halted:
                logger.warning(
                    f"[Risk Control] rejecting entry for {pair_tag}: "
                    f"{venue} halted ({self._exchange_halted[venue]})"
                )
                return None

        if hasattr(self.client, "get_balances"):
            balances = self.client.get_balances()
            avail_spot = float(balances.get("spot_available", 0.0))
            avail_fut = float(balances.get("futures_available", 0.0))
            usable_capital = min(avail_spot, avail_fut)

            if usable_capital < min_cap:
                logger.warning(
                    f"[State 1 Balance Check] Insufficient available balance for {pair_tag}: "
                    f"Spot Avail=${avail_spot:.2f}, Futures Avail=${avail_fut:.2f}, Min Required=${min_cap:.2f}. "
                    f"Rejecting entry signal."
                )
                return None

            # Target position size capped by available capital
            max_pos_usdt = min(max_pos_usdt, usable_capital)

            # Respect the per-exchange exposure cap before anything is locked.
            if not self._exposure_allows(pair_tag, pair_info, max_pos_usdt):
                return None

            # Reserve capital for the round-trip fees so the locked budget still
            # covers them; otherwise the exit eats into the hedge and the trade
            # can close nominally profitable but actually net-negative.
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
                    f"[State 1 Balance Check] Position size ${max_pos_usdt:.2f} < min required ${min_cap:.2f} for {pair_tag}"
                )
                return None

        return max_pos_usdt, min_cap
