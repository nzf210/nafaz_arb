"""Dual-leg funding preflight and entry sizing safeguards."""

import logging
import math
from typing import Optional

from executor.models import ExecutorPayload, PairInfo

logger = logging.getLogger("executor.engine")


class RiskFundingMixin:
    """Sizing and funding validation before submitting either order leg."""

    async def _compute_entry_sizing(
        self, pair_tag: str, payload: ExecutorPayload
    ) -> Optional[tuple[float, float]]:
        """
        Sizes an entry from the signal budget, exposure cap and fee reserve.
        Both legs commit the same USDT notional, so the budget is capped to
        min(spot, futures) available on every venue mode; when the balance is
        unreadable the size is left uncapped and the funding preflight decides.
        """
        directives = payload.execution_directives
        pair_info = payload.pair_info

        max_pos_usdt = directives.max_position_usdt
        if max_pos_usdt <= 0:
            max_pos_usdt = 100.0  # fallback budget

        min_cap = getattr(self.config, "min_allocated_capital", 10.0)
        mock_mode = bool(getattr(self.config, "mock_mode", False))

        # Both legs commit the same USDT notional, so the entry must be capped to
        # min(spot, futures) for live venues too, not just simulated ones: a
        # directive that outruns the thinner leg would otherwise reach the
        # funding preflight and be rejected as underfunded instead of sized down.
        balances = self._balance_snapshots.pop(pair_tag, None)
        if balances is None and mock_mode:
            balances = await self._check_balance_mismatch(
                pair_info,
                payload.pricing_snapshot.usdt_idr_rate,
                pair_tag,
            )
        if balances is None and not mock_mode:
            # Live uses its own probe rather than _check_balance_mismatch: a
            # transient read failure must not halt the venues, and the funding
            # preflight downstream already fails open on an unreadable balance.
            try:
                balances = await self._get_balance_snapshot(
                    pair_tag=pair_tag,
                    pair_info=pair_info,
                    usdt_idr_rate=payload.pricing_snapshot.usdt_idr_rate,
                )
            except Exception as exc:
                logger.warning(
                    f"[State 1 Balance Check] balance probe failed for {pair_tag}; "
                    f"sizing uncapped and deferred to the funding preflight: {exc}"
                )
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

        if not mock_mode or balances is not None:
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

    async def _verify_dual_leg_funding(
        self,
        pair_tag: str,
        pair_info: PairInfo,
        required_usdt: float,
        usdt_idr_rate: float,
    ) -> bool:
        """Confirms both legs can be funded before a live entry is submitted.

        Entry commits two legs at once: a spot buy that locks the local quote
        currency, and a 1x futures short that locks margin on the global venue.
        Checking them together prevents the failure mode where the spot leg fills
        but the hedge leg is rejected for margin, leaving unhedged exposure.
        Returns False (and publishes the reason) when either leg is short.
        """
        try:
            balances = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                pair_info=pair_info,
                usdt_idr_rate=usdt_idr_rate,
            )
        except Exception as exc:
            # A balance probe failure must not silently pass the gate; surface it
            # and let the caller decide, matching the pre-existing fail-open on
            # unsupported clients while logging loudly.
            logger.exception(
                "[State 1 Funding Preflight] balance probe failed for %s",
                pair_tag,
            )
            return True

        if not isinstance(balances, dict):
            return True

        try:
            avail_spot = float(balances.get("spot_available", 0.0) or 0.0)
            avail_fut = float(balances.get("futures_available", 0.0) or 0.0)
        except (TypeError, ValueError):
            logger.error(
                f"[State 1 Funding Preflight] unreadable balance payload for {pair_tag}"
            )
            return True

        if not (math.isfinite(avail_spot) and math.isfinite(avail_fut)):
            logger.error(
                f"[State 1 Funding Preflight] non-finite balance for {pair_tag}"
            )
            return True

        required = float(required_usdt)
        missing = []
        if avail_spot < required:
            missing.append(f"spot ${avail_spot:.2f} < ${required:.2f}")
        if avail_fut < required:
            missing.append(f"futures ${avail_fut:.2f} < ${required:.2f}")
        if not missing:
            logger.debug(
                f"[State 1 Funding Preflight] {pair_tag} funded: "
                f"spot=${avail_spot:.2f} futures=${avail_fut:.2f} required=${required:.2f}"
            )
            return True

        # Both legs commit the same USDT-equivalent notional, so the entry is
        # fundable exactly when the thinner leg covers it. Sizing is normally
        # capped to min(spot, futures) upstream, so reaching here means the
        # directive outran the balance (e.g. scanner allocated capital_total
        # for one leg while the preflight needs it on both); report the cap the
        # caller can actually use instead of only the shortfall.
        fundable_usdt = min(avail_spot, avail_fut)
        logger.warning(
            f"[State 1 Funding Preflight] {pair_tag} underfunded for a dual-leg "
            f"entry ({'; '.join(missing)}); fundable per leg ${fundable_usdt:.2f}"
        )
        await self._publish_execution_failure(
            pair_tag,
            "DUAL_LEG_UNDERFUNDED",
            "Saldo Spot dan/atau Futures tidak cukup untuk mengeksekusi kedua kaki sekaligus.",
            detail={
                "required_usdt_per_leg": required,
                "spot_available_usdt": avail_spot,
                "futures_available_usdt": avail_fut,
                "fundable_usdt_per_leg": fundable_usdt,
                "shortfall": missing,
            },
        )
        return False
