"""Balance snapshots, exposure limits, and pair-scoped entry gates."""

import inspect
import logging
import math
import time
from typing import Dict, Optional

from executor.models import PairInfo, PositionState
from executor.pair_registry import ExchangePairKey

logger = logging.getLogger("executor.engine")


class RiskExposureMixin:
    """Balance and exposure checks shared by entry execution."""

    async def _get_balance_snapshot(
        self,
        pair_tag: Optional[str] = None,
        pair_info: Optional[PairInfo] = None,
        symbol: Optional[str] = None,
        usdt_idr_rate: float = 0.0,
    ) -> Optional[dict]:
        """Returns a normalized USDT-equivalent balance snapshot from live or paper client."""
        if pair_info is None and pair_tag:
            pair_info = self._pair_infos.get(pair_tag)

        local_symbol = (pair_info.local_symbol if pair_info else symbol) or symbol or ""
        quote_asset = (pair_info.quote_asset if pair_info else "").upper()
        if not quote_asset and local_symbol:
            normalized = local_symbol.upper().split(":")[0].replace("/", "_").replace("-", "_")
            quote_asset = normalized.split("_")[-1] if "_" in normalized else "IDR"

        global_symbol = pair_info.global_symbol.upper() if pair_info else ""
        global_quote_asset = "USDC" if "USDC" in global_symbol else "USDT"
        if not global_symbol and quote_asset == "USDC":
            global_quote_asset = "USDC"

        getter = getattr(self.client, "get_balances", None)
        if not callable(getter):
            if getattr(self.config, "mock_mode", False):
                return None
            raise RuntimeError("exchange client does not implement get_balances")

        balance_kwargs = {
            "local_quote_asset": quote_asset or "IDR",
            "global_quote_asset": global_quote_asset,
            "usdt_idr_rate": float(usdt_idr_rate or 0.0),
        }
        try:
            signature = inspect.signature(getter)
        except (TypeError, ValueError):
            signature = None
        if signature is None or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        ):
            result = getter(**balance_kwargs)
        else:
            accepted = {
                key: value for key, value in balance_kwargs.items()
                if key in signature.parameters
            }
            result = getter(**accepted)
        if inspect.isawaitable(result):
            result = await result

        if (
            not isinstance(result, dict)
            or "spot_available" not in result
            or "futures_available" not in result
        ):
            raise ValueError("balance response missing spot_available/futures_available")
        for field in ("spot_available", "futures_available"):
            value = float(result[field])
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"balance response contains invalid {field}")
            result[field] = value
        return result

    async def _check_balance_mismatch(
        self,
        pair_info: Optional[PairInfo] = None,
        usdt_idr_rate: float = 0.0,
        pair_tag: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Detects unexpected available-balance drops and returns the same normalized
        snapshot for entry sizing. Live balance read failures halt new entries.
        Existing positions remain managed; this check never blocks a required hedge.
        """
        local_name = pair_info.local_exchange if pair_info else self.config.local_exchange_name
        global_name = pair_info.global_exchange if pair_info else self.config.global_exchange_name
        try:
            balances = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                pair_info=pair_info,
                usdt_idr_rate=usdt_idr_rate,
            )
        except Exception as exc:
            reason = f"balance feed unavailable: {exc}"
            self._exchange_halted[local_name] = reason
            self._exchange_halted[global_name] = reason
            self._persist_risk_state("exchange_halt", scope="exchange", exchange=local_name, reason=reason, value={"source": "balance_feed_unavailable"})
            self._persist_risk_state("exchange_halt", scope="exchange", exchange=global_name, reason=reason, value={"source": "balance_feed_unavailable"})
            logger.exception(
                "[Risk Control] balance read failed; both venues halted for new entries"
            )
            await self._publish_execution_failure(
                pair_tag or "unknown",
                "BALANCE_FEED_UNAVAILABLE",
                "Gagal membaca saldo live; entry dihentikan pada kedua exchange.",
                detail={"local_exchange": local_name, "global_exchange": global_name, "error": str(exc)},
            )
            return None

        if balances is None:
            if pair_tag:
                self._balance_snapshots.pop(pair_tag, None)
            return None
        if pair_tag:
            self._balance_snapshots[pair_tag] = balances

        fraction = float(getattr(self.config, "balance_mismatch_fraction", 0.0) or 0.0)
        if fraction <= 0:
            return balances

        venue_available = {
            local_name: float(balances.get("spot_available", 0.0) or 0.0),
            global_name: float(balances.get("futures_available", 0.0) or 0.0),
        }
        pending_local, pending_global = self._pending_entry_exposure_all()
        committed = self._open_exposure_by_venue()
        committed[local_name] = committed.get(local_name, 0.0) + pending_local
        committed[global_name] = committed.get(global_name, 0.0) + pending_global

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
            await self._publish_execution_failure(
                pair_tag or "unknown",
                "BALANCE_MISMATCH",
                f"Saldo {venue} berubah tidak sesuai exposure tercatat; entry dihentikan.",
                detail={"exchange": venue, "available_drop": drop, "committed_exposure": explained},
            )
        return balances

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

    def _recovery_scope_keys(self, pair_tag: str) -> list:
        """Candidate recovery gate keys for a pair, most specific first.

        Recovery gates exposure at whatever granularity it could prove: the pair,
        then the market, then the venue. Every level is checked so an unrelated
        symbol's orphan does not block trading on a healthy market.
        """
        keys = [pair_tag] if pair_tag else []
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag) if pair_tag else None
        # Recovery keys pair gates by the venue-qualified tag while an incoming
        # signal carries the local-only tag, so both must be checked.
        if pair_info is not None:
            try:
                keys.append(ExchangePairKey.from_pair_info(pair_info).tag.lower())
            except Exception:
                pass
        local_ex, global_ex = self._pair_venues(pair_tag)
        local_symbol = str(getattr(pair_info, "local_symbol", "") or "")
        global_symbol = str(getattr(pair_info, "global_symbol", "") or "")
        if not local_symbol and pair_tag and ":" in pair_tag:
            local_symbol = pair_tag.split(":", 1)[1]
        for venue, symbol in ((local_ex, local_symbol), (global_ex, global_symbol)):
            if venue and symbol:
                keys.append(f"{venue}:{symbol}")
        keys.extend(venue for venue in (local_ex, global_ex) if venue)
        return keys

    def _venue_halted_reason(self, pair_tag: str) -> str:
        """Returns the venue pair halted, or '' when clear."""
        # Recovery-installed gates first: an unresolved orphan or unverifiable
        # order blocks the pair/market/venue it affects, never the whole bot.
        recovery_halts = getattr(self, "_recovery_scope_halts", None) or {}
        if "*" in recovery_halts:
            return f"global: {recovery_halts['*']}"
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            failure = (getattr(self, "_failure_reasons", {}) or {}).get(pair_tag) or {}
            return f"{pair_tag}: {failure.get('reason') or 'position reconciliation required'}"
        for key in self._recovery_scope_keys(pair_tag):
            if key in recovery_halts:
                return f"{key}: {recovery_halts[key]}"
        local_ex, global_ex = self._pair_venues(pair_tag)
        if local_ex and local_ex in self._exchange_halted:
            return f"{local_ex}: {self._exchange_halted[local_ex]}"
        if global_ex and global_ex in self._exchange_halted:
            return f"{global_ex}: {self._exchange_halted[global_ex]}"
        return ""
