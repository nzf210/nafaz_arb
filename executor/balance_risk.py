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

from executor.models import ActivePosition, ExecutorPayload, PairInfo, PositionState
from executor.helpers import PENDING_ENTRY_KEY_PREFIX

logger = logging.getLogger("executor.engine")


class BalanceRiskMixin:
    """Exposure caps, per-venue halts and balance-drift detection."""

    async def _refresh_shared_entry_reservations(self) -> None:
        """Refresh shared pending and active reservations while venue locks are held."""
        pending_keys, active_keys = await self._shared_reservation_keys()
        for key in pending_keys or []:
            await self._restore_pending_entry_reservation(key)
        for key in active_keys or []:
            await self._restore_active_position_reservation(key)

    async def _shared_reservation_keys(self) -> tuple[list, list]:
        if not self.redis:
            if not getattr(self.config, "mock_mode", False):
                raise RuntimeError("Redis is required for live exposure reconciliation")
            return [], []
        try:
            pending = await self.redis.keys(PENDING_ENTRY_KEY_PREFIX + "*")
            active = await self.redis.keys("arb:active_pos:*")
            return pending, active
        except Exception as exc:
            raise RuntimeError(f"cannot enumerate shared exposure reservations: {exc}") from exc

    async def _restore_pending_entry_reservation(self, key: str) -> None:
        pair_tag = str(key).split(PENDING_ENTRY_KEY_PREFIX, 1)[-1]
        raw = await self.redis.get(key)
        if not raw:
            return
        try:
            info = json.loads(raw)
            self._validate_pending_entry_reservation(info)
            local_exchange = str(info["local_exchange"]).lower()
            global_exchange = str(info["global_exchange"]).lower()
            order_id = str(info["order_id"])
            existing = self.pending_entry_orders.get(pair_tag)
            if existing is None:
                self.pending_entry_orders[pair_tag] = info
                self.states.setdefault(pair_tag, PositionState.RECONCILIATION_REQUIRED)
                return
            identity_matches = (
                existing.get("order_id") == order_id
                and str(existing.get("local_exchange", "")).lower() == local_exchange
                and str(existing.get("global_exchange", "")).lower() == global_exchange
            )
            if not identity_matches:
                raise ValueError("in-memory and Redis pending entry identities disagree")
        except Exception as exc:
            logger.critical("[Risk Control] invalid shared pending entry %s: %s", key, exc)
            raise RuntimeError(f"unsafe pending entry reservation {key}: {exc}") from exc

    @staticmethod
    def _validate_pending_entry_reservation(info: dict) -> None:
        if not isinstance(info, dict):
            raise ValueError("pending order record is not an object")
        required = ("local_exchange", "global_exchange", "order_id", "symbol")
        if not all(str(info.get(field) or "").strip() for field in required):
            raise ValueError("pending order record is missing route/order identity")
        quantity = float(info.get("qty") or 0.0)
        if not math.isfinite(quantity) or quantity <= 0:
            raise ValueError("pending order record has invalid quantity")

    async def _restore_active_position_reservation(self, key: str) -> None:
        pair_tag = str(key).split("arb:active_pos:", 1)[-1]
        raw = await self.redis.get(key)
        if not raw:
            return
        try:
            restored = ActivePosition.from_dict(json.loads(raw))
            if restored.qty <= 0 or not math.isfinite(float(restored.qty)):
                raise ValueError("active position has invalid quantity")
            current = self.active_positions.get(pair_tag)
            if current is None:
                self.active_positions[pair_tag] = restored
                return
            route_matches = (
                current.local_exchange == restored.local_exchange
                and current.global_exchange == restored.global_exchange
            )
            quantity_matches = math.isclose(
                float(current.qty), float(restored.qty), rel_tol=1e-8, abs_tol=1e-12
            )
            if not route_matches or not quantity_matches:
                raise ValueError("in-memory and Redis active position snapshots disagree")
        except Exception as exc:
            logger.critical("[Risk Control] invalid shared active position %s: %s", key, exc)
            raise RuntimeError(f"unsafe active position reservation {key}: {exc}") from exc

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

    async def _get_balance_snapshot(
        self,
        pair_tag: Optional[str] = None,
        pair_info: Optional[PairInfo] = None,
        symbol: Optional[str] = None,
        usdt_idr_rate: float = 0.0,
    ) -> Optional[dict]:
        """Returns a normalized USDT-equivalent balance snapshot from the exchange client."""
        context = self._resolve_balance_context(pair_tag, pair_info, symbol)
        getter = getattr(self.client, "get_balances", None)
        if not callable(getter):
            if getattr(self.config, "mock_mode", False):
                return None
            raise RuntimeError("exchange client does not implement get_balances")
        kwargs = {
            "local_quote_asset": context["quote_asset"] or "IDR",
            "global_quote_asset": context["global_quote_asset"],
            "usdt_idr_rate": float(usdt_idr_rate or 0.0),
        }
        result = await self._invoke_balance_getter(getter, kwargs)
        return self._validate_balance_snapshot(result)

    def _resolve_balance_context(self, pair_tag, pair_info, symbol) -> dict:
        if pair_info is None and pair_tag:
            pair_info = self._pair_infos.get(pair_tag)
        local_symbol = (pair_info.local_symbol if pair_info else symbol) or symbol or ""
        quote_asset = self._resolve_local_quote_asset(pair_info, local_symbol)
        global_quote = self._resolve_global_quote_asset(pair_info, quote_asset)
        return {"quote_asset": quote_asset, "global_quote_asset": global_quote}

    @staticmethod
    def _resolve_local_quote_asset(pair_info, local_symbol: str) -> str:
        if pair_info and pair_info.quote_asset:
            return pair_info.quote_asset.upper()
        if not local_symbol:
            return ""
        normalized = local_symbol.upper().split(":")[0].replace("/", "_").replace("-", "_")
        return normalized.split("_")[-1] if "_" in normalized else "IDR"

    @staticmethod
    def _resolve_global_quote_asset(pair_info, quote_asset: str) -> str:
        global_symbol = pair_info.global_symbol.upper() if pair_info else ""
        if "USDC" in global_symbol:
            return "USDC"
        if not global_symbol and quote_asset == "USDC":
            return "USDC"
        return "USDT"

    async def _invoke_balance_getter(self, getter, kwargs: dict):
        try:
            signature = inspect.signature(getter)
        except (TypeError, ValueError):
            signature = None
        accepts_kwargs = signature is None or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        accepted = kwargs if accepts_kwargs else {
            key: value for key, value in kwargs.items() if key in signature.parameters
        }
        result = getter(**accepted)
        if inspect.isawaitable(result):
            result = await result
        return result

    @staticmethod
    def _validate_balance_snapshot(result: dict) -> dict:
        if not isinstance(result, dict) or not {"spot_available", "futures_available"} <= result.keys():
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
        local_name = pair_info.local_exchange if pair_info else self.config.local_exchange_name
        global_name = pair_info.global_exchange if pair_info else self.config.global_exchange_name
        balances = await self._read_balance_snapshot_or_halt(
            pair_info, usdt_idr_rate, pair_tag, local_name, global_name
        )
        if balances is None:
            if pair_tag:
                self._balance_snapshots.pop(pair_tag, None)
            return None
        if pair_tag:
            self._balance_snapshots[pair_tag] = balances
        await self._check_balance_thresholds(balances, local_name, global_name, pair_tag)
        return balances

    async def _check_balance_thresholds(
        self, balances: dict, local_name: str, global_name: str, pair_tag: Optional[str]
    ) -> None:
        fraction = float(getattr(self.config, "balance_mismatch_fraction", 0.0) or 0.0)
        if fraction <= 0:
            return
        available = {
            local_name: float(balances.get("spot_available", 0.0) or 0.0),
            global_name: float(balances.get("futures_available", 0.0) or 0.0),
        }
        committed = self._committed_entry_exposure(local_name, global_name)
        for venue, amount in available.items():
            await self._check_venue_balance_drop(
                venue, amount, committed.get(venue, 0.0), fraction, pair_tag
            )

    async def _read_balance_snapshot_or_halt(self, pair_info, fx_rate, pair_tag, local_name, global_name):
        try:
            return await self._get_balance_snapshot(
                pair_tag=pair_tag, pair_info=pair_info, usdt_idr_rate=fx_rate
            )
        except Exception as exc:
            reason = f"balance feed unavailable: {exc}"
            self._exchange_halted[local_name] = reason
            self._exchange_halted[global_name] = reason
            logger.exception("[Risk Control] balance read failed; both venues halted for new entries")
            await self._publish_execution_failure(
                pair_tag or "unknown", "BALANCE_FEED_UNAVAILABLE",
                "Gagal membaca saldo live; entry dihentikan pada kedua exchange.",
                detail={"local_exchange": local_name, "global_exchange": global_name, "error": str(exc)},
            )
            return None

    def _committed_entry_exposure(self, local_name: str, global_name: str) -> dict:
        pending_local, pending_global = self._pending_entry_exposure_all()
        committed = self._open_exposure_by_venue()
        committed[local_name] = committed.get(local_name, 0.0) + pending_local
        committed[global_name] = committed.get(global_name, 0.0) + pending_global
        return committed

    async def _check_venue_balance_drop(self, venue: str, available: float, committed: float, fraction: float, pair_tag: Optional[str]) -> None:
        last = self._last_balances.get(venue)
        self._last_balances[venue] = available
        if last is None or last <= 0:
            return
        drop = last - available
        if drop <= 0 or drop <= last * fraction:
            return
        if drop <= committed * 1.01:
            return
        await self._halt_balance_mismatch(venue, last, available, drop, committed, pair_tag)

    async def _halt_balance_mismatch(self, venue: str, previous: float, available: float, drop: float, committed: float, pair_tag: Optional[str]) -> None:
        reason = (
            f"balance mismatch: available dropped {previous:.2f} -> {available:.2f} "
            f"(-{drop:.2f}) with only {committed:.2f} committed"
        )
        self._exchange_halted[venue] = reason
        logger.critical("[Risk Control] Exchange %s HALTED: %s", venue, reason)
        await self._publish_execution_failure(
            pair_tag or "unknown", "BALANCE_MISMATCH",
            f"Saldo {venue} berubah tidak sesuai exposure tercatat; entry dihentikan.",
            detail={"exchange": venue, "available_drop": drop, "committed_exposure": committed},
        )

