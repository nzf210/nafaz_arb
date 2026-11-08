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


class BalanceRiskMixin:
    """Exposure caps, per-venue halts and balance-drift detection."""

    async def _refresh_shared_entry_reservations(self) -> None:
        """Refresh Redis pending orders and active positions while venue locks are held."""
        if not self.redis:
            if not getattr(self.config, "mock_mode", False):
                raise RuntimeError("Redis is required for live exposure reconciliation")
            return
        try:
            pending_keys = await self.redis.keys(PENDING_ENTRY_KEY_PREFIX + "*")
            active_keys = await self.redis.keys("arb:active_pos:*")
        except Exception as exc:
            raise RuntimeError(f"cannot enumerate shared exposure reservations: {exc}") from exc
        for key in pending_keys or []:
            pair_tag = str(key).split(PENDING_ENTRY_KEY_PREFIX, 1)[-1]
            raw = await self.redis.get(key)
            if not raw:
                continue
            try:
                info = json.loads(raw)
                local_exchange = str(info.get("local_exchange") or "").lower()
                global_exchange = str(info.get("global_exchange") or "").lower()
                order_id = str(info.get("order_id") or "")
                symbol = str(info.get("symbol") or "")
                qty = float(info.get("qty") or 0.0)
                if not all((local_exchange, global_exchange, order_id, symbol)):
                    raise ValueError("pending order record is missing route/order identity")
                if not math.isfinite(qty) or qty <= 0:
                    raise ValueError("pending order record has invalid quantity")
                existing = self.pending_entry_orders.get(pair_tag)
                if existing is None:
                    self.pending_entry_orders[pair_tag] = info
                    self.states.setdefault(pair_tag, PositionState.RECONCILIATION_REQUIRED)
                elif (existing.get("order_id") != order_id or
                      str(existing.get("local_exchange", "")).lower() != local_exchange or
                      str(existing.get("global_exchange", "")).lower() != global_exchange):
                    raise ValueError("in-memory and Redis pending entry identities disagree")
            except Exception as exc:
                logger.critical(f"[Risk Control] invalid shared pending entry {key}: {exc}")
                raise RuntimeError(f"unsafe pending entry reservation {key}: {exc}") from exc
        for key in active_keys or []:
            pair_tag = str(key).split("arb:active_pos:", 1)[-1]
            raw = await self.redis.get(key)
            if not raw:
                continue
            try:
                restored = ActivePosition.from_dict(json.loads(raw))
                if restored.qty <= 0 or not math.isfinite(float(restored.qty)):
                    raise ValueError("active position has invalid quantity")
                current = self.active_positions.get(pair_tag)
                if current is None:
                    self.active_positions[pair_tag] = restored
                elif (current.local_exchange != restored.local_exchange or
                      current.global_exchange != restored.global_exchange or
                      not math.isclose(float(current.qty), float(restored.qty), rel_tol=1e-8, abs_tol=1e-12)):
                    raise ValueError("in-memory and Redis active position snapshots disagree")
            except Exception as exc:
                logger.critical(f"[Risk Control] invalid shared active position {key}: {exc}")
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
