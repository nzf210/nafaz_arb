"""Spot order slot reservation and limit manager (Executor V2).

Enforces per-CEX Spot pending/open order caps (default 2 per CEX).
Reservations are atomic, bound to session and client_order_id,
and held across SUBMISSION_PENDING, SUBMISSION_UNKNOWN, OPEN,
PARTIALLY_FILLED, and CANCEL_PENDING until reconciled terminal.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("executor.order_slots")

DEFAULT_MAX_OPEN_SLOTS = 2


class SlotExhaustedError(RuntimeError):
    """Raised when all available Spot order slots for a venue are occupied."""


class OrderSlotManager:
    """Manages atomic order slot reservations per venue/account."""

    def __init__(self, pool: Any = None):
        self.pool = pool
        # In-memory tracking for mock mode or fallback
        # Key: (venue, account, slot_index) -> dict with reservation info
        self._mem_slots: Dict[Tuple[str, str, int], dict] = {}
        self._mem_max_slots: Dict[Tuple[str, str], int] = {}
        self._lock = asyncio.Lock()

    async def get_max_slots(self, venue: str, account: str = "default") -> int:
        """Fetch configured max open slots for a venue/account."""
        venue = venue.lower().strip()
        account = account.strip()
        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    val = await conn.fetchval(
                        "SELECT max_open_slots FROM public.spot_order_slot_config "
                        "WHERE venue = $1 AND account = $2",
                        venue, account,
                    )
                    if val is not None:
                        return int(val)
            except Exception as exc:
                logger.warning("Failed to fetch slot config for %s:%s, using default %d: %s",
                               venue, account, DEFAULT_MAX_OPEN_SLOTS, exc)
        return self._mem_max_slots.get((venue, account), DEFAULT_MAX_OPEN_SLOTS)

    async def set_max_slots(self, venue: str, max_slots: int, account: str = "default") -> None:
        """Update max open slots for a venue/account."""
        venue = venue.lower().strip()
        account = account.strip()
        max_slots = max(1, int(max_slots))
        self._mem_max_slots[(venue, account)] = max_slots
        if self.pool is not None:
            now = datetime.now(timezone.utc)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO public.spot_order_slot_config (venue, account, max_open_slots, updated_at) "
                    "VALUES ($1, $2, $3, $4) "
                    "ON CONFLICT (venue, account) DO UPDATE SET max_open_slots = EXCLUDED.max_open_slots, updated_at = EXCLUDED.updated_at",
                    venue, account, max_slots, now,
                )
        logger.info("Updated spot order slot limit for %s:%s to %d", venue, account, max_slots)

    async def reserve_slot(
        self,
        venue: str,
        *,
        client_order_id: str,
        session_id: Optional[str] = None,
        account: str = "default",
        market_type: str = "spot",
    ) -> int:
        """Atomically reserve the next available slot index for the venue.

        Returns reserved slot_index (0-based) or raises SlotExhaustedError.
        """
        venue = venue.lower().strip()
        account = account.strip()
        market_type = market_type.lower().strip()

        # Non-spot orders do not consume spot slots
        if market_type != "spot":
            return -1

        max_slots = await self.get_max_slots(venue, account)
        reservation_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        if self.pool is not None:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Check active reservations under advisory lock for venue to prevent race
                    venue_hash = hash(f"{venue}:{account}") & 0x7FFFFFFF
                    await conn.execute("SELECT pg_advisory_xact_lock($1)", venue_hash)

                    rows = await conn.fetch(
                        "SELECT slot_index FROM public.order_slot_reservations "
                        "WHERE venue = $1 AND account = $2 AND market_type = $3 AND status IN ('RESERVED', 'ACTIVE')",
                        venue, account, market_type,
                    )
                    used_slots = {int(r["slot_index"]) for r in rows}

                    available_slot = None
                    for idx in range(max_slots):
                        if idx not in used_slots:
                            available_slot = idx
                            break

                    if available_slot is None:
                        raise SlotExhaustedError(
                            f"All {max_slots} spot slots occupied on {venue}:{account} (active: {used_slots})"
                        )

                    await conn.execute(
                        "INSERT INTO public.order_slot_reservations "
                        "(reservation_id, venue, account, market_type, slot_index, session_id, "
                        " client_order_id, status, created_at, updated_at) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, 'RESERVED', $8, $8)",
                        reservation_id, venue, account, market_type, available_slot,
                        session_id, client_order_id, now,
                    )
                    logger.info("Reserved spot slot %d on %s for client_order_id=%s",
                                available_slot, venue, client_order_id)
                    return available_slot

        # In-memory reservation
        async with self._lock:
            used_slots = {
                slot_idx for (v, acc, slot_idx), info in self._mem_slots.items()
                if v == venue and acc == account and info.get("status") in ("RESERVED", "ACTIVE")
            }
            available_slot = None
            for idx in range(max_slots):
                if idx not in used_slots:
                    available_slot = idx
                    break
            if available_slot is None:
                raise SlotExhaustedError(
                    f"All {max_slots} in-memory spot slots occupied on {venue}:{account}"
                )
            self._mem_slots[(venue, account, available_slot)] = {
                "reservation_id": reservation_id,
                "client_order_id": client_order_id,
                "session_id": session_id,
                "status": "RESERVED",
                "created_at": now,
            }
            return available_slot

    async def activate_slot(self, venue: str, client_order_id: str, account: str = "default") -> None:
        """Mark reserved slot as ACTIVE once exchange confirms placement."""
        venue = venue.lower().strip()
        account = account.strip()
        now = datetime.now(timezone.utc)

        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE public.order_slot_reservations "
                        "SET status = 'ACTIVE', updated_at = $1 "
                        "WHERE venue = $2 AND account = $3 AND client_order_id = $4 AND status = 'RESERVED'",
                        now, venue, account, client_order_id,
                    )
            except Exception as exc:
                logger.error("Failed to activate slot for %s %s: %s", venue, client_order_id, exc)

        async with self._lock:
            for (v, acc, slot_idx), info in self._mem_slots.items():
                if v == venue and acc == account and info.get("client_order_id") == client_order_id:
                    info["status"] = "ACTIVE"
                    break

    async def release_slot(
        self,
        venue: str,
        client_order_id: str,
        account: str = "default",
        reason: str = "TERMINAL",
    ) -> None:
        """Release slot atomically once order reaches terminal state and reconciliation is complete."""
        venue = venue.lower().strip()
        account = account.strip()
        now = datetime.now(timezone.utc)

        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    res = await conn.execute(
                        "UPDATE public.order_slot_reservations "
                        "SET status = 'RELEASED', updated_at = $1, released_at = $1 "
                        "WHERE venue = $2 AND account = $3 AND client_order_id = $4 AND status IN ('RESERVED', 'ACTIVE')",
                        now, venue, account, client_order_id,
                    )
                    logger.info("Released slot for %s client_order_id=%s (%s): %s",
                                venue, client_order_id, reason, res)
            except Exception as exc:
                logger.error("Failed to release slot for %s %s: %s", venue, client_order_id, exc)

        async with self._lock:
            for key, info in list(self._mem_slots.items()):
                if key[0] == venue and key[1] == account and info.get("client_order_id") == client_order_id:
                    info["status"] = "RELEASED"
                    info["released_at"] = now
                    break

    async def get_active_slots_count(self, venue: str, account: str = "default") -> int:
        """Return the count of currently occupied slots for a venue."""
        venue = venue.lower().strip()
        account = account.strip()
        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    val = await conn.fetchval(
                        "SELECT count(*) FROM public.order_slot_reservations "
                        "WHERE venue = $1 AND account = $2 AND market_type = 'spot' AND status IN ('RESERVED', 'ACTIVE')",
                        venue, account,
                    )
                    return int(val or 0)
            except Exception as exc:
                logger.error("Failed to query active slots for %s: %s", venue, exc)

        async with self._lock:
            return sum(
                1 for (v, acc, _), info in self._mem_slots.items()
                if v == venue and acc == account and info.get("status") in ("RESERVED", "ACTIVE")
            )
