"""Spot order slot reservation and limit manager (Executor V2).

Enforces per-CEX Spot pending/open order caps (default 2 per CEX).
Reservations are atomic, bound to session and client_order_id,
and held across SUBMISSION_PENDING, SUBMISSION_UNKNOWN, OPEN,
PARTIALLY_FILLED, and CANCEL_PENDING until reconciled terminal.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Set, Tuple

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
        """Fetch configured max open slots for a venue/account.
        Falls back to (venue, 'default'), then DEFAULT_MAX_OPEN_SLOTS.
        """
        venue = venue.lower().strip()
        account = account.lower().strip() if account else "default"
        if ":" in venue and account == "default":
            venue, account = venue.split(":", 1)
            venue = venue.lower().strip()
            account = account.lower().strip()

        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    # 1. Check specific (venue, account)
                    val = await conn.fetchval(
                        "SELECT max_open_slots FROM public.spot_order_slot_config "
                        "WHERE venue = $1 AND account = $2",
                        venue, account,
                    )
                    if val is not None:
                        return int(val)
                    # 2. Check fallback (venue, 'default') if account != 'default'
                    if account != "default":
                        val = await conn.fetchval(
                            "SELECT max_open_slots FROM public.spot_order_slot_config "
                            "WHERE venue = $1 AND account = 'default'",
                            venue,
                        )
                        if val is not None:
                            return int(val)
            except Exception as exc:
                logger.warning("Failed to fetch slot config for %s:%s, using default %d: %s",
                               venue, account, DEFAULT_MAX_OPEN_SLOTS, exc)
        if (venue, account) in self._mem_max_slots:
            return self._mem_max_slots[(venue, account)]
        if account != "default" and (venue, "default") in self._mem_max_slots:
            return self._mem_max_slots[(venue, "default")]
        return DEFAULT_MAX_OPEN_SLOTS

    async def set_max_slots(self, venue: str, max_slots: int, account: str = "default") -> None:
        """Update max open slots for a venue/account."""
        venue = venue.lower().strip()
        account = account.lower().strip() if account else "default"
        if ":" in venue and account == "default":
            venue, account = venue.split(":", 1)
            venue = venue.lower().strip()
            account = account.lower().strip()
        max_slots = max(0, int(max_slots))
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

    @staticmethod
    def _slot_lock_key(venue: str, account: str) -> int:
        """Return a process-independent signed bigint advisory-lock key."""
        payload = f"nafaz:spot-slot:{venue}:{account}".encode("utf-8")
        return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big", signed=True)

    async def reserve_slot(
        self,
        venue: str,
        *,
        client_order_id: str,
        session_id: Optional[str] = None,
        account: str = "default",
        market_type: str = "spot",
        max_slots_override: Optional[int] = None,
        venue_max_slots_override: Optional[int] = None,
    ) -> int:
        """Atomically reserve the next available slot index for the venue.

        Returns reserved slot_index (0-based) or raises SlotExhaustedError.
        """
        venue = venue.lower().strip()
        account = account.lower().strip() if account else "default"
        if ":" in venue and account == "default":
            venue, account = venue.split(":", 1)
            venue = venue.lower().strip()
            account = account.lower().strip()
        market_type = market_type.lower().strip()

        # Non-spot orders do not consume spot slots
        if market_type != "spot":
            return -1

        def _validated_override(name: str, value: Optional[int]) -> Optional[int]:
            if value is None:
                return None
            try:
                parsed = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be an integer") from exc
            if parsed < 0 or parsed > 100:
                raise ValueError(f"{name} must be between 0 and 100")
            return parsed

        account_override = _validated_override("max_slots_override", max_slots_override)
        venue_override = _validated_override("venue_max_slots_override", venue_max_slots_override)
        # Every quote-scoped order also owns a venue-wide reservation in the
        # default account. This enforces both limits across replicas/quotes.
        scopes = ["default"] if account == "default" else ["default", account]
        limits: Dict[str, int] = {}
        for scope_account in scopes:
            cap = await self.get_max_slots(venue, scope_account)
            override = venue_override if scope_account == "default" else account_override
            if account == "default" and scope_account == "default":
                values = [v for v in (account_override, venue_override) if v is not None]
                if values:
                    override = min(values)
            if override is not None:
                cap = min(cap, override)
            if cap <= 0:
                raise SlotExhaustedError(
                    f"Spot order placement is disabled on {venue}:{scope_account} (max_slots={cap})"
                )
            limits[scope_account] = cap

        now = datetime.now(timezone.utc)
        reservations: Dict[str, int] = {}

        if self.pool is not None:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Lock scopes in a deterministic order to avoid deadlocks.
                    # The key is stable across Python processes and replicas.
                    for scope_account in sorted(scopes):
                        lock_key = self._slot_lock_key(venue, scope_account)
                        await conn.execute(
                            "SELECT pg_advisory_xact_lock($1::bigint)", lock_key
                        )

                    for scope_account in scopes:
                        if scope_account == "default":
                            active_count = await conn.fetchval(
                                "SELECT count(DISTINCT client_order_id) "
                                "FROM public.order_slot_reservations "
                                "WHERE venue = $1 AND market_type = $2 "
                                "AND status IN ('RESERVED', 'ACTIVE')",
                                venue, market_type,
                            )
                            rows = await conn.fetch(
                                "SELECT slot_index FROM public.order_slot_reservations "
                                "WHERE venue = $1 AND account = 'default' AND market_type = $2 "
                                "AND status IN ('RESERVED', 'ACTIVE')",
                                venue, market_type,
                            )
                        else:
                            active_count = await conn.fetchval(
                                "SELECT count(DISTINCT client_order_id) "
                                "FROM public.order_slot_reservations "
                                "WHERE venue = $1 AND account = $2 AND market_type = $3 "
                                "AND status IN ('RESERVED', 'ACTIVE')",
                                venue, scope_account, market_type,
                            )
                            rows = await conn.fetch(
                                "SELECT slot_index FROM public.order_slot_reservations "
                                "WHERE venue = $1 AND account = $2 AND market_type = $3 "
                                "AND status IN ('RESERVED', 'ACTIVE')",
                                venue, scope_account, market_type,
                            )
                        used_slots = {int(row["slot_index"]) for row in rows}
                        if int(active_count or 0) >= limits[scope_account]:
                            raise SlotExhaustedError(
                                f"All {limits[scope_account]} spot slots occupied on "
                                f"{venue}:{scope_account} (active_count={active_count})"
                            )
                        available_slot = next(
                            (idx for idx in range(limits[scope_account]) if idx not in used_slots),
                            None,
                        )
                        if available_slot is None:
                            raise SlotExhaustedError(
                                f"No free slot index remains on {venue}:{scope_account} "
                                f"(active: {used_slots})"
                            )
                        reservations[scope_account] = available_slot

                    for scope_account in scopes:
                        slot_index = reservations[scope_account]
                        await conn.execute(
                            "INSERT INTO public.order_slot_reservations "
                            "(reservation_id, venue, account, market_type, slot_index, session_id, "
                            " client_order_id, status, created_at, updated_at) "
                            "VALUES ($1, $2, $3, $4, $5, $6, $7, 'RESERVED', $8, $8)",
                            str(uuid.uuid4()), venue, scope_account, market_type, slot_index,
                            session_id, client_order_id, now,
                        )
                        logger.info(
                            "Reserved spot slot %d on %s:%s for client_order_id=%s",
                            slot_index, venue, scope_account, client_order_id,
                        )
            return reservations[account]

        # In-memory mode mirrors both venue-wide and quote-scoped reservations.
        async with self._lock:
            for scope_account in scopes:
                active_ids = {
                    info.get("client_order_id")
                    for (v, acc, _slot_idx), info in self._mem_slots.items()
                    if v == venue
                    and (scope_account == "default" or acc == scope_account)
                    and info.get("status") in ("RESERVED", "ACTIVE")
                }
                used_slots = {
                    slot_idx for (v, acc, slot_idx), info in self._mem_slots.items()
                    if v == venue and acc == scope_account
                    and info.get("status") in ("RESERVED", "ACTIVE")
                }
                if len(active_ids) >= limits[scope_account]:
                    raise SlotExhaustedError(
                        f"All {limits[scope_account]} in-memory spot slots occupied on "
                        f"{venue}:{scope_account} (active_count={len(active_ids)})"
                    )
                available_slot = next(
                    (idx for idx in range(limits[scope_account]) if idx not in used_slots),
                    None,
                )
                if available_slot is None:
                    raise SlotExhaustedError(
                        f"No free in-memory slot index remains on {venue}:{scope_account}"
                    )
                reservations[scope_account] = available_slot

            for scope_account in scopes:
                slot_index = reservations[scope_account]
                self._mem_slots[(venue, scope_account, slot_index)] = {
                    "reservation_id": str(uuid.uuid4()),
                    "client_order_id": client_order_id,
                    "session_id": session_id,
                    "status": "RESERVED",
                    "created_at": now,
                }
        return reservations[account]

    async def activate_slot(self, venue: str, client_order_id: str, account: Optional[str] = None) -> None:
        """Activate every reservation (venue-wide and quote-scoped) for an order."""
        venue = venue.lower().strip()
        now = datetime.now(timezone.utc)
        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE public.order_slot_reservations "
                        "SET status = 'ACTIVE', updated_at = $1 "
                        "WHERE venue = $2 AND client_order_id = $3 AND status = 'RESERVED'",
                        now, venue, client_order_id,
                    )
            except Exception as exc:
                logger.error("Failed to activate slot for %s %s: %s", venue, client_order_id, exc)

        async with self._lock:
            for (v, _account, _slot_idx), info in self._mem_slots.items():
                if v == venue and info.get("client_order_id") == client_order_id:
                    info["status"] = "ACTIVE"

    async def release_slot(
        self,
        venue: str,
        client_order_id: str,
        account: Optional[str] = None,
        reason: str = "TERMINAL",
    ) -> None:
        """Release every reservation for an order after terminal reconciliation."""
        venue = venue.lower().strip()
        now = datetime.now(timezone.utc)
        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    res = await conn.execute(
                        "UPDATE public.order_slot_reservations "
                        "SET status = 'RELEASED', updated_at = $1, released_at = $1 "
                        "WHERE venue = $2 AND client_order_id = $3 "
                        "AND status IN ('RESERVED', 'ACTIVE')",
                        now, venue, client_order_id,
                    )
                    logger.info(
                        "Released slot for %s client_order_id=%s (%s): %s",
                        venue, client_order_id, reason, res,
                    )
            except Exception as exc:
                logger.error("Failed to release slot for %s %s: %s", venue, client_order_id, exc)

        async with self._lock:
            for key, info in list(self._mem_slots.items()):
                if key[0] == venue and info.get("client_order_id") == client_order_id:
                    info["status"] = "RELEASED"
                    info["released_at"] = now

    async def get_active_slots_count(self, venue: str, account: Optional[str] = None) -> int:
        """Return the count of currently occupied slots for a venue/account."""
        venue = venue.lower().strip()
        account = account.lower().strip() if account else None
        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    if account:
                        val = await conn.fetchval(
                            "SELECT count(DISTINCT client_order_id) FROM public.order_slot_reservations "
                            "WHERE venue = $1 AND account = $2 AND market_type = 'spot' AND status IN ('RESERVED', 'ACTIVE')",
                            venue, account,
                        )
                    else:
                        val = await conn.fetchval(
                            "SELECT count(DISTINCT client_order_id) FROM public.order_slot_reservations "
                            "WHERE venue = $1 AND market_type = 'spot' AND status IN ('RESERVED', 'ACTIVE')",
                            venue,
                        )
                    return int(val or 0)
            except Exception as exc:
                logger.error("Failed to query active slots for %s: %s", venue, exc)

        async with self._lock:
            return len({
                info.get("client_order_id")
                for (v, acc, _), info in self._mem_slots.items()
                if v == venue
                and (account is None or acc == account)
                and info.get("status") in ("RESERVED", "ACTIVE")
            })

    async def reconcile_active_slots(
        self,
        active_client_order_ids: Set[str],
        max_reserved_age_seconds: float = 60.0,
    ) -> int:
        """Release reservations whose client_order_id is not active or timed out.

        Releases:
        1. RESERVED slots older than max_reserved_age_seconds (never submitted/activated).
        2. ACTIVE or RESERVED slots older than max_reserved_age_seconds whose client_order_id
           is not in active_client_order_ids.

        Returns total count of released slots.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=float(max_reserved_age_seconds))
        released_count = 0
        active_ids = set(active_client_order_ids or set())

        if self.pool is not None:
            try:
                async with self.pool.acquire() as conn:
                    # 1. Release expired RESERVED slots (timed out before placement)
                    res1 = await conn.execute(
                        "UPDATE public.order_slot_reservations "
                        "SET status = 'RELEASED', updated_at = $1, released_at = $1 "
                        "WHERE status = 'RESERVED' AND created_at < $2",
                        now, cutoff,
                    )
                    # 2. Release ACTIVE or RESERVED slots not in active_ids (older than cutoff)
                    active_ids_list = list(active_ids)
                    if active_ids_list:
                        res2 = await conn.execute(
                            "UPDATE public.order_slot_reservations "
                            "SET status = 'RELEASED', updated_at = $1, released_at = $1 "
                            "WHERE status IN ('RESERVED', 'ACTIVE') "
                            "AND created_at < $2 "
                            "AND client_order_id != ALL($3::text[])",
                            now, cutoff, active_ids_list,
                        )
                    else:
                        res2 = await conn.execute(
                            "UPDATE public.order_slot_reservations "
                            "SET status = 'RELEASED', updated_at = $1, released_at = $1 "
                            "WHERE status IN ('RESERVED', 'ACTIVE') "
                            "AND created_at < $2",
                            now, cutoff,
                        )
                    for res in (res1, res2):
                        try:
                            released_count += int(res.split(" ")[-1])
                        except (ValueError, IndexError):
                            pass
                    if released_count > 0:
                        logger.info("Reconciled %d orphaned spot order slot reservations", released_count)
            except Exception as exc:
                logger.error("Failed to reconcile spot order slots: %s", exc)

        async with self._lock:
            for key, info in list(self._mem_slots.items()):
                if info.get("status") in ("RESERVED", "ACTIVE"):
                    coid = info.get("client_order_id")
                    created = info.get("created_at")
                    age = (now - created).total_seconds() if created else 999999.0
                    if age >= max_reserved_age_seconds and coid not in active_ids:
                        info["status"] = "RELEASED"
                        info["released_at"] = now
                        released_count += 1

        return released_count
