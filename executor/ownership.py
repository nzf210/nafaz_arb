"""Scoped ownership leases for Executor V3.

Ownership is keyed by credential/account scope + venue + market type + native
instrument. PostgreSQL time and monotonically increasing generations are
authoritative; Redis/local caches never grant mutation ownership.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Any


class OwnershipError(RuntimeError):
    """Base class for ownership lease failures."""


class OwnershipHeld(OwnershipError):
    """Another live coordinator currently owns the requested scope."""


class StaleOwnership(OwnershipError):
    """The worker's scope generation is missing, expired or superseded."""


@dataclass(frozen=True)
class ScopeLease:
    scope_key: str
    account_scope_id: str
    venue: str
    market_type: str
    instrument: str
    owner_id: str
    generation: int
    lease_expires_at: Any


def account_scope_id_for(config: Any, exchange: str) -> str:
    """Return a stable non-secret account identity derived from its API-key fingerprint.

    The raw key/secret is never returned or persisted. An explicit
    account_scope_id on the credentials object takes precedence.
    """
    venue = str(exchange or "").strip().lower()
    if not venue:
        raise ValueError("exchange is required for account scope identity")
    credentials = (getattr(config, "exchanges", {}) or {}).get(venue)
    explicit = str(getattr(credentials, "account_scope_id", "") or "").strip()
    if explicit:
        return explicit
    if not str(getattr(credentials, "api_key", "") or "").strip():
        fallback = None
        if venue == str(getattr(config, "local_exchange_name", "")).lower():
            fallback = getattr(config, "local_credentials", None)
        elif venue == str(getattr(config, "global_exchange_name", "")).lower():
            fallback = getattr(config, "global_credentials", None)
        if fallback is not None:
            credentials = fallback
    explicit = str(getattr(credentials, "account_scope_id", "") or "").strip()
    if explicit:
        return explicit
    api_key = str(getattr(credentials, "api_key", "") or "").strip()
    if not api_key:
        raise OwnershipError(
            f"cannot establish account scope for {venue}: credential identity is unavailable"
        )
    testnet = bool(getattr(credentials, "testnet", False))
    fingerprint = hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:32]
    return f"{venue}:{'testnet' if testnet else 'live'}:{fingerprint}"


def build_scope_key(
    account_scope_id: str, venue: str, market_type: str, instrument: str
) -> str:
    values = (
        str(account_scope_id or "").strip(),
        str(venue or "").strip().lower(),
        str(market_type or "").strip().lower(),
        str(instrument or "").strip().upper(),
    )
    if not all(values):
        raise ValueError("account_scope_id, venue, market_type and instrument are required")
    canonical = "\x1f".join(values)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def advisory_scope_lock_key(scope_key: str) -> int:
    """Return a process-independent signed bigint PostgreSQL advisory key."""
    digest = hashlib.sha256(("nafaz:scope:" + scope_key).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


class OwnershipLeaseService:
    """Lease acquisition and generation validation over an existing asyncpg connection."""

    def __init__(self, *, owner_id: str | None = None, lease_seconds: int = 90):
        self.owner_id = str(owner_id or uuid.uuid4().hex)
        self.lease_seconds = max(15, min(int(lease_seconds), 900))

    async def acquire(
        self, conn: Any, *, account_scope_id: str, venue: str,
        market_type: str, instrument: str,
    ) -> ScopeLease:
        account = str(account_scope_id or "").strip()
        exchange = str(venue or "").strip().lower()
        market = str(market_type or "").strip().lower()
        native_instrument = str(instrument or "").strip().upper()
        scope_key = build_scope_key(account, exchange, market, native_instrument)
        async with conn.transaction():
            await conn.execute(
                "select pg_advisory_xact_lock(hashtextextended('nafaz:ownership:' || $1, 0))",
                scope_key,
            )
            current = await conn.fetchrow(
                """
                SELECT owner_id, generation, lease_expires_at,
                       lease_expires_at > clock_timestamp() AS is_live
                FROM public.executor_ownership_leases
                WHERE scope_key=$1
                FOR UPDATE
                """,
                scope_key,
            )
            if current is None:
                generation = 1
                event_type = "ACQUIRED"
                row = await conn.fetchrow(
                    """
                    INSERT INTO public.executor_ownership_leases (
                        scope_key, account_scope_id, venue, market_type,
                        instrument, owner_id, generation, lease_expires_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,1,
                        clock_timestamp() + ($7 * interval '1 second')
                    )
                    RETURNING lease_expires_at
                    """,
                    scope_key, account, exchange, market, native_instrument,
                    self.owner_id, self.lease_seconds,
                )
            else:
                is_live = bool(current["is_live"])
                current_owner = str(current["owner_id"])
                if is_live and current_owner != self.owner_id:
                    raise OwnershipHeld(
                        f"scope {exchange}:{market}:{native_instrument} is owned by another live coordinator"
                    )
                generation = int(current["generation"])
                if not is_live:
                    generation += 1
                    event_type = "RECLAIMED"
                else:
                    event_type = "RENEWED"
                row = await conn.fetchrow(
                    """
                    UPDATE public.executor_ownership_leases
                    SET owner_id=$2, generation=$3,
                        lease_expires_at=clock_timestamp() + ($4 * interval '1 second'),
                        updated_at=clock_timestamp()
                    WHERE scope_key=$1
                    RETURNING lease_expires_at
                    """,
                    scope_key, self.owner_id, generation, self.lease_seconds,
                )
            event_key = uuid.uuid4().hex
            await conn.execute(
                """
                INSERT INTO public.executor_ownership_events (
                    scope_key, generation, owner_id, event_type,
                    event_idempotency_key, lease_expires_at, metadata
                ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
                """,
                scope_key, generation, self.owner_id, event_type, event_key,
                row["lease_expires_at"],
                '{"source":"mutation-boundary"}',
            )
        return ScopeLease(
            scope_key=scope_key,
            account_scope_id=account,
            venue=exchange,
            market_type=market,
            instrument=native_instrument,
            owner_id=self.owner_id,
            generation=generation,
            lease_expires_at=row["lease_expires_at"],
        )

    async def validate(self, conn: Any, lease: ScopeLease) -> None:
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM public.executor_ownership_leases
            WHERE scope_key=$1 AND account_scope_id=$2
              AND venue=$3 AND market_type=$4 AND instrument=$5
              AND owner_id=$6 AND generation=$7
              AND lease_expires_at > clock_timestamp()
            """,
            lease.scope_key, lease.account_scope_id, lease.venue,
            lease.market_type, lease.instrument, lease.owner_id,
            lease.generation,
        )
        if row is None:
            raise StaleOwnership(
                f"ownership generation {lease.generation} is expired or superseded for {lease.scope_key}"
            )

    async def release(self, conn: Any, lease: ScopeLease) -> bool:
        """Release a lease only when no unresolved venue mutation uses its generation."""
        async with conn.transaction():
            await conn.execute(
                "select pg_advisory_xact_lock(hashtextextended('nafaz:ownership:' || $1, 0))",
                lease.scope_key,
            )
            row = await conn.fetchrow(
                """
                UPDATE public.executor_ownership_leases AS lease
                SET lease_expires_at=clock_timestamp(), updated_at=clock_timestamp()
                WHERE lease.scope_key=$1 AND lease.owner_id=$2
                  AND lease.generation=$3
                  AND lease.lease_expires_at > clock_timestamp()
                  AND NOT EXISTS (
                      SELECT 1
                      FROM public.executor_submit_attempts AS attempt
                      WHERE attempt.owner_scope_key=lease.scope_key
                        AND attempt.owner_generation=lease.generation
                        AND attempt.status IN ('SUBMITTING','UNKNOWN')
                  )
                RETURNING lease.lease_expires_at
                """,
                lease.scope_key, lease.owner_id, lease.generation,
            )
            if row is None:
                return False
            await conn.execute(
                """
                INSERT INTO public.executor_ownership_events (
                    scope_key, generation, owner_id, event_type,
                    event_idempotency_key, lease_expires_at, metadata
                ) VALUES ($1,$2,$3,'RELEASED',$4,$5,'{"source":"explicit-release"}'::jsonb)
                """,
                lease.scope_key, lease.owner_id, lease.generation,
                uuid.uuid4().hex, row["lease_expires_at"],
            )
            return True

    async def release_all(self, conn: Any) -> int:
        """Release idle process-owned leases; unresolved venue mutations retain their fence."""
        async with conn.transaction():
            leases = await conn.fetch(
                """
                SELECT scope_key, generation
                FROM public.executor_ownership_leases
                WHERE owner_id=$1 AND lease_expires_at > clock_timestamp()
                ORDER BY scope_key
                """,
                self.owner_id,
            )
            released = 0
            for lease in leases:
                await conn.execute(
                    "select pg_advisory_xact_lock(hashtextextended('nafaz:ownership:' || $1, 0))",
                    lease["scope_key"],
                )
                row = await conn.fetchrow(
                    """
                    UPDATE public.executor_ownership_leases AS lease
                    SET lease_expires_at=clock_timestamp(), updated_at=clock_timestamp()
                    WHERE lease.scope_key=$1 AND lease.owner_id=$2
                      AND lease.generation=$3
                      AND lease.lease_expires_at > clock_timestamp()
                      AND NOT EXISTS (
                          SELECT 1
                          FROM public.executor_submit_attempts AS attempt
                          WHERE attempt.owner_scope_key=lease.scope_key
                            AND attempt.owner_generation=lease.generation
                            AND attempt.status IN ('SUBMITTING','UNKNOWN')
                      )
                    RETURNING lease.lease_expires_at
                    """,
                    lease["scope_key"], self.owner_id, lease["generation"],
                )
                if row is None:
                    continue
                await conn.execute(
                    """
                    INSERT INTO public.executor_ownership_events (
                        scope_key, generation, owner_id, event_type,
                        event_idempotency_key, lease_expires_at, metadata
                    ) VALUES ($1,$2,$3,'RELEASED',$4,$5,'{"source":"graceful-shutdown"}'::jsonb)
                    """,
                    lease["scope_key"], self.owner_id, lease["generation"],
                    uuid.uuid4().hex, row["lease_expires_at"],
                )
                released += 1
            return released
