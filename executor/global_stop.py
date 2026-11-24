"""PostgreSQL-backed global STOP state and cross-instance order-submit fencing.

The transaction-scoped advisory lock is deliberately held across the venue
create-order request. This makes STOP and a new-order request linearizable:
whichever obtains the lock first completes its critical section first. The
exchange side effect is still not atomic with PostgreSQL; callers must use a
stable client order id and reconcile ambiguous acknowledgements.
"""
from __future__ import annotations

import asyncio
import json
import logging
import hashlib
import hmac
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from executor.helpers import parse_pair_tag_local_market, resolve_local_market_identity

logger = logging.getLogger("executor.global_stop")


def _json_mapping(value: Any) -> Optional[dict]:
    """Decode JSONB values consistently across asyncpg codecs and test doubles."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return None
    return value if isinstance(value, dict) else None


def _market_exposure_scope(exchange: Any, symbol: Any, pair_key: Any, recovery_context: Any) -> dict:
    """Resolve and cross-check the durable local spot market identity.

    Recovery metadata may enrich base/quote fields, but it must never redirect
    the reservation to another venue or contradict the actual local symbol or
    a structured pair key.
    """
    context = _json_mapping(recovery_context) or {}
    explicit = _json_mapping(context.get("local_market_scope")) or {}
    durable_exchange = str(exchange or "").strip().lower()
    local_exchange = str(explicit.get("local_exchange") or durable_exchange).strip().lower()
    raw_symbol = str(symbol or "").strip()
    pair = str(pair_key or "").strip()
    if not durable_exchange or local_exchange != durable_exchange:
        raise GlobalStopError(
            "spot BUY local market exchange does not match the durable order intent"
        )
    if not raw_symbol or not pair:
        raise GlobalStopError(
            "spot BUY intent lacks a symbol or pair key for durable market exposure identity"
        )

    explicit_base = str(
        explicit.get("base_asset") or context.get("base_asset") or ""
    ).strip()
    explicit_quote = str(
        explicit.get("quote_asset") or context.get("quote_asset") or ""
    ).strip()
    identity = resolve_local_market_identity(raw_symbol, explicit_base, explicit_quote)
    if identity is None:
        raise GlobalStopError(
            "spot BUY symbol and explicit base/quote metadata are incomplete or contradictory"
        )
    base_asset, normalized_quote = identity

    tag_exchange, tag_base, tag_quote = parse_pair_tag_local_market(pair)
    if tag_exchange and (
        tag_exchange != durable_exchange
        or tag_base != base_asset
        or tag_quote != normalized_quote
    ):
        raise GlobalStopError(
            "spot BUY pair key does not match the durable exchange and local symbol"
        )
    return {
        "local_exchange": durable_exchange,
        "base_asset": str(base_asset).upper(),
        "quote_asset": str(normalized_quote).upper(),
        "pair_key": pair,
    }

# Stable, application-wide PostgreSQL advisory-lock keys. Do not change
# without coordinating every executor instance using this schema.
FENCE_LOCK_KEY = 731_904_221
# Risk-reducing operations serialize with one another, but never wait behind
# an entry/STOP critical section. A close must not be stranded by a slow entry.
DERISK_LOCK_KEY = 731_904_222
STOP_CHANNEL = "arb:global_stop"


class GlobalStopError(RuntimeError):
    """Raised when global admission cannot be proven safe."""


class GlobalStopService:
    def __init__(self, dsn: str, redis: Any = None, *, min_size: int = 1, max_size: int = 5):
        self.dsn = str(dsn or "").strip()
        self.redis = redis
        self.min_size = min_size
        self.max_size = max_size
        self.pool = None
        # Set only when the driver is missing or the DSN is unusable: retrying
        # such a configuration on every order would add latency for nothing.
        self._pool_unusable = False

    @property
    def enabled(self) -> bool:
        return self.pool is not None

    @staticmethod
    def _is_connection_error(exc: BaseException) -> bool:
        """True for errors that mean the pool itself is dead, not the statement."""
        name = type(exc).__name__
        return name in (
            "ConnectionDoesNotExistError", "ConnectionFailureError",
            "InterfaceError", "TooManyConnectionsError", "PostgresConnectionError",
            "OSError", "ConnectionResetError", "TimeoutError",
        )

    async def ensure_pool(self) -> bool:
        """Return a usable pool, transparently reconnecting after a drop.

        PostgreSQL is the canonical fencing authority, so an instance that has
        lost its connection must prove the authority is reachable again rather
        than silently keep trading. A failed reconnect leaves the pool empty and
        the caller fails closed.
        """
        if self.pool is not None:
            return True
        if self._pool_unusable or not self.dsn:
            return False
        try:
            import asyncpg
        except ImportError:
            self._pool_unusable = True
            logger.critical("asyncpg missing; PostgreSQL STOP fencing is unavailable")
            return False
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn, min_size=self.min_size, max_size=self.max_size,
                command_timeout=10,
            )
            logger.warning("PostgreSQL STOP/fencing pool re-established")
            return True
        except Exception:
            logger.exception("PostgreSQL STOP/fencing pool reconnect failed")
            self.pool = None
            return False

    def _demote_broken_pool(self, exc: BaseException) -> None:
        if not self._is_connection_error(exc):
            return
        pool = self.pool
        self.pool = None
        if pool is not None:
            try:
                asyncio.create_task(pool.close())
            except Exception:
                pass
        logger.critical(
            "PostgreSQL STOP/fencing connection lost (%s); the next order will re-prove "
            "the authority before submitting", type(exc).__name__,
        )

    async def start(self) -> None:
        if self.pool is not None:
            return
        if not self.dsn:
            raise GlobalStopError("POSTGRES_DSN is required for PostgreSQL global STOP fencing")
        try:
            import asyncpg
        except ImportError as exc:
            raise GlobalStopError("asyncpg is required for PostgreSQL fencing") from exc
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn, min_size=self.min_size, max_size=self.max_size,
            command_timeout=10,
        )
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "select is_stopped, epoch, reason from public.executor_global_control where singleton_id = 1"
                )
                if row is None:
                    raise GlobalStopError("global control row is missing; apply PostgreSQL fencing migration")
        except Exception:
            await self.close()
            raise
        logger.info("PostgreSQL global STOP/fencing service connected (epoch=%s stopped=%s)", row["epoch"], row["is_stopped"])

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    def _require_pool(self):
        if self.pool is None:
            raise GlobalStopError("PostgreSQL global STOP service is not initialized")

    async def read_state(self) -> dict:
        self._require_pool()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select is_stopped, epoch, reason, source, metadata, updated_at "
                "from public.executor_global_control where singleton_id = 1"
            )
        if row is None:
            raise GlobalStopError("global control row is missing")
        return dict(row)

    async def prepare_submit_attempt(
        self, *, client_order_id: str, exchange: str, market_type: str,
        symbol: str, side: str, order_type: str, quantity: float,
        price: float, pair_key: str, recovery_context: Optional[dict] = None,
        amount_spec: Optional[dict] = None,
    ) -> dict:
        """Commit a durable PREPARED intent before any venue HTTP request."""
        self._require_pool()
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required")
        values = (
            client_order_id, str(exchange or "").lower()[:100],
            str(market_type or "").lower()[:30], str(symbol or "")[:160],
            str(side or "").lower()[:16], str(order_type or "").lower()[:24],
            float(quantity), float(price), str(pair_key or "")[:200],
            json.dumps(recovery_context or {}, separators=(",", ":"), default=str),
            json.dumps(amount_spec or {}, separators=(",", ":"), sort_keys=True, default=str),
        )
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "insert into public.executor_submit_attempts "
                "(client_order_id, exchange, market_type, symbol, side, order_type, "
                "quantity, price, pair_key, recovery_context, amount_spec, status) "
                "values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,'PREPARED') "
                "on conflict (client_order_id) do nothing "
                "returning client_order_id, status, created_at",
                *values,
            )
            if row is None:
                existing = await conn.fetchrow(
                    "select client_order_id, status, created_at, exchange, market_type, symbol, "
                    "side, order_type, quantity, price, pair_key, amount_spec, recovery_context "
                    "from public.executor_submit_attempts "
                    "where client_order_id = $1",
                    client_order_id,
                )
                if existing is None or existing["status"] != "PREPARED":
                    raise GlobalStopError(
                        f"client order id already has a non-reusable submit attempt: {client_order_id}"
                    )
                expected = {
                    "exchange": values[1], "market_type": values[2], "symbol": values[3],
                    "side": values[4], "order_type": values[5], "quantity": values[6],
                    "price": values[7], "pair_key": values[8],
                }
                stored_amount_spec = _json_mapping(existing.get("amount_spec"))
                stored_recovery_context = _json_mapping(
                    existing.get("recovery_context")
                )
                expected_amount_spec = json.loads(values[10])
                expected_recovery_context = json.loads(values[9])
                if (
                    any(existing[key] != value for key, value in expected.items())
                    or stored_amount_spec != expected_amount_spec
                    or stored_recovery_context != expected_recovery_context
                ):
                    raise GlobalStopError(
                        f"client order id collision with a different order intent: {client_order_id}"
                    )
                row = existing
        return dict(row)


    async def _reserve_market_exposure_in_transaction(self, conn: Any, *, client_order_id: str, scope: dict) -> None:
        """Claim one local spot market atomically with PREPARED -> SUBMITTING."""
        existing_pair = await conn.fetchrow(
            "select reservation_key, local_exchange, base_asset, quote_asset "
            "from public.executor_market_exposure_reservations "
            "where pair_key=$1 and status in ('ACTIVE','RECONCILIATION_REQUIRED') "
            "order by created_at limit 1 for update",
            scope["pair_key"],
        )
        if existing_pair is not None and (
            str(existing_pair["local_exchange"]).lower() != scope["local_exchange"]
            or str(existing_pair["base_asset"]).upper() != scope["base_asset"]
            or str(existing_pair["quote_asset"]).upper() != scope["quote_asset"]
        ):
            raise GlobalStopError(
                "pair key already owns a different active local market exposure reservation"
            )
        existing = await conn.fetchrow(
            "select reservation_key, pair_key, source_client_order_id "
            "from public.executor_market_exposure_reservations "
            "where lower(local_exchange)=$1 and upper(base_asset)=$2 "
            "and upper(quote_asset)=$3 and status in ('ACTIVE','RECONCILIATION_REQUIRED') "
            "order by created_at limit 1 for update",
            scope["local_exchange"], scope["base_asset"], scope["quote_asset"],
        )
        if existing is not None:
            raise GlobalStopError(
                "local market already has an active exposure reservation: "
                f"{scope['local_exchange']}:{scope['base_asset']}/{scope['quote_asset']} "
                f"owned by pair_key={existing['pair_key']}"
            )
        await conn.execute(
            "insert into public.executor_market_exposure_reservations "
            "(reservation_key, local_exchange, base_asset, quote_asset, pair_key, "
            "source_client_order_id, status, source) "
            "values ($1,$2,$3,$4,$5,$6,'ACTIVE','SPOT_ENTRY_SUBMIT')",
            client_order_id, scope["local_exchange"], scope["base_asset"],
            scope["quote_asset"], scope["pair_key"], client_order_id,
        )

    async def backfill_market_exposure_reservations(self, records: Optional[list] = None) -> int:
        """Backfill active markets from PostgreSQL and Redis before entry is unlocked."""
        self._require_pool()
        pending = [dict(row) for row in (records or []) if isinstance(row, dict)]
        inserted = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                positions = await conn.fetch(
                    "select pair_key, local_exchange, local_symbol as symbol, base_asset, quote_asset, "
                    "id::text as position_ref, status from public.positions where is_mock=false "
                    "and status in ('ENTRY_PENDING','ENTRY_PARTIAL','HEDGED_ACTIVE','EXIT_PENDING','RECONCILIATION_REQUIRED')"
                )
                pending.extend(dict(row) for row in positions)
                unresolved = await conn.fetch(
                    "select client_order_id, exchange, market_type, symbol, side, pair_key, recovery_context, status "
                    "from public.executor_submit_attempts where status in ('SUBMITTING','UNKNOWN') "
                    "and lower(market_type) in ('spot','local') and lower(side)='buy'"
                )
                for attempt in unresolved:
                    row = dict(attempt)
                    row["local_exchange"] = row.get("exchange")
                    row["source_client_order_id"] = row.get("client_order_id")
                    row["source"] = "UNRESOLVED_SUBMIT"
                    pending.append(row)
                for record in pending:
                    context = record.get("recovery_context")
                    scope = _market_exposure_scope(
                        record.get("local_exchange") or record.get("exchange"),
                        record.get("symbol") or record.get("local_symbol"),
                        record.get("pair_key"),
                        context if context is not None else {
                            "local_market_scope": {
                                "local_exchange": record.get("local_exchange") or record.get("exchange"),
                                "base_asset": record.get("base_asset"),
                                "quote_asset": record.get("quote_asset"),
                            }
                        },
                    )
                    by_pair = await conn.fetchrow(
                        "select reservation_key, local_exchange, base_asset, quote_asset "
                        "from public.executor_market_exposure_reservations where pair_key=$1 "
                        "and status in ('ACTIVE','RECONCILIATION_REQUIRED') order by created_at limit 1 for update",
                        scope["pair_key"],
                    )
                    if by_pair is not None and (
                        str(by_pair["local_exchange"]).lower() != scope["local_exchange"]
                        or str(by_pair["base_asset"]).upper() != scope["base_asset"]
                        or str(by_pair["quote_asset"]).upper() != scope["quote_asset"]
                    ):
                        raise GlobalStopError("active pair_key has conflicting durable market identity: "+scope["pair_key"])
                    existing = await conn.fetchrow(
                        "select reservation_key, pair_key from public.executor_market_exposure_reservations "
                        "where lower(local_exchange)=$1 and upper(base_asset)=$2 and upper(quote_asset)=$3 "
                        "and status in ('ACTIVE','RECONCILIATION_REQUIRED') order by created_at limit 1 for update",
                        scope["local_exchange"], scope["base_asset"], scope["quote_asset"],
                    )
                    if existing is not None:
                        if str(existing["pair_key"]) != scope["pair_key"]:
                            raise GlobalStopError(
                                "two active pair keys claim the same local market: "
                                f"{scope['local_exchange']}:{scope['base_asset']}/{scope['quote_asset']}"
                            )
                        continue
                    source_coid = str(record.get("source_client_order_id") or record.get("client_order_id") or "").strip() or None
                    position_ref = str(record.get("position_ref") or record.get("opened_at") or "").strip()
                    reservation_key = source_coid or (
                        "legacy:" + hashlib.sha256("|".join((
                            scope["local_exchange"], scope["base_asset"], scope["quote_asset"],
                            scope["pair_key"], position_ref,
                        )).encode("utf-8")).hexdigest()
                    )
                    reservation_status = (
                        "RECONCILIATION_REQUIRED"
                        if str(record.get("status") or "").upper() == "RECONCILIATION_REQUIRED"
                        or record.get("source") == "UNRESOLVED_SUBMIT" else "ACTIVE"
                    )
                    await conn.execute(
                        "insert into public.executor_market_exposure_reservations "
                        "(reservation_key, local_exchange, base_asset, quote_asset, pair_key, source_client_order_id, status, source) "
                        "values ($1,$2,$3,$4,$5,$6,$7,'STARTUP_BACKFILL')",
                        reservation_key, scope["local_exchange"], scope["base_asset"], scope["quote_asset"],
                        scope["pair_key"], source_coid, reservation_status,
                    )
                    inserted += 1
                open_orders = await conn.fetch(
                    "select local_order_id::text as local_order_id, pair_key, exchange_name, symbol, "
                    "client_order_id, recovery_status from public.order_registry "
                    "where is_mock=false and lower(market_type)='spot' and lower(side)='buy' and reduce_only=false "
                    "and lower(status) not in ('closed','filled','canceled','cancelled','rejected','expired')"
                )
                for order in open_orders:
                    pair_key = str(order.get("pair_key") or "").strip()
                    if not pair_key:
                        raise GlobalStopError("open live spot BUY order has no pair_key; market claim cannot be proven")
                    scope = _market_exposure_scope(
                        order.get("exchange_name"), order.get("symbol"), pair_key, {}
                    )
                    order_client_id = str(order.get("client_order_id") or "").strip() or None
                    reservation = await conn.fetchrow(
                        "select reservation_key, local_exchange, base_asset, quote_asset, "
                        "source_client_order_id, status from public.executor_market_exposure_reservations "
                        "where pair_key=$1 and status in ('ACTIVE','RECONCILIATION_REQUIRED') "
                        "order by created_at limit 1 for update",
                        pair_key,
                    )
                    if reservation is not None:
                        if (
                            str(reservation["local_exchange"]).lower() != scope["local_exchange"]
                            or str(reservation["base_asset"]).upper() != scope["base_asset"]
                            or str(reservation["quote_asset"]).upper() != scope["quote_asset"]
                        ):
                            raise GlobalStopError(
                                "open live spot BUY registry order conflicts with its pair-key reservation"
                            )
                        reserved_client_id = str(reservation.get("source_client_order_id") or "").strip()
                        if reserved_client_id and order_client_id and reserved_client_id != order_client_id:
                            raise GlobalStopError(
                                "multiple open live spot BUY intents share one pair-key reservation"
                            )
                        continue

                    market_reservation = await conn.fetchrow(
                        "select reservation_key, pair_key from public.executor_market_exposure_reservations "
                        "where lower(local_exchange)=$1 and upper(base_asset)=$2 and upper(quote_asset)=$3 "
                        "and status in ('ACTIVE','RECONCILIATION_REQUIRED') order by created_at limit 1 for update",
                        scope["local_exchange"], scope["base_asset"], scope["quote_asset"],
                    )
                    if market_reservation is not None:
                        raise GlobalStopError(
                            "open live spot BUY registry order conflicts with another active market reservation"
                        )

                    if order_client_id:
                        prior_client_reservation = await conn.fetchrow(
                            "select reservation_key, local_exchange, base_asset, quote_asset, pair_key, status "
                            "from public.executor_market_exposure_reservations "
                            "where source_client_order_id=$1",
                            order_client_id,
                        )
                        if prior_client_reservation is not None:
                            raise GlobalStopError(
                                "open live spot BUY order has a previously released or mismatched market reservation"
                            )

                    recovery_status = str(order.get("recovery_status") or "").upper()
                    reservation_status = (
                        "RECONCILIATION_REQUIRED"
                        if recovery_status in {"UNVERIFIED", "ORPHAN_ORDER", "PHANTOM"}
                        else "ACTIVE"
                    )
                    local_order_id = str(order.get("local_order_id") or "").strip()
                    reservation_key = order_client_id or (
                        "registry:" + hashlib.sha256("|".join((
                            scope["local_exchange"], scope["base_asset"], scope["quote_asset"],
                            pair_key, local_order_id or str(order.get("symbol") or ""),
                        )).encode("utf-8")).hexdigest()
                    )
                    await conn.execute(
                        "insert into public.executor_market_exposure_reservations "
                        "(reservation_key, local_exchange, base_asset, quote_asset, pair_key, "
                        "source_client_order_id, status, source) "
                        "values ($1,$2,$3,$4,$5,$6,$7,'ORDER_REGISTRY_BACKFILL')",
                        reservation_key, scope["local_exchange"], scope["base_asset"],
                        scope["quote_asset"], pair_key, order_client_id, reservation_status,
                    )
                    inserted += 1
        return inserted

    async def release_market_exposure_reservation(self, pair_key: str, *, evidence: dict) -> int:
        """Release only after both spot and futures legs are venue-verified flat."""
        self._require_pool()
        source = str((evidence or {}).get("source") or "").strip()
        allowed_sources = {
            "VERIFIED_EXIT_LIFECYCLE",
            "VERIFIED_FORCE_CLOSE_LIFECYCLE",
            "VERIFIED_ZERO_FILL_ENTRY_TERMINAL",
        }
        if (
            source not in allowed_sources
            or (evidence or {}).get("spot_flat") is not True
            or (evidence or {}).get("futures_flat") is not True
            or (evidence or {}).get("venue_verified") is not True
        ):
            raise GlobalStopError(
                "market exposure reservation release requires an approved verified-flat lifecycle source"
            )
        payload = json.dumps(evidence, separators=(",", ":"), default=str)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                result = await conn.execute(
                    "update public.executor_market_exposure_reservations "
                    "set status='RELEASED', released_at=now(), release_evidence=$2::jsonb, updated_at=now() "
                    "where pair_key=$1 and status in ('ACTIVE','RECONCILIATION_REQUIRED')",
                    str(pair_key or "").strip(), payload,
                )
        try:
            return int(str(result).rsplit(" ", 1)[-1])
        except (TypeError, ValueError):
            return 0

    @asynccontextmanager
    async def derisk_submit_fence(self, *, client_order_id: str) -> AsyncIterator[Any]:
        """Serialize a risk-reducing submit without allowing STOP to block it.

        STOP never rejects exits/unwinds/rollbacks/force-closes. De-risk uses a
        separate session advisory key: it serializes other de-risk requests but
        does not wait behind a slow entry/STOP fence. An unresolved entry is
        advisory only for de-risking: leaving known exposure open is worse than
        allowing a bounded risk-reducing order to proceed.
        """
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required before submit")
        if not await self.ensure_pool():
            raise GlobalStopError("PostgreSQL STOP serialization is unavailable; de-risking may proceed unfenced")

        yielded_to_caller = False
        try:
            async with self.pool.acquire() as conn:
                locked = False
                try:
                    await conn.execute("select pg_advisory_lock($1)", DERISK_LOCK_KEY)
                    locked = True
                    async with conn.transaction():
                        control = await conn.fetchrow(
                            "select is_stopped, epoch, reason from public.executor_global_control "
                            "where singleton_id = 1 for update"
                        )
                        if control is None:
                            raise GlobalStopError("global control row is missing")
                        if control["is_stopped"]:
                            logger.warning(
                                "de-risking submit %s proceeds under global STOP (epoch=%s reason=%s)",
                                client_order_id, control["epoch"], control["reason"] or "unspecified",
                            )
                        unresolved = await conn.fetchrow(
                            "select client_order_id, exchange, symbol, status from public.executor_submit_attempts "
                            "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 "
                            "order by created_at limit 1",
                            client_order_id,
                        )
                        if unresolved is not None:
                            logger.warning(
                                "de-risking submit %s proceeds while an unresolved submit exists "
                                "(%s:%s client_order_id=%s status=%s)",
                                client_order_id, unresolved["exchange"], unresolved["symbol"],
                                unresolved["client_order_id"], unresolved["status"],
                            )
                    yielded_to_caller = True
                    yield conn
                finally:
                    if locked:
                        try:
                            await conn.execute("select pg_advisory_unlock($1)", DERISK_LOCK_KEY)
                        except Exception as unlock_error:
                            self._demote_broken_pool(unlock_error)
                            logger.warning(
                                "could not release PostgreSQL de-risking session lock; "
                                "connection loss must be treated as ambiguous"
                            )
        except GlobalStopError:
            raise
        except Exception as exc:
            # Do not demote the PostgreSQL pool for a venue TimeoutError or
            # ConnectionResetError raised by the caller inside the yielded scope.
            if not yielded_to_caller:
                self._demote_broken_pool(exc)
            raise

    @asynccontextmanager
    async def order_submit_fence(
        self, *, client_order_id: str, pre_submit_guard: Optional[Any] = None,
        scope_exchange: Optional[str] = None,
        scope_symbol: Optional[str] = None,
    ) -> AsyncIterator[Any]:
        """Hold a session advisory lock across a durable SUBMITTING venue request.

        The SUBMITTING row is committed before yielding and survives a lost
        PostgreSQL session. A venue transport error must not be mistaken for a
        PostgreSQL disconnect; the yielded scope contains the external request.
        An optional asynchronous ``pre_submit_guard`` runs after acquiring the
        shared session lock but before changing the durable attempt to
        SUBMITTING. A false result or guard error fails closed while leaving the
        attempt PREPARED, so the caller can safely mark it ABORTED.
        When ``scope_exchange`` and ``scope_symbol`` are provided, they
        must exactly match the durable PREPARED intent; the unresolved query then
        uses that stored identity rather than trusting caller-supplied scope values.
        This prevents a wrong scope argument from bypassing an unresolved order.
        """
        if not await self.ensure_pool():
            raise GlobalStopError("PostgreSQL STOP fencing is unavailable; create-order blocked")
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required before submit")

        yielded_to_caller = False
        try:
            async with self.pool.acquire() as conn:
                locked = False
                try:
                    # Fail fast instead of waiting until asyncpg's command
                    # timeout when another venue request owns the global fence.
                    lock_acquired = await conn.fetchval(
                        "select pg_try_advisory_lock($1)", FENCE_LOCK_KEY
                    )
                    if not lock_acquired:
                        raise GlobalStopError(
                            "another venue submission currently holds the global submit fence"
                        )
                    locked = True
                    if pre_submit_guard is not None:
                        try:
                            allowed = pre_submit_guard()
                            if hasattr(allowed, "__await__"):
                                allowed = await allowed
                        except Exception as guard_error:
                            raise GlobalStopError(
                                "pre-submit admission guard failed closed: "
                                f"{type(guard_error).__name__}: {guard_error}"
                            ) from guard_error
                        if not allowed:
                            raise GlobalStopError(
                                "pre-submit admission guard rejected an already active or pending pair"
                            )
                    async with conn.transaction():
                        control = await conn.fetchrow(
                            "select is_stopped, epoch, reason from public.executor_global_control "
                            "where singleton_id = 1 for update"
                        )
                        if control is None:
                            raise GlobalStopError("global control row is missing")
                        if control["is_stopped"]:
                            raise GlobalStopError(
                                f"global STOP active (epoch={control['epoch']} reason={control['reason'] or 'unspecified'})"
                            )
                        intent = await conn.fetchrow(
                            "select exchange, market_type, symbol, side, pair_key, recovery_context, status "
                            "from public.executor_submit_attempts where client_order_id = $1 for update",
                            client_order_id,
                        )
                        if intent is None or intent["status"] != "PREPARED":
                            raise GlobalStopError(f"submit requires a durable PREPARED intent: {client_order_id}")
                        durable_exchange = str(intent["exchange"] or "").strip().lower()
                        durable_symbol = str(intent["symbol"] or "").strip()
                        scope_requested = scope_exchange is not None or scope_symbol is not None
                        if scope_requested:
                            if not str(scope_exchange or "").strip() or not str(scope_symbol or "").strip():
                                raise GlobalStopError("both scope_exchange and scope_symbol are required for scoped submit fencing")
                            if (
                                str(scope_exchange).strip().lower() != durable_exchange
                                or str(scope_symbol).strip() != durable_symbol
                            ):
                                raise GlobalStopError("provided submit fence scope does not match the durable PREPARED intent")
                        if (
                            str(intent.get("market_type") or "").strip().lower() in ("spot", "local")
                            and str(intent.get("side") or "").strip().lower() == "buy"
                        ):
                            market_scope = _market_exposure_scope(
                                durable_exchange, durable_symbol, intent.get("pair_key"), intent.get("recovery_context")
                            )
                            await self._reserve_market_exposure_in_transaction(
                                conn, client_order_id=client_order_id, scope=market_scope
                            )
                        if scope_requested:
                            unresolved = await conn.fetchrow(
                                "select client_order_id, exchange, symbol, status from public.executor_submit_attempts "
                                "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 "
                                "and exchange = $2 and symbol = $3 order by created_at limit 1",
                                client_order_id, durable_exchange, durable_symbol,
                            )
                        else:
                            unresolved = await conn.fetchrow(
                                "select client_order_id, exchange, symbol, status from public.executor_submit_attempts "
                                "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 order by created_at limit 1",
                                client_order_id,
                            )
                        if unresolved is not None:
                            raise GlobalStopError(
                                "unresolved in-flight submit blocks new entries: "
                                f"{unresolved['exchange']}:{unresolved['symbol']} "
                                f"client_order_id={unresolved['client_order_id']} status={unresolved['status']}"
                            )
                        attempt = await conn.fetchrow(
                            "update public.executor_submit_attempts "
                            "set status='SUBMITTING', submit_started_at=now(), "
                            "submit_count=submit_count+1, fence_epoch=$2, updated_at=now() "
                            "where client_order_id=$1 and status='PREPARED' "
                            "returning client_order_id",
                            client_order_id, int(control["epoch"]),
                        )
                        if attempt is None:
                            raise GlobalStopError(
                                f"durable PREPARED submit attempt missing or already used: {client_order_id}"
                            )
                    # The SUBMITTING state is committed while the session lock is
                    # still held. A connection loss now releases the lock but not
                    # this durable row.
                    yielded_to_caller = True
                    yield conn
                finally:
                    if locked:
                        try:
                            await conn.execute("select pg_advisory_unlock($1)", FENCE_LOCK_KEY)
                        except Exception as unlock_error:
                            self._demote_broken_pool(unlock_error)
                            logger.warning(
                                "could not explicitly release PostgreSQL submit session lock; "
                                "connection loss must be treated as ambiguous"
                            )
        except GlobalStopError:
            raise
        except Exception as exc:
            # ConnectionResetError/TimeoutError from the venue belongs to the
            # yielded request, not to the PostgreSQL pool. A real DB disconnect
            # is detected by the failed session unlock above.
            if not yielded_to_caller:
                self._demote_broken_pool(exc)
            raise

    async def mark_submit_acknowledged(
        self, client_order_id: str, order: dict
    ) -> None:
        """Persist a positive venue acknowledgement; failure leaves SUBMITTING durable."""
        self._require_pool()
        order_id = str((order or {}).get("id") or "")
        if not order_id:
            raise GlobalStopError("cannot acknowledge submit without a venue order id")
        await self._update_submit_attempt(
            client_order_id, "ACKNOWLEDGED", exchange_order_id=order_id,
            venue_response=order, error=None,
        )

    async def mark_submit_rejected(self, client_order_id: str, error: str) -> None:
        """Resolve a definitive rejection and release only its unfilled market claim."""
        await self._update_submit_attempt(
            client_order_id, "REJECTED", exchange_order_id=None,
            venue_response=None, error=str(error)[:2000],
        )
        async with self.pool.acquire() as conn:
            await conn.execute(
                "update public.executor_market_exposure_reservations "
                "set status='RELEASED', released_at=now(), "
                "release_evidence=jsonb_build_object('source','DEFINITIVE_VENUE_REJECTION'), updated_at=now() "
                "where source_client_order_id=$1 and status in ('ACTIVE','RECONCILIATION_REQUIRED')",
                client_order_id,
            )

    async def mark_submit_aborted(self, client_order_id: str, reason: str) -> None:
        """Retire an intent proven not to have crossed the venue HTTP boundary."""
        await self._update_submit_attempt(
            client_order_id, "ABORTED", exchange_order_id=None,
            venue_response=None, error=str(reason)[:2000],
        )

    async def mark_submit_unknown(self, client_order_id: str, error: str) -> None:
        """Annotate an ambiguous attempt; if DB is down SUBMITTING remains blocking."""
        await self._update_submit_attempt(
            client_order_id, "UNKNOWN", exchange_order_id=None,
            venue_response=None, error=str(error)[:2000],
        )

    async def _update_submit_attempt(
        self, client_order_id: str, status: str, *, exchange_order_id: Optional[str],
        venue_response: Optional[dict], error: Optional[str],
    ) -> None:
        self._require_pool()
        if status not in {"ACKNOWLEDGED", "REJECTED", "UNKNOWN", "ABORTED"}:
            raise ValueError("invalid submit-attempt transition")
        # ABORTED is proof that the venue boundary was never crossed. It may
        # retire PREPARED only; it must never clear a SUBMITTING/UNKNOWN attempt.
        allowed_from = (
            "status = 'PREPARED'"
            if status == "ABORTED"
            else "status in ('PREPARED','SUBMITTING','UNKNOWN')"
        )
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "update public.executor_submit_attempts set status=$2, "
                "exchange_order_id=coalesce($3, exchange_order_id), "
                "venue_response=case when $4::jsonb is null then venue_response else $4::jsonb end, "
                "last_error=$5, resolved_at=case when $2 in ('ACKNOWLEDGED','REJECTED','ABORTED') then now() else null end, "
                f"updated_at=now() where client_order_id=$1 and {allowed_from}",
                client_order_id, status, exchange_order_id,
                json.dumps(venue_response, separators=(",", ":"), default=str) if venue_response is not None else None,
                error,
            )
            if result.endswith(" 0"):
                raise GlobalStopError(
                    f"submit attempt transition rejected for {client_order_id} (current state may have changed)"
                )

    async def list_unresolved_submit_attempts(self, *, limit: int = 100) -> list:
        """Return attempts whose venue outcome is not yet proven."""
        self._require_pool()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "select client_order_id, exchange, market_type, symbol, side, order_type, "
                "quantity, price, pair_key, amount_spec, recovery_context, status, "
                "submit_started_at, last_error from public.executor_submit_attempts "
                "where status in ('SUBMITTING','UNKNOWN') order by submit_started_at limit $1",
                max(1, min(int(limit), 1000)),
            )
        return [dict(row) for row in rows]

    async def reconcile_submit_attempt_found(
        self, client_order_id: str, order: dict
    ) -> None:
        """Adopt an order found by exact venue client-order-id lookup."""
        await self.mark_submit_acknowledged(client_order_id, order)

    async def reconcile_submit_attempt_not_found(
        self, client_order_id: str, *, evidence: str
    ) -> None:
        """Record a negative lookup but deliberately keep the attempt UNKNOWN.

        Absence from an eventually consistent lookup endpoint is not proof that
        a create-order request was rejected. Only an authoritative venue
        rejection or an operator-approved resolution may release the global gate.
        """
        self._require_pool()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "update public.executor_submit_attempts set last_lookup_at=now(), "
                "last_lookup_result='NOT_FOUND', last_error=$2, updated_at=now() "
                "where client_order_id=$1 and status in ('SUBMITTING','UNKNOWN')",
                client_order_id, str(evidence)[:2000],
            )


    async def resolve_submit_attempt_signed(
        self, envelope: dict, *, secret: str, max_age_seconds: int = 300
    ) -> dict:
        """Apply an operator-authenticated resolution with replay protection.

        The operator signs the canonical JSON payload with the out-of-band
        HMAC key. A resolution always requires two independent evidence
        references; a venue lookup returning NOT_FOUND is never sufficient.
        """
        self._require_pool()
        if not isinstance(envelope, dict) or not isinstance(envelope.get("payload"), dict):
            raise GlobalStopError("signed operator resolution payload is required")
        if not secret or len(secret.encode("utf-8")) < 32:
            raise GlobalStopError("OPERATOR_RESOLUTION_HMAC_SECRET must be at least 32 bytes")
        payload = envelope["payload"]
        signature = str(envelope.get("signature") or "").lower()
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        expected = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise GlobalStopError("operator resolution signature is invalid")
        try:
            issued_at = int(payload["issued_at"])
        except (KeyError, TypeError, ValueError) as exc:
            raise GlobalStopError("operator resolution issued_at is invalid") from exc
        if abs(int(time.time()) - issued_at) > max(1, min(int(max_age_seconds), 600)):
            raise GlobalStopError("operator resolution signature is expired or not yet valid")

        client_id = str(payload.get("client_order_id") or "").strip()
        operator = str(payload.get("operator_id") or "").strip()
        nonce = str(payload.get("nonce") or "").strip()
        decision = str(payload.get("decision") or "").strip().upper()
        evidence_a = str(payload.get("venue_evidence_ref") or "").strip()
        evidence_b = str(payload.get("independent_evidence_ref") or "").strip()
        order_id = str(payload.get("exchange_order_id") or "").strip()
        rationale = str(payload.get("rationale") or "").strip()
        if not all((client_id, operator, nonce, evidence_a, evidence_b, rationale)):
            raise GlobalStopError("operator, attempt, nonce, two evidence references and rationale are required")
        if evidence_a == evidence_b:
            raise GlobalStopError("venue and independent evidence must be distinct")
        if len(nonce) > 128 or len(operator) > 160 or len(evidence_a) > 1000 or len(evidence_b) > 1000:
            raise GlobalStopError("operator resolution fields exceed their limits")
        decisions = {
            "FOUND": ("ACKNOWLEDGED", order_id),
            "DEFINITIVE_REJECTION": ("REJECTED", ""),
            "PROVEN_NOT_SUBMITTED": ("ABORTED", ""),
        }
        if decision not in decisions:
            raise GlobalStopError("decision must be FOUND, DEFINITIVE_REJECTION, or PROVEN_NOT_SUBMITTED")
        target_status, _ = decisions[decision]
        if decision == "FOUND" and not order_id:
            raise GlobalStopError("FOUND resolution requires the exact venue order id")
        payload_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        venue_response = None
        if decision == "FOUND":
            venue_response = {
                "id": order_id, "manual_resolution": True,
                "venue_evidence_ref": evidence_a,
                "independent_evidence_ref": evidence_b,
            }

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                row = await conn.fetchrow(
                    "select status from public.executor_submit_attempts "
                    "where client_order_id=$1 for update", client_id,
                )
                if row is None or row["status"] not in ("SUBMITTING", "UNKNOWN"):
                    raise GlobalStopError("attempt is missing or no longer unresolved")
                await conn.execute(
                    "insert into public.executor_submit_resolution_audit "
                    "(client_order_id, operator_id, decision, venue_evidence_ref, "
                    "independent_evidence_ref, rationale, nonce, signed_payload_sha256) "
                    "values ($1,$2,$3,$4,$5,$6,$7,$8)",
                    client_id, operator, decision, evidence_a, evidence_b,
                    rationale[:2000], nonce, payload_hash,
                )
                result = await conn.execute(
                    "update public.executor_submit_attempts set status=$2, "
                    "exchange_order_id=case when $3='' then exchange_order_id else $3 end, "
                    "venue_response=case when $4::jsonb is null then venue_response else $4::jsonb end, "
                    "last_error=$5, resolved_at=now(), updated_at=now() "
                    "where client_order_id=$1 and status in ('SUBMITTING','UNKNOWN')",
                    client_id, target_status, order_id,
                    json.dumps(venue_response, separators=(",", ":")) if venue_response else None,
                    ("operator resolution: " + rationale)[:2000],
                )
                if result.endswith(" 0"):
                    raise GlobalStopError("attempt changed during operator resolution")
                if target_status in ("REJECTED", "ABORTED"):
                    await conn.execute(
                        "update public.executor_market_exposure_reservations "
                        "set status='RELEASED', released_at=now(), "
                        "release_evidence=jsonb_build_object('source','SIGNED_OPERATOR_RESOLUTION','decision',$2::text), "
                        "updated_at=now() where source_client_order_id=$1 "
                        "and status in ('ACTIVE','RECONCILIATION_REQUIRED')",
                        client_id, decision,
                    )
        return {
            "client_order_id": client_id, "status": target_status,
            "operator_id": operator, "decision": decision,
            "signed_payload_sha256": payload_hash,
        }

    async def request_stop(
        self, reason: str, *, source: str, metadata: Optional[dict] = None
    ) -> dict:
        """Persist STOP under the same lock used by all create-order submits."""
        self._require_pool()
        reason = str(reason or "").strip().upper()
        source = str(source or "").strip()[:100]
        if not reason or not source:
            raise ValueError("reason and source are required")
        payload = json.dumps(metadata or {}, separators=(",", ":"), default=str)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                row = await self.request_stop_in_transaction(
                    conn, reason, source=source, metadata=metadata
                )
                if row is None:
                    raise GlobalStopError("global control row is missing; STOP was not persisted")
        result = dict(row)
        # Redis is an acceleration/notification channel only. PostgreSQL is canonical.
        if self.redis is not None:
            try:
                await self.redis.publish(STOP_CHANNEL, json.dumps({
                    "is_stopped": True, "epoch": int(result["epoch"]),
                    "reason": reason, "source": source,
                }))
            except Exception:
                logger.exception("STOP committed to PostgreSQL but Redis notification failed")
        return result


    async def request_stop_in_transaction(
        self, conn: Any, reason: str, *, source: str, metadata: Optional[dict] = None
    ) -> dict:
        """Commit STOP while the caller holds the shared session advisory lock."""
        reason = str(reason or "").strip().upper()
        source = str(source or "").strip()[:100]
        payload = json.dumps(metadata or {}, separators=(",", ":"), default=str)
        async with conn.transaction():
            row = await conn.fetchrow(
                "update public.executor_global_control "
                "set is_stopped = true, epoch = epoch + case when is_stopped then 0 else 1 end, "
                "reason = case when is_stopped then reason else $1 end, "
                "source = case when is_stopped then source else $2 end, "
                "metadata = case when is_stopped then metadata else $3::jsonb end, updated_at = now() "
                "where singleton_id = 1 "
                "returning is_stopped, epoch, reason, source, metadata, updated_at",
                reason, source, payload,
            )
        if row is None:
            raise GlobalStopError("global control row is missing; STOP was not persisted")
        return dict(row)

    async def notify_state(self, state: dict) -> None:
        """Best-effort Redis notification after PostgreSQL transaction commits."""
        if self.redis is None:
            return
        try:
            await self.redis.publish(STOP_CHANNEL, json.dumps({
                "is_stopped": bool(state.get("is_stopped")),
                "epoch": int(state.get("epoch", 0)),
                "reason": state.get("reason"),
                "source": state.get("source"),
            }))
        except Exception:
            logger.exception("Global STOP committed to PostgreSQL but Redis notification failed")

    async def acknowledge_resume(self, *, operator: str, expected_epoch: int) -> dict:
        """Clear STOP only by explicit operator acknowledgement of current epoch."""
        self._require_pool()
        if not operator:
            raise ValueError("operator acknowledgement is required")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                row = await conn.fetchrow(
                    "update public.executor_global_control "
                    "set is_stopped = false, epoch = epoch + 1, reason = null, "
                    "source = $1, metadata = jsonb_build_object('resumed_from_epoch', $2), updated_at = now() "
                    "where singleton_id = 1 and is_stopped = true and epoch = $2 "
                    "returning is_stopped, epoch, reason, source, metadata, updated_at",
                    operator[:100], expected_epoch,
                )
                if row is None:
                    raise GlobalStopError("STOP state changed or is not active; refusing stale resume")
        result = dict(row)
        if self.redis is not None:
            try:
                await self.redis.publish(STOP_CHANNEL, json.dumps({
                    "is_stopped": False, "epoch": int(result["epoch"]),
                    "source": operator,
                }))
            except Exception:
                logger.exception("Resume committed to PostgreSQL but Redis notification failed")
        return result
