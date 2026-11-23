"""Submit admission and mutation fencing methods for GlobalStopService."""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from executor.global_stop_common import (
    FENCE_LOCK_KEY, GlobalStopError, logger, _json_mapping,
)
from executor.ownership import (
    OwnershipError, advisory_scope_lock_key, build_scope_key,
)
from executor.risk_control import RiskPermissionDenied, risk_scope_keys
from executor.execution_repository_fencing import (
    append_leg_event_in_transaction, transition_leg_in_transaction,
)


class GlobalStopSubmitMixin:
    async def prepare_submit_attempt(
        self, *, client_order_id: str, exchange: str, market_type: str,
        symbol: str, side: str, order_type: str, quantity: float,
        price: float, pair_key: str, recovery_context: Optional[dict] = None,
        amount_spec: Optional[dict] = None,
        account_scope_id: Optional[str] = None,
        mutation_intent_id: Optional[str] = None,
    ) -> dict:
        """Commit one PREPARED external attempt linked to its immutable logical intent."""
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
            str(account_scope_id or "").strip() or None,
            str(mutation_intent_id or "").strip() or None,
        )
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if values[12] is not None:
                    logical = await conn.fetchrow(
                        """
                        SELECT operation, status, account_scope_id, venue, market_type,
                               instrument, pair_key, client_order_id, side, order_type,
                               quantity, price
                        FROM public.executor_mutation_intents
                        WHERE intent_id=$1 FOR UPDATE
                        """,
                        values[12],
                    )
                    if logical is None or logical["operation"] != "CREATE" or logical["status"] != "PREPARED":
                        raise GlobalStopError("linked logical CREATE intent is missing or not PREPARED")
                    if any((
                        str(logical["account_scope_id"]) != str(values[11]),
                        str(logical["venue"]).lower() != values[1],
                        str(logical["market_type"]).lower() != values[2],
                        str(logical["instrument"]) != values[3],
                        str(logical["pair_key"]) != values[8],
                        str(logical["client_order_id"]) != values[0],
                        str(logical["side"]).lower() != values[4],
                        str(logical["order_type"]).lower() != values[5],
                        Decimal(str(logical["quantity"])) != Decimal(str(values[6])),
                        Decimal(str(logical["price"])) != Decimal(str(values[7])),
                    )):
                        raise GlobalStopError("external submit parameters differ from immutable logical intent")

                row = await conn.fetchrow(
                    "insert into public.executor_submit_attempts "
                    "(client_order_id, exchange, market_type, symbol, side, order_type, "
                    "quantity, price, pair_key, recovery_context, amount_spec, status, account_scope_id, mutation_intent_id) "
                    "values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,'PREPARED',$12,$13) "
                    "on conflict (client_order_id) do nothing "
                    "returning client_order_id, status, created_at, account_scope_id, mutation_intent_id",
                    *values,
                )
                if row is None:
                    existing = await conn.fetchrow(
                        "select client_order_id, status, created_at, exchange, market_type, symbol, "
                        "side, order_type, quantity, price, pair_key, amount_spec, recovery_context, "
                        "account_scope_id, mutation_intent_id from public.executor_submit_attempts "
                        "where client_order_id = $1 for update",
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
                        "account_scope_id": values[11],
                        "mutation_intent_id": values[12],
                    }
                    stored_amount_spec = _json_mapping(existing.get("amount_spec"))
                    stored_recovery_context = _json_mapping(existing.get("recovery_context"))
                    expected_amount_spec = json.loads(values[10])
                    expected_recovery_context = json.loads(values[9])
                    if (
                        any(existing.get(key) != value for key, value in expected.items())
                        or Decimal(str(existing["quantity"])) != Decimal(str(values[6]))
                        or Decimal(str(existing["price"])) != Decimal(str(values[7]))
                        or stored_amount_spec != expected_amount_spec
                        or stored_recovery_context != expected_recovery_context
                    ):
                        raise GlobalStopError(
                            f"client order id collision with a different order intent: {client_order_id}"
                        )
                    row = existing
        return dict(row)

    @asynccontextmanager
    async def mutation_reconciliation_fence(
        self, *, intent_id: str, intent_service: Any, actor_id: str,
    ) -> AsyncIterator[Any]:
        """Acquire exact-scope ownership for read-only reconciliation under STOP."""
        key = str(intent_id or "").strip()
        if not key or intent_service is None:
            raise GlobalStopError("durable mutation intent and repository are required")
        if not await self.ensure_pool():
            raise GlobalStopError("PostgreSQL mutation reconciliation is unavailable")

        global_locked = False
        scope_locked = False
        yielded_to_caller = False
        scope_key = ""
        lease = None
        try:
            async with self.pool.acquire() as conn:
                try:
                    global_locked = bool(await conn.fetchval(
                        "select pg_try_advisory_lock_shared($1)", FENCE_LOCK_KEY
                    ))
                    if not global_locked:
                        raise GlobalStopError("global STOP transition is in progress")
                    async with conn.transaction():
                        intent = await conn.fetchrow(
                            """
                            SELECT operation, status, account_scope_id, venue,
                                   market_type, instrument
                            FROM public.executor_mutation_intents
                            WHERE intent_id=$1 FOR UPDATE
                            """,
                            key,
                        )
                        if intent is None or str(intent["status"]).upper() not in {
                            "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED",
                        }:
                            raise GlobalStopError("reconciliation requires an unresolved mutation intent")
                        scope_key = build_scope_key(
                            str(intent["account_scope_id"]), str(intent["venue"]),
                            str(intent["market_type"]), str(intent["instrument"]),
                        )
                    lock_key = advisory_scope_lock_key(scope_key)
                    scope_locked = bool(await conn.fetchval(
                        "select pg_try_advisory_lock($1)", lock_key
                    ))
                    if not scope_locked:
                        raise GlobalStopError(
                            f"another mutation owns reconciliation scope {scope_key[:12]}"
                        )
                    async with conn.transaction():
                        intent = await conn.fetchrow(
                            """
                            SELECT operation, status, account_scope_id, venue,
                                   market_type, instrument
                            FROM public.executor_mutation_intents
                            WHERE intent_id=$1 FOR UPDATE
                            """,
                            key,
                        )
                        if intent is None or str(intent["status"]).upper() not in {
                            "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED",
                        }:
                            raise GlobalStopError("mutation intent changed before reconciliation")
                        lease = await self.ownership.acquire(
                            conn,
                            account_scope_id=str(intent["account_scope_id"]),
                            venue=str(intent["venue"]),
                            market_type=str(intent["market_type"]),
                            instrument=str(intent["instrument"]),
                        )
                        await self.ownership.validate(conn, lease)
                        reconciled_intent = await intent_service.reclaim_for_reconciliation_in_transaction(
                            conn, key, lease=lease, actor_id=actor_id,
                        )
                    yielded_to_caller = True
                    yield conn, lease, reconciled_intent
                finally:
                    if scope_locked:
                        try:
                            await conn.execute(
                                "select pg_advisory_unlock($1)", advisory_scope_lock_key(scope_key)
                            )
                        except Exception as exc:
                            self._demote_broken_pool(exc)
                            logger.warning("failed to release mutation reconciliation scope lock")
                    if global_locked:
                        try:
                            await conn.execute(
                                "select pg_advisory_unlock_shared($1)", FENCE_LOCK_KEY
                            )
                        except Exception as exc:
                            self._demote_broken_pool(exc)
                            logger.warning("failed to release mutation reconciliation global lock")
                    if lease is not None:
                        try:
                            async with conn.transaction():
                                await self.ownership.release(conn, lease)
                        except Exception:
                            logger.exception("failed to release reconciliation ownership lease")
        except GlobalStopError:
            raise
        except Exception as exc:
            if not yielded_to_caller:
                self._demote_broken_pool(exc)
            raise

    @asynccontextmanager
    async def cancel_submit_fence(
        self, *, intent_id: str, intent_service: Any, actor_id: str,
    ) -> AsyncIterator[Any]:
        """Serialize a protective CANCEL without allowing global STOP to block it."""
        key = str(intent_id or "").strip()
        if not key or intent_service is None:
            raise GlobalStopError("durable cancel intent and repository are required")
        if not await self.ensure_pool():
            raise GlobalStopError("PostgreSQL cancellation fencing is unavailable")

        global_locked = False
        scope_locked = False
        yielded_to_caller = False
        scope_key = ""
        try:
            async with self.pool.acquire() as conn:
                try:
                    global_locked = bool(await conn.fetchval(
                        "select pg_try_advisory_lock_shared($1)", FENCE_LOCK_KEY
                    ))
                    if not global_locked:
                        raise GlobalStopError("global STOP transition is in progress")
                    async with conn.transaction():
                        intent = await conn.fetchrow(
                            """
                            SELECT operation, status, account_scope_id, venue,
                                   market_type, instrument, pair_key
                            FROM public.executor_mutation_intents
                            WHERE intent_id=$1 FOR UPDATE
                            """,
                            key,
                        )
                        if intent is None or intent["operation"] != "CANCEL" or intent["status"] != "PREPARED":
                            raise GlobalStopError("cancel requires an exact PREPARED logical intent")
                        scope_key = build_scope_key(
                            str(intent["account_scope_id"]), str(intent["venue"]),
                            str(intent["market_type"]), str(intent["instrument"]),
                        )
                    lock_key = advisory_scope_lock_key(scope_key)
                    scope_locked = bool(await conn.fetchval(
                        "select pg_try_advisory_lock($1)", lock_key
                    ))
                    if not scope_locked:
                        raise GlobalStopError(
                            f"another mutation already owns cancel scope {scope_key[:12]}"
                        )
                    async with conn.transaction():
                        intent = await conn.fetchrow(
                            """
                            SELECT operation, status, account_scope_id, venue,
                                   market_type, instrument, pair_key
                            FROM public.executor_mutation_intents
                            WHERE intent_id=$1 FOR UPDATE
                            """,
                            key,
                        )
                        if intent is None or intent["operation"] != "CANCEL" or intent["status"] != "PREPARED":
                            raise GlobalStopError("cancel intent changed before mutation fence")
                        control = await conn.fetchrow(
                            "SELECT is_stopped, epoch, reason FROM public.executor_global_control WHERE singleton_id=1"
                        )
                        if control is None:
                            raise GlobalStopError("global control row is missing")
                        lease = await self.ownership.acquire(
                            conn,
                            account_scope_id=str(intent["account_scope_id"]),
                            venue=str(intent["venue"]),
                            market_type=str(intent["market_type"]),
                            instrument=str(intent["instrument"]),
                        )
                        await self.ownership.validate(conn, lease)
                        await intent_service.mark_submitting_in_transaction(
                            conn, key, lease=lease, actor_id=actor_id,
                            risk_snapshot={
                                "action_class": "CANCEL",
                                "allowed": True,
                                "global_stop_active": bool(control["is_stopped"]),
                                "global_control_epoch": int(control["epoch"]),
                            },
                        )
                        if control["is_stopped"]:
                            logger.warning(
                                "protective cancel %s admitted while global STOP is active (epoch=%s)",
                                key, control["epoch"],
                            )
                    yielded_to_caller = True
                    yield conn, lease
                finally:
                    if scope_locked:
                        try:
                            await conn.execute(
                                "select pg_advisory_unlock($1)", advisory_scope_lock_key(scope_key)
                            )
                        except Exception as exc:
                            self._demote_broken_pool(exc)
                            logger.warning("failed to release protective cancel scope lock")
                    if global_locked:
                        try:
                            await conn.execute(
                                "select pg_advisory_unlock_shared($1)", FENCE_LOCK_KEY
                            )
                        except Exception as exc:
                            self._demote_broken_pool(exc)
                            logger.warning("failed to release protective cancel global lock")
        except GlobalStopError:
            raise
        except Exception as exc:
            if not yielded_to_caller:
                self._demote_broken_pool(exc)
            raise

    @asynccontextmanager
    async def derisk_submit_fence(
        self, *, client_order_id: str, account_scope_id: str,
        venue: str, market_type: str, instrument: str,
    ) -> AsyncIterator[Any]:
        """Fence one exact de-risk scope without serializing unrelated instruments.

        Global STOP does not reject protective actions. The hierarchical durable
        DE_RISK_ALLOWED control and the account/venue/market/instrument lease do.
        """
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required before submit")
        venue_key = str(venue or "").strip().lower()
        market_key = str(market_type or "").strip().lower()
        native_instrument = str(instrument or "").strip().upper()
        try:
            scope_key = build_scope_key(
                account_scope_id, venue_key, market_key, native_instrument
            )
        except ValueError as exc:
            raise GlobalStopError(
                "complete account/venue/market/instrument scope is required for de-risk"
            ) from exc
        if not await self.ensure_pool():
            raise GlobalStopError(
                "PostgreSQL STOP serialization is unavailable; Redis emergency lease required"
            )

        lock_key = advisory_scope_lock_key(scope_key)
        yielded_to_caller = False
        try:
            async with self.pool.acquire() as conn:
                locked = False
                try:
                    locked = bool(
                        await conn.fetchval("select pg_try_advisory_lock($1)", lock_key)
                    )
                    if not locked:
                        raise GlobalStopError(
                            f"another de-risk mutation already owns scope {scope_key[:12]}"
                        )
                    async with conn.transaction():
                        control = await conn.fetchrow(
                            "select is_stopped, epoch, reason "
                            "from public.executor_global_control where singleton_id = 1"
                        )
                        if control is None:
                            raise GlobalStopError("global control row is missing")
                        if control["is_stopped"]:
                            logger.warning(
                                "de-risking submit %s proceeds under global STOP "
                                "(epoch=%s reason=%s)",
                                client_order_id, control["epoch"],
                                control["reason"] or "unspecified",
                            )
                        risk_snapshot = await self.risk_control_plane.assert_allowed(
                            conn,
                            scope_keys=risk_scope_keys(
                                account_scope_id=account_scope_id,
                                venue=venue_key,
                                market_type=market_key,
                                instrument=native_instrument,
                            ),
                            action_class="DERISK",
                        )
                        lease = await self.ownership.acquire(
                            conn,
                            account_scope_id=account_scope_id,
                            venue=venue_key,
                            market_type=market_key,
                            instrument=native_instrument,
                        )
                        await self.ownership.validate(conn, lease)
                        unresolved = await conn.fetchrow(
                            """
                            SELECT client_order_id, exchange, symbol, status
                            FROM public.executor_submit_attempts
                            WHERE account_scope_id=$1 AND exchange=$2
                              AND market_type=$3 AND symbol=$4
                              AND status IN ('SUBMITTING','UNKNOWN')
                              AND client_order_id <> $5
                            ORDER BY created_at LIMIT 1
                            """,
                            account_scope_id, venue_key, market_key, native_instrument,
                            client_order_id,
                        )
                        if unresolved is not None:
                            logger.warning(
                                "de-risking submit %s proceeds while scoped unresolved "
                                "submit exists (client_order_id=%s status=%s)",
                                client_order_id, unresolved["client_order_id"],
                                unresolved["status"],
                            )
                        logger.debug(
                            "de-risk scope %s admitted with ownership generation %s and risk snapshot %s",
                            scope_key[:12], lease.generation,
                            risk_snapshot.get("action_class"),
                        )
                    yielded_to_caller = True
                    yield conn
                finally:
                    if locked:
                        try:
                            await conn.execute(
                                "select pg_advisory_unlock($1)", lock_key
                            )
                        except Exception as unlock_error:
                            self._demote_broken_pool(unlock_error)
                            logger.warning(
                                "could not release scoped PostgreSQL de-risk lock for %s; "
                                "venue result requires reconciliation",
                                client_order_id,
                            )
        except GlobalStopError:
            raise
        except Exception as exc:
            # Do not demote the pool for venue errors raised after yielding.
            if not yielded_to_caller:
                self._demote_broken_pool(exc)
            raise

    async def _run_pre_submit_guard(self, guard: Optional[Any]) -> None:
        if guard is None:
            return
        try:
            allowed = guard()
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

    async def _load_durable_submit_intent(
        self, conn: Any, *, client_order_id: str, scope_requested: bool,
        scope_exchange: Optional[str], scope_symbol: Optional[str],
        scope_market_type: Optional[str],
        expected_mutation_intent_id: Optional[str] = None,
    ) -> Optional[Any]:
        if not scope_requested:
            return None
        if not str(scope_exchange or "").strip() or not str(scope_symbol or "").strip():
            raise GlobalStopError(
                "both scope_exchange and scope_symbol are required for scoped submit fencing"
            )
        intent = await conn.fetchrow(
            "select exchange, market_type, symbol, status, account_scope_id, pair_key, "
            "mutation_intent_id "
            "from public.executor_submit_attempts "
            "where client_order_id = $1 for update",
            client_order_id,
        )
        if intent is None or intent["status"] != "PREPARED":
            raise GlobalStopError(
                f"scoped submit requires a durable PREPARED intent: {client_order_id}"
            )
        stored_mutation_intent_id = str(intent.get("mutation_intent_id") or "").strip() or None
        expected_intent_id = str(expected_mutation_intent_id or "").strip() or None
        if stored_mutation_intent_id != expected_intent_id:
            raise GlobalStopError("submit attempt mutation intent linkage does not match")
        durable_exchange = str(intent["exchange"] or "").strip().lower()
        durable_market = str(intent["market_type"] or "").strip().lower()
        durable_symbol = str(intent["symbol"] or "").strip()
        if (
            str(scope_exchange).strip().lower() != durable_exchange
            or str(scope_symbol).strip() != durable_symbol
            or (
                scope_market_type
                and str(scope_market_type).strip().lower() != durable_market
            )
        ):
            raise GlobalStopError(
                "provided submit fence scope does not match the durable PREPARED intent"
            )
        return intent

    async def _acquire_submit_ownership(
        self, conn: Any, *, intent: Optional[Any],
        account_scope_id: Optional[str], action_class: str,
        scope_pair_key: Optional[str], scope_session_id: Optional[str],
        risk_metrics: Optional[dict], risk_limits: Optional[dict],
    ) -> tuple[Optional[Any], dict]:
        if not account_scope_id:
            if bool(getattr(self, "require_scoped_submits", False)):
                raise GlobalStopError("account_scope_id is required for every LIVE submit")
            return None, {}
        if intent is None or str(intent["account_scope_id"] or "") != str(account_scope_id):
            raise GlobalStopError(
                "account scope does not match the durable PREPARED intent"
            )
        venue = str(intent["exchange"] or "").strip().lower()
        market_type = str(intent["market_type"] or "").strip().lower()
        instrument = str(intent["symbol"] or "").strip()
        lease = await self.ownership.acquire(
            conn,
            account_scope_id=account_scope_id,
            venue=venue,
            market_type=market_type,
            instrument=instrument,
        )
        await self.ownership.validate(conn, lease)
        keys = risk_scope_keys(
            account_scope_id=account_scope_id,
            venue=venue,
            market_type=market_type,
            instrument=instrument,
            pair_key=str(scope_pair_key or intent["pair_key"] or ""),
            session_id=str(scope_session_id or ""),
        )
        try:
            snapshot = await self.risk_control_plane.assert_allowed(
                conn, scope_keys=keys, action_class=action_class,
                risk_metrics=risk_metrics, risk_limits=risk_limits,
            )
        except RiskPermissionDenied as risk_error:
            raise GlobalStopError(str(risk_error)) from risk_error
        return lease, snapshot

    async def _find_unresolved_submit(
        self, conn: Any, *, client_order_id: str,
        intent: Optional[Any], account_scope_id: Optional[str],
    ) -> Optional[Any]:
        if intent is None:
            return await conn.fetchrow(
                "select client_order_id, exchange, symbol, status "
                "from public.executor_submit_attempts "
                "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 "
                "order by created_at limit 1",
                client_order_id,
            )
        return await conn.fetchrow(
            "select client_order_id, exchange, symbol, status "
            "from public.executor_submit_attempts "
            "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 "
            "and exchange = $2 and market_type = $3 and symbol = $4 "
            "and ($5::text is null or account_scope_id = $5) "
            "order by created_at limit 1",
            client_order_id,
            str(intent["exchange"] or "").strip().lower(),
            str(intent["market_type"] or "").strip().lower(),
            str(intent["symbol"] or "").strip(),
            str(account_scope_id or "").strip() or None,
        )

    @staticmethod
    def _assert_global_stop_permission(
        control: Any, *, client_order_id: str, action_class: str
    ) -> None:
        if control["is_stopped"] and action_class == "ENTRY":
            raise GlobalStopError(
                f"global STOP active (epoch={control['epoch']} reason={control['reason'] or 'unspecified'})"
            )
        if control["is_stopped"] and action_class == "HEDGE":
            logger.warning(
                "protective hedge %s proceeds under global STOP (epoch=%s)",
                client_order_id, control["epoch"],
            )

    async def _mark_submit_attempt_submitting(
        self, conn: Any, *, client_order_id: str,
        global_epoch: int, lease: Optional[Any], risk_snapshot: dict,
    ) -> None:
        attempt = await conn.fetchrow(
            "update public.executor_submit_attempts "
            "set status='SUBMITTING', submit_started_at=now(), "
            "submit_count=submit_count+1, fence_epoch=$2, "
            "owner_scope_key=$3, owner_generation=$4, "
            "risk_control_snapshot=$5::jsonb, updated_at=now() "
            "where client_order_id=$1 and status='PREPARED' "
            "returning client_order_id",
            client_order_id, global_epoch,
            lease.scope_key if lease else None,
            lease.generation if lease else None,
            json.dumps(risk_snapshot or {}, sort_keys=True, separators=(",", ":"), default=str),
        )
        if attempt is None:
            raise GlobalStopError(
                f"durable PREPARED submit attempt missing or already used: {client_order_id}"
            )

    @asynccontextmanager
    async def order_submit_fence(
        self, *, client_order_id: str, pre_submit_guard: Optional[Any] = None,
        scope_exchange: Optional[str] = None,
        scope_symbol: Optional[str] = None,
        scope_market_type: Optional[str] = None,
        account_scope_id: Optional[str] = None,
        action_class: str = "ENTRY",
        scope_pair_key: Optional[str] = None,
        scope_session_id: Optional[str] = None,
        risk_metrics: Optional[dict] = None,
        risk_limits: Optional[dict] = None,
        mutation_intent_id: Optional[str] = None,
        mutation_intent_service: Optional[Any] = None,
        mutation_intent_actor_id: Optional[str] = None,
        mutation_intent_lease_callback: Optional[Any] = None,
    ) -> AsyncIterator[Any]:
        """Fence a venue mutation with shared global STOP and exclusive instrument ownership.

        Different account/venue/market/instrument scopes may submit concurrently.
        STOP/resume still take the exclusive global advisory key and therefore
        linearize against every new-entry admission. One native instrument has
        one mutation owner at a time. The durable SUBMITTING row is committed
        before the external request and records the observed ownership generation.
        """
        if not await self.ensure_pool():
            raise GlobalStopError("PostgreSQL STOP fencing is unavailable; create-order blocked")
        action = str(action_class or "ENTRY").strip().upper()
        if mutation_intent_id and not account_scope_id:
            raise GlobalStopError("V3 mutation intents require an account-scoped venue lease")
        if (
            not client_order_id or len(client_order_id) > 128
            or action not in {"ENTRY", "HEDGE", "DERISK"}
            or (
                bool(getattr(self, "require_scoped_submits", False))
                and (
                    not account_scope_id
                    or not str(scope_exchange or "").strip()
                    or not str(scope_symbol or "").strip()
                    or not str(scope_market_type or "").strip()
                )
            )
            or (action in {"HEDGE", "DERISK"} and not account_scope_id)
        ):
            raise GlobalStopError(
                "valid client order id and scoped ENTRY/HEDGE action (including DERISK) are required"
            )

        yielded_to_caller = False
        shared_locked = False
        scope_locked = False
        scope_lock_key = None
        try:
            async with self.pool.acquire() as conn:
                try:
                    shared_locked = bool(await conn.fetchval(
                        "select pg_try_advisory_lock_shared($1)", FENCE_LOCK_KEY
                    ))
                    if not shared_locked:
                        raise GlobalStopError(
                            "global STOP transition currently owns the submit fence"
                        )

                    if account_scope_id:
                        if not all((
                            str(scope_exchange or "").strip(),
                            str(scope_symbol or "").strip(),
                            str(scope_market_type or "").strip(),
                        )):
                            raise GlobalStopError(
                                "scoped ownership requires venue, market_type and native instrument"
                            )
                        scope_key = build_scope_key(
                            account_scope_id, scope_exchange, scope_market_type, scope_symbol
                        )
                        scope_lock_key = advisory_scope_lock_key(scope_key)
                        scope_locked = bool(await conn.fetchval(
                            "select pg_try_advisory_lock($1)", scope_lock_key
                        ))
                        if not scope_locked:
                            raise GlobalStopError(
                                "another mutation currently owns this account/venue/market/instrument scope"
                            )

                    await self._run_pre_submit_guard(pre_submit_guard)
                    async with conn.transaction():
                        control = await conn.fetchrow(
                            "select is_stopped, epoch, reason from public.executor_global_control "
                            "where singleton_id = 1"
                        )
                        if control is None:
                            raise GlobalStopError("global control row is missing")
                        scope_requested = bool(
                            scope_exchange is not None or scope_symbol is not None
                            or account_scope_id is not None
                        )
                        intent = await self._load_durable_submit_intent(
                            conn,
                            client_order_id=client_order_id,
                            scope_requested=scope_requested,
                            scope_exchange=scope_exchange,
                            scope_symbol=scope_symbol,
                            scope_market_type=scope_market_type,
                            expected_mutation_intent_id=mutation_intent_id,
                        )
                        # Session-bound V3 ENTRY intents may cross the venue
                        # boundary only for the prepared Spot BUY intent after
                        # the session coordinator has advanced the
                        # aggregate. This check shares the submit transaction and
                        # locks the session, leg and coordinator lease rows.
                        if action == "ENTRY" and mutation_intent_id:
                            logical_session_intent = await conn.fetchrow(
                                """
                                SELECT session_id, leg_id, account_scope_id, venue,
                                       market_type, instrument, pair_key, side
                                FROM public.executor_mutation_intents
                                WHERE intent_id=$1 FOR UPDATE
                                """,
                                mutation_intent_id,
                            )
                            session_id = str(
                                logical_session_intent.get("session_id") or ""
                            ).strip() if logical_session_intent is not None else ""
                            leg_id = str(
                                logical_session_intent.get("leg_id") or ""
                            ).strip() if logical_session_intent is not None else ""
                            if logical_session_intent is None:
                                raise GlobalStopError("linked V3 logical mutation intent is missing")
                            if bool(session_id) != bool(leg_id):
                                raise GlobalStopError(
                                    "session-bound ENTRY intent requires both session_id and leg_id"
                                )
                            if session_id and leg_id:
                                if (
                                    str(logical_session_intent["side"]).lower() != "buy"
                                ):
                                    raise GlobalStopError(
                                        "session-bound ENTRY is Spot-first; Futures orders must be obligation-backed HEDGE intents"
                                    )
                                session_gate = await conn.fetchrow(
                                    """
                                    SELECT s.status AS session_status,
                                           s.fencing_generation,
                                           l.status AS leg_status,
                                           l.leg_type AS leg_type
                                    FROM public.execution_sessions s
                                    JOIN public.execution_legs l
                                      ON l.session_id=s.session_id AND l.leg_id=$2
                                    JOIN public.executor_ownership_leases cl
                                      ON cl.scope_key=s.coordinator_scope_key
                                     AND cl.account_scope_id=s.account_scope_id
                                     AND cl.venue='coordinator'
                                     AND cl.market_type='session'
                                     AND cl.instrument=s.session_id
                                     AND cl.owner_id=s.coordinator_owner_id
                                     AND cl.generation=s.fencing_generation
                                     AND cl.lease_expires_at > clock_timestamp()
                                    WHERE s.session_id=$1
                                      AND s.account_scope_id=$3
                                      AND l.venue=$4
                                      AND l.market_type=$5
                                      AND l.symbol=$6
                                      AND s.pair_key=$7
                                    FOR UPDATE OF s, l, cl
                                    """,
                                    session_id, leg_id, str(intent["account_scope_id"]),
                                    str(intent["exchange"]).lower(),
                                    str(intent["market_type"]).lower(),
                                    str(intent["symbol"]),
                                    str(logical_session_intent["pair_key"]),
                                )
                                if session_gate is None or (
                                    str(session_gate["session_status"]).upper() != "ENTRY_IN_PROGRESS"
                                    or str(session_gate["leg_status"]).upper() != "PREPARED"
                                    or str(session_gate["leg_type"]).upper() != "SPOT"
                                ):
                                    raise GlobalStopError(
                                        "session-bound ENTRY intent is not admitted for submission: "
                                        "Spot-first ENTRY requires session ENTRY_IN_PROGRESS, Spot leg PREPARED, and active coordinator lease"
                                    )
                                # Commit the aggregate leg's SUBMITTING state in
                                # the same transaction as the external attempt and
                                # logical intent. Fill ingestion can now safely
                                # accept an immediate private-stream trade.
                                await transition_leg_in_transaction(
                                    conn,
                                    session_id=session_id,
                                    leg_id=leg_id,
                                    expected_state="PREPARED",
                                    target_state="SUBMITTING",
                                    fencing_generation=int(session_gate.get("fencing_generation") or 0),
                                    event_type="LEG_SUBMISSION_STARTED",
                                    idempotency_key=f"mutation-submit:{mutation_intent_id}",
                                    actor_id=str(mutation_intent_actor_id or "executor"),
                                    correlation_id=client_order_id,
                                    payload={
                                        "mutation_intent_id": mutation_intent_id,
                                        "venue": str(logical_session_intent["venue"]),
                                        "market_type": str(logical_session_intent["market_type"]),
                                        "instrument": str(logical_session_intent["instrument"]),
                                    },
                                )
                        if action == "HEDGE" and mutation_intent_id:
                            hedge_intent = await conn.fetchrow(
                                """
                                SELECT session_id, leg_id, account_scope_id, venue,
                                       market_type, instrument, pair_key, side,
                                       quantity, client_order_id, request_payload
                                FROM public.executor_mutation_intents
                                WHERE intent_id=$1 FOR UPDATE
                                """,
                                mutation_intent_id,
                            )
                            if hedge_intent is None:
                                raise GlobalStopError("linked V3 HEDGE mutation intent is missing")
                            session_id = str(hedge_intent["session_id"] or "").strip()
                            leg_id = str(hedge_intent["leg_id"] or "").strip()
                            if not session_id or not leg_id or str(hedge_intent["side"]).lower() != "sell":
                                raise GlobalStopError("HEDGE intent must bind a session Futures SELL leg")
                            request_payload = hedge_intent["request_payload"]
                            if isinstance(request_payload, str):
                                request_payload = json.loads(request_payload)
                            request_metadata = (
                                request_payload.get("request_metadata")
                                if isinstance(request_payload, dict) else None
                            )
                            request_metadata = request_metadata if isinstance(request_metadata, dict) else {}
                            reservation_id = str(request_metadata.get("hedge_reservation_id") or "").strip()
                            if not reservation_id:
                                raise GlobalStopError("HEDGE intent must reference a durable hedge reservation")
                            hedge_gate = await conn.fetchrow(
                                """
                                SELECT s.status AS session_status,
                                       s.fencing_generation,
                                       l.status AS leg_status,
                                       l.leg_type,
                                       l.position_side AS leg_position_side,
                                       l.base_asset AS leg_base_asset,
                                       l.base_per_unit AS leg_base_per_unit,
                                       l.quantity_unit AS leg_quantity_unit,
                                       r.status AS reservation_status,
                                       r.client_order_id AS reservation_client_order_id,
                                       r.reserved_base_quantity,
                                       r.futures_venue,
                                       r.futures_market_type,
                                       r.futures_symbol,
                                       r.base_asset AS reservation_base_asset,
                                       r.position_side AS reservation_position_side,
                                       r.quantity_unit AS reservation_quantity_unit,
                                       r.base_per_unit AS reservation_base_per_unit
                                FROM public.execution_sessions s
                                JOIN public.execution_legs l
                                  ON l.session_id=s.session_id AND l.leg_id=$2
                                JOIN public.executor_ownership_leases cl
                                  ON cl.scope_key=s.coordinator_scope_key
                                 AND cl.account_scope_id=s.account_scope_id
                                 AND cl.venue='coordinator'
                                 AND cl.market_type='session'
                                 AND cl.instrument=s.session_id
                                 AND cl.owner_id=s.coordinator_owner_id
                                 AND cl.generation=s.fencing_generation
                                 AND cl.lease_expires_at > clock_timestamp()
                                JOIN public.execution_hedge_reservations r
                                  ON r.reservation_id=$8::uuid AND r.session_id=s.session_id
                                WHERE s.session_id=$1
                                  AND s.account_scope_id=$3
                                  AND l.leg_type='FUTURES'
                                  AND l.venue=$4
                                  AND l.market_type=$5
                                  AND l.symbol=$6
                                  AND s.pair_key=$7
                                FOR UPDATE OF s, l, cl, r
                                """,
                                session_id, leg_id, str(hedge_intent["account_scope_id"]),
                                str(hedge_intent["venue"]).lower(),
                                str(hedge_intent["market_type"]).lower(),
                                str(hedge_intent["instrument"]),
                                str(hedge_intent["pair_key"]),
                                reservation_id,
                            )
                            if hedge_gate is None:
                                raise GlobalStopError(
                                    "HEDGE intent lacks an active session coordinator lease, exact Futures leg, or reservation"
                                )
                            if (
                                str(hedge_gate["session_status"]).upper() not in {"HEDGE_PENDING", "PARTIALLY_HEDGED"}
                                or str(hedge_gate["leg_position_side"] or "").upper() != "SHORT"
                                or str(hedge_gate["reservation_status"]).upper() != "SUBMITTED"
                                or str(hedge_gate["reservation_client_order_id"] or "") != client_order_id
                                or str(hedge_gate["futures_venue"]).lower() != str(hedge_intent["venue"]).lower()
                                or str(hedge_gate["futures_market_type"]).lower() != str(hedge_intent["market_type"]).lower()
                                or str(hedge_gate["futures_symbol"]) != str(hedge_intent["instrument"])
                                or str(hedge_gate["reservation_base_asset"]).upper() != str(hedge_gate["leg_base_asset"]).upper()
                                or str(hedge_gate["reservation_position_side"]).upper() != "SHORT"
                                or str(hedge_gate["reservation_quantity_unit"]).upper() != str(hedge_gate["leg_quantity_unit"]).upper()
                                or Decimal(str(hedge_gate["reservation_base_per_unit"])) != Decimal(str(hedge_gate["leg_base_per_unit"]))
                                or Decimal(str(hedge_intent["quantity"])) * Decimal(str(hedge_gate["leg_base_per_unit"]))
                                   != Decimal(str(hedge_gate["reserved_base_quantity"]))
                            ):
                                raise GlobalStopError("HEDGE intent and reservation route/quantity do not match")
                            reservation_items = await conn.fetchrow(
                                """
                                SELECT count(*) AS item_count,
                                       coalesce(sum(reserved_base_quantity),0) AS item_quantity
                                FROM public.execution_hedge_reservation_items
                                WHERE reservation_id=$1::uuid
                                """,
                                reservation_id,
                            )
                            if (
                                int(reservation_items["item_count"]) <= 0
                                or Decimal(str(reservation_items["item_quantity"]))
                                   != Decimal(str(hedge_gate["reserved_base_quantity"]))
                            ):
                                raise GlobalStopError("HEDGE reservation has no conserved obligation allocation")
                            leg_state = str(hedge_gate["leg_status"]).upper()
                            event_payload = {
                                "mutation_intent_id": mutation_intent_id,
                                "reservation_id": reservation_id,
                                "venue": str(hedge_intent["venue"]),
                                "market_type": str(hedge_intent["market_type"]),
                                "instrument": str(hedge_intent["instrument"]),
                                "reserved_base_quantity": str(hedge_gate["reserved_base_quantity"]),
                            }
                            if leg_state in {"CREATED", "NOT_STARTED"}:
                                await transition_leg_in_transaction(
                                    conn, session_id=session_id, leg_id=leg_id,
                                    expected_state=leg_state, target_state="PREPARED",
                                    fencing_generation=int(hedge_gate["fencing_generation"]),
                                    event_type="HEDGE_LEG_PREPARED",
                                    idempotency_key=f"hedge-leg-prepared:{mutation_intent_id}",
                                    actor_id=str(mutation_intent_actor_id or "executor"),
                                    correlation_id=client_order_id, payload=event_payload,
                                )
                                leg_state = "PREPARED"
                            if leg_state == "PREPARED":
                                await transition_leg_in_transaction(
                                    conn, session_id=session_id, leg_id=leg_id,
                                    expected_state="PREPARED", target_state="SUBMITTING",
                                    fencing_generation=int(hedge_gate["fencing_generation"]),
                                    event_type="HEDGE_LEG_SUBMISSION_STARTED",
                                    idempotency_key=f"mutation-submit:{mutation_intent_id}",
                                    actor_id=str(mutation_intent_actor_id or "executor"),
                                    correlation_id=client_order_id, payload=event_payload,
                                )
                            elif leg_state in {"SUBMITTING", "OPEN", "PARTIALLY_FILLED"}:
                                await append_leg_event_in_transaction(
                                    conn, session_id=session_id, leg_id=leg_id,
                                    fencing_generation=int(hedge_gate["fencing_generation"]),
                                    event_type="HEDGE_ORDER_SUBMISSION_STARTED",
                                    idempotency_key=f"mutation-submit:{mutation_intent_id}",
                                    actor_id=str(mutation_intent_actor_id or "executor"),
                                    correlation_id=client_order_id, payload=event_payload,
                                )
                            else:
                                raise GlobalStopError(
                                    f"Futures leg state {leg_state} cannot accept an obligation-backed hedge order"
                                )
                        lease, risk_snapshot = await self._acquire_submit_ownership(
                            conn,
                            intent=intent,
                            account_scope_id=account_scope_id,
                            action_class=action,
                            scope_pair_key=scope_pair_key,
                            scope_session_id=scope_session_id,
                            risk_metrics=risk_metrics,
                            risk_limits=risk_limits,
                        )
                        unresolved = await self._find_unresolved_submit(
                            conn,
                            client_order_id=client_order_id,
                            intent=intent,
                            account_scope_id=account_scope_id,
                        )
                        if unresolved is not None:
                            raise GlobalStopError(
                                "unresolved in-flight submit blocks new entries: "
                                f"{unresolved['exchange']}:{unresolved['symbol']} "
                                f"client_order_id={unresolved['client_order_id']} status={unresolved['status']}"
                            )
                        self._assert_global_stop_permission(
                            control, client_order_id=client_order_id, action_class=action
                        )
                        await self._mark_submit_attempt_submitting(
                            conn,
                            client_order_id=client_order_id,
                            global_epoch=int(control["epoch"]),
                            lease=lease,
                            risk_snapshot=risk_snapshot,
                        )
                        if mutation_intent_id:
                            if mutation_intent_service is None or lease is None:
                                raise GlobalStopError(
                                    "V3 submit requires the durable mutation intent service and scoped lease"
                                )
                            await mutation_intent_service.mark_submitting_in_transaction(
                                conn, mutation_intent_id, lease=lease,
                                actor_id=str(mutation_intent_actor_id or "executor"),
                                risk_snapshot=risk_snapshot,
                            )
                    if mutation_intent_lease_callback is not None:
                        mutation_intent_lease_callback(lease)
                    yielded_to_caller = True
                    yield conn
                finally:
                    if scope_locked and scope_lock_key is not None:
                        try:
                            await conn.execute(
                                "select pg_advisory_unlock($1)", scope_lock_key
                            )
                        except Exception as unlock_error:
                            self._demote_broken_pool(unlock_error)
                            logger.warning(
                                "could not release scoped ownership mutation lock; "
                                "treat the venue outcome as ambiguous"
                            )
                    if shared_locked:
                        try:
                            await conn.execute(
                                "select pg_advisory_unlock_shared($1)", FENCE_LOCK_KEY
                            )
                        except Exception as unlock_error:
                            self._demote_broken_pool(unlock_error)
                            logger.warning(
                                "could not release PostgreSQL shared submit fence; "
                                "treat the venue outcome as ambiguous"
                            )
        except (GlobalStopError, OwnershipError):
            raise
        except Exception as exc:
            if not yielded_to_caller:
                self._demote_broken_pool(exc)
            raise

