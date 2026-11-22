"""Submit admission and mutation fencing methods for GlobalStopService."""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from executor.global_stop_common import (
    FENCE_LOCK_KEY, GlobalStopError, logger, _json_mapping,
)
from executor.ownership import (
    OwnershipError, advisory_scope_lock_key, build_scope_key,
)
from executor.risk_control import RiskPermissionDenied, risk_scope_keys


class GlobalStopSubmitMixin:
    async def prepare_submit_attempt(
        self, *, client_order_id: str, exchange: str, market_type: str,
        symbol: str, side: str, order_type: str, quantity: float,
        price: float, pair_key: str, recovery_context: Optional[dict] = None,
        amount_spec: Optional[dict] = None,
        account_scope_id: Optional[str] = None,
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
            str(account_scope_id or "").strip() or None,
        )
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "insert into public.executor_submit_attempts "
                "(client_order_id, exchange, market_type, symbol, side, order_type, "
                "quantity, price, pair_key, recovery_context, amount_spec, status, account_scope_id) "
                "values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,'PREPARED',$12) "
                "on conflict (client_order_id) do nothing "
                "returning client_order_id, status, created_at, account_scope_id",
                *values,
            )
            if row is None:
                existing = await conn.fetchrow(
                    "select client_order_id, status, created_at, exchange, market_type, symbol, "
                    "side, order_type, quantity, price, pair_key, amount_spec, recovery_context, "
                    "account_scope_id from public.executor_submit_attempts "
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
                    "account_scope_id": values[11],
                }
                stored_amount_spec = _json_mapping(existing.get("amount_spec"))
                stored_recovery_context = _json_mapping(existing.get("recovery_context"))
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
        try:
            scope_key = build_scope_key(
                account_scope_id, venue, market_type, instrument
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
                                venue=venue,
                                market_type=market_type,
                                instrument=instrument,
                            ),
                            action_class="DERISK",
                        )
                        lease = await self.ownership.acquire(
                            conn,
                            account_scope_id=account_scope_id,
                            venue=venue,
                            market_type=market_type,
                            instrument=instrument,
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
                            account_scope_id, venue, market_type, instrument,
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
    ) -> Optional[Any]:
        if not scope_requested:
            return None
        if not str(scope_exchange or "").strip() or not str(scope_symbol or "").strip():
            raise GlobalStopError(
                "both scope_exchange and scope_symbol are required for scoped submit fencing"
            )
        intent = await conn.fetchrow(
            "select exchange, market_type, symbol, status, account_scope_id, pair_key "
            "from public.executor_submit_attempts "
            "where client_order_id = $1 for update",
            client_order_id,
        )
        if intent is None or intent["status"] != "PREPARED":
            raise GlobalStopError(
                f"scoped submit requires a durable PREPARED intent: {client_order_id}"
            )
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
        if (
            not client_order_id or len(client_order_id) > 128
            or action not in {"ENTRY", "HEDGE"}
            or (
                bool(getattr(self, "require_scoped_submits", False))
                and (
                    not account_scope_id
                    or not str(scope_exchange or "").strip()
                    or not str(scope_symbol or "").strip()
                    or not str(scope_market_type or "").strip()
                )
            )
            or (action == "HEDGE" and not account_scope_id)
        ):
            raise GlobalStopError(
                "valid client order id and scoped ENTRY/HEDGE action are required"
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

