"""Submit admission and mutation fencing methods for GlobalStopService."""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from executor.global_stop_common import (
    DERISK_LOCK_KEY, FENCE_LOCK_KEY, GlobalStopError, logger,
)
from executor.ownership import (
    OwnershipError, OwnershipLeaseService, advisory_scope_lock_key, build_scope_key,
)
from executor.risk_control import RiskControlPlane, RiskPermissionDenied, risk_scope_keys


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
        scope_market_type: Optional[str] = None,
        account_scope_id: Optional[str] = None,
        action_class: str = "ENTRY",
        scope_pair_key: Optional[str] = None,
        scope_session_id: Optional[str] = None,
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
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required")
        action = str(action_class or "ENTRY").strip().upper()
        if action not in {"ENTRY", "HEDGE"}:
            raise GlobalStopError("order_submit_fence action_class must be ENTRY or HEDGE")
        if action == "HEDGE" and not account_scope_id:
            raise GlobalStopError("protective hedge submit requires explicit account-scoped ownership")

        yielded_to_caller = False
        shared_locked = False
        scope_locked = False
        scope_lock_key = None
        lease = None
        try:
            async with self.pool.acquire() as conn:
                try:
                    shared_acquired = await conn.fetchval(
                        "select pg_try_advisory_lock_shared($1)", FENCE_LOCK_KEY
                    )
                    if not shared_acquired:
                        raise GlobalStopError(
                            "global STOP transition currently owns the submit fence"
                        )
                    shared_locked = True

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
                        acquired = await conn.fetchval(
                            "select pg_try_advisory_lock($1)", scope_lock_key
                        )
                        if not acquired:
                            raise GlobalStopError(
                                "another mutation currently owns this account/venue/market/instrument scope"
                            )
                        scope_locked = True

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
                            "where singleton_id = 1"
                        )
                        if control is None:
                            raise GlobalStopError("global control row is missing")

                        scope_requested = (
                            scope_exchange is not None or scope_symbol is not None
                            or account_scope_id is not None
                        )
                        intent = None
                        if scope_requested:
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
                                or (scope_market_type and str(scope_market_type).strip().lower() != durable_market)
                            ):
                                raise GlobalStopError(
                                    "provided submit fence scope does not match the durable PREPARED intent"
                                )
                            if account_scope_id:
                                if str(intent["account_scope_id"] or "") != str(account_scope_id):
                                    raise GlobalStopError(
                                        "account scope does not match the durable PREPARED intent"
                                    )
                                lease = await self.ownership.acquire(
                                    conn,
                                    account_scope_id=account_scope_id,
                                    venue=durable_exchange,
                                    market_type=durable_market,
                                    instrument=durable_symbol,
                                )
                                await self.ownership.validate(conn, lease)
                                keys = risk_scope_keys(
                                    account_scope_id=account_scope_id,
                                    venue=durable_exchange,
                                    market_type=durable_market,
                                    instrument=durable_symbol,
                                    pair_key=str(scope_pair_key or intent["pair_key"] or ""),
                                    session_id=str(scope_session_id or ""),
                                )
                                try:
                                    await self.risk_control_plane.assert_allowed(
                                        conn, scope_keys=keys, action_class=action
                                    )
                                except RiskPermissionDenied as risk_error:
                                    raise GlobalStopError(str(risk_error)) from risk_error
                            unresolved = await conn.fetchrow(
                                "select client_order_id, exchange, symbol, status from public.executor_submit_attempts "
                                "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 "
                                "and exchange = $2 and market_type = $3 and symbol = $4 "
                                "and ($5::text is null or account_scope_id = $5) "
                                "order by created_at limit 1",
                                client_order_id, durable_exchange, durable_market,
                                durable_symbol, str(account_scope_id or "").strip() or None,
                            )
                        else:
                            unresolved = await conn.fetchrow(
                                "select client_order_id, exchange, symbol, status from public.executor_submit_attempts "
                                "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 "
                                "order by created_at limit 1",
                                client_order_id,
                            )
                        if unresolved is not None:
                            raise GlobalStopError(
                                "unresolved in-flight submit blocks new entries: "
                                f"{unresolved['exchange']}:{unresolved['symbol']} "
                                f"client_order_id={unresolved['client_order_id']} status={unresolved['status']}"
                            )
                        if control["is_stopped"] and action == "ENTRY":
                            raise GlobalStopError(
                                f"global STOP active (epoch={control['epoch']} reason={control['reason'] or 'unspecified'})"
                            )
                        if control["is_stopped"] and action == "HEDGE":
                            logger.warning(
                                "protective hedge %s proceeds under global STOP (epoch=%s)",
                                client_order_id, control["epoch"],
                            )
                        attempt = await conn.fetchrow(
                            "update public.executor_submit_attempts "
                            "set status='SUBMITTING', submit_started_at=now(), "
                            "submit_count=submit_count+1, fence_epoch=$2, "
                            "owner_scope_key=$3, owner_generation=$4, updated_at=now() "
                            "where client_order_id=$1 and status='PREPARED' "
                            "returning client_order_id",
                            client_order_id, int(control["epoch"]),
                            lease.scope_key if lease else None,
                            lease.generation if lease else None,
                        )
                        if attempt is None:
                            raise GlobalStopError(
                                f"durable PREPARED submit attempt missing or already used: {client_order_id}"
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

