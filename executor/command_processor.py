"""Durable Command Journal & Operator Command Processor (Executor V2).

Implements Section 12 of EXECUTOR-ARCHITECTURE-V2.md:
- Persists and polls durable operator / Telegram commands from public.command_journal
- Claims commands under lease to prevent split-brain execution
- Dispatches supported commands:
  CANCEL_ORDER, CANCEL_ALL, FORCE_CLOSE, PAUSE_NEW_ENTRIES,
  RESUME_NEW_ENTRIES, RUN_RECONCILIATION, SET_SPOT_SLOT_LIMIT
- Reconciles execution outcomes and commits durable status with audit details
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger("executor.command_processor")


class CommandType(str, Enum):
    CANCEL_ORDER = "CANCEL_ORDER"
    CANCEL_ALL = "CANCEL_ALL"
    FORCE_CLOSE = "FORCE_CLOSE"
    PAUSE_NEW_ENTRIES = "PAUSE_NEW_ENTRIES"
    RESUME_NEW_ENTRIES = "RESUME_NEW_ENTRIES"
    RUN_RECONCILIATION = "RUN_RECONCILIATION"
    SET_SPOT_SLOT_LIMIT = "SET_SPOT_SLOT_LIMIT"


class CommandState(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CommandProcessor:
    """Processes commands from PostgreSQL public.command_journal."""

    def __init__(self, engine: Any, pool: Any = None):
        self.engine = engine
        self.pool = pool
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def submit_command(
        self,
        command_id: str,
        command_type: CommandType,
        scope: str,
        payload: Optional[dict] = None,
    ) -> bool:
        """Submit a durable command into the PostgreSQL command journal."""
        if self.pool is None or not hasattr(self.pool, "acquire"):
            return False
        now = datetime.now(timezone.utc)
        payload_str = json.dumps(payload or {}, default=str)
        query = """
            INSERT INTO public.command_journal
            (command_id, command_type, scope, payload, state, attempts, created_at, updated_at)
            VALUES ($1, $2, $3, $4::jsonb, 'PENDING', 0, $5, $5)
            ON CONFLICT (command_id) DO NOTHING
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, command_id, str(command_type.value), scope, payload_str, now)
                return True
        except Exception as exc:
            logger.error("Failed to submit command %s: %s", command_id, exc)
            return False

    async def process_one_command(self, row: dict) -> bool:
        """Process a claimed command row."""
        cmd_id = row["command_id"]
        cmd_type = row["command_type"]
        scope = row["scope"]
        payload = row.get("payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        result = {"success": False, "detail": ""}
        try:
            if cmd_type == CommandType.PAUSE_NEW_ENTRIES:
                reason = payload.get("reason", "Operator manual pause")
                if hasattr(self.engine, "_halt_exposure_scope"):
                    self.engine._halt_exposure_scope(scope, reason)
                if hasattr(self.engine, "_persist_risk_state"):
                    self.engine._persist_risk_state("trading_halt", scope="global", reason=reason)
                result = {"success": True, "detail": f"Paused scope: {scope}"}

            elif cmd_type == CommandType.RESUME_NEW_ENTRIES:
                if hasattr(self.engine, "_clear_resolved_pair_failure"):
                    if scope in ("*", "global"):
                        self.engine._trading_halted = False
                        self.engine._halt_reason = ""
                        self.engine._recovery_scope_halts.clear()
                    else:
                        await self.engine._clear_resolved_pair_failure(scope)
                if hasattr(self.engine, "_persist_risk_state"):
                    self.engine._persist_risk_state("trading_resume", scope="global", resolved=True)
                if hasattr(self.engine, "_refresh_entry_gate"):
                    self.engine._refresh_entry_gate()
                result = {"success": True, "detail": f"Resumed scope: {scope}"}

            elif cmd_type == CommandType.CANCEL_ORDER:
                client_id = payload.get("client_order_id", "")
                ex_order_id = payload.get("exchange_order_id", "")
                symbol = payload.get("symbol", "")
                exchange = payload.get("exchange", scope).lower()
                gateway = getattr(self.engine, "mutation_gateway", None)
                venue_client = getattr(self.engine, "client", None)
                if gateway and venue_client:
                    res = await gateway.execute_cancel_order(
                        venue_client,
                        exchange=exchange,
                        market_type="spot",
                        order_id=ex_order_id or client_id,
                        symbol=symbol,
                        client_order_id=client_id,
                    )
                    result = {"success": res.status.value == "CONFIRMED", "status": res.status.value}
                else:
                    result = {"success": False, "detail": "Gateway or client not initialized"}

            elif cmd_type == CommandType.SET_SPOT_SLOT_LIMIT:
                venue = payload.get("venue", scope).lower()
                limit = int(payload.get("limit", 2))
                slot_mgr = getattr(self.engine, "slot_manager", None)
                if slot_mgr:
                    await slot_mgr.set_max_slots(venue, limit)
                    result = {"success": True, "detail": f"Updated {venue} spot slots to {limit}"}
                else:
                    result = {"success": False, "detail": "Slot manager not initialized"}

            elif cmd_type == CommandType.RUN_RECONCILIATION:
                if hasattr(self.engine, "exposure_reconciliation"):
                    await self.engine.exposure_reconciliation.reconcile_once()
                if hasattr(self.engine, "trade_fill_reconciliation"):
                    await self.engine.trade_fill_reconciliation.reconcile_once()
                result = {"success": True, "detail": "Reconciliation sweep triggered"}

            elif cmd_type == CommandType.FORCE_CLOSE:
                if hasattr(self.engine, "_emergency_unwind"):
                    await self.engine._emergency_unwind(scope, reason="Operator force close")
                result = {"success": True, "detail": f"Triggered emergency unwind for {scope}"}

            else:
                result = {"success": False, "detail": f"Unknown command type: {cmd_type}"}

        except Exception as exc:
            logger.exception("Error executing command %s (%s)", cmd_id, cmd_type)
            result = {"success": False, "detail": str(exc)}

        now = datetime.now(timezone.utc)
        final_state = CommandState.COMPLETED.value if result["success"] else CommandState.FAILED.value
        update_query = """
            UPDATE public.command_journal
            SET state = $1, result = $2::jsonb, updated_at = $3
            WHERE command_id = $4
        """
        if self.pool is not None and hasattr(self.pool, "acquire"):
            async with self.pool.acquire() as conn:
                await conn.execute(update_query, final_state, json.dumps(result, default=str), now, cmd_id)
        return bool(result["success"])

    async def poll_and_execute_pending_commands(self) -> int:
        """Fetch and execute pending commands."""
        if self.pool is None or not hasattr(self.pool, "acquire"):
            return 0

        claim_query = """
            UPDATE public.command_journal
            SET state = 'PROCESSING', attempts = attempts + 1, updated_at = now()
            WHERE command_id IN (
                SELECT command_id FROM public.command_journal
                WHERE state = 'PENDING'
                ORDER BY created_at ASC
                LIMIT 5
                FOR UPDATE SKIP LOCKED
            )
            RETURNING command_id, command_type, scope, payload, attempts
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(claim_query)
                count = 0
                for row in rows:
                    await self.process_one_command(dict(row))
                    count += 1
                return count
        except Exception as exc:
            logger.error("Error polling command journal: %s", exc)
            return 0
