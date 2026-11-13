"""PostgreSQL-backed durable risk state and realized-PnL event persistence.

Provides native asyncpg storage for risk state and PnL ledger when operating
without Supabase REST. Canonical source of truth in PostgreSQL.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger("executor.postgres_risk")

_POSITION_NS = uuid.UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6")


class PostgresRiskStateService:
    """PostgreSQL native risk state and realized loss ledger manager."""

    def __init__(self, dsn: str = "", pool: Any = None):
        self.dsn = str(dsn or "").strip()
        self.pool = pool
        self._owned_pool = False
        self._persistence_failed = False

    @property
    def enabled(self) -> bool:
        return self.pool is not None and hasattr(self.pool, "acquire")

    def risk_persistence_failed(self) -> bool:
        return self._persistence_failed

    def mark_risk_persistence_failure(self) -> None:
        self._persistence_failed = True

    async def start(self) -> None:
        if self.pool is not None:
            return
        if not self.dsn:
            return
        try:
            import asyncpg
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn, min_size=1, max_size=3, command_timeout=10
            )
            self._owned_pool = True
            logger.info("PostgresRiskStateService connected to PostgreSQL")
        except Exception:
            logger.exception("Failed to initialize PostgreSQL pool for risk state")
            self._persistence_failed = True
            raise

    async def close(self) -> None:
        if self._owned_pool and self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def fetch_active_risk_states(self, *, is_mock: bool = True) -> list[dict]:
        """Fetch all active (unresolved) risk states from public.risk_state."""
        if not self.enabled:
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT state_id, state_type, scope, exchange, pair_key, reason, "
                    "value, created_at, updated_at, resolved_at, is_mock "
                    "FROM public.risk_state "
                    "WHERE is_mock = $1 AND resolved_at IS NULL "
                    "ORDER BY updated_at ASC",
                    bool(is_mock),
                )
                results = []
                for row in rows:
                    item = dict(row)
                    val = item.get("value")
                    if isinstance(val, str):
                        try:
                            item["value"] = json.loads(val)
                        except (TypeError, ValueError):
                            pass
                    results.append(item)
                return results
        except Exception as exc:
            logger.error("Failed to fetch active risk states from PostgreSQL: %s", exc)
            self._persistence_failed = True
            raise RuntimeError("Durable PostgreSQL risk state could not be restored") from exc

    async def fetch_daily_realized_loss_usdt(self, loss_day: str, *, is_mock: bool = True) -> float:
        """Calculate total realized losses for the specified UTC day."""
        if not self.enabled:
            return 0.0
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT coalesce(sum(pnl_usdt), 0.0) AS total_loss "
                    "FROM public.risk_pnl_events "
                    "WHERE loss_day_utc = $1 AND is_mock = $2 AND pnl_usdt < 0",
                    str(loss_day),
                    bool(is_mock),
                )
                val = float(row["total_loss"] if row and row["total_loss"] is not None else 0.0)
                return abs(val)
        except Exception as exc:
            logger.error("Failed to fetch daily realized loss from PostgreSQL: %s", exc)
            raise RuntimeError("Durable PostgreSQL realized-PnL ledger unavailable") from exc

    async def persist_risk_state(
        self,
        state_type: str,
        *,
        scope: str = "global",
        value: Any = None,
        reason: str = "",
        exchange: Optional[str] = None,
        pair_key: Optional[str] = None,
        resolved: bool = False,
        state_id: Optional[str] = None,
        is_mock: bool = True,
    ) -> bool:
        """Upsert a mutable risk-state snapshot synchronously into public.risk_state."""
        if not self.enabled:
            return False

        identity = state_id or str(
            uuid.uuid5(
                _POSITION_NS,
                f"risk:{state_type}:{scope}:{(exchange or '').lower()}:{pair_key or ''}",
            )
        )
        now = datetime.now(timezone.utc)
        resolved_at = now if resolved else None

        val_json = (
            value
            if isinstance(value, (dict, list))
            else ({} if value is None else {"raw": str(value)})
        )
        val_str = json.dumps(val_json, default=str)

        query = """
            INSERT INTO public.risk_state
            (state_id, state_type, scope, exchange, pair_key, reason, value,
             created_at, updated_at, resolved_at, is_mock)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11)
            ON CONFLICT (state_id) DO UPDATE SET
                reason = EXCLUDED.reason,
                value = EXCLUDED.value,
                updated_at = EXCLUDED.updated_at,
                resolved_at = EXCLUDED.resolved_at
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    query,
                    identity,
                    str(state_type),
                    str(scope),
                    exchange.lower() if exchange else None,
                    pair_key,
                    str(reason or ""),
                    val_str,
                    now,
                    now,
                    resolved_at,
                    bool(is_mock),
                )
            return True
        except Exception as exc:
            logger.error("Failed to persist risk state %s to PostgreSQL: %s", state_type, exc)
            self._persistence_failed = True
            return False

    async def risk_pnl_event(
        self,
        event_id: str,
        *,
        pair_key: str,
        pnl_usdt: float,
        loss_day: str,
        is_mock: bool = True,
    ) -> str:
        """Insert immutable realized-PnL event into public.risk_pnl_events."""
        if not self.enabled:
            return "failed"
        now = datetime.now(timezone.utc)
        query = """
            INSERT INTO public.risk_pnl_events
            (event_id, pair_key, pnl_usdt, loss_day_utc, is_mock, occurred_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    query,
                    str(event_id),
                    str(pair_key),
                    float(pnl_usdt),
                    str(loss_day),
                    bool(is_mock),
                    now,
                )
                return "inserted" if row is not None else "duplicate"
        except Exception as exc:
            logger.error("Failed to insert risk PnL event %s: %s", event_id, exc)
            return "failed"
