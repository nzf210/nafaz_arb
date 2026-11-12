"""supabase query operations for the durable Supabase writer."""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

import aiohttp

from executor.supabase_common import (
    _AUTHORIZATION_BEARER_PREFIX, _JSON_CONTENT_TYPE, _TIMEOUT, _num, logger,
)



class SupabaseQueryMixin:
    """Focused writer methods grouped by persistence responsibility."""

    async def _read_candidate_table(
        self, session, table: str, select: str, headers: dict,
        cutoff: str, limit: int,
    ) -> list:
        params = {
            "select": select,
            "is_mock": "eq.false",
            "updated_at": f"gte.{cutoff}",
            "filled_qty": "gt.0",
            "order": "updated_at.desc",
            "limit": str(max(1, min(int(limit), 10000))),
        }
        async with session.get(
            f"{self.url}/rest/v1/{table}", params=params, headers=headers
        ) as resp:
            if resp.status >= 300:
                body = (await resp.text())[:500]
                raise RuntimeError(
                    f"{table} read failed with HTTP {resp.status}: {body}"
                )
            rows = await resp.json()
        if not isinstance(rows, list):
            raise RuntimeError(f"{table} read returned a malformed payload")
        return [row for row in rows if isinstance(row, dict)]

    @staticmethod
    def _fill_candidate_identity(row: dict) -> str:
        exchange = str(row.get("exchange_name") or "").lower()
        market = str(row.get("market_type") or "").lower()
        key = str(
            row.get("client_order_id") or row.get("exchange_order_id") or ""
        )
        return f"{exchange}:{market}:{key}" if exchange and market and key else ""

    @staticmethod
    def _candidate_value(row: dict, existing: dict, field: str):
        return row.get(field) or existing.get(field)

    @staticmethod
    def _candidate_raw_payload(row: dict) -> dict:
        raw = row.get("raw_payload")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (TypeError, ValueError):
                raw = {}
        return raw if isinstance(raw, dict) else {}

    @staticmethod
    def _candidate_completed_at(row: dict, existing: dict):
        completed_at = existing.get("completed_at")
        terminal_statuses = (
            "closed", "filled", "canceled", "cancelled", "rejected", "expired"
        )
        status = str(row.get("status") or "").lower()
        if completed_at is None and status in terminal_statuses:
            return row.get("updated_at")
        return completed_at

    def _merge_registry_fill_candidate(
        self, candidates: Dict[str, Dict[str, Any]], row: dict
    ) -> None:
        identity = self._fill_candidate_identity(row)
        if not identity:
            return
        existing = candidates.get(identity, {})
        raw = self._candidate_raw_payload(row)
        completed_at = self._candidate_completed_at(row, existing)
        candidates[identity] = {
            **existing,
            "pair_key": self._candidate_value(row, existing, "pair_key"),
            "exchange_name": self._candidate_value(row, existing, "exchange_name"),
            "market_type": self._candidate_value(row, existing, "market_type"),
            "leg": self._candidate_value(row, existing, "leg"),
            "exchange_order_id": self._candidate_value(
                row, existing, "exchange_order_id"
            ),
            "client_order_id": self._candidate_value(
                row, existing, "client_order_id"
            ),
            "symbol": self._candidate_value(row, existing, "symbol"),
            "side": self._candidate_value(row, existing, "side"),
            "status": self._candidate_value(row, existing, "status"),
            "requested_qty": _num(
                row.get("requested_qty"), _num(existing.get("requested_qty"))
            ),
            "filled_qty": max(
                _num(row.get("filled_qty")), _num(existing.get("filled_qty"))
            ),
            "remaining_qty": _num(
                row.get("remaining_qty"), _num(existing.get("remaining_qty"))
            ),
            "limit_price": self._candidate_value(row, existing, "limit_price"),
            "average_fill_price": self._candidate_value(
                row, existing, "average_fill_price"
            ),
            "reduce_only": bool(
                row.get("reduce_only", existing.get("reduce_only", False))
            ),
            "submitted_at": row.get("created_at") or existing.get("submitted_at"),
            "completed_at": completed_at,
            "local_amount_spec": raw.get("local_amount_spec"),
            "global_amount_spec": raw.get("global_amount_spec"),
            "is_mock": row.get("is_mock", False),
        }

    @staticmethod
    def _fill_candidate_table_queries() -> tuple:
        return (
            (
                "orders",
                "id,pair_key,exchange_name,market_type,leg,exchange_order_id,"
                "client_order_id,symbol,side,status,requested_qty,filled_qty,"
                "remaining_qty,limit_price,average_fill_price,reduce_only,"
                "submitted_at,completed_at,updated_at,is_mock",
            ),
            (
                "order_registry",
                "local_order_id,client_order_id,exchange_order_id,pair_key,"
                "exchange_name,market_type,leg,order_role,symbol,side,order_type,"
                "status,requested_qty,filled_qty,remaining_qty,limit_price,"
                "average_fill_price,reduce_only,recovery_status,is_mock,"
                "raw_payload,created_at,updated_at",
            ),
        )

    async def _read_fill_candidate_tables(
        self, session, headers: dict, cutoff: str, limit: int
    ) -> tuple:
        results = []
        errors = []
        for table, select in self._fill_candidate_table_queries():
            try:
                rows = await self._read_candidate_table(
                    session, table, select, headers, cutoff, limit
                )
            except Exception as exc:
                errors.append(f"{table}: {exc}")
                rows = []
            results.append(rows)
        orders, registry = results
        if not orders and not registry and errors:
            raise RuntimeError("; ".join(errors))
        return orders, registry

    def _build_fill_candidate_map(
        self, orders: list, registry: list
    ) -> Dict[str, Dict[str, Any]]:
        candidates: Dict[str, Dict[str, Any]] = {}
        for row in orders:
            identity = self._fill_candidate_identity(row)
            if identity:
                candidates[identity] = dict(row)
        for row in registry:
            self._merge_registry_fill_candidate(candidates, row)
        return candidates

    async def fetch_recent_order_fill_candidates(
        self, days: int = 90, limit: int = 2000
    ) -> list:
        """Read recent live orders and registry metadata for restart-safe fill replay."""
        if not self.enabled or not self.url or not self.key:
            return []
        cutoff = datetime.fromtimestamp(
            time.time() - max(1, int(days)) * 86400, tz=timezone.utc
        ).isoformat()
        headers = {
            "apikey": self.key,
            "Authorization": _AUTHORIZATION_BEARER_PREFIX + self.key,
            "Accept": _JSON_CONTENT_TYPE,
            "Accept-Encoding": "identity",
        }
        session = self._session
        created = False
        try:
            if session is None or session.closed:
                session = aiohttp.ClientSession(timeout=_TIMEOUT)
                created = True
            orders, registry = await self._read_fill_candidate_tables(
                session, headers, cutoff, limit
            )
            candidates = self._build_fill_candidate_map(orders, registry)
            return list(candidates.values())
        except Exception as exc:
            logger.error("recent order fill candidate read failed: %s", exc)
            raise RuntimeError(
                "recent order fill candidates could not be read"
            ) from exc
        finally:
            if created and session is not None:
                try:
                    await session.close()
                except Exception:
                    pass

    @staticmethod
    def _open_order_record_query(limit: int) -> dict:
        return {
            "select": (
                "local_order_id,client_order_id,venue_client_order_id,exchange_order_id,pair_key,position_key,"
                "exchange_name,market_type,leg,order_role,symbol,side,order_type,status,"
                "requested_qty,filled_qty,remaining_qty,limit_price,average_fill_price,"
                "reduce_only,recovery_status,is_mock,raw_payload"
            ),
            "recovery_status": "in.(MANAGED,ADOPTED,UNVERIFIED)",
            "status": "not.in.(closed,filled,canceled,cancelled,rejected,expired)",
            "order": "updated_at.desc",
            "limit": str(max(1, int(limit))),
        }

    async def _read_open_order_registry_rows(self, params: dict) -> Any:
        session = self._session
        created = False
        try:
            if session is None or session.closed:
                session = aiohttp.ClientSession(timeout=_TIMEOUT)
                created = True
            headers = {
                "apikey": self.key,
                "Authorization": _AUTHORIZATION_BEARER_PREFIX + self.key,
                "Accept": _JSON_CONTENT_TYPE,
                "Accept-Encoding": "identity",
            }
            async with session.get(
                f"{self.url}/rest/v1/order_registry", params=params, headers=headers
            ) as resp:
                if resp.status >= 300:
                    body = (await resp.text())[:500]
                    raise RuntimeError(
                        f"order registry read failed with HTTP {resp.status}: {body}"
                    )
                return await resp.json()
        except Exception as exc:
            logger.error("order registry read failed: %s", exc)
            raise RuntimeError("durable order registry could not be read") from exc
        finally:
            if created and session is not None:
                try:
                    await session.close()
                except Exception:
                    pass

    async def fetch_open_order_records(self, limit: int = 500) -> list:
        """Reads non-terminal order-registry rows for startup recovery.

        The outbox is write-only, so a flushed or partial Redis cache needs a
        read path back into the durable record. A disabled writer returns an
        empty list; an enabled-but-unreadable registry raises so startup can
        fail closed instead of mistaking a database outage for zero orders.
        """
        if not self.enabled or not self.url or not self.key:
            return []
        params = self._open_order_record_query(limit)
        rows = await self._read_open_order_registry_rows(params)
        if not isinstance(rows, list):
            raise RuntimeError("order registry read returned a malformed payload")
        return [row for row in rows if isinstance(row, dict)]

    # ------------------------------------------------------------------- flush
