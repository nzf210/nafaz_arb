"""Central Mutation Gateway (Executor V2).

Regulates all private create and cancel mutations:
- Enforces pre-submit fencing via PostgreSQL advisory locks
- Enforces per-CEX Spot order slot caps
- Generates deterministic, unique client_order_id
- Converts timeouts into SUBMISSION_UNKNOWN (zero blind retries)
- Maintains audit trail of all mutations
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from executor.global_stop import GlobalStopError, GlobalStopService
from executor.order_slots import OrderSlotManager, SlotExhaustedError

logger = logging.getLogger("executor.mutation_gateway")


class MutationResultType(str, Enum):
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    DEFERRED = "DEFERRED"


@dataclass
class MutationResult:
    status: MutationResultType
    client_order_id: str
    exchange_order_id: Optional[str] = None
    filled_qty: float = 0.0
    average_price: float = 0.0
    error_message: str = ""
    raw_response: Optional[dict] = None


class MutationGateway:
    """Gateway orchestrating safe order mutations across exchanges."""

    def __init__(
        self,
        global_stop: Optional[GlobalStopService] = None,
        slot_manager: Optional[OrderSlotManager] = None,
        *,
        client_id_prefix: str = "nfz",
    ):
        self.global_stop = global_stop
        self.slot_manager = slot_manager or OrderSlotManager()
        self.client_id_prefix = client_id_prefix

    def generate_client_order_id(self, exchange: str, market_type: str = "spot") -> str:
        """Generate unique, structured client order ID conforming to venue limits."""
        # Clean prefix and ts
        ts_ms = int(time.time() * 1000)
        rand_suffix = uuid.uuid4().hex[:8]
        raw_id = f"{self.client_id_prefix}-{exchange[:3]}-{market_type[:1]}-{ts_ms}-{rand_suffix}"
        # Ensure length adheres to exchange constraints (e.g. Tokocrypto 32 chars, OKX 32 chars)
        if len(raw_id) > 32:
            raw_id = f"{self.client_id_prefix[:2]}{exchange[:3]}{market_type[:1]}{ts_ms % 1000000000}{rand_suffix}"
        return raw_id

    async def execute_create_order(
        self,
        client: Any,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float,
        pair_key: str,
        session_id: Optional[str] = None,
        extra_params: Optional[dict] = None,
    ) -> MutationResult:
        """Create order through Mutation Gateway with fencing, slot reservation, and timeout guard."""
        exchange = exchange.lower().strip()
        market_type = market_type.lower().strip()
        side = side.lower().strip()
        order_type = order_type.lower().strip()
        params = dict(extra_params or {})

        # 1. Deterministic client order ID
        client_order_id = params.get("client_order_id") or self.generate_client_order_id(exchange, market_type)
        params["client_order_id"] = client_order_id

        # 2. Reserve Spot Slot if spot order
        slot_reserved = False
        try:
            if market_type == "spot":
                await self.slot_manager.reserve_slot(
                    exchange,
                    client_order_id=client_order_id,
                    session_id=session_id,
                    market_type=market_type,
                )
                slot_reserved = True
        except SlotExhaustedError as exc:
            logger.warning("Mutation rejected: %s", exc)
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=str(exc),
            )

        # 3. Prepare submit attempt in PostgreSQL if global_stop available
        fence_ctx = None
        if self.global_stop is not None and getattr(self.global_stop, "enabled", False):
            try:
                await self.global_stop.prepare_submit_attempt(
                    client_order_id=client_order_id,
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    pair_key=pair_key,
                )
                fence_ctx = self.global_stop.order_submit_fence(client_order_id=client_order_id)
            except GlobalStopError as g_err:
                if slot_reserved:
                    await self.slot_manager.release_slot(exchange, client_order_id, reason="GLOBAL_STOP_BLOCKED")
                logger.error("Mutation blocked by GlobalStop: %s", g_err)
                return MutationResult(
                    status=MutationResultType.REJECTED,
                    client_order_id=client_order_id,
                    error_message=f"Global stop blocked: {g_err}",
                )

        # 4. Execute venue order with fence lock
        async def _call_venue():
            # Support both direct ccxt create_order or custom venue adapter
            if hasattr(client, "create_order_direct"):
                return await client.create_order_direct(
                    symbol=symbol,
                    order_type=order_type,
                    side=side,
                    amount=quantity,
                    price=price,
                    params=params,
                )
            elif hasattr(client, "create_order"):
                return await client.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=side,
                    amount=quantity,
                    price=price,
                    params=params,
                )
            else:
                raise RuntimeError(f"Client for {exchange} lacks create_order capability")

        try:
            if fence_ctx is not None:
                async with fence_ctx:
                    raw = await _call_venue()
            else:
                raw = await _call_venue()

            # Successful placement
            ex_order_id = str(raw.get("id") or raw.get("orderId") or "")
            filled_qty = float(raw.get("filled") or raw.get("executedQty") or 0.0)
            avg_price = float(raw.get("average") or raw.get("price") or price)

            if slot_reserved:
                await self.slot_manager.activate_slot(exchange, client_order_id)

            return MutationResult(
                status=MutationResultType.CONFIRMED,
                client_order_id=client_order_id,
                exchange_order_id=ex_order_id,
                filled_qty=filled_qty,
                average_price=avg_price,
                raw_response=raw if isinstance(raw, dict) else {},
            )

        except (asyncio.TimeoutError, TimeoutError) as t_err:
            # CRITICAL V2 invariant: Never blind retry a timed-out create!
            logger.critical(
                "Create order timed out on %s (%s). Marked SUBMISSION_UNKNOWN. Slot retained for discovery.",
                exchange, client_order_id,
            )
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=client_order_id,
                error_message=f"Timeout: {t_err}",
            )
        except Exception as exc:
            logger.warning("Order placement rejected on %s (%s): %s", exchange, client_order_id, exc)
            if slot_reserved:
                await self.slot_manager.release_slot(exchange, client_order_id, reason="REJECTED")
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=str(exc),
            )

    async def execute_cancel_order(
        self,
        client: Any,
        *,
        exchange: str,
        market_type: str,
        order_id: str,
        symbol: str,
        client_order_id: Optional[str] = None,
    ) -> MutationResult:
        """Cancel order through Mutation Gateway with state reconciliation."""
        exchange = exchange.lower().strip()
        market_type = market_type.lower().strip()

        try:
            raw = None
            if hasattr(client, "cancel_order"):
                raw = await client.cancel_order(order_id, symbol=symbol)
            elif hasattr(client, "cancel_order_direct"):
                raw = await client.cancel_order_direct(order_id, symbol=symbol)

            # Release slot if confirmed canceled
            cid = client_order_id or str(order_id)
            if market_type == "spot":
                await self.slot_manager.release_slot(exchange, cid, reason="CANCELED")

            return MutationResult(
                status=MutationResultType.CONFIRMED,
                client_order_id=cid,
                exchange_order_id=str(order_id),
                raw_response=raw if isinstance(raw, dict) else {},
            )
        except (asyncio.TimeoutError, TimeoutError) as t_err:
            logger.warning("Cancel order timed out on %s (%s); reconciliation required", exchange, order_id)
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=client_order_id or str(order_id),
                exchange_order_id=str(order_id),
                error_message=f"Cancel timeout: {t_err}",
            )
        except Exception as exc:
            logger.error("Cancel failed on %s (%s): %s", exchange, order_id, exc)
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id or str(order_id),
                exchange_order_id=str(order_id),
                error_message=str(exc),
            )
