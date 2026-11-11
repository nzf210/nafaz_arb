"""engine order intent methods for arbitrage engine orchestration."""

import time
from typing import Any

from executor.models import OrderRecord
from executor.engine_common import logger



class EngineOrderIntentMixin:
    """Focused orchestration responsibilities, composed by ArbitrageEngine."""

    async def _register_order(
        self,
        pair_tag: str,
        client_order_id: str,
        order: dict,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        role: str,
        side: str,
        order_type: str,
        price: float,
        quantity: float,
        reduce_only: bool = False,
        local_amount_spec: Any = None,
        global_amount_spec: Any = None,
        recovery_context: Any = None,
    ) -> Any:
        """Records one submitted order in the universal registry.

        Registration is best-effort and never raises: an unreachable Redis or
        Supabase must not turn an accepted order into a failed placement (which
        would make the caller retry and double-fill). The order remains visible
        to the next startup sweep through the venue itself.
        """
        if self.order_registry is None:
            return None
        try:
            record = OrderRecord(
                local_order_id=OrderRecord.local_id_for(
                    exchange, client_order_id, str(order.get("id") or "")
                ),
                client_order_id=client_order_id or "",
                exchange_order_id=str(order.get("id") or ""),
                pair_key=pair_tag,
                position_id=f"position:{pair_tag}" if pair_tag else "",
                exchange=str(exchange or "").lower(),
                market_type=market_type,
                symbol=symbol,
                side=side,
                order_type=order_type,
                order_role=role,
                price=float(price or 0.0),
                original_quantity=float(quantity or order.get("amount") or 0.0),
                filled_quantity=float(order.get("filled") or 0.0),
                remaining_quantity=max(
                    0.0,
                    float(quantity or order.get("amount") or 0.0)
                    - float(order.get("filled") or 0.0),
                ),
                average_fill_price=float(order.get("average") or 0.0),
                status=str(order.get("status") or "open"),
                reduce_only=bool(reduce_only),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
                recovery_context=recovery_context,
                local_amount_spec=(
                    local_amount_spec.to_dict()
                    if hasattr(local_amount_spec, "to_dict")
                    else local_amount_spec
                ),
                global_amount_spec=(
                    global_amount_spec.to_dict()
                    if hasattr(global_amount_spec, "to_dict")
                    else global_amount_spec
                ),
            )
            return await self.order_registry.register(record)
        except Exception:
            logger.exception(
                "[OrderRegistry] failed to register %s order %s for %s",
                role, client_order_id or order.get("id"), pair_tag,
            )
            return None

    async def _register_order_intent(
        self,
        pair_tag: str,
        client_order_id: str,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        role: str,
        side: str,
        order_type: str,
        price: float,
        quantity: float,
        reduce_only: bool = False,
        local_amount_spec: Any = None,
        global_amount_spec: Any = None,
        recovery_context: Any = None,
        required: bool = True,
    ) -> Any:
        """Durably records an order intent before submitting it to a venue.

        The client id is already known at this point. If the process dies after
        the exchange accepts the order but before it returns an exchange id, the
        startup open-order sweep can still bind the venue order to this intent.
        """
        # LIVE trading must never proceed with Redis-only order tracking. The
        # pre-submit intent is the last safe point to reject a configuration
        # that cannot durably journal every exchange order for restart recovery.
        if not bool(getattr(self.config, "mock_mode", False)) and not bool(
            getattr(self.supabase, "enabled", False)
        ):
            raise RuntimeError(
                "refusing LIVE order submission: Supabase durable order persistence is disabled"
            )
        record = await self._register_order(
            pair_tag,
            client_order_id,
            {"status": "submitting"},
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            role=role,
            side=side,
            order_type=order_type,
            price=price,
            quantity=quantity,
            reduce_only=reduce_only,
            local_amount_spec=local_amount_spec,
            global_amount_spec=global_amount_spec,
            recovery_context=recovery_context,
        )
        if required and record is None:
            raise RuntimeError(
                f"refusing to submit {role} order: durable order intent could not be recorded"
            )
        return record
