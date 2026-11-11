"""engine order intent methods for arbitrage engine orchestration."""

from dataclasses import dataclass
from typing import Any

from executor.models import OrderRecord, PositionState
from executor.engine_common import logger


@dataclass(frozen=True)
class OrderRegistrationMetadata:
    """Normalized exchange and recovery metadata shared by order registration."""

    exchange: str
    market_type: str
    symbol: str
    role: str
    side: str
    order_type: str
    price: float
    quantity: float
    reduce_only: bool = False
    local_amount_spec: Any = None
    global_amount_spec: Any = None
    recovery_context: Any = None


def _serialize_amount_spec(value: Any) -> Any:
    serializer = getattr(value, "to_dict", None)
    return serializer() if callable(serializer) else value


class EngineOrderIntentMixin:
    """Focused orchestration responsibilities, composed by ArbitrageEngine."""

    async def _register_order(
        self,
        pair_tag: str,
        client_order_id: str,
        order: dict,
        **metadata: Any,
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
            spec = OrderRegistrationMetadata(**metadata)
            record = OrderRecord(
                local_order_id=OrderRecord.local_id_for(
                    spec.exchange, client_order_id, str(order.get("id") or "")
                ),
                client_order_id=client_order_id or "",
                exchange_order_id=str(order.get("id") or ""),
                pair_key=pair_tag,
                position_id=f"position:{pair_tag}" if pair_tag else "",
                exchange=str(spec.exchange or "").lower(),
                market_type=spec.market_type,
                symbol=spec.symbol,
                side=spec.side,
                order_type=spec.order_type,
                order_role=spec.role,
                price=float(spec.price or 0.0),
                original_quantity=float(spec.quantity or order.get("amount") or 0.0),
                filled_quantity=float(order.get("filled") or 0.0),
                remaining_quantity=max(
                    0.0,
                    float(spec.quantity or order.get("amount") or 0.0)
                    - float(order.get("filled") or 0.0),
                ),
                average_fill_price=float(order.get("average") or 0.0),
                status=str(order.get("status") or "open"),
                reduce_only=bool(spec.reduce_only),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
                recovery_context=spec.recovery_context,
                local_amount_spec=_serialize_amount_spec(spec.local_amount_spec),
                global_amount_spec=_serialize_amount_spec(spec.global_amount_spec),
            )
            return await self.order_registry.register(record)
        except Exception:
            logger.exception(
                "[OrderRegistry] failed to register %s order %s for %s",
                metadata.get("role"), client_order_id or order.get("id"), pair_tag,
            )
            return None

    async def _register_order_intent(
        self,
        pair_tag: str,
        client_order_id: str,
        **metadata: Any,
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
        required = bool(metadata.pop("required", True))
        role = metadata.get("role", "unknown")
        record = await self._register_order(
            pair_tag,
            client_order_id,
            {"status": "submitting"},
            **metadata,
        )
        if required and record is None:
            raise RuntimeError(
                f"refusing to submit {role} order: durable order intent could not be recorded"
            )
        return record

    async def _mark_order_submission_ambiguous(
        self, pair_tag: str, exchange: str, client_order_id: str, reason: str
    ) -> None:
        """Fail closed when a venue may have accepted an unacknowledged submit."""
        registry = getattr(self, "order_registry", None)
        if registry is not None and client_order_id:
            try:
                record = await registry.find_by_client_id(exchange, client_order_id)
                if record is not None:
                    record.recovery_status = "UNVERIFIED"
                    await registry.register(record)
                    await registry.mark_pending_recovery(
                        record.local_order_id, reason
                    )
            except Exception:
                logger.exception(
                    "[OrderRegistry] could not mark ambiguous submission %s pending",
                    client_order_id,
                )
        self._halt_exposure_scope(
            pair_tag, "exchange order submission acknowledgement is ambiguous"
        )
        self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
        await self._publish_execution_failure(
            pair_tag,
            "ORDER_SUBMISSION_UNCERTAIN",
            "Exchange mungkin telah menerima order, tetapi acknowledgement belum "
            "dapat diverifikasi. Tidak ada retry submit otomatis; pair diblokir "
            "sampai order ditemukan melalui client order ID atau direkonsiliasi operator.",
            detail={
                "exchange": exchange,
                "client_order_id": client_order_id,
                "reason": str(reason)[:1000],
            },
            state=PositionState.RECONCILIATION_REQUIRED,
        )
        self._refresh_entry_gate()
