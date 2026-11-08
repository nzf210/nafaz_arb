"""Recovery-time verification for persisted active positions."""

import json
import logging

from executor.models import PositionState

logger = logging.getLogger("executor.engine")


class RecoveredPositionMixin:
    async def _verify_recovered_active_positions(self) -> None:
        if not self.active_positions:
            return
        if not hasattr(self.client, "fetch_positions"):
            await self._mark_all_recovered_positions_unknown()
            return
        groups = self._group_recovered_positions()
        for (local_exchange, global_exchange, global_symbol), items in groups.items():
            expected_qty = sum(pos.qty for _, pos in items)
            actual_qty, reason = await self._fetch_recovered_short_quantity(
                local_exchange, global_exchange, global_symbol
            )
            if not reason:
                tolerance = max(1e-10, expected_qty * 1e-4)
                if abs(actual_qty - expected_qty) <= tolerance:
                    logger.info(
                        "[Recovery] verified %s short quantity %s against Redis snapshot %s",
                        global_symbol, actual_qty, expected_qty,
                    )
                    continue
                reason = f"futures quantity mismatch after restart: expected={expected_qty}, exchange={actual_qty}"
            logger.critical("[Recovery] %s: %s; restored positions blocked", global_symbol, reason)
            await self._mark_recovered_positions_required(items, global_symbol, actual_qty, reason)

    def _group_recovered_positions(self):
        groups = {}
        for pair_tag, pos in self.active_positions.items():
            global_symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            route = (pos.local_exchange, pos.global_exchange, global_symbol)
            groups.setdefault(route, []).append((pair_tag, pos))
        return groups

    async def _mark_all_recovered_positions_unknown(self) -> None:
        logger.critical(
            "[Recovery] live client cannot fetch futures positions; all restored positions require manual reconciliation"
        )
        groups = {}
        for pair_tag, pos in self.active_positions.items():
            symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            groups.setdefault(symbol, []).append((pair_tag, pos))
        for symbol, items in groups.items():
            await self._mark_recovered_positions_required(
                items, symbol, None, "Live futures position API is unavailable after restart.",
                failure_code="RECOVERY_POSITION_STATE_UNKNOWN",
            )

    async def _fetch_recovered_short_quantity(self, local_exchange: str, global_exchange: str, global_symbol: str):
        try:
            if hasattr(self.client, "set_route"):
                self.client.set_route(local_exchange, global_exchange)
            positions = await self.client.fetch_positions(global_symbol)
            if positions is None:
                raise ValueError("fetch_positions returned None")
            total = 0.0
            for position in positions:
                size, side = self._parse_recovered_position(position)
                if size <= 1e-12:
                    continue
                if side != "short":
                    raise ValueError(f"unexpected non-short futures exposure (side={side or 'unknown'})")
                total += size
            return total, ""
        except Exception as exc:
            return None, f"cannot verify futures position after restart: {exc}"

    @staticmethod
    def _parse_recovered_position(position: dict) -> tuple[float, str]:
        if not isinstance(position, dict):
            raise ValueError("invalid position payload")
        info = position.get("info") or {}
        side = RecoveredPositionMixin._position_side(position, info)
        contracts = position.get("contracts")
        if contracts is not None:
            return RecoveredPositionMixin._parse_contract_position(position, contracts, side)
        return RecoveredPositionMixin._parse_signed_position(position, info, side)

    @staticmethod
    def _position_side(position: dict, info: dict) -> str:
        return str(
            position.get("side") or position.get("positionSide")
            or info.get("posSide") or info.get("positionSide") or ""
        ).lower()

    @staticmethod
    def _parse_contract_position(position: dict, contracts, side: str) -> tuple[float, str]:
        size = abs(float(contracts)) * abs(float(position.get("contractSize") or 1.0))
        return size, side

    @staticmethod
    def _parse_signed_position(position: dict, info: dict, side: str) -> tuple[float, str]:
        raw_amount = position.get("positionAmt", info.get("positionAmt"))
        if raw_amount is None:
            raise ValueError("position size unavailable")
        signed = float(raw_amount)
        resolved_side = side or ("short" if signed < 0 else "long")
        return abs(signed), resolved_side

    async def _mark_recovered_positions_required(
        self, items, global_symbol: str, actual_qty, reason: str,
        failure_code: str = "RECOVERY_POSITION_MISMATCH",
    ) -> None:
        for pair_tag, pos in items:
            pos.state = "RECONCILIATION_REQUIRED"
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))
            self._mark_failed(
                pair_tag, failure_code, reason,
                detail={
                    "global_symbol": global_symbol,
                    "expected_qty": pos.qty,
                    "exchange_total_short_qty": actual_qty,
                },
                state=PositionState.RECONCILIATION_REQUIRED,
            )
