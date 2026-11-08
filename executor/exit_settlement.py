"""Exit fill settlement helpers for the paired spot/perpetual position."""

import json
import logging
import math
import time
from dataclasses import dataclass

from executor import helpers
from executor.models import PositionState
from executor.helpers import (
    ARB_TRADE_HISTORY_KEY,
    ARB_TRADE_HISTORY_MAX_LEN,
    BALANCES_KEY,
    build_client_order_id,
)

logger = logging.getLogger("executor.engine")


@dataclass
class ExitSettlement:
    closed_trade: dict
    net_profit_usdt: float
    net_profit_idr: float
    spot_cost_usdt: float
    spot_pnl_usdt: float
    futures_margin_usdt: float
    futures_pnl_usdt: float
    funding_usdt: float
    fees_usdt: float
    usdt_idr_rate: float


class ExitSettlementMixin:
    def _resolve_spot_exit_fill_price(self, order_info: dict, spot_order: dict | None, qty: float) -> tuple[float, float | None]:
        exit_price = float(order_info.get("price", 0.0))
        if not isinstance(spot_order, dict):
            return self._validate_spot_exit_price(exit_price), None
        prior_cost = float(order_info.get("closed_spot_cost_idr", 0.0) or 0.0)
        cumulative_cost = self._extract_cumulative_spot_cost(spot_order)
        if cumulative_cost is not None:
            exit_price = self._incremental_spot_fill_price(exit_price, cumulative_cost, prior_cost, qty)
        else:
            exit_price = self._fallback_spot_fill_price(exit_price, spot_order)
        return self._validate_spot_exit_price(exit_price), cumulative_cost

    @staticmethod
    def _extract_cumulative_spot_cost(spot_order: dict) -> float | None:
        cumulative = spot_order.get("cost")
        if cumulative is None and spot_order.get("average") is not None:
            try:
                cumulative = float(spot_order["average"]) * float(spot_order.get("filled") or 0.0)
            except (TypeError, ValueError):
                return None
        if cumulative is None:
            return None
        try:
            return float(cumulative)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _incremental_spot_fill_price(fallback: float, cumulative_cost: float, prior_cost: float, qty: float) -> float:
        incremental_cost = cumulative_cost - prior_cost
        if incremental_cost > 0 and qty > 0:
            return incremental_cost / qty
        return fallback

    @staticmethod
    def _fallback_spot_fill_price(fallback: float, spot_order: dict) -> float:
        try:
            candidate = float(spot_order.get("price") or 0.0)
        except (TypeError, ValueError):
            return fallback
        return candidate if candidate > 0 and math.isfinite(candidate) else fallback

    @staticmethod
    def _validate_spot_exit_price(price: float) -> float:
        if price <= 0 or not math.isfinite(price):
            raise ValueError("spot exit fill price unavailable; refusing to report fabricated PnL")
        return price

    async def _load_exit_usdt_idr_rate(self, pair_tag: str) -> float:
        raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if not raw:
            return helpers.FALLBACK_USDT_IDR_RATE
        try:
            snapshot = json.loads(raw)
            return float(snapshot.get("usdt_idr_rate", helpers.FALLBACK_USDT_IDR_RATE))
        except Exception:
            logger.debug("could not parse latest exit FX snapshot for %s", pair_tag, exc_info=True)
            return helpers.FALLBACK_USDT_IDR_RATE

    async def _close_futures_short_for_exit(self, pair_tag: str, pos, qty: float) -> dict:
        global_symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
        client_order_id = build_client_order_id(pair_tag, "unwind", self._next_order_seq())
        order = await self.client.close_global_short_market(
            global_symbol, qty, client_order_id=client_order_id
        )
        self._validate_futures_close_order(order, qty)
        self.supabase.order(
            pair_tag, "futures_exit", pos.global_exchange, "perpetual",
            global_symbol, order, client_order_id=client_order_id,
            reduce_only=True, is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        return order

    @staticmethod
    def _validate_futures_close_order(order: dict, requested_qty: float) -> None:
        if not isinstance(order, dict):
            raise ValueError("futures close returned no verifiable order payload after spot exit fill")
        status = str(order.get("status", "")).lower()
        if status not in ("closed", "filled"):
            raise ValueError(f"futures close is not terminal after spot exit fill: {status}")
        try:
            filled = float(order.get("filled") or 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        tolerance = max(1e-12, requested_qty * 1e-8)
        if filled + tolerance < requested_qty:
            raise ValueError(f"futures close fill incomplete after spot sold: requested={requested_qty}, filled={filled}")
        if filled > requested_qty + tolerance:
            raise ValueError(f"futures close overfilled: requested={requested_qty}, filled={filled}")

    async def _settle_exit_fill(
        self, pair_tag: str, order_info: dict, pos, qty: float, close_order: dict,
        exit_price_idr: float, cumulative_spot_cost: float | None,
        entry_price_idr: float, short_entry_usdt: float, usdt_rate: float,
    ) -> ExitSettlement:
        total_qty_before = pos.qty
        order_info["closed_short_qty"] += qty
        pos.qty -= qty
        pos.updated_at = int(time.time() * 1000)
        await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

        short_exit_usdt = helpers.extract_fill_price(close_order)
        if short_exit_usdt <= 0:
            raise ValueError("futures close fill price unavailable after spot sell; PnL cannot be safely finalized")

        spot_pnl_idr = qty * (exit_price_idr - entry_price_idr)
        spot_pnl_usdt = spot_pnl_idr / usdt_rate if usdt_rate > 0 else 0.0
        futures_pnl_usdt = qty * (short_entry_usdt - short_exit_usdt)
        fill_ratio = qty / total_qty_before if total_qty_before > 0 else 1.0
        funding_usdt = float(pos.accumulated_funding_usdt) * fill_ratio
        pos.accumulated_funding_usdt -= funding_usdt

        fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.001) or 0.0)
        capital_usdt = qty * short_entry_usdt
        fees_usdt = capital_usdt * fee_pct
        net_profit_usdt = spot_pnl_usdt + futures_pnl_usdt + funding_usdt - fees_usdt
        if cumulative_spot_cost is not None:
            order_info["closed_spot_cost_idr"] = cumulative_spot_cost
        net_profit_idr = net_profit_usdt * usdt_rate
        roi_pct = net_profit_usdt / capital_usdt * 100.0 if capital_usdt > 0 else 0.0
        is_mock = getattr(self.config, "mock_mode", False)

        self._log_realized_exit_pnl(
            pair_tag, net_profit_usdt, roi_pct, spot_pnl_usdt,
            futures_pnl_usdt, funding_usdt, fees_usdt,
        )
        closed_trade = {
            "trade_id": f"trade-{int(time.time()*1000)}",
            "local_exchange": pos.local_exchange,
            "global_exchange": pos.global_exchange,
            "symbol": pos.symbol,
            "qty": qty,
            "entry_price_idr": entry_price_idr,
            "exit_price_idr": exit_price_idr,
            "short_entry_usdt": short_entry_usdt,
            "short_exit_usdt": short_exit_usdt,
            "spot_pnl_idr": spot_pnl_idr,
            "futures_pnl_usdt": futures_pnl_usdt,
            "accumulated_funding_usdt": funding_usdt,
            "estimated_fees_usdt": fees_usdt,
            "fees_estimated": True,
            "net_profit_usdt": net_profit_usdt,
            "net_profit_idr": net_profit_idr,
            "net_roi_pct": roi_pct,
            "is_mock": is_mock,
            "closed_at_ms": int(time.time() * 1000),
        }
        spot_cost = getattr(pos, "spot_cost_usdt", qty * entry_price_idr / usdt_rate) * fill_ratio
        futures_margin = getattr(pos, "futures_margin_usdt", qty * short_entry_usdt) * fill_ratio
        return ExitSettlement(
            closed_trade, net_profit_usdt, net_profit_idr, spot_cost,
            spot_pnl_usdt, futures_margin, futures_pnl_usdt, funding_usdt, fees_usdt, usdt_rate,
        )

    def _log_realized_exit_pnl(self, pair_tag: str, net_profit: float, roi: float, spot_pnl: float, futures_pnl: float, funding: float, fees: float) -> None:
        if net_profit < 0:
            logger.warning(
                "[State 4 Realized Loss] %s net=%+.4f USDT ROI=%.3f%% spot=%+.4f futures=%+.4f funding=%+.4f fees=-%.4f",
                pair_tag, net_profit, roi, spot_pnl, futures_pnl, funding, fees,
            )
            return
        logger.info(
            "[State 4 Realized Profit] %s net=+%.4f USDT ROI=+%.3f%% spot=%+.4f futures=%+.4f",
            pair_tag, net_profit, roi, spot_pnl, futures_pnl,
        )

    async def _persist_exit_settlement(self, pair_tag: str, pos, settlement: ExitSettlement) -> None:
        trade = settlement.closed_trade
        await self.redis.lpush(ARB_TRADE_HISTORY_KEY, json.dumps(trade))
        await self.redis.ltrim(ARB_TRADE_HISTORY_KEY, 0, ARB_TRADE_HISTORY_MAX_LEN)
        self.supabase.trade_closed(pair_tag, trade, pos)
        if hasattr(self.client, "apply_realized_trade"):
            self.client.apply_realized_trade(
                spot_cost_usdt=settlement.spot_cost_usdt,
                spot_pnl_usdt=settlement.spot_pnl_usdt,
                spot_fees_usdt=settlement.fees_usdt / 2,
                futures_margin_usdt=settlement.futures_margin_usdt,
                futures_pnl_usdt=settlement.futures_pnl_usdt,
                funding_usdt=settlement.funding_usdt,
                futures_fees_usdt=settlement.fees_usdt / 2,
            )
            balances = await self._get_balance_snapshot(
                pair_tag=pair_tag, symbol=pos.symbol,
                usdt_idr_rate=settlement.usdt_idr_rate,
            )
            if balances is not None:
                await self.redis.set(BALANCES_KEY, json.dumps(balances))
        pipe = self.redis.pipeline()
        pipe.hincrby("arb:stats", "total_closed", 1)
        if settlement.net_profit_usdt > 0:
            pipe.hincrby("arb:stats", "win_count", 1)
        pipe.hincrbyfloat("arb:stats", "total_pnl_usdt", settlement.net_profit_usdt)
        pipe.hincrbyfloat("arb:stats", "total_pnl_idr", settlement.net_profit_idr)
        pipe.publish(self.ARB_TRADE_EVENTS, json.dumps({"event": "POSITION_CLOSED", **trade}))
        await pipe.execute()
        self.record_realized_pnl(settlement.net_profit_usdt)
        await self.publish_status()

    async def _mark_exit_reconciliation_required(self, pair_tag: str, order_info: dict, pos, qty: float, error: Exception) -> None:
        pos.state = "RECONCILIATION_REQUIRED"
        pos.updated_at = int(time.time() * 1000)
        self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
        await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))
        self._mark_failed(
            pair_tag,
            "EXIT_HEDGE_RECONCILIATION_REQUIRED",
            "Spot exit filled but futures close could not be verified; automatic exit retries blocked.",
            detail={
                "spot_order_id": order_info.get("order_id"),
                "spot_sold_qty": qty,
                "futures_close_error": str(error),
            },
            state=PositionState.RECONCILIATION_REQUIRED,
        )
        logger.critical(
            "[State 4 CRITICAL] Futures close not verified after spot exit fill for %s: %s. Manual reconciliation required.",
            pair_tag, error,
        )
        await self.publish_status()
