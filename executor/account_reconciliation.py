"""Account-wide exchange reconciliation and venue halt decisions."""

import logging
import math
from typing import Dict

from executor.models import PositionState

logger = logging.getLogger("executor.engine")


def normalize_exchange_symbol(value: str) -> str:
    return str(value or "").split(":")[0].replace("/", "").replace("-", "").replace("_", "").upper()


class AccountWideReconciliationMixin:
    async def _verify_account_wide_exchange_state(self) -> None:
        readers = self._get_account_state_readers()
        if readers is None:
            return
        positions_reader, orders_reader = readers
        expected, tags = self._expected_futures_positions()
        reports = await self._fetch_account_state_reports(positions_reader, orders_reader)
        if reports is None:
            return
        position_reports, order_reports = reports
        self._reconcile_account_futures(position_reports, expected, tags)
        self._reconcile_account_open_orders(order_reports)
        expected_spot, spot_tags = self._expected_spot_inventory()
        if expected_spot:
            await self._reconcile_account_spot_inventory(expected_spot, spot_tags)

    def _get_account_state_readers(self):
        positions_reader = getattr(self.client, "fetch_account_positions_report", None)
        orders_reader = getattr(self.client, "fetch_account_open_orders_report", None)
        if callable(positions_reader) and callable(orders_reader):
            return positions_reader, orders_reader
        reason = "account-wide positions/open-orders readers are unavailable"
        perpetual_venues = set(getattr(self.client, "perpetual_clients", {}) or {})
        spot_venues = set(getattr(self.client, "spot_clients", {}) or {})
        if not perpetual_venues:
            perpetual_venues.add(str(getattr(self.config, "global_exchange_name", "") or "").lower())
        if not spot_venues:
            spot_venues.add(str(getattr(self.config, "local_exchange_name", "") or "").lower())
        venues = perpetual_venues | spot_venues
        self._halt_venues(venues, reason)
        logger.critical("[Reconciliation] %s; all live entries blocked", reason)
        return None

    def _halt_venues(self, venues, reason: str) -> None:
        for venue in venues:
            if venue:
                self._exchange_halted[str(venue).lower()] = reason

    def _expected_futures_positions(self):
        expected: Dict[str, Dict[str, float]] = {}
        tags: Dict[tuple, list] = {}
        for pair_tag, pos in self.active_positions.items():
            venue = str(pos.global_exchange or "").lower()
            symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            normalized = normalize_exchange_symbol(symbol)
            venue_positions = expected.setdefault(venue, {})
            venue_positions[normalized] = venue_positions.get(normalized, 0.0) + float(pos.qty)
            tags.setdefault((venue, normalized), []).append(pair_tag)
        return expected, tags

    async def _fetch_account_state_reports(self, positions_reader, orders_reader):
        try:
            return await positions_reader(), await orders_reader()
        except Exception as exc:
            reason = f"account-wide exchange state query failed: {exc}"
            venues = set(getattr(self.client, "perpetual_clients", {}))
            venues.update(getattr(self.client, "spot_clients", {}))
            self._halt_venues(venues, reason)
            logger.exception("[Reconciliation] account-wide exchange state unavailable")
            return None

    def _reconcile_account_futures(self, reports, expected_by_venue, tags_by_venue_symbol) -> None:
        for report in reports or []:
            venue = str(report.get("name") or "").lower()
            if report.get("status") != "ok":
                self._exchange_halted[venue] = f"cannot read account positions: {report.get('error', 'unknown error')}"
                continue
            actual, malformed_reason = self._parse_account_futures_positions(report)
            if malformed_reason:
                self._exchange_halted[venue] = malformed_reason
                logger.critical("[Reconciliation] %s: %s", venue, malformed_reason)
                continue
            self._compare_account_futures_positions(
                venue, expected_by_venue.get(venue, {}), actual, tags_by_venue_symbol
            )

    def _parse_account_futures_position(self, position: dict):
        metadata, reason = self._account_position_metadata(position)
        if reason:
            return "", 0.0, reason
        symbol, info = metadata
        side = self._resolve_account_position_side(position, info)
        try:
            size, side = self._account_position_size(position, info, side)
        except (TypeError, ValueError):
            return "", 0.0, f"invalid position size for {symbol}"
        reason = self._account_position_validation_error(size, side, symbol)
        if reason:
            return "", 0.0, reason
        return normalize_exchange_symbol(symbol), size, ""

    @staticmethod
    def _account_position_metadata(position: dict):
        if not isinstance(position, dict):
            return None, "malformed futures position row"
        symbol = position.get("symbol")
        if not symbol:
            return None, "futures position missing symbol"
        info = position.get("info") or {}
        if not isinstance(info, dict):
            return None, f"malformed position metadata for {symbol}"
        return (symbol, info), ""

    @staticmethod
    def _account_position_validation_error(size, side: str, symbol: str) -> str:
        if size is None:
            return f"position size unavailable for {symbol}"
        if not isinstance(size, (int, float)) or not math.isfinite(size) or size < 0:
            return f"invalid position size for {symbol}"
        if size > 1e-12 and side != "short":
            return f"unexpected {side or 'unknown-side'} futures exposure on {symbol}"
        return ""

    @staticmethod
    def _resolve_account_position_side(position: dict, info: dict) -> str:
        return str(
            position.get("side") or position.get("positionSide")
            or info.get("posSide") or info.get("positionSide") or ""
        ).lower()

    @staticmethod
    def _account_position_size(position: dict, info: dict, side: str):
        contracts = position.get("contracts")
        if contracts is not None:
            size = abs(float(contracts)) * abs(float(position.get("contractSize") or 1.0))
        else:
            raw_amount = position.get("positionAmt", info.get("positionAmt"))
            if raw_amount is None:
                return None, side
            signed_amount = float(raw_amount)
            size = abs(signed_amount)
            if not side:
                side = "short" if signed_amount < 0 else "long"
        return size, side

    def _compare_account_futures_positions(self, venue, expected, actual, tags_by_venue_symbol) -> None:
        for symbol in set(expected) | set(actual):
            expected_qty = expected.get(symbol, 0.0)
            actual_qty = actual.get(symbol, 0.0)
            tolerance = max(1e-10, expected_qty * 1e-4)
            if abs(expected_qty - actual_qty) <= tolerance:
                continue
            reason = (
                f"account-wide futures mismatch for {symbol}: "
                f"recorded={expected_qty}, exchange={actual_qty}"
            )
            self._exchange_halted[venue] = reason
            logger.critical("[Reconciliation] %s: %s", venue, reason)
            self._mark_account_position_mismatch(
                tags_by_venue_symbol.get((venue, symbol), []),
                "ACCOUNT_POSITION_MISMATCH", reason,
                {"exchange": venue, "symbol": symbol, "recorded_qty": expected_qty, "actual_qty": actual_qty},
            )

    def _mark_account_position_mismatch(self, pair_tags, code: str, reason: str, detail: dict) -> None:
        for pair_tag in pair_tags:
            pos = self.active_positions[pair_tag]
            pos.state = "RECONCILIATION_REQUIRED"
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            self._mark_failed(
                pair_tag, code, reason, detail=detail,
                state=PositionState.RECONCILIATION_REQUIRED,
            )

    def _known_spot_order_ids(self) -> set:
        return {
            (str(info.get("local_exchange") or "").lower(), str(info.get("order_id")))
            for info in list(self.pending_entry_orders.values()) + list(self.pending_exit_orders.values())
            if info.get("order_id") and info.get("local_exchange")
        }

    def _reconcile_account_open_orders(self, reports) -> None:
        known_ids = self._known_spot_order_ids()
        for report in reports or []:
            venue = str(report.get("name") or "").lower()
            market = str(report.get("market") or "")
            if report.get("status") != "ok":
                self._exchange_halted[venue] = f"cannot read {market} open orders: {report.get('error', 'unknown error')}"
                continue
            self._reconcile_venue_open_orders(venue, market, report.get("orders") or [], known_ids)

    def _reconcile_venue_open_orders(self, venue: str, market: str, orders, known_ids: set) -> None:
        for order in orders:
            order_id = str(order.get("id") or "")
            known = market == "spot" and (venue, order_id) in known_ids
            if known:
                continue
            reason = f"untracked {market} open order {order_id} on {venue}"
            self._exchange_halted[venue] = reason
            logger.critical("[Reconciliation] %s; new entries blocked", reason)

    def _expected_spot_inventory(self):
        expected: Dict[str, Dict[str, float]] = {}
        tags: Dict[tuple, list] = {}
        for pair_tag, pos in self.active_positions.items():
            venue = str(pos.local_exchange or "").lower()
            symbol = str(pos.symbol or "").upper()
            base_asset = symbol.split(":")[0].replace("/", "_").replace("-", "_").split("_")[0]
            if not venue or not base_asset:
                continue
            venue_assets = expected.setdefault(venue, {})
            venue_assets[base_asset] = venue_assets.get(base_asset, 0.0) + float(pos.qty)
            tags.setdefault((venue, base_asset), []).append(pair_tag)
        return expected, tags

    async def _reconcile_account_spot_inventory(self, expected, tags_by_asset) -> None:
        balance_reader = getattr(self.client, "fetch_balance_report", None)
        if not callable(balance_reader):
            self._halt_venues(expected, "spot balance reader unavailable for active-position reconciliation")
            return
        try:
            reports = await balance_reader()
        except Exception as exc:
            self._halt_venues(expected, f"cannot read spot balances: {exc}")
            return
        by_venue = {
            str(report.get("name") or "").lower(): report
            for report in reports or [] if report.get("market") == "spot"
        }
        for venue, assets in expected.items():
            self._reconcile_spot_venue_inventory(venue, assets, by_venue.get(venue), tags_by_asset)

    def _reconcile_spot_venue_inventory(self, venue: str, assets: dict, report, tags_by_asset) -> None:
        if not report or report.get("status") != "ok":
            self._exchange_halted[venue] = (
                f"cannot verify spot inventory: {(report or {}).get('error', 'balance report missing')}"
            )
            return
        totals = {
            str(asset.get("asset") or "").upper(): float(asset.get("total") or 0.0)
            for asset in report.get("assets") or []
        }
        for asset, expected_qty in assets.items():
            actual_qty = totals.get(asset, 0.0)
            tolerance = max(1e-10, expected_qty * 1e-4)
            if actual_qty + tolerance >= expected_qty:
                continue
            reason = f"spot inventory shortfall for {asset}: recorded={expected_qty}, exchange_total={actual_qty}"
            self._exchange_halted[venue] = reason
            logger.critical("[Reconciliation] %s: %s", venue, reason)
            self._mark_account_position_mismatch(
                tags_by_asset.get((venue, asset), []),
                "SPOT_INVENTORY_MISMATCH", reason,
                {"exchange": venue, "asset": asset, "recorded_qty": expected_qty, "actual_qty": actual_qty},
            )
