"""Read-only periodic reconciliation of managed exposure against live CEX state.

This worker never places, cancels, or modifies orders. It only reads exchange
balances/positions, records balance snapshots, and halts new entries when a
confirmed exposure mismatch is detected. Existing position management remains
enabled so exits and emergency de-risking are not disabled by this monitor.
"""
import asyncio
import logging
import math
import os
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, Tuple

from executor.models import PositionState

logger = logging.getLogger("executor.reconciliation")


def _finite(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite exchange quantity")
    return number


def _position_short_qty(positions: Iterable[dict]) -> float:
    total = 0.0
    for position in positions:
        if not isinstance(position, dict):
            raise ValueError("malformed futures position")
        info = position.get("info") or {}
        side = str(
            position.get("side")
            or position.get("positionSide")
            or info.get("posSide")
            or info.get("positionSide")
            or ""
        ).lower()
        contracts = position.get("contracts")
        if contracts is not None:
            qty = abs(_finite(contracts)) * abs(_finite(position.get("contractSize") or 1))
            if qty <= 1e-12:
                continue
            if side != "short":
                raise ValueError("unexpected non-short futures exposure")
            total += qty
            continue
        raw = position.get("positionAmt", info.get("positionAmt"))
        if raw is None:
            raise ValueError("futures position quantity unavailable")
        signed = _finite(raw)
        if abs(signed) <= 1e-12:
            continue
        if signed >= 0 or side == "long":
            raise ValueError("unexpected long/unsigned futures exposure")
        total += abs(signed)
    return total


class ExposureReconciliationWorker:
    """Periodic read-only exchange exposure monitor."""

    def __init__(self, engine: Any):
        self.engine = engine
        self.interval = max(60, int(os.getenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS", "300")))
        self.relative_tolerance = min(
            0.05, max(0.0, float(os.getenv("EXPOSURE_RECONCILIATION_REL_TOLERANCE", "0.005")))
        )
        self.absolute_tolerance = max(
            1e-12, float(os.getenv("EXPOSURE_RECONCILIATION_ABS_TOLERANCE", "1e-8"))
        )
        self._reported: Dict[str, str] = {}

    def _tolerance(self, expected: float) -> float:
        return max(self.absolute_tolerance, abs(expected) * self.relative_tolerance)

    async def run(self) -> None:
        """Run immediately, then periodically until cancelled."""
        while True:
            try:
                if not self.engine.config.mock_mode:
                    await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("periodic exchange exposure reconciliation failed")
            await asyncio.sleep(self.interval)

    async def reconcile_once(self) -> None:
        client = self.engine.client
        report_fn = getattr(client, "fetch_balance_report", None)
        if not callable(report_fn):
            await self._alert(
                "EXPOSURE_BALANCE_UNAVAILABLE", "unknown",
                "Exchange balance report API is unavailable; exposure reconciliation is incomplete.",
                {"reason": "fetch_balance_report_not_supported"},
            )
            return

        reports = await report_fn()
        report_map = {}
        for report in reports or []:
            if isinstance(report, dict):
                report_map[(str(report.get("name", "")).lower(), str(report.get("market", "")).lower())] = report
                await self._persist_balance_report(report)

        spot_expected = defaultdict(float)
        futures_expected = defaultdict(float)
        spot_pairs = defaultdict(list)
        futures_pairs = defaultdict(list)
        for pair_key, position in self.engine.active_positions.items():
            if float(getattr(position, "qty", 0) or 0) <= 0:
                continue
            pair_info = self.engine._pair_infos.get(pair_key)
            if pair_info is None:
                logger.warning("skip spot reconciliation for %s: venue/symbol mapping unavailable", pair_key)
                continue
            local_name = str(pair_info.local_exchange).lower()
            global_name = str(pair_info.global_exchange).lower()
            base_asset = str(pair_info.base_asset).upper()
            global_symbol = str(pair_info.global_symbol)
            if base_asset:
                spot_expected[(local_name, base_asset)] += float(position.qty)
                spot_pairs[(local_name, base_asset)].append(pair_key)
            if global_symbol:
                futures_expected[(global_name, global_symbol)] += float(position.qty)
                futures_pairs[(global_name, global_symbol)].append(pair_key)

        for (exchange, asset), expected in spot_expected.items():
            report = report_map.get((exchange, "spot"))
            if not report or report.get("status") != "ok":
                await self._alert(
                    "EXPOSURE_BALANCE_UNAVAILABLE", exchange,
                    f"Cannot verify {asset} spot balance on {exchange}.",
                    {"asset": asset, "pair_keys": spot_pairs[(exchange, asset)]},
                )
                continue
            actual = self._asset_total(report, asset)
            if actual + self._tolerance(expected) < expected:
                await self._mismatch(
                    "SPOT_BASE_BALANCE_DEFICIT", exchange,
                    f"Spot {asset} balance is below NAFAZ managed quantity on {exchange}.",
                    {"asset": asset, "expected_managed_qty": expected, "actual_total_qty": actual,
                     "pair_keys": spot_pairs[(exchange, asset)]},
                )
            else:
                self._clear_issue(f"SPOT_BASE_BALANCE_DEFICIT:{exchange}:{asset}", exchange)

        for (exchange, symbol), expected in futures_expected.items():
            try:
                actual = _position_short_qty(await client.fetch_positions(symbol))
            except Exception as exc:
                await self._alert(
                    "EXPOSURE_POSITION_UNAVAILABLE", exchange,
                    f"Cannot verify futures position {symbol} on {exchange}.",
                    {"symbol": symbol, "error": str(exc), "pair_keys": futures_pairs[(exchange, symbol)]},
                )
                continue
            if abs(actual - expected) > self._tolerance(expected):
                await self._mismatch(
                    "FUTURES_POSITION_MISMATCH", exchange,
                    f"Futures short quantity does not match NAFAZ managed exposure on {exchange}.",
                    {"symbol": symbol, "expected_short_qty": expected, "actual_short_qty": actual,
                     "pair_keys": futures_pairs[(exchange, symbol)]},
                )
            else:
                self._clear_issue(f"FUTURES_POSITION_MISMATCH:{exchange}:{symbol}", exchange)

    @staticmethod
    def _asset_total(report: dict, asset: str) -> float:
        for item in report.get("assets", []):
            if str(item.get("asset", "")).upper() == asset:
                value = _finite(item.get("total", 0))
                if value < 0:
                    raise ValueError("negative spot asset balance")
                return value
        return 0.0

    def _persist_balance_report(self, report: dict) -> None:
        if report.get("status") != "ok":
            return
        market = str(report.get("market", "")).lower()
        if market not in ("spot", "futures"):
            return
        for asset in report.get("assets", []):
            try:
                total = _finite(asset.get("total", 0))
                free = _finite(asset.get("free", 0))
                used = _finite(asset.get("used", 0))
                if min(total, free, used) < 0:
                    raise ValueError("negative balance")
                self.engine.supabase._emit(
                    "balance_snapshots",
                    {
                        "exchange_name": str(report.get("name", "")),
                        "market_type": "perpetual" if market == "futures" else "spot",
                        "asset": str(asset.get("asset", "")),
                        "total": total,
                        "free": free,
                        "used": used,
                        "is_mock": False,
                        "observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "raw_payload": {"source": "periodic_exposure_reconciliation"},
                    },
                )
            except (TypeError, ValueError):
                logger.exception("invalid balance snapshot from %s", report.get("name"))

    async def _mismatch(self, reason: str, exchange: str, message: str, detail: dict) -> None:
        key = f"{reason}:{exchange}:{detail.get('symbol') or detail.get('asset') or ''}"
        self.engine._exchange_halted[exchange] = f"exposure reconciliation: {reason}"
        await self._alert(reason, exchange, message, detail, key=key)

    async def _alert(
        self, reason: str, exchange: str, message: str, detail: dict, key: str = ""
    ) -> None:
        issue_key = key or f"{reason}:{exchange}"
        fingerprint = repr(sorted(detail.items(), key=lambda item: item[0]))
        if self._reported.get(issue_key) == fingerprint:
            return
        self._reported[issue_key] = fingerprint
        logger.critical("[Exposure Reconciliation] %s %s: %s", exchange, reason, message)
        await self.engine._publish_execution_failure(
            f"reconciliation:{exchange}", reason, message,
            detail={"exchange": exchange, **detail},
            state=PositionState.RECONCILIATION_REQUIRED,
        )

    def _clear_issue(self, issue_key: str, exchange: str) -> None:
        self._reported.pop(issue_key, None)
        halt = self.engine._exchange_halted.get(exchange, "")
        if halt.startswith("exposure reconciliation:"):
            self.engine._exchange_halted.pop(exchange, None)
            logger.info("[Exposure Reconciliation] %s mismatch cleared; entry halt released", exchange)
