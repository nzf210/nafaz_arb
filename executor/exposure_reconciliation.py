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
from typing import Any, Dict, Iterable

from executor.models import PositionState

logger = logging.getLogger("executor.reconciliation")


def _finite(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite exchange quantity")
    return number


def _env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        # Blank (e.g. compose `${VAR:-}`) means unset, not invalid.
        value = float(os.getenv(name) or default)
    except (TypeError, ValueError):
        logger.warning("invalid %s; using default %s", name, default)
        return default
    if not math.isfinite(value):
        logger.warning("non-finite %s; using default %s", name, default)
        return default
    return min(maximum, max(minimum, value))

def _env_interval() -> int:
    try:
        value = int(os.getenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS") or "300")
    except (TypeError, ValueError):
        logger.warning("invalid reconciliation interval; using 300 seconds")
        return 300
    return max(60, value)


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
            contract_size_raw = position.get("contractSize")
            if contract_size_raw is None:
                raise ValueError("futures contract size unavailable")
            contract_size = _finite(contract_size_raw)
            if contract_size <= 0:
                raise ValueError("invalid futures contract size")
            qty = abs(_finite(contracts)) * contract_size
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
        self.interval = _env_interval()
        self.relative_tolerance = _env_float(
            "EXPOSURE_RECONCILIATION_REL_TOLERANCE", 0.005, 0.0, 0.05
        )
        self.absolute_tolerance = _env_float(
            "EXPOSURE_RECONCILIATION_ABS_TOLERANCE", 1e-8, 1e-12, 1.0
        )
        self._reported: Dict[str, str] = {}
        self._previous_halts: Dict[str, str] = {}

    def _tolerance(self, expected: float) -> float:
        return max(self.absolute_tolerance, abs(expected) * self.relative_tolerance)

    async def run(self) -> None:
        """Run periodically after the synchronous startup reconciliation."""
        while True:
            await asyncio.sleep(self.interval)
            try:
                if not self.engine.config.mock_mode:
                    await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("periodic exchange exposure reconciliation failed")

    async def reconcile_once(self) -> None:
        client = self.engine.client
        report_fn = getattr(client, "fetch_balance_report", None)
        if not callable(report_fn):
            await self._halt_active_venues(
                "EXPOSURE_BALANCE_UNAVAILABLE",
                "Exchange balance report API is unavailable; exposure reconciliation is incomplete.",
                {"reason": "fetch_balance_report_not_supported"},
            )
            return

        try:
            reports = await report_fn()
        except Exception as exc:
            await self._halt_active_venues(
                "EXPOSURE_BALANCE_UNAVAILABLE",
                "Exchange balance report request failed; exposure reconciliation is incomplete.",
                {"error": str(exc)},
            )
            return
        report_map = {}
        for report in reports or []:
            if isinstance(report, dict):
                report_map[(str(report.get("name", "")).lower(), str(report.get("market", "")).lower())] = report
                self._persist_balance_report(report)

        spot_expected = defaultdict(float)
        futures_expected = defaultdict(float)
        spot_pairs = defaultdict(list)
        futures_pairs = defaultdict(list)
        missing_mapping_venues = set()
        invalid_qty_venues = set()
        for pair_key, position in self.engine.active_positions.items():
            pair_info = self.engine._pair_infos.get(pair_key)
            try:
                expected_qty = _finite(getattr(position, "qty", 0))
            except (TypeError, ValueError):
                expected_qty = 0.0
                invalid_qty = True
            else:
                invalid_qty = expected_qty <= 0
            if invalid_qty:
                known_venues = [
                    str(getattr(position, "local_exchange", "") or getattr(pair_info, "local_exchange", "")).strip().lower(),
                    str(getattr(position, "global_exchange", "") or getattr(pair_info, "global_exchange", "")).strip().lower(),
                ]
                if not any(known_venues):
                    known_venues = [
                        str(getattr(self.engine.config, "local_exchange_name", "")).strip().lower(),
                        str(getattr(self.engine.config, "global_exchange_name", "")).strip().lower(),
                    ]
                for exchange in [venue for venue in known_venues if venue] or ["unknown"]:
                    invalid_qty_venues.add(exchange)
                    self._set_halt(exchange, "active position quantity invalid")
                    await self._alert(
                        "EXPOSURE_EXPECTED_QTY_INVALID", exchange,
                        f"Cannot reconcile active position {pair_key}: managed quantity is invalid.",
                        {"pair_key": pair_key, "qty": getattr(position, "qty", None)},
                    )
                continue
            local_name = str(
                getattr(position, "local_exchange", "")
                or getattr(pair_info, "local_exchange", "")
            ).strip().lower()
            global_name = str(
                getattr(position, "global_exchange", "")
                or getattr(pair_info, "global_exchange", "")
            ).strip().lower()
            base_asset = str(
                getattr(position, "base_asset", "")
                or getattr(pair_info, "base_asset", "")
            ).strip().upper()
            global_symbol = str(
                getattr(position, "global_symbol", "")
                or getattr(pair_info, "global_symbol", "")
            ).strip()
            if not local_name or not global_name or not base_asset or not global_symbol:
                missing_venues = [name for name in (local_name, global_name) if name]
                if not missing_venues:
                    missing_venues = [
                        str(getattr(self.engine.config, "local_exchange_name", "")).strip().lower(),
                        str(getattr(self.engine.config, "global_exchange_name", "")).strip().lower(),
                    ]
                for exchange in [venue for venue in missing_venues if venue] or ["unknown"]:
                    missing_mapping_venues.add(exchange)
                    self._set_halt(
                        exchange, "active position mapping unavailable"
                    )
                    await self._alert(
                        "EXPOSURE_MAPPING_UNAVAILABLE", exchange,
                        f"Cannot reconcile restored position {pair_key}: exchange/symbol mapping is incomplete.",
                        {"pair_key": pair_key, "base_asset": base_asset,
                         "global_symbol": global_symbol},
                    )
                continue
            spot_expected[(local_name, base_asset)] += expected_qty
            spot_pairs[(local_name, base_asset)].append(pair_key)
            futures_expected[(global_name, global_symbol)] += expected_qty
            futures_pairs[(global_name, global_symbol)].append(pair_key)

        active_spot_keys = set(spot_expected)
        active_futures_keys = set(futures_expected)
        active_spot_venues = {exchange for exchange, _ in active_spot_keys}
        active_futures_venues = {exchange for exchange, _ in active_futures_keys}
        for issue_key in list(self._reported):
            reason, _, remainder = issue_key.partition(":")
            exchange, _, identifier = remainder.partition(":")
            if not exchange:
                continue
            if reason == "EXPOSURE_MAPPING_UNAVAILABLE" and exchange not in missing_mapping_venues:
                self._clear_issue(issue_key, exchange)
            elif reason == "EXPOSURE_EXPECTED_QTY_INVALID" and exchange not in invalid_qty_venues:
                self._clear_issue(issue_key, exchange)
            elif reason in ("EXPOSURE_BALANCE_UNAVAILABLE", "EXPOSURE_BALANCE_INVALID") and exchange not in active_spot_venues:
                self._clear_issue(issue_key, exchange)
            elif reason == "EXPOSURE_POSITION_UNAVAILABLE" and exchange not in active_futures_venues:
                self._clear_issue(issue_key, exchange)
            elif reason == "SPOT_BASE_BALANCE_DEFICIT" and (exchange, identifier) not in active_spot_keys:
                self._clear_issue(issue_key, exchange)
            elif reason == "FUTURES_POSITION_MISMATCH" and (exchange, identifier) not in active_futures_keys:
                self._clear_issue(issue_key, exchange)

        for (exchange, asset), expected in spot_expected.items():
            report = report_map.get((exchange, "spot"))
            if not report or report.get("status") != "ok":
                self._set_halt(exchange, "spot balance unavailable")
                await self._alert(
                    "EXPOSURE_BALANCE_UNAVAILABLE", exchange,
                    f"Cannot verify {asset} spot balance on {exchange}.",
                    {"asset": asset, "pair_keys": spot_pairs[(exchange, asset)]},
                )
                continue
            self._clear_issue(f"EXPOSURE_BALANCE_UNAVAILABLE:{exchange}", exchange)
            try:
                actual = self._asset_total(report, asset)
            except (TypeError, ValueError) as exc:
                self._set_halt(exchange, "malformed spot balance")
                await self._alert(
                    "EXPOSURE_BALANCE_INVALID", exchange,
                    f"Exchange returned an invalid {asset} spot balance.",
                    {"asset": asset, "error": str(exc),
                     "pair_keys": spot_pairs[(exchange, asset)]},
                )
                continue
            self._clear_issue(f"EXPOSURE_BALANCE_INVALID:{exchange}", exchange)
            if actual + self._tolerance(expected) < expected:
                await self._mismatch(
                    "SPOT_BASE_BALANCE_DEFICIT", exchange,
                    f"Spot {asset} balance is below NAFAZ managed quantity on {exchange}.",
                    {"asset": asset, "expected_managed_qty": expected, "actual_total_qty": actual,
                     "pair_keys": spot_pairs[(exchange, asset)]},
                )
            else:
                self._clear_issue(f"SPOT_BASE_BALANCE_DEFICIT:{exchange}:{asset}", exchange)

        # Reconcile every configured futures account, not only symbols already
        # represented in active_positions. Manual or orphan positions on another
        # contract are still real account exposure and must block new entries.
        base_client = getattr(self.engine, "_base_client", client)
        venue_inventory = getattr(base_client, "balance_exchanges", {}) or {}
        configured_futures = {
            str(name).strip().lower()
            for name, entry in venue_inventory.items()
            if isinstance(entry, dict)
            and str(entry.get("market") or "").lower() == "futures"
            and entry.get("client") is not None
        }
        configured_futures.update(active_futures_venues)
        default_global = str(
            getattr(self.engine.config, "global_exchange_name", "") or ""
        ).strip().lower()
        if default_global:
            configured_futures.add(default_global)

        def normalize_position_symbol(value: str) -> str:
            return (
                str(value).split(":")[0]
                .replace("/", "")
                .replace("-", "")
                .replace("_", "")
                .upper()
            )

        expected_by_venue = defaultdict(dict)
        expected_labels = defaultdict(dict)
        for (exchange, symbol), expected in futures_expected.items():
            symbol_key = normalize_position_symbol(symbol)
            expected_by_venue[exchange][symbol_key] = (
                expected_by_venue[exchange].get(symbol_key, 0.0) + expected
            )
            expected_labels[exchange][symbol_key] = symbol

        for exchange in sorted(configured_futures):
            try:
                venue_report = report_map.get((exchange, "futures"))
                if not venue_report or venue_report.get("status") != "ok":
                    raise RuntimeError("configured futures venue balance report unavailable")

                venue_client = None
                resolver = getattr(base_client, "for_venue", None)
                if callable(resolver):
                    venue_client = resolver(exchange)
                if venue_client is None:
                    candidate = getattr(client, "global_fut", None)
                    candidate_name = str(getattr(candidate, "id", "") or "").lower()
                    if candidate is not None and (not candidate_name or candidate_name == exchange):
                        venue_client = client
                    elif callable(getattr(client, "fetch_all_positions", None)):
                        venue_client = client
                if venue_client is None:
                    raise RuntimeError(f"authenticated futures client unavailable for {exchange}")

                fetch_all = getattr(venue_client, "fetch_all_positions", None)
                if not callable(fetch_all):
                    raise RuntimeError("account-wide futures position endpoint is unavailable")
                positions = await fetch_all()
                if positions is None or not isinstance(positions, (list, tuple)):
                    raise ValueError("account-wide futures positions response is malformed")

                actual_by_symbol = defaultdict(float)
                actual_labels = {}
                for position in positions:
                    if not isinstance(position, dict):
                        raise ValueError("account-wide futures position row is malformed")
                    symbol = position.get("symbol")
                    if not isinstance(symbol, str) or not symbol.strip():
                        raise ValueError("account-wide futures position is missing symbol")
                    symbol_key = normalize_position_symbol(symbol)
                    if not symbol_key:
                        raise ValueError("account-wide futures position symbol is invalid")
                    qty = _position_short_qty([position])
                    if qty > 0:
                        actual_by_symbol[symbol_key] += qty
                        actual_labels[symbol_key] = symbol

            except Exception as exc:
                self._set_halt(exchange, "account-wide futures positions unavailable")
                await self._alert(
                    "EXPOSURE_POSITION_UNAVAILABLE", exchange,
                    f"Cannot enumerate all futures positions on {exchange}; account exposure is unknown.",
                    {"error": str(exc)},
                )
                continue

            self._clear_issue(f"EXPOSURE_POSITION_UNAVAILABLE:{exchange}", exchange)
            # A complete account-wide snapshot supersedes all previous per-symbol
            # mismatch reports for this venue; re-add any mismatch still present.
            for issue_key in list(self._reported):
                if issue_key.startswith(f"FUTURES_POSITION_MISMATCH:{exchange}:"):
                    self._clear_issue(issue_key, exchange)

            symbol_keys = (
                set(expected_by_venue.get(exchange, {}))
                | set(actual_by_symbol)
            )
            for symbol_key in sorted(symbol_keys):
                expected = expected_by_venue.get(exchange, {}).get(symbol_key, 0.0)
                actual = actual_by_symbol.get(symbol_key, 0.0)
                tolerance = self._tolerance(expected)
                if abs(actual - expected) > tolerance:
                    symbol = (
                        expected_labels.get(exchange, {}).get(symbol_key)
                        or actual_labels.get(symbol_key)
                        or symbol_key
                    )
                    await self._mismatch(
                        "FUTURES_POSITION_MISMATCH", exchange,
                        f"Futures short quantity does not match NAFAZ managed exposure on {exchange}.",
                        {
                            "symbol": symbol,
                            "expected_short_qty": expected,
                            "actual_short_qty": actual,
                            "pair_keys": futures_pairs.get((exchange, symbol), []),
                            "account_wide": True,
                        },
                    )

    async def _halt_active_venues(
        self, reason: str, message: str, detail: dict
    ) -> None:
        for pair_key, position in self.engine.active_positions.items():
            pair_info = self.engine._pair_infos.get(pair_key)
            venues = {
                str(getattr(position, "local_exchange", "") or getattr(pair_info, "local_exchange", "")).strip().lower(),
                str(getattr(position, "global_exchange", "") or getattr(pair_info, "global_exchange", "")).strip().lower(),
            }
            if not any(venues):
                venues = {
                    str(getattr(self.engine.config, "local_exchange_name", "")).strip().lower(),
                    str(getattr(self.engine.config, "global_exchange_name", "")).strip().lower(),
                }
            for exchange in [venue for venue in venues if venue] or ["unknown"]:
                self._set_halt(exchange, reason)
                await self._alert(
                    reason, exchange, message,
                    {"pair_key": pair_key, **detail},
                )

    @staticmethod
    def _asset_total(report: dict, asset: str) -> float:
        assets = report.get("assets", [])
        if not isinstance(assets, list):
            raise ValueError("exchange balance assets payload is not a list")
        for item in assets:
            if not isinstance(item, dict):
                raise ValueError("exchange balance asset row is malformed")
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
        assets = report.get("assets", [])
        if not isinstance(assets, list):
            logger.error("invalid balance assets payload from %s", report.get("name"))
            return
        for asset in assets:
            if not isinstance(asset, dict):
                logger.error("invalid balance asset row from %s", report.get("name"))
                continue
            try:
                total = _finite(asset.get("total", 0))
                free = _finite(asset.get("free", 0))
                used = _finite(asset.get("used", 0))
                if min(total, free, used) < 0:
                    raise ValueError("negative balance")
                self.engine.supabase.balance_snapshot(
                    exchange=str(report.get("name", "")),
                    market_type="perpetual" if market == "futures" else "spot",
                    asset=str(asset.get("asset", "")),
                    total=total,
                    free=free,
                    used=used,
                    is_mock=False,
                    raw_payload={"source": "periodic_exposure_reconciliation"},
                )
            except (TypeError, ValueError):
                logger.exception("invalid balance snapshot from %s", report.get("name"))

    async def _mismatch(self, reason: str, exchange: str, message: str, detail: dict) -> None:
        key = f"{reason}:{exchange}:{detail.get('symbol') or detail.get('asset') or ''}"
        self._set_halt(exchange, reason)
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

    def _set_halt(self, exchange: str, reason: str) -> None:
        current = self.engine._exchange_halted.get(exchange, "")
        if not current.startswith("exposure reconciliation:"):
            self._previous_halts[exchange] = current
        self.engine._exchange_halted[exchange] = f"exposure reconciliation: {reason}"

    def _clear_issue(self, issue_key: str, exchange: str) -> None:
        self._reported.pop(issue_key, None)
        still_mismatched = any(
            len(parts := key.split(":")) > 1 and parts[1] == exchange
            for key in self._reported
        )
        halt = self.engine._exchange_halted.get(exchange, "")
        if halt.startswith("exposure reconciliation:") and not still_mismatched:
            previous = self._previous_halts.pop(exchange, "")
            if previous:
                self.engine._exchange_halted[exchange] = previous
            else:
                self.engine._exchange_halted.pop(exchange, None)
            logger.info("[Exposure Reconciliation] %s reconciliation issue cleared", exchange)
