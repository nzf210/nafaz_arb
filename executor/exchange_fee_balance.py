"""exchange fee balance methods for the exchange facade."""

import math
import time
from typing import Optional, Dict, Any

from executor.exchange_common import _fetch_exchange_balance
from executor.venues import get_venue



class ExchangeFeeBalanceMixin:
    """Focused asynchronous exchange adapter operations."""

    async def _read_tokocrypto_spot_fee(self, exchange, resolved: str):
        return await get_venue("tokocrypto").fetch_fee(exchange, resolved)

    async def _read_ccxt_fee(self, exchange, resolved: str, market_kind: str):
        fetcher = getattr(exchange, "fetch_trading_fee", None)
        if not callable(fetcher):
            return None
        raw = await fetcher(resolved)
        if not isinstance(raw, dict):
            raise ValueError(f"invalid {market_kind} trading fee response")
        maker, taker = raw.get("maker"), raw.get("taker")
        if maker is None or taker is None:
            raise ValueError(f"incomplete {market_kind} trading fee response")
        return float(maker) * 100.0, float(taker) * 100.0

    async def _read_exchange_fee_profile(
        self, exchange: Any, symbol: str, market_kind: str,
        cached_profiles: Optional[Dict[str, Dict[str, float]]],
    ):
        exchange_id = str(getattr(exchange, "id", "") or "").lower()
        resolved = await self._resolve_symbol(exchange, symbol, market_kind)
        cache_key = (exchange_id, market_kind, resolved)
        cached = (cached_profiles or {}).get(
            "spot" if market_kind == "spot" else "perp"
        )
        if cached and cached.get("fetched_at_ms", 0) > 0:
            return cached
        cached = self._fee_cache.get(cache_key)
        if cached and time.time() - cached[0] < 300:
            return cached[1]

        if exchange_id == "tokocrypto" and market_kind == "spot":
            rates = await self._read_tokocrypto_spot_fee(exchange, resolved)
        else:
            rates = await self._read_ccxt_fee(exchange, resolved, market_kind)
        if rates is None:
            return None
        maker_pct, taker_pct = rates
        if not all(
            math.isfinite(value) and 0 <= value <= 100
            for value in (maker_pct, taker_pct)
        ):
            raise ValueError(f"invalid {market_kind} fee rates")
        profile = {
            "maker": maker_pct,
            "taker": taker_pct,
            "fetched_at_ms": int(time.time() * 1000),
        }
        self._fee_cache[cache_key] = (time.time(), profile)
        return profile

    async def fetch_pair_fee_rates(
        self, pair_info: Any,
        cached_profiles: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> Dict[str, Any]:
        """Fetch account-specific fee rates independently for spot and perpetual."""
        errors = {}
        profiles = {}
        venues = (
            ("spot", self.local, pair_info.local_symbol, "spot"),
            ("perp", self.global_fut, pair_info.global_symbol, "futures"),
        )
        for key, exchange, symbol, market_kind in venues:
            try:
                profiles[key] = await self._read_exchange_fee_profile(
                    exchange, symbol, market_kind, cached_profiles
                )
            except Exception as exc:
                profiles[key] = None
                errors[key] = f"{type(exc).__name__}: {exc}"
        available = [
            profile for profile in profiles.values() if profile is not None
        ]
        source = "CONFIG"
        if len(available) == 2:
            source = "EXCHANGE_API"
        elif available:
            source = "PARTIAL_EXCHANGE_API"
        return {
            "spot": profiles["spot"],
            "perp": profiles["perp"],
            "source": source,
            "verified": len(available) == 2,
            "fetched_at_ms": max(
                (int(profile.get("fetched_at_ms", 0)) for profile in available),
                default=0,
            ),
            "errors": errors,
        }

    @staticmethod
    @staticmethod
    def _balance_report_asset(
        asset: str, free_map: dict, used_map: dict, total_map: dict
    ):
        free_raw = free_map.get(asset)
        used_raw = used_map.get(asset)
        total_raw = total_map.get(asset)
        free = float(free_raw or 0.0)
        total = float(total_raw) if total_raw is not None else 0.0
        used = (
            float(used_raw)
            if used_raw is not None
            else max(0.0, total - free)
        )
        if total_raw is None:
            total = free + used
        if not all(
            math.isfinite(value) and value >= 0
            for value in (free, used, total)
        ):
            raise ValueError(f"invalid balance value for {asset}")
        if not (free or used or total):
            return None
        return {
            "asset": str(asset),
            "free": free,
            "used": used,
            "total": total,
        }

    @staticmethod
    def _balance_report_assets(raw: Dict[str, Any]) -> list:
        free_map = raw.get("free") or {}
        used_map = raw.get("used") or {}
        total_map = raw.get("total") or {}
        assets = []
        for asset in sorted(set(free_map) | set(used_map) | set(total_map)):
            row = ExchangeFeeBalanceMixin._balance_report_asset(
                asset, free_map, used_map, total_map
            )
            if row is not None:
                assets.append(row)
        return assets

    async def _fetch_balance_report_for_exchange(
        self, name: str, entry: dict
    ) -> dict:
        market = str(entry.get("market") or "unknown")
        exchange = entry.get("client")
        init_error = str(entry.get("error") or "")
        if exchange is None:
            status = (
                "not_configured"
                if "not configured" in init_error.lower()
                else "error"
            )
            return {
                "name": name,
                "market": market,
                "status": status,
                "error": init_error or "exchange client is unavailable",
                "assets": [],
            }
        try:
            raw = await _fetch_exchange_balance(exchange, market)
            assets = self._balance_report_assets(raw)
            return {
                "name": name, "market": market, "status": "ok", "assets": assets
            }
        except Exception as exc:
            return {
                "name": name, "market": market, "status": "error",
                "error": str(exc), "assets": [],
            }

    async def fetch_balance_report(self) -> list:
        """Fetch read-only balances for every configured venue independently."""
        reports = []
        for name, entry in sorted(self.balance_exchanges.items()):
            reports.append(
                await self._fetch_balance_report_for_exchange(name, entry)
            )
        return reports

    @staticmethod
    def _balance_amount(
        balance: Dict[str, Any], asset: str, bucket: str
    ) -> float:
        section = balance.get(bucket)
        if isinstance(section, dict) and asset in section:
            value = float(section.get(asset) or 0.0)
            if not math.isfinite(value) or value < 0:
                raise ValueError(
                    f"exchange returned invalid {asset} {bucket} balance"
                )
            return value
        per_asset = balance.get(asset)
        if isinstance(per_asset, dict):
            value = float(per_asset.get(bucket) or 0.0)
            if not math.isfinite(value) or value < 0:
                raise ValueError(
                    f"exchange returned invalid {asset} {bucket} balance"
                )
            return value
        value = (
            float(section.get(asset) or 0.0)
            if isinstance(section, dict) else 0.0
        )
        if not math.isfinite(value) or value < 0:
            raise ValueError(
                f"exchange returned invalid {asset} {bucket} balance"
            )
        return value

    async def get_balances(
        self,
        local_quote_asset: str = "IDR",
        global_quote_asset: str = "USDT",
        usdt_idr_rate: float = 0.0,
    ) -> Dict[str, Any]:
        """Fetch normalized balances using only supported quote conversions."""
        local_quote = str(local_quote_asset or "").upper()
        global_quote = str(global_quote_asset or "").upper()
        if local_quote not in ("IDR", "USDT", "USDC") or global_quote not in (
            "USDT", "USDC"
        ):
            raise ValueError(
                f"unsupported balance quote conversion: local={local_quote}, global={global_quote}"
            )

        spot_raw = await self.local.fetch_balance()
        futures_raw = await _fetch_exchange_balance(self.global_fut, "futures")
        spot_free = self._balance_amount(spot_raw, local_quote, "free")
        spot_total = self._balance_amount(spot_raw, local_quote, "total")
        if local_quote == "IDR":
            if not math.isfinite(float(usdt_idr_rate)) or usdt_idr_rate <= 0:
                raise ValueError(
                    "live USDT/IDR rate is required to normalize IDR spot balance"
                )
            spot_free /= usdt_idr_rate
            spot_total /= usdt_idr_rate

        futures_free = self._balance_amount(futures_raw, global_quote, "free")
        futures_total = self._balance_amount(futures_raw, global_quote, "total")
        return {
            "spot_available": spot_free,
            "futures_available": futures_free,
            "spot_total": spot_total,
            "futures_total": futures_total,
            "spot_quote_asset": local_quote,
            "futures_quote_asset": global_quote,
            "stablecoin_peg_assumed": (
                local_quote == "USDC" or global_quote == "USDC"
            ),
        }
