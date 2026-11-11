"""exchange fee balance methods for the exchange facade."""

import math
import time
from typing import Optional, Dict, Any



class ExchangeFeeBalanceMixin:
    """Focused asynchronous exchange adapter operations."""

    async def fetch_pair_fee_rates(self, pair_info: Any, cached_profiles: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Any]:
        """Fetch account-specific rates where the venue exposes a supported endpoint.

        CCXT rates are fractions and are converted to percentage points. Indodax
        currently has no CCXT fetchTradingFee implementation; its explicit
        scanner fee profile is retained while the perpetual venue can still
        refresh independently. Tokocrypto's account endpoint reports bips.
        """
        async def read(exchange: Any, symbol: str, market_kind: str):
            exchange_id = str(getattr(exchange, "id", "") or "").lower()
            resolved = await self._resolve_symbol(exchange, symbol, market_kind)
            cache_key = (exchange_id, market_kind, resolved)
            cached = (cached_profiles or {}).get("spot" if market_kind == "spot" else "perp")
            if cached and cached.get("fetched_at_ms", 0) > 0:
                return cached
            cached = self._fee_cache.get(cache_key)
            if cached and time.time() - cached[0] < 300:
                return cached[1]
            fetcher = getattr(exchange, "fetch_trading_fee", None)
            if exchange_id == "tokocrypto" and market_kind == "spot":
                endpoint = (
                    getattr(exchange, "privateGetOpenV1AccountSpot", None)
                    or getattr(exchange, "private_get_open_v1_account_spot", None)
                )
                if callable(endpoint):
                    raw = await endpoint()
                    account = raw.get("data", {}) if isinstance(raw, dict) else {}
                    if not isinstance(account, dict):
                        raise ValueError("invalid Tokocrypto account fee response")
                    maker_bips = account.get("makerCommission")
                    taker_bips = account.get("takerCommission")
                    if maker_bips is None or taker_bips is None:
                        raise ValueError("Tokocrypto account response omitted commission rates")
                    # Tokocrypto account commission values are basis points (1 bp = 0.01%).
                    maker_pct, taker_pct = float(maker_bips) / 100.0, float(taker_bips) / 100.0
                elif callable(fetcher):
                    raw = await fetcher(resolved)
                    maker, taker = raw.get("maker"), raw.get("taker")
                    if maker is None or taker is None:
                        raise ValueError("incomplete Tokocrypto spot trading fee response")
                    maker_pct, taker_pct = float(maker) * 100.0, float(taker) * 100.0
                else:
                    return None
            elif callable(fetcher):
                raw = await fetcher(resolved)
                if not isinstance(raw, dict):
                    raise ValueError(f"invalid {market_kind} trading fee response")
                maker, taker = raw.get("maker"), raw.get("taker")
                if maker is None or taker is None:
                    raise ValueError(f"incomplete {market_kind} trading fee response")
                maker_pct, taker_pct = float(maker) * 100.0, float(taker) * 100.0
            else:
                return None
            if not all(math.isfinite(v) and 0 <= v <= 100 for v in (maker_pct, taker_pct)):
                raise ValueError(f"invalid {market_kind} fee rates")
            rates = {
                "maker": maker_pct,
                "taker": taker_pct,
                "fetched_at_ms": int(time.time() * 1000),
            }
            self._fee_cache[cache_key] = (time.time(), rates)
            return rates

        errors = {}
        profiles = {}
        for key, exchange, symbol, market_kind in (
            ("spot", self.local, pair_info.local_symbol, "spot"),
            ("perp", self.global_fut, pair_info.global_symbol, "futures"),
        ):
            try:
                profiles[key] = await read(exchange, symbol, market_kind)
            except Exception as exc:
                profiles[key] = None
                errors[key] = f"{type(exc).__name__}: {exc}"

        available = [profile for profile in profiles.values() if profile is not None]
        return {
            "spot": profiles["spot"],
            "perp": profiles["perp"],
            "source": "EXCHANGE_API" if len(available) == 2 else ("PARTIAL_EXCHANGE_API" if available else "CONFIG"),
            "verified": len(available) == 2,
            "fetched_at_ms": max((int(profile.get("fetched_at_ms", 0)) for profile in available), default=0),
            "errors": errors,
        }

    async def fetch_balance_report(self) -> list:
        """Fetch read-only balances for every configured venue independently."""
        reports = []
        for name, entry in sorted(self.balance_exchanges.items()):
            market = str(entry.get("market") or "unknown")
            exchange = entry.get("client")
            init_error = str(entry.get("error") or "")
            if exchange is None:
                reports.append({
                    "name": name, "market": market,
                    "status": "not_configured" if "not configured" in init_error.lower() else "error",
                    "error": init_error or "exchange client is unavailable", "assets": [],
                })
                continue
            try:
                raw = await _fetch_exchange_balance(exchange, market)
                free_map, used_map, total_map = raw.get("free") or {}, raw.get("used") or {}, raw.get("total") or {}
                assets = []
                for asset in sorted(set(free_map) | set(used_map) | set(total_map)):
                    free_raw, used_raw, total_raw = free_map.get(asset), used_map.get(asset), total_map.get(asset)
                    free = float(free_raw or 0.0)
                    total = float(total_raw) if total_raw is not None else 0.0
                    used = float(used_raw) if used_raw is not None else max(0.0, total - free)
                    if total_raw is None: total = free + used
                    if not all(math.isfinite(v) and v >= 0 for v in (free, used, total)):
                        raise ValueError(f"invalid balance value for {asset}")
                    if free or used or total:
                        assets.append({"asset": str(asset), "free": free, "used": used, "total": total})
                reports.append({"name": name, "market": market, "status": "ok", "assets": assets})
            except Exception as exc:
                reports.append({"name": name, "market": market, "status": "error", "error": str(exc), "assets": []})
        return reports

    async def get_balances(
        self,
        local_quote_asset: str = "IDR",
        global_quote_asset: str = "USDT",
        usdt_idr_rate: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Fetches normalized available balances for entry sizing and risk checks.

        Local IDR is converted using the live USDT/IDR rate supplied by the
        scanner snapshot. USDT and USDC are treated as stable quote units for
        sizing; unsupported quote assets fail closed rather than guessing.
        """
        local_quote = str(local_quote_asset or "").upper()
        global_quote = str(global_quote_asset or "").upper()
        supported = ("IDR", "USDT", "USDC")
        if local_quote not in supported or global_quote not in ("USDT", "USDC"):
            raise ValueError(
                f"unsupported balance quote conversion: local={local_quote}, global={global_quote}"
            )

        spot_raw = await self.local.fetch_balance()
        futures_raw = await _fetch_exchange_balance(self.global_fut, "futures")

        def amount(balance: Dict[str, Any], asset: str, bucket: str) -> float:
            section = balance.get(bucket)
            if isinstance(section, dict) and asset in section:
                value = float(section.get(asset) or 0.0)
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"exchange returned invalid {asset} {bucket} balance")
                return value
            per_asset = balance.get(asset)
            if isinstance(per_asset, dict):
                value = float(per_asset.get(bucket) or 0.0)
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"exchange returned invalid {asset} {bucket} balance")
                return value
            value = float(section.get(asset) or 0.0) if isinstance(section, dict) else 0.0
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"exchange returned invalid {asset} {bucket} balance")
            return value

        spot_free = amount(spot_raw, local_quote, "free")
        spot_total = amount(spot_raw, local_quote, "total")
        if local_quote == "IDR":
            if not math.isfinite(float(usdt_idr_rate)) or usdt_idr_rate <= 0:
                raise ValueError("live USDT/IDR rate is required to normalize IDR spot balance")
            spot_free /= usdt_idr_rate
            spot_total /= usdt_idr_rate

        futures_free = amount(futures_raw, global_quote, "free")
        futures_total = amount(futures_raw, global_quote, "total")

        return {
            "spot_available": spot_free,
            "futures_available": futures_free,
            "spot_total": spot_total,
            "futures_total": futures_total,
            "spot_quote_asset": local_quote,
            "futures_quote_asset": global_quote,
            "stablecoin_peg_assumed": local_quote == "USDC" or global_quote == "USDC",
        }
