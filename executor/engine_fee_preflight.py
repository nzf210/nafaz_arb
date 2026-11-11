"""Account fee refresh and entry preflight validation."""

import json
import math
import time

from executor.fee_engine import FeeRates, round_trip_fees_usdt
from executor.engine_common import logger


class EngineFeePreflightMixin:
    """Validates current account fee data before an entry can be submitted."""

    @staticmethod
    def _fee_profile_cache_key(exchange_name: str, market_name: str, symbol: str) -> str:
        normalized = (
            str(symbol).lower().replace("/", "_").replace("-", "_").replace(":", "_")
        )
        return f"arb:fee_profile:{exchange_name.lower()}:{market_name}:{normalized}"

    async def _load_cached_fee_profile(
        self, exchange_name: str, market_name: str, symbol: str
    ):
        if self.redis is None:
            return None
        raw = await self.redis.get(
            self._fee_profile_cache_key(exchange_name, market_name, symbol)
        )
        if not raw:
            return None
        try:
            profile = json.loads(raw)
            fetched_at = int(profile.get("fetched_at_ms", 0))
            age_ms = int(time.time() * 1000) - fetched_at
            if (
                profile.get("source") != "EXCHANGE_API"
                or profile.get("verified") is not True
                or not 0 <= age_ms <= 300_000
            ):
                return None
            return {
                "maker": float(profile["maker_fee_pct"]),
                "taker": float(profile["taker_fee_pct"]),
                "fetched_at_ms": fetched_at,
            }
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            logger.warning(
                "[Fee Engine] ignoring malformed cached %s fee profile", market_name
            )
            return None

    async def _load_cached_fee_profiles(self, pair_info) -> dict:
        profiles = {}
        for market_name, exchange_name, symbol in (
            ("spot", pair_info.local_exchange, pair_info.local_symbol),
            ("perp", pair_info.global_exchange, pair_info.global_symbol),
        ):
            profile = await self._load_cached_fee_profile(
                exchange_name, market_name, symbol
            )
            if profile is not None:
                profiles[market_name] = profile
        return profiles

    @staticmethod
    def _apply_fee_snapshot(directives, fee_snapshot) -> None:
        spot_rates = fee_snapshot.get("spot")
        perp_rates = fee_snapshot.get("perp")
        execution_mode = str(getattr(self.config, "execution_mode", "TAKER_TAKER")).upper()
        if spot_rates is not None:
            directives.spot_entry_fee_pct = (
                spot_rates["maker"]
                if execution_mode == "MAKER_TAKER"
                else spot_rates["taker"]
            )
            directives.spot_exit_fee_pct = spot_rates["taker"]
        if perp_rates is not None:
            directives.perp_entry_fee_pct = perp_rates["taker"]
            directives.perp_exit_fee_pct = perp_rates["taker"]
        directives.fee_source = fee_snapshot["source"]
        directives.fee_verified = bool(fee_snapshot["verified"])
        directives.fee_fetched_at_ms = int(fee_snapshot["fetched_at_ms"])

    async def _store_fee_profile(
        self, exchange_name: str, market_name: str, symbol: str, rates
    ) -> None:
        if self.redis is None or rates is None:
            return
        await self.redis.set(
            self._fee_profile_cache_key(exchange_name, market_name, symbol),
            json.dumps({
                "maker_fee_pct": rates["maker"],
                "taker_fee_pct": rates["taker"],
                "source": "EXCHANGE_API",
                "verified": True,
                "fetched_at_ms": int(rates["fetched_at_ms"]),
            }),
            ex=300,
        )

    async def _refresh_fee_snapshot(self, pair_tag, payload, directives) -> None:
        if (
            bool(getattr(self.config, "mock_mode", False))
            or not hasattr(self.client, "fetch_pair_fee_rates")
        ):
            return
        try:
            pair_info = payload.pair_info
            cached_profiles = await self._load_cached_fee_profiles(pair_info)
            fee_snapshot = await self.client.fetch_pair_fee_rates(
                pair_info, cached_profiles
            )
            self._apply_fee_snapshot(directives, fee_snapshot)
            await self._store_fee_profile(
                pair_info.local_exchange, "spot", pair_info.local_symbol,
                fee_snapshot.get("spot"),
            )
            await self._store_fee_profile(
                pair_info.global_exchange, "perp", pair_info.global_symbol,
                fee_snapshot.get("perp"),
            )
            logger.info(
                "[Fee Engine] refreshed account fee tiers for %s: spot=%s perp=%s",
                pair_tag, fee_snapshot.get("spot"), fee_snapshot.get("perp"),
            )
        except Exception as fee_error:
            logger.warning(
                "[Fee Engine] authenticated fee refresh unavailable for %s; "
                "using scanner fee snapshot (%s, verified=%s): %s",
                pair_tag, directives.fee_source, directives.fee_verified, fee_error,
            )

    async def _validate_dynamic_fee_edge(self, pair_tag, directives) -> bool:
        if directives.fee_source in ("EXCHANGE_API", "PARTIAL_EXCHANGE_API") and directives.quantity > 0:
            try:
                dynamic_fees = round_trip_fees_usdt(
                    directives.quantity,
                    directives.spot_entry_price_usdt,
                    directives.spot_exit_price_usdt,
                    directives.perp_entry_price_usdt,
                    directives.perp_exit_price_usdt,
                    FeeRates(
                        directives.spot_entry_fee_pct,
                        directives.spot_exit_fee_pct,
                        directives.perp_entry_fee_pct,
                        directives.perp_exit_fee_pct,
                        directives.fee_source,
                        directives.fee_verified,
                        directives.fee_fetched_at_ms,
                        directives.fee_asset_surcharge_pct,
                    ),
                )
                directives.estimated_net_profit_usdt += (
                    directives.estimated_fees_usdt - dynamic_fees
                )
                directives.estimated_fees_usdt = dynamic_fees
                if directives.estimated_net_profit_usdt <= 0:
                    self._mark_failed(
                        pair_tag,
                        "NET_EDGE_ERODED_BY_ACTUAL_FEES",
                        "Entry rejected: authenticated account fee tiers reduce estimated net profit to zero or below.",
                        detail={
                            "fee_source": directives.fee_source,
                            "estimated_fees_usdt": dynamic_fees,
                            "adjusted_net_profit_usdt": directives.estimated_net_profit_usdt,
                        },
                    )
                    await self.publish_status()
                    return False
            except (TypeError, ValueError) as fee_error:
                self._mark_failed(
                    pair_tag,
                    "FEE_REVALIDATION_FAILED",
                    "Entry rejected because the authenticated fee snapshot could not be applied safely.",
                    detail={"error": str(fee_error), "fee_source": directives.fee_source},
                )
                await self.publish_status()
                return False
        return True

    async def _validate_entry_market_rules(self, pair_tag, pair_info) -> bool:
        if pair_info.local_amount_spec is None or pair_info.global_amount_spec is None:
            self._mark_failed(
                pair_tag,
                "MARKET_AMOUNT_RULES_MISSING",
                "Spot and perpetual market amount rules are required; entry was not submitted.",
                detail={
                    "local_symbol": pair_info.local_symbol,
                    "global_symbol": pair_info.global_symbol,
                },
            )
            await self.publish_status()
            return False
        if pair_info.local_amount_spec.unit != "BASE":
            self._mark_failed(
                pair_tag,
                "LOCAL_AMOUNT_UNIT_UNSUPPORTED",
                "Spot market amount rules must be expressed in base-asset units.",
                detail={"local_symbol": pair_info.local_symbol},
            )
            await self.publish_status()
            return False
        return True
