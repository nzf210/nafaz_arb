"""Account fee refresh and entry preflight validation."""

import json
import math
import time

from executor.fee_engine import FeeRates, round_trip_fees_usdt
from executor.engine_common import logger


class EngineFeePreflightMixin:
    """Validates current account fee data before an entry can be submitted."""

    async def _refresh_fee_snapshot(self, pair_tag, payload, directives) -> None:
        if not bool(getattr(self.config, "mock_mode", False)) and hasattr(self.client, "fetch_pair_fee_rates"):
            try:
                cached_profiles = {}
                def fee_cache_key(exchange_name, market_name, symbol):
                    normalized = str(symbol).lower().replace("/", "_").replace("-", "_").replace(":", "_")
                    return f"arb:fee_profile:{exchange_name.lower()}:{market_name}:{normalized}"

                if self.redis is not None:
                    for market_name, exchange_name, symbol in (
                        ("spot", payload.pair_info.local_exchange, payload.pair_info.local_symbol),
                        ("perp", payload.pair_info.global_exchange, payload.pair_info.global_symbol),
                    ):
                        cached_raw = await self.redis.get(
                            fee_cache_key(exchange_name, market_name, symbol)
                        )
                        if not cached_raw:
                            continue
                        try:
                            cached_profile = json.loads(cached_raw)
                            fetched_at = int(cached_profile.get("fetched_at_ms", 0))
                            age_ms = int(time.time() * 1000) - fetched_at
                            if (
                                cached_profile.get("source") == "EXCHANGE_API"
                                and cached_profile.get("verified") is True
                                and 0 <= age_ms <= 300_000
                            ):
                                cached_profiles[market_name] = {
                                    "maker": float(cached_profile["maker_fee_pct"]),
                                    "taker": float(cached_profile["taker_fee_pct"]),
                                    "fetched_at_ms": fetched_at,
                                }
                        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                            logger.warning("[Fee Engine] ignoring malformed cached %s fee profile", market_name)

                fee_snapshot = await self.client.fetch_pair_fee_rates(
                    payload.pair_info, cached_profiles
                )
                spot_rates = fee_snapshot.get("spot")
                perp_rates = fee_snapshot.get("perp")
                maker_taker = str(getattr(self.config, "execution_mode", "TAKER_TAKER")).upper()
                if spot_rates is not None:
                    directives.spot_entry_fee_pct = (
                        spot_rates["maker"] if maker_taker == "MAKER_TAKER" else spot_rates["taker"]
                    )
                    directives.spot_exit_fee_pct = spot_rates["taker"]
                if perp_rates is not None:
                    directives.perp_entry_fee_pct = perp_rates["taker"]
                    directives.perp_exit_fee_pct = perp_rates["taker"]
                directives.fee_source = fee_snapshot["source"]
                directives.fee_verified = bool(fee_snapshot["verified"])
                directives.fee_fetched_at_ms = int(fee_snapshot["fetched_at_ms"])
                if self.redis is not None:
                    if spot_rates is not None:
                        await self.redis.set(
                            f"arb:fee_profile:{payload.pair_info.local_exchange.lower()}:spot:{payload.pair_info.local_symbol.lower().replace(chr(47), chr(95)).replace(chr(45), chr(95)).replace(chr(58), chr(95))}",
                            json.dumps({
                                "maker_fee_pct": spot_rates["maker"],
                                "taker_fee_pct": spot_rates["taker"],
                                "source": "EXCHANGE_API",
                                "verified": True,
                                "fetched_at_ms": int(spot_rates["fetched_at_ms"]),
                            }),
                            ex=300,
                        )
                    if perp_rates is not None:
                        await self.redis.set(
                            f"arb:fee_profile:{payload.pair_info.global_exchange.lower()}:perp:{payload.pair_info.global_symbol.lower().replace(chr(47), chr(95)).replace(chr(45), chr(95)).replace(chr(58), chr(95))}",
                            json.dumps({
                                "maker_fee_pct": perp_rates["maker"],
                                "taker_fee_pct": perp_rates["taker"],
                                "source": "EXCHANGE_API",
                                "verified": True,
                                "fetched_at_ms": int(perp_rates["fetched_at_ms"]),
                            }),
                            ex=300,
                        )
                logger.info(
                    "[Fee Engine] refreshed account fee tiers for %s: spot=%s perp=%s",
                    pair_tag, spot_rates, perp_rates,
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
