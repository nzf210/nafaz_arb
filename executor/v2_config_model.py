"""Typed immutable V2 configuration domain models."""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

class V2ConfigValidationError(ValueError):
    """Raised when a V2 snapshot violates its schema or safety contract."""


@dataclass(frozen=True)
class V2VenueModel:
    venue_id: str
    exchange: str
    market_type: str
    account_id: str
    enabled: bool
    market_data_enabled: bool
    execution_enabled: bool
    market_data_adapter: Optional[str]
    execution_adapter: Optional[str]
    credential_refs: tuple[tuple[str, str], ...]
    position_mode: Optional[str]
    margin_mode: Optional[str]
    required_leverage: Optional[Decimal]


@dataclass(frozen=True)
class V2AccountModel:
    account_id: str
    venue_id: str
    label: str
    enabled: bool


@dataclass(frozen=True)
class V2RiskProfileModel:
    profile_id: str
    max_daily_loss_usdt: Decimal
    max_exposure_per_account_usdt: Decimal
    max_exposure_per_route_usdt: Decimal
    max_entry_slippage_pct: Decimal
    max_consecutive_order_failures: int
    max_open_spot_orders: int
    max_open_futures_orders: int
    require_fresh_balance: bool
    require_position_reconciliation: bool


@dataclass(frozen=True)
class V2RouteModel:
    route_id: str
    spot_venue_id: str
    spot_account_id: str
    spot_quote_assets: tuple[str, ...]
    futures_venue_id: str
    futures_account_id: str
    futures_settle_assets: tuple[str, ...]
    allowed_symbols: tuple[str, ...]
    denied_symbols: tuple[str, ...]
    risk_profile: str
    enabled: bool
    capability_verified: bool


@dataclass(frozen=True)
class V2ConfigModel:
    schema_version: int
    mode: str
    execution_mode: str
    signals_enabled: bool
    startup_reconciliation_required: bool
    storage_driver: str
    storage_dsn_env: Optional[str]
    redis_enabled: bool
    redis_url_env: Optional[str]
    scanner_enabled: bool
    scanner_quote_assets: tuple[str, ...]
    venues: tuple[V2VenueModel, ...]
    accounts: tuple[V2AccountModel, ...]
    routes: tuple[V2RouteModel, ...]
    risk_profiles: tuple[V2RiskProfileModel, ...]

    @property
    def risk_profile_ids(self) -> tuple[str, ...]:
        return tuple(profile.profile_id for profile in self.risk_profiles)
