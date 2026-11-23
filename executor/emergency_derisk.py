"""Redis-backed emergency ownership for risk-reducing orders during DB outages."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger(__name__)

_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class EmergencyDeriskError(RuntimeError):
    """Emergency de-risk ownership could not be established safely."""


@dataclass
class EmergencyDeriskLease:
    redis: Any
    key: str
    token: str
    owner_id: str
    client_order_id: str
    scope_hash: str
    ttl_ms: int

    @classmethod
    async def acquire(
        cls, redis: Any, *, owner_id: str, designated_owner_id: str,
        client_order_id: str, account_scope_id: str, venue: str,
        market_type: str, instrument: str, ttl_ms: int = 120000,
    ) -> "EmergencyDeriskLease":
        owner = str(owner_id or "").strip()
        designated = str(designated_owner_id or "").strip()
        client_id = str(client_order_id or "").strip()
        scope_parts = (
            str(account_scope_id or "").strip(),
            str(venue or "").strip().lower(),
            str(market_type or "").strip().lower(),
            str(instrument or "").strip().upper(),
        )
        if not owner or not designated or owner != designated:
            raise EmergencyDeriskError(
                "this executor is not the designated emergency de-risk owner"
            )
        if not client_id or len(client_id) > 128:
            raise EmergencyDeriskError("stable client_order_id is required")
        if any(not part for part in scope_parts):
            raise EmergencyDeriskError("complete account/venue/market/instrument scope is required")
        if redis is None:
            raise EmergencyDeriskError("Redis is unavailable for emergency de-risk ownership")
        if int(ttl_ms) < 1000 or int(ttl_ms) > 300000:
            raise EmergencyDeriskError("emergency de-risk lease TTL must be between 1s and 5m")

        canonical_scope = "|".join(scope_parts)
        scope_hash = hashlib.sha256(canonical_scope.encode("utf-8")).hexdigest()
        key = f"nafaz:executor:emergency-derisk:{scope_hash}"
        token = f"{owner}:{client_id}"
        try:
            acquired = await redis.set(key, token, nx=True, px=int(ttl_ms))
        except Exception as exc:
            raise EmergencyDeriskError(
                f"could not acquire Redis emergency ownership: {exc}"
            ) from exc
        if acquired not in (True, "OK", b"OK"):
            raise EmergencyDeriskError(
                f"another emergency de-risk mutation owns scope {scope_hash[:12]}"
            )
        return cls(
            redis=redis,
            key=key,
            token=token,
            owner_id=owner,
            client_order_id=client_id,
            scope_hash=scope_hash,
            ttl_ms=int(ttl_ms),
        )

    async def release(self) -> None:
        try:
            await self.redis.eval(_RELEASE_SCRIPT, 1, self.key, self.token)
        except Exception:
            # Never issue an unconditional DEL: the lease may have expired and
            # been acquired by another process. The bounded TTL is the fallback.
            logger.exception(
                "Could not release emergency de-risk lease for %s; TTL will expire",
                self.client_order_id,
            )

    async def __aenter__(self) -> "EmergencyDeriskLease":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()
