"""Registri identitas rute dan klien executor yang tidak dapat diubah.

Satu rute diidentifikasi oleh kedua kaki transaksi. Simbol spot saja tidak
cukup karena satu pasar spot dapat dipasangkan dengan beberapa venue futures.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional


SUPPORTED_SPOT_VENUES = frozenset({"tokocrypto", "indodax"})
SUPPORTED_FUTURES_VENUES = frozenset({"okx", "mexc", "binance", "bybit"})


def _normalize(value: str, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


# Scanner menambahkan awalan berikut agar simbol dari adaptor berbeda tetap unik.
# Awalan ini hanya namespace internal venue, bukan bagian dari kode aset.
_VENUE_SYMBOL_PREFIXES = {
    "tokocrypto": ("TOKO:",),
    "mexc": ("MEXC:",),
}
_SUPPORTED_QUOTES = (
    "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BIDR", "IDRT", "IDR",
    "USD", "BTC", "ETH", "BNB",
)


def _normalize_symbol(value: str, field: str, venue: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError(f"{field} must be a qualified BASE/QUOTE symbol")

    # Hapus hanya awalan namespace milik adaptor untuk venue yang sesuai.
    # Jangan menghapus teks sembarang sebelum tanda titik dua karena bisa berupa aset asli.
    for prefix in _VENUE_SYMBOL_PREFIXES.get(venue, ()):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
            break

    # ID instrumen SWAP asli OKX berbentuk BASE-QUOTE-SWAP. SWAP adalah jenis
    # kontrak, bukan komponen aset ketiga.
    for suffix in ("-SWAP", "_SWAP", "/SWAP"):
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
            break

    normalized = normalized.replace("-", "/").replace("_", "/")
    if "/" not in normalized:
        # Binance/Bybit dapat menampilkan simbol ringkas (BTCUSDT); pisahkan hanya
        # berdasarkan akhiran kuotasi yang dikenal, dari yang terpanjang.
        for quote in _SUPPORTED_QUOTES:
            if normalized.endswith(quote) and len(normalized) > len(quote):
                normalized = f"{normalized[:-len(quote)]}/{quote}"
                break

    if "/" not in normalized:
        raise ValueError(f"{field} must be a qualified BASE/QUOTE symbol")
    base, quote = normalized.split("/", 1)
    if not base or not quote or "/" in quote:
        raise ValueError(f"{field} must contain exactly one BASE/QUOTE pair")
    return f"{base}/{quote}"


@dataclass(frozen=True, slots=True)
class ExchangePairKey:
    local_exchange: str
    local_symbol: str
    global_exchange: str
    global_symbol: str

    @classmethod
    def create(
        cls, local_exchange: str, local_symbol: str,
        global_exchange: str, global_symbol: str,
    ) -> "ExchangePairKey":
        key = cls(
            _normalize(local_exchange, "local_exchange"),
            _normalize_symbol(local_symbol, "local_symbol", _normalize(local_exchange, "local_exchange")),
            _normalize(global_exchange, "global_exchange"),
            _normalize_symbol(global_symbol, "global_symbol", _normalize(global_exchange, "global_exchange")),
        )
        if key.local_exchange not in SUPPORTED_SPOT_VENUES:
            raise ValueError(f"unsupported spot venue: {key.local_exchange}")
        if key.global_exchange not in SUPPORTED_FUTURES_VENUES:
            raise ValueError(f"unsupported futures venue: {key.global_exchange}")
        local_base = key.local_symbol.split("/", 1)[0]
        global_base = key.global_symbol.split("/", 1)[0]
        if local_base != global_base:
            raise ValueError(
                f"base asset mismatch: spot={local_base}, futures={global_base}"
            )
        return key

    @classmethod
    def from_pair_info(cls, pair_info: Any) -> "ExchangePairKey":
        """Bangun kunci dari dataclass PairInfo executor atau pemetaan JSON."""
        if isinstance(pair_info, dict):
            values = pair_info
        else:
            values = {
                "local_exchange": getattr(pair_info, "local_exchange", ""),
                "local_symbol": getattr(pair_info, "local_symbol", ""),
                "global_exchange": getattr(pair_info, "global_exchange", ""),
                "global_symbol": getattr(pair_info, "global_symbol", ""),
            }
        return cls.create(
            values.get("local_exchange", ""),
            values.get("local_symbol", ""),
            values.get("global_exchange", ""),
            values.get("global_symbol", ""),
        )

    @property
    def tag(self) -> str:
        return (
            f"{self.local_exchange}:{self.local_symbol.replace('/', '_')}"
            f"__{self.global_exchange}:{self.global_symbol.replace('/', '_')}"
        )


class PairClientRegistry:
    """Selesaikan klien hanya untuk kombinasi venue dan simbol kedua kaki yang tepat."""

    def __init__(self) -> None:
        self._clients: Dict[ExchangePairKey, Any] = {}

    def register(self, key: ExchangePairKey, client: Any) -> None:
        if client is None:
            raise ValueError("pair client cannot be None")
        existing = self._clients.get(key)
        if existing is not None and existing is not client:
            raise ValueError(f"pair client already registered: {key.tag}")
        self._clients[key] = client

    def resolve(self, key: ExchangePairKey) -> Any:
        try:
            return self._clients[key]
        except KeyError as exc:
            raise LookupError(f"no client registered for pair: {key.tag}") from exc

    def contains(self, key: ExchangePairKey) -> bool:
        return key in self._clients

    def unregister(self, key: ExchangePairKey) -> Optional[Any]:
        return self._clients.pop(key, None)

    def keys(self) -> Iterable[ExchangePairKey]:
        return tuple(self._clients)
