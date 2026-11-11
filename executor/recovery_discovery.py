"""recovery discovery helpers for the universal order lifecycle."""

import logging
import time
from typing import Any, Dict, Optional

from executor.models import OrderRecord, OrderRole, RecoveryStatus
from executor.recovery_utils import _spec_dict

logger = logging.getLogger("executor.order_recovery")



class RecoveryDiscoveryMixin:
    """Focused order-recovery responsibilities."""

    def _recovery_venue_targets(self) -> list:
        """Exchange/symbol pairs whose orders must be swept at startup.

        Every configured venue is swept even when nothing is locally tracked:
        the orders a restart loses are exactly the ones with no local record.
        """
        catalogues = self._recovery_symbol_catalogues()
        targets = []
        seen = set()
        # Account-wide catalogues come first so a single call per venue/market
        # covers both known symbols and orders with no local record at all.
        for catalogue in catalogues:
            key = (catalogue["exchange"], "", catalogue["market"])
            if key in seen:
                continue
            seen.add(key)
            targets.append((
                catalogue["exchange"], "", catalogue["pair_tag"],
                catalogue["market"], catalogue["amount_spec"],
            ))
        for entry in self._recovery_symbol_spec():
            key = (entry["exchange"], entry["symbol"], entry["market"])
            # Skip per-symbol targets the account-wide sweep already covers.
            if key in seen or (entry["exchange"], "", entry["market"]) in seen:
                continue
            seen.add(key)
            targets.append((
                entry["exchange"], entry["symbol"], entry["pair_tag"],
                entry["market"], entry["amount_spec"],
            ))
        return targets

    def _recovery_symbol_spec(self) -> list:
        """Symbols the bot currently cares about, taken from tracked state."""
        entries = []

        def add(local_exchange, symbol, pair_tag, market, spec):
            if not local_exchange or not symbol:
                return
            entries.append({
                "exchange": str(local_exchange).lower(),
                "symbol": str(symbol),
                "pair_tag": pair_tag,
                "market": market,
                "amount_spec": spec,
            })

        for pair_tag, pos in self.active_positions.items():
            add(
                pos.local_exchange, pos.symbol, pair_tag, "spot",
                _spec_dict(getattr(pos, "local_amount_spec", None)),
            )
            add(
                pos.global_exchange, pos.global_symbol, pair_tag, "futures",
                _spec_dict(getattr(pos, "global_amount_spec", None)),
            )
        for pair_tag, order in self.pending_entry_orders.items():
            add(
                order.get("local_exchange"), order.get("symbol"), pair_tag, "spot",
                order.get("local_amount_spec"),
            )
        for pair_tag, order in self.pending_exit_orders.items():
            add(
                order.get("local_exchange") or getattr(self.active_positions.get(pair_tag), "local_exchange", ""),
                order.get("symbol"), pair_tag, "spot", order.get("local_amount_spec"),
            )
        return entries

    def _recovery_symbol_catalogues(self) -> list:
        """Account-wide sweeps so orders carrying no local record are still found.

        A whole-account read (no symbol filter) is what catches an order placed
        one millisecond before a crash. Perpetual venues are included because a
        resting hedge leg is the most dangerous order to lose: it carries margin
        and is invisible in the local spot book.
        """
        catalogues = []
        for exchange, market in (
            (str(self.config.local_exchange_name or "").lower(), "spot"),
            (str(self.config.global_exchange_name or "").lower(), "futures"),
        ):
            if not exchange:
                continue
            if self._client_for_exchange(exchange) is None:
                continue
            catalogues.append({
                "exchange": exchange,
                "market": market,
                "pair_tag": "",
                "amount_spec": None,
            })
        return catalogues

    async def _fetch_recovery_orders(
        self, exchange: str, symbol: str, market: str, amount_spec: Any
    ) -> list:
        """Reads resting orders for one venue leg, resolving its own client."""
        client = self._client_for_exchange(exchange)
        if client is None:
            raise NotImplementedError(f"no client is bound to exchange {exchange}")
        spec = amount_spec
        if spec is None:
            spec = self._default_amount_spec_for(exchange, market)
        method_name = (
            "fetch_global_open_orders" if market == "futures" else "fetch_local_open_orders"
        )
        fetch = getattr(client, method_name, None)
        if not callable(fetch):
            # Without a listing endpoint the bot cannot rule out resting orders it
            # never recorded. Mock clients are exempt: paper mode has no venue to
            # lose an order on.
            if getattr(self.config, "mock_mode", False):
                raise NotImplementedError(
                    f"{exchange} client has no {method_name}; skipped in paper mode"
                )
            raise RuntimeError(
                f"{exchange} client cannot list {market} open orders; resting orders "
                "cannot be verified after restart"
            )
        return await fetch(symbol or None, spec)

    def _client_for_exchange(self, exchange: str) -> Any:
        """Resolves the exchange client that owns a venue, or None when unbound."""
        exchange = str(exchange or "").lower()
        if not exchange:
            return None
        configured_local = str(getattr(self.config, "local_exchange_name", "")).lower()
        configured_global = str(getattr(self.config, "global_exchange_name", "")).lower()
        if getattr(self.config, "mock_mode", False):
            if exchange in (configured_local, configured_global):
                return self._base_client
            return None
        registry = self._pair_client_registry
        keys_fn = getattr(registry, "keys", None)
        for key in (keys_fn() if callable(keys_fn) else ()):
            local_name = str(getattr(key, "local_exchange", "")).lower()
            global_name = str(getattr(key, "global_exchange", "")).lower()
            if exchange in (local_name, global_name):
                try:
                    return registry.resolve(key)
                except Exception:
                    continue
        if exchange in (configured_local, configured_global):
            return self._base_client
        return None

    @staticmethod
    def _default_amount_spec_for(exchange: str, market: str) -> Optional[dict]:
        """Fractional step used only when nothing better is known.

        Futures quantities are left in the venue's own units (contracts on some
        venues) because converting them needs the contract's real
        ``base_per_unit``; guessing 1.0 there would misreport a discovered hedge.
        """
        if market == "futures":
            return None
        return {"unit": "BASE", "step": 1e-12, "min": 0.0, "base_per_unit": 1.0}

    async def _adopt_venue_order(
        self,
        order: Dict[str, Any],
        exchange: str,
        requested_symbol: str,
        requested_pair_tag: str,
        market: str,
        amount_spec: Any,
    ) -> None:
        """Links one resting venue order to a position, or flags it as an orphan."""
        if not isinstance(order, dict):
            return
        exchange_order_id = str(order.get("id") or "")
        client_order_id = str(order.get("clientOrderId") or order.get("client_order_id") or "")
        symbol = str(order.get("symbol") or requested_symbol or "")
        if not exchange_order_id and not client_order_id:
            await self._orphan(
                order, exchange, symbol, requested_pair_tag,
                "venue order carries no order id or client order id",
                market_type="perpetual" if market == "futures" else "spot",
            )
            return

        if await self._already_managed(exchange, exchange_order_id, client_order_id):
            return

        record, pair_tag = await self._link_venue_order(
            order, exchange, symbol, market, exchange_order_id, client_order_id
        )
        if record is None:
            await self._orphan(
                order, exchange, symbol, requested_pair_tag,
                "active venue order matches no tracked pair, position, pending record or cancelled base order",
                market_type="perpetual" if market == "futures" else "spot",
            )
            return

        adopted = await self._apply_adopted_order(record, order, pair_tag, amount_spec)
        if adopted:
            await self.order_registry.clear_pending_recovery(record.local_order_id)
            logger.warning(
                "[Recovery] adopted venue order %s (%s, %s) for %s",
                exchange_order_id or client_order_id, exchange, record.order_role, pair_tag,
            )

    async def _already_managed(
        self, exchange: str, exchange_order_id: str, client_order_id: str
    ) -> bool:
        """True when this venue order is already bound or already reconciled.

        A registry record that exists but is not yet bound (no acknowledged
        exchange order id, or marked pending) must still go through adoption:
        skipping it is exactly how an order stays unmanaged after a restart.
        """
        record = None
        if client_order_id:
            record = await self.order_registry.find_by_client_id(exchange, client_order_id)
        if record is None and exchange_order_id:
            record = await self.order_registry.find_by_exchange_id(exchange, exchange_order_id)
        if record is None:
            return False
        if record.is_terminal:
            return True
        return bool(record.exchange_order_id) and record.recovery_status in (
            RecoveryStatus.ADOPTED.value,
            RecoveryStatus.MANAGED.value,
        )

    async def _find_existing_venue_record(
        self, exchange: str, exchange_order_id: str,
        client_order_id: str, symbol: str,
    ) -> Optional[OrderRecord]:
        record = None
        if client_order_id:
            record = await self.order_registry.find_by_client_id(
                exchange, client_order_id
            )
        if record is None and exchange_order_id:
            record = await self.order_registry.find_by_exchange_id(
                exchange, exchange_order_id
            )
        if record is None:
            record = await self._link_legacy_pending_record(
                exchange, exchange_order_id, client_order_id, symbol
            )
        return record

    def _complete_venue_record_link(
        self, record: OrderRecord, exchange: str, symbol: str, market: str
    ) -> tuple:
        pair_tag = record.pair_key or self._match_position_by_catalogue(
            exchange, symbol
        )
        record.pair_key = pair_tag
        record.position_id = record.position_id or self._position_id_for(pair_tag)
        record.market_type = record.market_type or (
            "perpetual" if market == "futures" else "spot"
        )
        record.exchange = record.exchange or exchange
        record.symbol = record.symbol or symbol
        if not record.order_role:
            record.order_role = self._infer_role(pair_tag, market)
        if not pair_tag:
            return None, ""
        return record, pair_tag

    async def _link_venue_order(
        self,
        order: Dict[str, Any],
        exchange: str,
        symbol: str,
        market: str,
        exchange_order_id: str,
        client_order_id: str,
    ) -> tuple:
        """Resolve an existing registry record or create one from a tracked market."""
        record = await self._find_existing_venue_record(
            exchange, exchange_order_id, client_order_id, symbol
        )
        if record is not None:
            return self._complete_venue_record_link(
                record, exchange, symbol, market
            )
        # A requested pair is only a scan hint, not proof of ownership.
        pair_tag = self._match_position_by_catalogue(exchange, symbol)
        if not pair_tag:
            return None, ""
        role = self._infer_role(pair_tag, market)
        return self._record_from_order(
            order, exchange, symbol, pair_tag, market, role,
            exchange_order_id, client_order_id,
        ), pair_tag

    async def _link_legacy_pending_record(
        self, exchange: str, exchange_order_id: str, client_order_id: str, symbol: str
    ) -> Optional[OrderRecord]:
        """Resolves an order against the pre-registry pending-entry records.

        Those records predate the registry and only exist in Redis, so they are
        migrated here: the surviving record keeps the pair identity that the
        venue payload alone cannot provide.
        """
        if not exchange_order_id and not client_order_id:
            return None
        for pair_tag, order_info in self.pending_entry_orders.items():
            stored_id = str(order_info.get("order_id") or "")
            stored_coid = str(order_info.get("client_order_id") or "")
            matches_exchange_id = bool(
                exchange_order_id and stored_id == exchange_order_id
            )
            matches_client_id = bool(
                client_order_id and stored_coid == client_order_id
            )
            if not (matches_exchange_id or matches_client_id):
                continue
            record = OrderRecord(
                local_order_id=OrderRecord.local_id_for(exchange, stored_coid, stored_id),
                client_order_id=stored_coid,
                exchange_order_id=stored_id,
                pair_key=pair_tag,
                position_id=self._position_id_for(pair_tag),
                exchange=exchange,
                market_type="spot",
                symbol=str(order_info.get("symbol") or symbol),
                side="buy",
                order_type="limit",
                order_role=OrderRole.ENTRY.value,
                price=float(order_info.get("price", 0.0) or 0.0),
                original_quantity=float(order_info.get("qty", 0.0) or 0.0),
                filled_quantity=float(order_info.get("filled", 0.0) or 0.0),
                is_mock=bool(order_info.get("is_mock", getattr(self.config, "mock_mode", False))),
            )
            return await self.order_registry.register(record)
        return None

    def _match_position_by_catalogue(self, exchange: str, symbol: str) -> str:
        """Last-resort link for an order with no known client or exchange id.

        This is what recovers an order placed moments before a crash: the client
        order id was never acknowledged and no record exists, so the only
        surviving evidence is the venue's own symbol. A match therefore requires
        identical base **and** quote on the same venue — never a fuzzy or
        same-base-only match, because that would attach an order to the wrong
        position and misreport exposure.
        """
        base, quote = self._symbol_parts(symbol)
        if not base or not quote or not exchange:
            return ""
        exchange = exchange.lower()
        candidates = set(self.active_positions) | set(self.pending_entry_orders)
        matches = []
        for pair_tag in candidates:
            tracked_symbol, tracked_exchanges = self._tracked_market(pair_tag)
            if exchange not in tracked_exchanges:
                continue
            tracked_base, tracked_quote = self._symbol_parts(tracked_symbol)
            if (tracked_base, tracked_quote) == (base, quote):
                matches.append(pair_tag)
        # A market can be shared by strategies whose futures leg differs. The
        # spot symbol alone cannot identify which strategy owns an unlabelled
        # order; choosing the first set member would attach it nondeterministically.
        return matches[0] if len(matches) == 1 else ""

    def _tracked_market(self, pair_tag: str) -> tuple:
        """Returns (local symbol, venues) for a tracked pair."""
        pos = self.active_positions.get(pair_tag)
        if pos is not None:
            return pos.symbol, {
                str(pos.local_exchange or "").lower(),
                str(pos.global_exchange or "").lower(),
            }
        pending = self.pending_entry_orders.get(pair_tag) or {}
        return str(pending.get("symbol", "")), {
            str(pending.get("local_exchange", "")).lower(),
            str(pending.get("global_exchange", "")).lower(),
        }

    @staticmethod
    def _symbol_parts(symbol: str) -> tuple:
        """Splits a venue symbol into (base, quote) using only known formats."""
        cleaned = str(symbol or "").upper().split(":")[0]
        if not cleaned:
            return "", ""
        for separator in ("/", "-", "_"):
            if separator in cleaned:
                base, _, quote = cleaned.partition(separator)
                return base.strip(), quote.strip()
        # Compact form, e.g. BTCIDR / BTCUSDT. Longest known quote wins so
        # BTCUSDT is not read as BTCU + ST.
        for quote in ("USDT", "USDC", "IDR", "BIDR", "BTC", "ETH"):
            if cleaned.endswith(quote) and len(cleaned) > len(quote):
                return cleaned[: -len(quote)], quote
        return "", ""

    def _infer_role(self, pair_tag: str, market: str) -> str:
        """Classifies a discovered order from the state that already tracks it.

        Every order the bot intentionally leaves resting is an entry or an exit;
        a resting futures order is an unexpected hedge leg. Nothing else is
        inferred, so an unknown order never starts controlling a position.
        """
        if pair_tag in self.pending_exit_orders:
            return OrderRole.EXIT_SPOT.value
        if pair_tag in self.pending_entry_orders:
            return OrderRole.ENTRY.value
        if market == "futures":
            return OrderRole.HEDGE.value
        if pair_tag in self.active_positions:
            return OrderRole.EXIT_SPOT.value
        return OrderRole.ENTRY.value

    def _record_from_order(
        self,
        order: Dict[str, Any],
        exchange: str,
        symbol: str,
        pair_tag: str,
        market: str,
        role: str,
        exchange_order_id: str,
        client_order_id: str,
    ) -> OrderRecord:
        return OrderRecord(
            local_order_id=OrderRecord.local_id_for(exchange, client_order_id, exchange_order_id),
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            pair_key=pair_tag,
            position_id=self._position_id_for(pair_tag),
            exchange=exchange,
            market_type="perpetual" if market == "futures" else "spot",
            symbol=symbol,
            side=str(order.get("side") or ""),
            order_type=str(order.get("type") or ""),
            order_role=role,
            price=float(order.get("price", 0.0) or 0.0),
            original_quantity=float(order.get("amount", 0.0) or 0.0),
            filled_quantity=float(order.get("filled", 0.0) or 0.0),
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
