"""Durable active-position recovery helpers for the executor."""

import json
import time

from executor.models import PositionState, ActivePosition
from executor.pair_registry import ExchangePairKey
from executor.engine_common import logger


class EngineRecoveryPositionsMixin:
    async def _migrate_active_position_key(
        self, pair_tag: str, key: str, pos: ActivePosition
    ):
        if "__" in pair_tag or not pos.global_exchange or not pos.global_symbol:
            return pair_tag, key
        migrated_tag = ExchangePairKey.from_pair_info({
            "local_exchange": pos.local_exchange,
            "local_symbol": pos.symbol,
            "global_exchange": pos.global_exchange,
            "global_symbol": pos.global_symbol,
        }).tag.lower()
        if migrated_tag == pair_tag:
            return pair_tag, key
        migrated_key = f"arb:active_pos:{migrated_tag}"
        if await self.redis.get(migrated_key):
            pos.state = "RECONCILIATION_REQUIRED"
            self.active_positions[pair_tag] = pos
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            await self.redis.set(key, json.dumps(pos.to_dict()))
            self._mark_failed(
                pair_tag,
                "LEGACY_POSITION_KEY_COLLISION",
                "Legacy position cannot be migrated because the venue-qualified key already exists.",
                detail={"legacy_key": key, "target_key": migrated_key},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            return None
        await self.redis.set(migrated_key, json.dumps(pos.to_dict()))
        await self.redis.delete(key)
        return migrated_tag, migrated_key

    async def _retain_invalid_recovered_position(
        self, pair_tag: str, key: str, pos: ActivePosition
    ) -> None:
        self.active_positions[pair_tag] = pos
        pos.state = "RECONCILIATION_REQUIRED"
        self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
        await self.redis.set(key, json.dumps(pos.to_dict()))
        self._mark_failed(
            pair_tag,
            "RECOVERED_POSITION_QTY_INVALID",
            f"Recovered position quantity is invalid ({pos.qty}); exchange exposure must be checked.",
            detail={"qty": pos.qty, "symbol": pos.symbol},
            state=PositionState.RECONCILIATION_REQUIRED,
        )
        logger.critical(
            f"[Recovery] retaining {pair_tag}: recovered qty is {pos.qty}; "
            "manual reconciliation required"
        )

    def _restore_active_position_state(
        self, pair_tag: str, pos: ActivePosition
    ) -> None:
        raw_state = str(getattr(pos, "state", "HEDGED_ACTIVE") or "HEDGED_ACTIVE").upper()
        try:
            state = PositionState(raw_state)
        except ValueError:
            state = PositionState.RECONCILIATION_REQUIRED
            pos.state = state.value

        if state in (PositionState.IDLE, PositionState.CLOSED):
            self.active_positions.pop(pair_tag, None)
            self.states[pair_tag] = state
            return

        self.active_positions[pair_tag] = pos
        self.states[pair_tag] = state
        if state in (PositionState.RECONCILIATION_REQUIRED, PositionState.FAILED):
            self._failure_reasons.setdefault(pair_tag, {
                "reason": (
                    "RESTORED_RECONCILIATION_REQUIRED"
                    if state == PositionState.RECONCILIATION_REQUIRED
                    else "RESTORED_FAILED_POSITION"
                ),
                "message": (
                    "Position restored in an unresolved terminal safety state; "
                    "manual reconciliation is required before new exposure."
                ),
                "detail": {"qty": pos.qty, "symbol": pos.symbol},
                "timestamp_ms": int(time.time() * 1000),
            })
            logger.critical(
                f"[Recovery] Position {pair_tag} restored with {state.value}; "
                "manual reconciliation is required."
            )
        else:
            self._failure_reasons.pop(pair_tag, None)

    async def _recover_active_position_record(self, key: str) -> bool:
        pair_tag = key.split("arb:active_pos:", 1)[-1]
        try:
            raw = await self.redis.get(key)
            if not raw:
                return False
            pos = ActivePosition.from_dict(json.loads(raw))
            if str(pos.state or "").upper() in (
                PositionState.IDLE.value, PositionState.CLOSED.value
            ):
                terminal_times = getattr(
                    self, "_recovery_terminal_position_timestamps", None
                )
                if not isinstance(terminal_times, dict):
                    terminal_times = {}
                    self._recovery_terminal_position_timestamps = terminal_times
                terminal_times[pair_tag] = int(pos.updated_at or 0)
                await self.redis.delete(key)
                self.active_positions.pop(pair_tag, None)
                self.states[pair_tag] = PositionState(str(pos.state).upper())
                return False
            migrated = await self._migrate_active_position_key(pair_tag, key, pos)
            if migrated is None:
                return False
            pair_tag, key = migrated
            if pos.qty <= 0:
                await self._retain_invalid_recovered_position(pair_tag, key, pos)
                return False

            self._restore_active_position_state(pair_tag, pos)
            logger.warning(
                f"[Recovery] Restored position {pair_tag}: qty={pos.qty}, "
                f"entry_idr={pos.entry_price_idr}, short_entry_usdt={pos.short_entry_usdt}, "
                f"state={self.states[pair_tag].value}"
            )
            self.supabase.position_opened(
                pair_tag,
                pos,
                getattr(self, "_pair_infos", {}).get(pair_tag),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            return True
        except Exception as exc:
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            self._mark_failed(
                pair_tag,
                "ACTIVE_POSITION_RECORD_INVALID",
                f"Persisted active position cannot be parsed: {exc}",
                detail={"redis_key": key},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            logger.exception(
                f"[Recovery] invalid active position record {key}; Redis key retained"
            )
            return False

    async def _recover_active_positions(self) -> None:
        """Restore Redis positions, then backfill cache misses from durable Supabase."""
        try:
            keys = await self.redis.keys("arb:active_pos:*")
        except Exception as exc:
            logger.exception("[Recovery] failed to scan arb:active_pos:* keys")
            raise RuntimeError("active position cache could not be enumerated") from exc

        recovered = 0
        for key in keys or []:
            if await self._recover_active_position_record(key):
                recovered += 1

        if bool(getattr(self.supabase, "enabled", False)):
            reader = getattr(self.supabase, "fetch_open_position_records", None)
            if not callable(reader):
                raise RuntimeError(
                    "LIVE recovery requires a durable active-position reader"
                )
            durable_rows = await reader()
            if not isinstance(durable_rows, list):
                raise RuntimeError("durable active-position read returned malformed rows")
            for row in durable_rows:
                if not isinstance(row, dict) or bool(row.get("is_mock", False)):
                    continue
                pair_tag = str(row.get("pair_key") or "").strip()
                if not pair_tag:
                    continue
                snapshot = row.get("state_snapshot")
                if isinstance(snapshot, str):
                    try:
                        snapshot = json.loads(snapshot)
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            f"durable position {pair_tag} has malformed state_snapshot"
                        ) from exc
                if not isinstance(snapshot, dict) or not snapshot:
                    raise RuntimeError(
                        f"durable position {pair_tag} has no recoverable state_snapshot"
                    )
                snapshot = dict(snapshot)
                snapshot["state"] = str(row.get("status") or snapshot.get("state") or "RECONCILIATION_REQUIRED")
                durable_updated = int(snapshot.get("updated_at") or 0)
                terminal_times = getattr(
                    self, "_recovery_terminal_position_timestamps", {}
                )
                if (
                    pair_tag in terminal_times
                    and durable_updated <= int(terminal_times.get(pair_tag) or 0)
                ):
                    continue
                cached = self.active_positions.get(pair_tag)
                if cached is not None and durable_updated <= int(cached.updated_at or 0):
                    continue
                terminal_times.pop(pair_tag, None)
                key = f"arb:active_pos:{pair_tag}"
                await self.redis.set(key, json.dumps(snapshot, allow_nan=False))
                if not await self._recover_active_position_record(key):
                    raise RuntimeError(
                        f"durable active position {pair_tag} could not be restored safely"
                    )
                recovered += 1

        if recovered:
            logger.warning(
                f"[Recovery] {recovered} active position(s) restored - resuming management"
            )
