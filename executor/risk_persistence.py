import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


class RiskPersistenceMixin:
        def _risk_persistence_latch_path(self) -> str:
            configured = str(
                getattr(self.config, "risk_persistence_latch_path", "") or ""
            ).strip()
            return configured or os.path.join(
                "data", "executor", "risk-persistence-failed.json"
            )

        def _write_risk_persistence_latch(self, state_type: str, exc: BaseException) -> bool:
            path = self._risk_persistence_latch_path()
            directory = os.path.dirname(os.path.abspath(path))
            try:
                os.makedirs(directory, exist_ok=True)
                payload = {
                    "state_type": str(state_type),
                    "error": f"{type(exc).__name__}: {exc}",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8", dir=directory,
                    prefix=".risk-persistence-", suffix=".tmp", delete=False,
                ) as handle:
                    json.dump(payload, handle, sort_keys=True)
                    handle.flush()
                    os.fsync(handle.fileno())
                    temporary = handle.name
                os.replace(temporary, path)
                directory_fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                return True
            except Exception:
                logger.exception("Could not write durable local risk persistence latch")
                return False

        def _clear_risk_persistence_latch(self) -> bool:
            path = self._risk_persistence_latch_path()
            try:
                if os.path.exists(path):
                    os.unlink(path)
                    directory_fd = os.open(os.path.dirname(os.path.abspath(path)), os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                return True
            except Exception:
                logger.exception("Could not clear durable local risk persistence latch")
                return False

        def _mark_risk_persistence_failed(self, state_type: str, exc: BaseException) -> None:
            if bool(getattr(self.config, "mock_mode", False)):
                logger.warning("Mock risk-state persistence failed for %s: %s", state_type, exc)
                return
            self._write_risk_persistence_latch(state_type, exc)
            writer = getattr(self, "supabase", None)
            pg_risk = getattr(self, "postgres_risk", None)
            if writer is not None and getattr(writer, "enabled", False):
                try:
                    writer.mark_risk_persistence_failure()
                except Exception:
                    logger.exception("Could not persist risk failure latch to local outbox")
            if pg_risk is not None:
                try:
                    pg_risk.mark_risk_persistence_failure()
                except Exception:
                    logger.exception("Could not mark PostgreSQL risk persistence failure")
            self._entry_blocked = True
            self._risk_persistence_failed = True
            logger.critical(
                "LIVE entry blocked: risk state %s was not durably persisted: %s",
                state_type, exc,
            )

        def _on_risk_state_persisted(self, task, state_type: str) -> None:
            tasks = getattr(self, "_risk_persistence_tasks", set())
            if task not in tasks:
                return
            tasks.discard(task)
            self._risk_persistence_tasks = tasks
            self._risk_persistence_pending_count = max(
                0, int(getattr(self, "_risk_persistence_pending_count", 1)) - 1
            )
            self._risk_persistence_pending = self._risk_persistence_pending_count > 0
            try:
                persisted = bool(task.result())
            except Exception as exc:
                self._mark_risk_persistence_failed(state_type, exc)
                return
            if not persisted:
                self._mark_risk_persistence_failed(
                    state_type, RuntimeError("PostgreSQL risk-state writer returned false")
                )

        async def _flush_risk_state_persistence(self) -> bool:
            tasks = list(getattr(self, "_risk_persistence_tasks", set()))
            results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
            if tasks:
                await asyncio.sleep(0)
            return (
                not bool(getattr(self, "_risk_persistence_pending", False))
                and all(result is True for result in results)
            )

        def _persist_risk_state(
            self, state_type: str, *, scope: str = "global", exchange: str = None,
            pair_key: str = None, reason: str = "", value=None, resolved: bool = False,
        ) -> bool:
            """Journal safety state locally before asynchronous canonical persistence."""
            writer = getattr(self, "supabase", None)
            pg_risk = getattr(self, "postgres_risk", None)
            has_pg = pg_risk is not None and getattr(pg_risk, "enabled", False)
            has_sb = writer is not None and getattr(writer, "enabled", False)
            is_mock = bool(getattr(self.config, "mock_mode", False))

            if not has_pg and not has_sb:
                if not is_mock:
                    self._entry_blocked = True
                    self._risk_persistence_failed = True
                    logger.critical("LIVE entry blocked: durable risk-state storage is unavailable")
                return False

            # The SQLite outbox is the synchronous durability boundary when present.
            outbox_saved = False
            if has_sb:
                try:
                    outbox_saved = bool(writer.risk_state(
                        state_type=state_type, scope=scope, exchange=exchange,
                        pair_key=pair_key, reason=reason, value=value, resolved=resolved,
                        is_mock=is_mock,
                    ))
                except Exception as exc:
                    logger.exception("Local risk-state outbox write raised for %s", state_type)
                    if not has_pg:
                        self._mark_risk_persistence_failed(state_type, exc)
                        return False

            if has_pg:
                payload = {
                    "state_type": state_type,
                    "scope": scope,
                    "exchange": exchange,
                    "pair_key": pair_key,
                    "reason": reason,
                    "value": value,
                    "resolved": resolved,
                    "is_mock": is_mock,
                }
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop is not None and loop.is_running():
                    self._risk_persistence_pending_count = (
                        int(getattr(self, "_risk_persistence_pending_count", 0)) + 1
                    )
                    self._risk_persistence_pending = True
                    task = loop.create_task(pg_risk.persist_risk_state(**payload))
                    tasks = getattr(self, "_risk_persistence_tasks", set())
                    tasks.add(task)
                    self._risk_persistence_tasks = tasks
                    task.add_done_callback(
                        lambda completed, kind=state_type: self._on_risk_state_persisted(
                            completed, kind
                        )
                    )
                    return True
                try:
                    ok = bool(asyncio.run(pg_risk.persist_risk_state(**payload)))
                except Exception as exc:
                    self._mark_risk_persistence_failed(state_type, exc)
                    return False
                if not ok:
                    self._mark_risk_persistence_failed(
                        state_type, RuntimeError("PostgreSQL risk-state writer returned false")
                    )
                    return False
                return True

            if has_sb and outbox_saved:
                return True
            if not is_mock:
                self._mark_risk_persistence_failed(
                    state_type, RuntimeError("no durable risk-state backend accepted the event")
                )
            return False

        def _restore_risk_persistence_failure(self, writer=None) -> None:
            backends = (
                getattr(self, "postgres_risk", None),
                getattr(self, "supabase", None),
                writer,
            )
            failed = any(
                bool(getattr(backend, "risk_persistence_failed", lambda: False)())
                for backend in backends if backend is not None
            )
            failed = failed or os.path.exists(self._risk_persistence_latch_path())
            if not failed:
                return
            self._risk_persistence_failed = True
            self._trading_halted = True
            self._halt_reason = "RISK_STATE_PERSISTENCE_FAILED"
            self._entry_blocked = True

        async def _restore_daily_loss_ledger(self, writer, fallback=None) -> None:
            try:
                loss = await writer.fetch_daily_realized_loss_usdt(
                    self._utc_day(), is_mock=bool(getattr(self.config, "mock_mode", False))
                )
            except Exception:
                if fallback is None:
                    raise
                logger.critical(
                    "Primary daily-loss ledger unavailable; restoring from durable fallback"
                )
                self._mark_risk_persistence_failed(
                    "daily_loss_restore", RuntimeError("primary daily-loss ledger unavailable")
                )
                self._risk_persistence_failed = True
                self._trading_halted = True
                self._halt_reason = "RISK_STATE_PERSISTENCE_UNAVAILABLE"
                self._entry_blocked = True
                loss = await fallback.fetch_daily_realized_loss_usdt(
                    self._utc_day(), is_mock=bool(getattr(self.config, "mock_mode", False))
                )
            if fallback is not None:
                try:
                    fallback_loss = await fallback.fetch_daily_realized_loss_usdt(
                        self._utc_day(), is_mock=bool(getattr(self.config, "mock_mode", False))
                    )
                    loss = max(float(loss), float(fallback_loss))
                except Exception:
                    logger.warning("Fallback daily-loss ledger could not be read; using primary")
            self._daily_realized_loss_usdt = float(loss)
            self._loss_day = self._utc_day()
            self._risk_state_restored = True
            limit = float(getattr(self.config, "max_daily_loss_usdt", 0.0) or 0.0)
            if self._halt_reason == "DAILY_LOSS_LIMIT" and (
                limit <= 0 or self._daily_realized_loss_usdt < limit
            ):
                self._trading_halted = False
                self._halt_reason = ""
                self._persist_risk_state("trading_halt", scope="global", resolved=True)
            manual_resume_today = getattr(self, "_manual_resume_day", None) == self._loss_day
            if limit > 0 and self._daily_realized_loss_usdt >= limit and (
                not self._trading_halted and not manual_resume_today
            ):
                self._trading_halted = True
                self._halt_reason = "DAILY_LOSS_LIMIT"
                self._persist_risk_state(
                    "trading_halt",
                    scope="global",
                    reason="DAILY_LOSS_LIMIT",
                    value={
                        "halt_reason": "DAILY_LOSS_LIMIT",
                        "loss_usdt": self._daily_realized_loss_usdt,
                        "loss_day": self._loss_day,
                    },
                )
            self._roll_loss_day_if_needed()

        async def _restore_risk_state(self) -> None:
            """Restore risk state before recovery; degrade to protect-only if PG is down."""
            pg_risk = getattr(self, "postgres_risk", None)
            has_pg = pg_risk is not None and getattr(pg_risk, "enabled", False)
            writer = getattr(self, "supabase", None)
            has_sb = writer is not None and getattr(writer, "enabled", False)

            if not has_pg and not has_sb:
                self._risk_state_restored = True
                if not getattr(self.config, "mock_mode", False):
                    self._trading_halted = True
                    self._halt_reason = "RISK_STATE_PERSISTENCE_UNAVAILABLE"
                    self._entry_blocked = True
                    self._risk_persistence_failed = True
                    logger.critical(
                        "LIVE startup fail-closed: configure durable PostgreSQL or Supabase risk-state storage"
                    )
                return

            active_backend = pg_risk if has_pg else writer
            rows = []
            if has_pg:
                try:
                    rows = await pg_risk.fetch_active_risk_states(
                        is_mock=bool(getattr(self.config, "mock_mode", False))
                    )
                except Exception as pg_error:
                    self._mark_risk_persistence_failed("risk_state_restore", pg_error)
                    self._risk_persistence_failed = True
                    self._trading_halted = True
                    self._halt_reason = "RISK_STATE_PERSISTENCE_UNAVAILABLE"
                    self._entry_blocked = True
                    if not has_sb:
                        # Continue in protect-only mode; no new exposure can pass readiness.
                        self._risk_state_restored = True
                        logger.critical(
                            "PostgreSQL risk state unavailable and no fallback exists: %s",
                            pg_error,
                        )
                        return
                    logger.critical(
                        "PostgreSQL risk state unavailable; restoring protect-only state from Supabase: %s",
                        pg_error,
                    )
                    active_backend = writer
                    rows = await writer.fetch_active_risk_states(
                        is_mock=bool(getattr(self.config, "mock_mode", False))
                    )
                else:
                    if has_sb:
                        try:
                            sb_rows = await writer.fetch_active_risk_states(
                                is_mock=bool(getattr(self.config, "mock_mode", False))
                            )
                            merged = {str(row.get("state_id")): dict(row) for row in rows}
                            for row in sb_rows:
                                state_id = str(row.get("state_id") or "")
                                if not state_id:
                                    continue
                                current = merged.get(state_id)
                                incoming_at = str(row.get("updated_at") or "")
                                current_at = str((current or {}).get("updated_at") or "")
                                if current is None or incoming_at > current_at:
                                    merged[state_id] = dict(row)
                                    persisted = await pg_risk.persist_risk_state(
                                        state_type=str(row.get("state_type") or ""),
                                        scope=str(row.get("scope") or "global"),
                                        exchange=row.get("exchange"),
                                        pair_key=row.get("pair_key"),
                                        reason=str(row.get("reason") or ""),
                                        value=row.get("value"),
                                        resolved=False,
                                        state_id=state_id,
                                        is_mock=bool(getattr(self.config, "mock_mode", False)),
                                    )
                                    if not persisted:
                                        self._mark_risk_persistence_failed(
                                            "risk_state_reconcile",
                                            RuntimeError(f"could not persist restored risk state {state_id}"),
                                        )
                                        self._risk_persistence_failed = True
                                        self._trading_halted = True
                                        self._halt_reason = "RISK_STATE_PERSISTENCE_UNAVAILABLE"
                                        self._entry_blocked = True
                            rows = list(merged.values())
                        except Exception as reconcile_error:
                            logger.exception(
                                "Could not reconcile Supabase risk-state fallback into PostgreSQL"
                            )
                            self._mark_risk_persistence_failed(
                                "risk_state_reconcile", reconcile_error
                            )
                            self._risk_persistence_failed = True
                            self._trading_halted = True
                            self._halt_reason = "RISK_STATE_PERSISTENCE_UNAVAILABLE"
                            self._entry_blocked = True
            elif has_sb:
                rows = await writer.fetch_active_risk_states(
                    is_mock=bool(getattr(self.config, "mock_mode", False))
                )

            for row in rows:
                self._restore_risk_record(row)
            self._restore_risk_persistence_failure(active_backend)
            fallback = writer if has_pg and has_sb and active_backend is pg_risk else None
            await self._restore_daily_loss_ledger(active_backend, fallback=fallback)
            logger.warning("Restored %d unresolved durable risk-state records", len(rows))

