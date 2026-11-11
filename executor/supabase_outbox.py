"""Crash-resilient local outbox for best-effort Supabase persistence.

SQLite is used only as a local journal. No network request is performed from
trading callbacks; the async worker delivers rows to Supabase and deletes them
only after a successful idempotent upsert.
"""
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class OutboxFull(RuntimeError):
    """Raised when the configured durable outbox capacity is exhausted."""


class DurableOutbox:
    def __init__(self, path: str, max_pending: int = 50000):
        self.path = path if path == ":memory:" else str(Path(path).expanduser())
        self.max_pending = max(1, int(max_pending))
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(
            self.path, timeout=5.0, isolation_level=None, check_same_thread=False
        )
        with self._lock:
            self._db.execute("PRAGMA busy_timeout=5000")
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute(
                """CREATE TABLE IF NOT EXISTS pending_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    conflict_target TEXT,
                    row_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    last_error TEXT
                )"""
            )
            self._db.execute(
                """CREATE INDEX IF NOT EXISTS idx_pending_outbox_due
                   ON pending_outbox(next_attempt_at, id)"""
            )
            self._db.execute(
                """CREATE TABLE IF NOT EXISTS writer_state (
                    state_key TEXT PRIMARY KEY,
                    numeric_value REAL NOT NULL,
                    updated_at REAL NOT NULL
                )"""
            )

    def enqueue(self, table: str, conflict_target: Optional[str], row: Dict[str, Any]) -> int:
        payload = json.dumps(row, separators=(",", ":"), ensure_ascii=False, default=str)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                count = self._db.execute(
                    "SELECT COUNT(*) FROM pending_outbox"
                ).fetchone()[0]
                if count >= self.max_pending:
                    raise OutboxFull(f"durable outbox capacity reached ({self.max_pending})")
                cursor = self._db.execute(
                    """INSERT INTO pending_outbox
                       (table_name, conflict_target, row_json, created_at)
                       VALUES (?, ?, ?, ?)""",
                    (table, conflict_target, payload, time.time()),
                )
                self._db.execute("COMMIT")
                return int(cursor.lastrowid)
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def enqueue_once(
        self,
        table: str,
        conflict_target: Optional[str],
        row: Dict[str, Any],
        state_key: str,
    ) -> bool:
        """Atomically enqueue an immutable event once across retries/restarts."""
        payload = json.dumps(row, separators=(",", ":"), ensure_ascii=False, default=str)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                exists = self._db.execute(
                    "SELECT 1 FROM writer_state WHERE state_key=?", (state_key,)
                ).fetchone()
                if exists:
                    self._db.execute("COMMIT")
                    return False
                count = self._db.execute("SELECT COUNT(*) FROM pending_outbox").fetchone()[0]
                if count >= self.max_pending:
                    raise OutboxFull(f"durable outbox capacity reached ({self.max_pending})")
                self._db.execute(
                    """INSERT INTO pending_outbox
                       (table_name, conflict_target, row_json, created_at)
                       VALUES (?, ?, ?, ?)""",
                    (table, conflict_target, payload, time.time()),
                )
                self._db.execute(
                    """INSERT INTO writer_state(state_key, numeric_value, updated_at)
                       VALUES (?, 1, ?)
                       ON CONFLICT(state_key) DO NOTHING""",
                    (state_key, time.time()),
                )
                self._db.execute("COMMIT")
                return True
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def enqueue_cumulative_fill(
        self,
        table: str,
        conflict_target: Optional[str],
        row: Dict[str, Any],
        state_key: str,
        cumulative: float,
    ) -> float:
        """Atomically journal a new fill delta and advance its cumulative watermark."""
        value = max(0.0, float(cumulative))
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                previous_row = self._db.execute(
                    "SELECT numeric_value FROM writer_state WHERE state_key=?",
                    (state_key,),
                ).fetchone()
                previous = float(previous_row[0]) if previous_row else 0.0
                delta = value - previous
                if delta <= 1e-12:
                    self._db.execute("COMMIT")
                    return 0.0
                count = self._db.execute("SELECT COUNT(*) FROM pending_outbox").fetchone()[0]
                if count >= self.max_pending:
                    raise OutboxFull(f"durable outbox capacity reached ({self.max_pending})")
                fill_row = dict(row)
                fill_row["qty"] = delta
                fill_row["quote_qty"] = delta * float(fill_row.get("price") or 0.0)
                payload = json.dumps(fill_row, separators=(",", ":"), ensure_ascii=False, default=str)
                self._db.execute(
                    """INSERT INTO pending_outbox
                       (table_name, conflict_target, row_json, created_at)
                       VALUES (?, ?, ?, ?)""",
                    (table, conflict_target, payload, time.time()),
                )
                self._db.execute(
                    """INSERT INTO writer_state(state_key, numeric_value, updated_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(state_key) DO UPDATE SET
                         numeric_value=excluded.numeric_value,
                         updated_at=excluded.updated_at""",
                    (state_key, value, time.time()),
                )
                self._db.execute("COMMIT")
                return delta
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def pending(self, limit: int = 50) -> List[Tuple[int, str, Optional[str], Dict[str, Any]]]:
        with self._lock:
            rows = self._db.execute(
                """SELECT id, table_name, conflict_target, row_json
                   FROM pending_outbox
                   WHERE next_attempt_at <= ?
                   ORDER BY id LIMIT ?""",
                (time.time(), max(1, int(limit))),
            ).fetchall()
        return [(int(i), table, conflict, json.loads(payload))
                for i, table, conflict, payload in rows]

    def ack(self, ids: List[int]) -> None:
        if not ids:
            return
        with self._lock:
            self._db.executemany("DELETE FROM pending_outbox WHERE id=?", [(i,) for i in ids])

    def retry(self, ids: List[int], error: str) -> None:
        if not ids:
            return
        with self._lock:
            self._db.executemany(
                """UPDATE pending_outbox
                   SET attempts=attempts+1,
                       next_attempt_at=?,
                       last_error=?
                   WHERE id=?""",
                [
                    (time.time() + min(300, 2 **  min(8, attempt)), str(error)[:500], item_id)
                    for item_id in ids
                    for attempt in [self._attempts(item_id) + 1]
                ],
            )

    def _attempts(self, item_id: int) -> int:
        row = self._db.execute(
            "SELECT attempts FROM pending_outbox WHERE id=?", (item_id,)
        ).fetchone()
        return int(row[0]) if row else 0

    def pending_count(self) -> int:
        with self._lock:
            return int(self._db.execute("SELECT COUNT(*) FROM pending_outbox").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            self._db.close()
