"""SQLite 持久化：API 告警表 + 接收端 deliveryId 去重/关闭表。

两个存储分别建库：
* :class:`AlertStore`  属于发送 API，进程重启后凭 status=pending/delivering/
  confirming 的记录恢复投递或终态核对；delivered/failed 为终态，任何迟到的
  worker 都不能改写。
* :class:`ReceiverStore` 属于接收模拟器，deliveryId 唯一约束保证同一投递
  即使因断连/重启被重放多次，业务层也只接纳一次；finalize 写入的关闭墓碑
  与接纳在同一把锁下线性化——一旦某 deliveryId 被关闭，迟到的接纳请求
  （如对端 stall 醒来的处理线程）将被拒绝，保证“API 最终 failed 的投递
  此后绝不被接收端接纳”。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple

STATUS_PENDING = "pending"
STATUS_DELIVERING = "delivering"
# 重试耗尽后、与接收端核对最终去向期间的非终态；核对不可达时可长期停留
STATUS_CONFIRMING = "confirming"
STATUS_DELIVERED = "delivered"
STATUS_FAILED = "failed"
TERMINAL_STATUSES = (STATUS_DELIVERED, STATUS_FAILED)
ACTIVE_STATUSES = (STATUS_PENDING, STATUS_DELIVERING, STATUS_CONFIRMING)


def _connect(path: str) -> sqlite3.Connection:
    if path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class AlertStore:
    """发送侧告警存储，线程安全。"""

    def __init__(self, path: str) -> None:
        self._conn = _connect(path)
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id     TEXT PRIMARY KEY,
                    alert_key    TEXT NOT NULL UNIQUE,
                    body         BLOB NOT NULL,
                    fingerprint  TEXT NOT NULL,
                    delivery_id  TEXT NOT NULL UNIQUE,
                    signature    TEXT NOT NULL,
                    status       TEXT NOT NULL,
                    attempts     INTEGER NOT NULL DEFAULT 0,
                    last_result  TEXT,
                    last_http_status INTEGER,
                    failure_reason TEXT,
                    accepted_at  REAL NOT NULL,
                    updated_at   REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS delivery_attempts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id    TEXT NOT NULL,
                    attempt_no  INTEGER NOT NULL,
                    http_status INTEGER,
                    outcome     TEXT NOT NULL,
                    detail      TEXT NOT NULL,
                    at          REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_alerts_status
                    ON alerts(status, updated_at);
                """
            )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        # BEGIN IMMEDIATE 让“按键查重 + 插入”成为原子操作，避免并发双受理。
        with self._lock:
            for attempt in range(5):
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError:
                    if attempt == 4:
                        raise
                    time.sleep(0.05 * (attempt + 1))
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def admit(
        self,
        alert_key: str,
        body: bytes,
        fingerprint: str,
        delivery_id: str,
        signature: str,
    ) -> Tuple[str, Dict[str, Any]]:
        """受理入口。

        返回 (mode, row)：
        * ``"created"``  首次受理，已生成 alertId/deliveryId；
        * ``"replay"``   同键同内容，原样回放历史受理结果；
        * ``"conflict"`` 同键异内容，调用方必须得到冲突。

        签名只在首次受理时计算一次并入库；重放与所有重试都复用它，
        保证每次请求体/deliveryId/签名逐字节相同。
        """
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM alerts WHERE alert_key = ?", (alert_key,)
            ).fetchone()
            if existing is not None:
                if existing["fingerprint"] == fingerprint:
                    return "replay", dict(existing)
                return "conflict", dict(existing)

            now = time.time()
            row = {
                "alert_id": "alt-" + uuid.uuid4().hex[:20],
                "alert_key": alert_key,
                "body": body,
                "fingerprint": fingerprint,
                "delivery_id": delivery_id,
                "signature": signature,
                "status": STATUS_PENDING,
                "attempts": 0,
                "last_result": None,
                "last_http_status": None,
                "failure_reason": None,
                "accepted_at": now,
                "updated_at": now,
            }
            conn.execute(
                """INSERT INTO alerts
                   (alert_id, alert_key, body, fingerprint, delivery_id, signature,
                    status, attempts, last_result, last_http_status, failure_reason,
                    accepted_at, updated_at)
                   VALUES (:alert_id, :alert_key, :body, :fingerprint, :delivery_id,
                           :signature, :status, :attempts, :last_result,
                           :last_http_status, :failure_reason, :accepted_at,
                           :updated_at)""",
                row,
            )
            return "created", row

    def get(self, alert_id: str) -> Optional[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM alerts WHERE alert_id = ?", (alert_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_by_key(self, alert_key: str) -> Optional[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM alerts WHERE alert_key = ?", (alert_key,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def begin_attempt(self, alert_id: str) -> Optional[int]:
        """把记录置为 delivering 并把尝试数 +1。

        终态记录不会被重新置活，返回 None；confirming（重试耗尽待核对）记录
        也不再发起新的投递尝试，返回 None。返回值为本次尝试序号（从 1 开始）。
        """
        now = time.time()
        with self._transaction() as conn:
            cur = conn.execute(
                """UPDATE alerts
                   SET status = ?, attempts = attempts + 1, updated_at = ?
                   WHERE alert_id = ? AND status IN (?, ?)""",
                (STATUS_DELIVERING, now, alert_id,
                 STATUS_PENDING, STATUS_DELIVERING),
            )
            if cur.rowcount == 0:
                return None
            row = conn.execute(
                "SELECT attempts FROM alerts WHERE alert_id = ?", (alert_id,)
            ).fetchone()
            return int(row["attempts"])

    def mark_confirming(self, alert_id: str) -> bool:
        """投递重试耗尽后转入 confirming（非终态），等待与接收端核对终态。"""
        now = time.time()
        with self._transaction() as conn:
            cur = conn.execute(
                """UPDATE alerts
                   SET status = ?, updated_at = ?
                   WHERE alert_id = ? AND status IN (?, ?)""",
                (STATUS_CONFIRMING, now, alert_id,
                 STATUS_PENDING, STATUS_DELIVERING),
            )
            return cur.rowcount > 0

    def record_attempt(
        self,
        alert_id: str,
        attempt_no: int,
        http_status: Optional[int],
        outcome: str,
        detail: str,
    ) -> None:
        """写入审计明细并更新“最近结果”，但不改变终态状态。"""
        now = time.time()
        with self._transaction() as conn:
            conn.execute(
                """INSERT INTO delivery_attempts
                   (alert_id, attempt_no, http_status, outcome, detail, at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (alert_id, attempt_no, http_status, outcome, detail, now),
            )
            conn.execute(
                """UPDATE alerts
                   SET last_result = ?, last_http_status = ?, updated_at = ?
                   WHERE alert_id = ?""",
                (detail, http_status, now, alert_id),
            )

    def finish(
        self,
        alert_id: str,
        status: str,
        failure_reason: Optional[str] = None,
    ) -> bool:
        """仅允许 pending/delivering/confirming -> 终态的一次性跃迁。"""
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"非法终态: {status}")
        now = time.time()
        with self._transaction() as conn:
            cur = conn.execute(
                """UPDATE alerts
                   SET status = ?, failure_reason = COALESCE(?, failure_reason),
                       updated_at = ?
                   WHERE alert_id = ? AND status IN (?, ?, ?)""",
                (status, failure_reason, now, alert_id,
                 STATUS_PENDING, STATUS_DELIVERING, STATUS_CONFIRMING),
            )
            return cur.rowcount > 0

    def due_for_attempt(
        self, stale_seconds: float = 1.0, include_active: bool = False
    ) -> List[Dict[str, Any]]:
        """恢复/兜底用：pending 立即捞，delivering/confirming 超过
        stale_seconds 视为卡死。

        进程刚启动时内存中没有任何在途投递，应传 include_active=True
        把全部非终态记录一并恢复（接收端幂等保证不会重复接纳，
        confirming 记录则重新与接收端核对终态）。
        """
        cutoff = time.time() - stale_seconds
        if include_active:
            cur = self._conn.execute(
                """SELECT * FROM alerts
                   WHERE status IN (?, ?, ?)
                   ORDER BY accepted_at""",
                (STATUS_PENDING, STATUS_DELIVERING, STATUS_CONFIRMING),
            )
        else:
            cur = self._conn.execute(
                """SELECT * FROM alerts
                   WHERE status = ?
                      OR (status IN (?, ?) AND updated_at < ?)
                   ORDER BY accepted_at""",
                (STATUS_PENDING, STATUS_DELIVERING, STATUS_CONFIRMING, cutoff),
            )
        return [dict(r) for r in cur.fetchall()]

    def count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0])

    def close(self) -> None:
        self._conn.close()


class ReceiverStore:
    """接收侧去重存储，deliveryId 即幂等键。

    两张表：
    * ``deliveries``         已接纳的 deliveryId（幂等去重 + 篡改检测）；
    * ``closed_deliveries``  发送端重试耗尽后通过 finalize 关闭的 deliveryId
      墓碑。``admit`` 与 ``finalize`` 在同一把锁下线性化：某个 deliveryId
      一旦被关闭，之后任何迟到的接纳请求（如 stall 醒来的处理线程）都会被
      拒绝，从而保证“发送端最终 failed 的投递绝不被接收端接纳”。
    """

    def __init__(self, path: str) -> None:
        self._conn = _connect(path)
        self._lock = threading.RLock()
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    delivery_id TEXT PRIMARY KEY,
                    alert_id    TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    accepted_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS closed_deliveries (
                    delivery_id TEXT PRIMARY KEY,
                    closed_at   REAL NOT NULL
                );
                """
            )

    def admit(
        self, delivery_id: str, alert_id: str, fingerprint: str
    ) -> str:
        """返回 ``new`` / ``duplicate`` / ``tampered`` / ``closed``。"""
        now = time.time()
        with self._lock:
            for attempt in range(5):
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError:
                    if attempt == 4:
                        raise
                    time.sleep(0.05 * (attempt + 1))
            try:
                closed = self._conn.execute(
                    "SELECT 1 FROM closed_deliveries WHERE delivery_id = ?",
                    (delivery_id,),
                ).fetchone()
                if closed is not None:
                    self._conn.rollback()
                    return "closed"
                row = self._conn.execute(
                    "SELECT fingerprint FROM deliveries WHERE delivery_id = ?",
                    (delivery_id,),
                ).fetchone()
                if row is not None:
                    mode = (
                        "duplicate"
                        if row["fingerprint"] == fingerprint
                        else "tampered"
                    )
                    self._conn.rollback()
                    return mode
                self._conn.execute(
                    """INSERT INTO deliveries
                       (delivery_id, alert_id, fingerprint, accepted_at)
                       VALUES (?, ?, ?, ?)""",
                    (delivery_id, alert_id, fingerprint, now),
                )
                self._conn.commit()
                return "new"
            except Exception:
                self._conn.rollback()
                raise

    def finalize(self, delivery_id: str) -> str:
        """与发送端核对 deliveryId 的最终去向，返回 ``accepted`` / ``closed``。

        已接纳 → ``accepted``；否则原子写入关闭墓碑并返回 ``closed``。
        幂等且与 :meth:`admit` 线性化：返回 ``closed`` 之后，该 deliveryId
        永远不会再被接纳。
        """
        now = time.time()
        with self._lock:
            for attempt in range(5):
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError:
                    if attempt == 4:
                        raise
                    time.sleep(0.05 * (attempt + 1))
            try:
                row = self._conn.execute(
                    "SELECT 1 FROM deliveries WHERE delivery_id = ?",
                    (delivery_id,),
                ).fetchone()
                if row is not None:
                    self._conn.rollback()
                    return "accepted"
                self._conn.execute(
                    """INSERT OR IGNORE INTO closed_deliveries
                       (delivery_id, closed_at) VALUES (?, ?)""",
                    (delivery_id, now),
                )
                self._conn.commit()
                return "closed"
            except Exception:
                self._conn.rollback()
                raise

    def list_deliveries(self) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT delivery_id, alert_id, accepted_at FROM deliveries "
            "ORDER BY accepted_at"
        )
        return [dict(r) for r in cur.fetchall()]

    def list_closed(self) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT delivery_id, closed_at FROM closed_deliveries "
            "ORDER BY closed_at"
        )
        return [dict(r) for r in cur.fetchall()]

    def count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])

    def reset(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM deliveries")
            self._conn.execute("DELETE FROM closed_deliveries")
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()
