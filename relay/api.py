"""告警可靠投递 API。

POST /api/alerts         受理（首次/幂等回放/冲突）
GET  /api/alerts/{id}    值班员查询：状态、尝试次数、最近结果与明确结论
GET  /health             健康检查

投递由进程内 worker 池执行：每次尝试用受理时入库的同一 body/deliveryId/
签名；pending/delivering 记录在服务重启后自动恢复，凭接收端幂等收敛。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

from .httpclient import (
    DEFAULT_BASE_DELAY,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_TIMEOUT,
    RetryPolicy,
    deliver_once,
)
from .signing import canonical_body, sign, validate_alert_payload
from .store import (
    STATUS_DELIVERED,
    STATUS_FAILED,
    AlertStore,
)


@dataclass
class Config:
    receiver_host: str = "receiver"
    receiver_port: int = 8081
    receiver_path: str = "/gateway/alerts"
    secret: str = "earthquake-relay-secret"
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    timeout: float = DEFAULT_TIMEOUT
    backoff_base: float = DEFAULT_BASE_DELAY
    workers: int = 4
    sweep_interval: float = 1.0

    @staticmethod
    def from_env() -> "Config":
        return Config(
            receiver_host=os.getenv("RECEIVER_HOST", "receiver"),
            receiver_port=int(os.getenv("RECEIVER_PORT", "8081")),
            receiver_path=os.getenv("RECEIVER_PATH", "/gateway/alerts"),
            secret=os.getenv("SHARED_SECRET", "earthquake-relay-secret"),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", str(DEFAULT_MAX_ATTEMPTS))),
            timeout=float(os.getenv("REQUEST_TIMEOUT", str(DEFAULT_TIMEOUT))),
            backoff_base=float(os.getenv("BACKOFF_BASE", str(DEFAULT_BASE_DELAY))),
            workers=int(os.getenv("WORKER_THREADS", "4")),
            sweep_interval=float(os.getenv("SWEEP_INTERVAL", "1.0")),
        )


class DeliveryPool:
    """单飞去重的投递 worker 池 + 周期兜底扫描。"""

    def __init__(self, store: AlertStore, cfg: Config) -> None:
        self.store = store
        self.cfg = cfg
        self.policy = RetryPolicy(max_attempts=cfg.max_attempts,
                                  base_delay=cfg.backoff_base)
        self._q: "queue.Queue[str]" = queue.Queue()
        self._assigned: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._sweeper: Optional[threading.Thread] = None

    def start(self) -> None:
        for i in range(max(1, self.cfg.workers)):
            t = threading.Thread(
                target=self._worker_loop, name=f"delivery-{i}", daemon=True
            )
            t.start()
            self._threads.append(t)
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="delivery-sweep", daemon=True
        )
        self._sweeper.start()
        # 进程重启恢复：新进程没有任何在途投递，捞回全部非终态告警，
        # 凭接收端 deliveryId 幂等去重安全重投。
        for row in self.store.due_for_attempt(include_active=True):
            self.enqueue(row["alert_id"])

    def stop(self) -> None:
        self._stop.set()

    def enqueue(self, alert_id: str) -> None:
        with self._lock:
            if alert_id in self._assigned:
                return
            self._assigned.add(alert_id)
        self._q.put(alert_id)

    # ---- 内部 ----
    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                alert_id = self._q.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                self._deliver(alert_id)
            except Exception as exc:  # pragma: no cover - 防御性
                try:
                    self.store.finish(alert_id, STATUS_FAILED, f"投递器异常: {exc}")
                except Exception:
                    pass  # 进程关闭竞态：存储可能已关闭
            finally:
                with self._lock:
                    self._assigned.discard(alert_id)
                self._q.task_done()

    def _sweep_loop(self) -> None:
        """兜底：未达终态且当前无人认领的告警重新入队。"""
        while not self._stop.wait(self.cfg.sweep_interval):
            try:
                for row in self.store.due_for_attempt():
                    self.enqueue(row["alert_id"])
            except Exception:  # pragma: no cover - 扫描不应杀死线程
                pass

    def _deliver(self, alert_id: str) -> None:
        while not self._stop.is_set():
            row = self.store.get(alert_id)
            if row is None:
                return
            if row["status"] in (STATUS_DELIVERED, STATUS_FAILED):
                return
            attempts = int(row["attempts"])
            if attempts >= self.cfg.max_attempts:
                self.store.finish(
                    alert_id,
                    STATUS_FAILED,
                    f"重试耗尽：{attempts} 次尝试后接收端仍未确认接纳",
                )
                return

            attempt_no = self.store.begin_attempt(alert_id)
            if attempt_no is None:
                return  # 已被其他流程置为终态
            result = deliver_once(
                host=self.cfg.receiver_host,
                port=self.cfg.receiver_port,
                path=self.cfg.receiver_path,
                body=row["body"],
                alert_id=alert_id,
                delivery_id=row["delivery_id"],
                signature=row["signature"],
                timeout=self.cfg.timeout,
            )
            # 进程/池已进入停止流程（如重启），本次在途结果交给新进程
            # 凭接收端幂等收敛，本 worker 不再改写状态。
            if self._stop.is_set():
                return
            self.store.record_attempt(
                alert_id,
                attempt_no,
                result.http_status,
                result.outcome,
                result.detail,
            )

            if result.outcome == "accepted":
                self.store.finish(alert_id, STATUS_DELIVERED)
                return
            if result.outcome == "unretryable":
                self.store.finish(
                    alert_id,
                    STATUS_FAILED,
                    f"接收端返回不可重试响应（HTTP {result.http_status}），"
                    f"立即失败：{result.detail}",
                )
                return
            if attempt_no >= self.cfg.max_attempts:
                self.store.finish(
                    alert_id,
                    STATUS_FAILED,
                    f"重试耗尽：已尝试 {attempt_no} 次（超时/断连/5xx），"
                    f"接收端未确认接纳；最近结果：{result.detail}",
                )
                return
            if self._stop.wait(self.policy.backoff(attempt_no)):
                return


def status_view(row: Dict[str, Any]) -> Dict[str, Any]:
    """值班员视图：除机器字段外给出明确的人类可读结论。"""
    status = row["status"]
    if status == STATUS_DELIVERED:
        conclusion = "✅ 告警已被应急广播网关唯一接纳（deliveryId 幂等确认）"
        terminal = True
    elif status == STATUS_FAILED:
        conclusion = f"❌ 最终失败：{row.get('failure_reason') or row.get('last_result')}"
        terminal = True
    elif status == "delivering":
        conclusion = "⏳ 正在投递，已开始尝试，尚未得到接收端确认"
        terminal = False
    else:
        conclusion = "⏳ 已受理，等待投递"
        terminal = False
    return {
        "alertId": row["alert_id"],
        "alertKey": row["alert_key"],
        "deliveryId": row["delivery_id"],
        "status": status,
        "attempts": row["attempts"],
        "lastResult": row["last_result"],
        "lastHttpStatus": row["last_http_status"],
        "failureReason": row["failure_reason"],
        "terminal": terminal,
        "conclusion": conclusion,
        "acceptedAt": row["accepted_at"],
        "updatedAt": row["updated_at"],
    }


def build_handler(store: AlertStore, cfg: Config, pool: Optional[DeliveryPool] = None):
    class ApiHandler(BaseHTTPRequestHandler):
        server_version = "SeismicAlertRelay/1.0"

        def log_message(self, *_args: Any) -> None:
            pass

        def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._send_json(200, {"status": "ok", "alerts": store.count()})
                return
            if self.path.startswith("/api/alerts/"):
                alert_id = self.path.rsplit("/", 1)[-1]
                row = store.get(alert_id)
                if row is None:
                    self._send_json(404, {"error": "告警不存在", "alertId": alert_id})
                    return
                self._send_json(200, status_view(row))
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/api/alerts":
                self._send_json(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json(400, {"error": "Content-Length 无效"})
                return
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                self._send_json(400, {"error": "请求体不是合法 JSON"})
                return

            errors = validate_alert_payload(payload)
            if errors:
                self._send_json(400, {"error": "告警校验失败", "details": errors})
                return

            body = canonical_body(payload)
            fingerprint_hash = hashlib.sha256(body).hexdigest()
            delivery_id = "dlv-" + uuid.uuid4().hex
            signature = sign(cfg.secret, delivery_id, body)
            mode, row = store.admit(
                payload["alertKey"], body, fingerprint_hash,
                delivery_id, signature,
            )
            if mode == "conflict":
                self._send_json(
                    409,
                    {
                        "error": "冲突：同一 alertKey 已受理但内容不同",
                        "alertKey": payload["alertKey"],
                        "existingAlertId": row["alert_id"],
                        "existingDeliveryId": row["delivery_id"],
                    },
                )
                return

            response = {
                "alertId": row["alert_id"],
                "deliveryId": row["delivery_id"],
                "status": row["status"],
                "replayed": mode == "replay",
            }
            if mode == "created" and pool is not None:
                pool.enqueue(row["alert_id"])
            self._send_json(201 if mode == "created" else 200, response)

    return ApiHandler


def create_server(
    host: str,
    port: int,
    store: AlertStore,
    cfg: Config,
    start_pool: bool = True,
) -> tuple[ThreadingHTTPServer, Optional[DeliveryPool]]:
    pool: Optional[DeliveryPool] = None
    if start_pool:
        pool = DeliveryPool(store, cfg)
    server = ThreadingHTTPServer(
        (host, port), build_handler(store, cfg, pool)
    )
    server.daemon_threads = True
    if pool is not None:
        pool.start()
    return server, pool


def main() -> None:
    parser = argparse.ArgumentParser(description="地震告警可靠投递 API")
    parser.add_argument("--host", default=os.getenv("API_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "8080")))
    parser.add_argument("--db", default=os.getenv("API_DB", "/data/api.db"))
    args = parser.parse_args()

    cfg = Config.from_env()
    store = AlertStore(args.db)
    server, pool = create_server(args.host, args.port, store, cfg)
    print(f"[api] 监听 {args.host}:{args.port}，db={args.db}，"
          f"下游 {cfg.receiver_host}:{cfg.receiver_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if pool:
            pool.stop()
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
