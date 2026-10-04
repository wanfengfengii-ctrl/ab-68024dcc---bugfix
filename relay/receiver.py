"""应急广播网关接收模拟器。

职责：
1. 校验共享密钥签名（HMAC-SHA256），签名不符返回 401（不可重试）；
2. 以 deliveryId 为幂等键落库：重复投递原样返回“已接纳”，同 deliveryId
   不同请求体返回 409（篡改检测）；进程重启后去重表仍然有效；
3. 投递路径为 ``POST /gateway/alerts``；终态核对路径为
   ``POST /gateway/deliveries/resolve``：发送端四次投递用尽且结果全部未知时，
   原子地给出“已接纳 / 已封存（永不接纳）”的确定结论（见
   :meth:`ReceiverStore.resolve`）。故障注入只作用于投递路径，不作用于核对；
4. 通过 /admin/faults 注入网络/服务故障，供冒烟测试验证重试与收敛：
   - http_error: 前 N 次请求返回指定状态码（默认 503，可重试）
   - always:    始终返回指定状态码（默认 404，演示 4xx 立即失败）
   - drop_before_accept: 前 N 次直接断开连接（尚未接纳）
   - drop_after_accept:  前 N 次先落库接纳、再断开连接（结果未知场景）
   - stall:     前 N 次延迟响应（模拟对端超时）
   - ok:        清除故障
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

from .signing import signature_valid
from .store import STATE_SEALED, ReceiverStore

DELIVERIES_PATH = "/gateway/alerts"
RESOLVE_PATH = "/gateway/deliveries/resolve"


class FaultController:
    """线程安全的故障脚本。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mode = "ok"
        self._count = 0
        self._status = 503
        self._seconds = 10.0

    def configure(
        self,
        mode: str,
        count: int = 0,
        status: int = 503,
        seconds: float = 10.0,
    ) -> None:
        with self._lock:
            self._mode = mode
            self._count = max(0, int(count))
            self._status = int(status)
            self._seconds = float(seconds)

    def take(self) -> Dict[str, Any]:
        """取出下一次请求应执行的故障动作（一次性计数故障逐次消耗）。"""
        with self._lock:
            if self._mode == "ok":
                return {"mode": "ok"}
            if self._mode == "always":
                return {"mode": "http", "status": self._status}
            action: Dict[str, Any]
            if self._mode == "http_error":
                action = {"mode": "http", "status": self._status}
            elif self._mode == "stall":
                action = {"mode": "stall", "seconds": self._seconds}
            else:  # drop_before_accept / drop_after_accept
                action = {"mode": self._mode}
            self._count -= 1
            if self._count <= 0:
                self._mode = "ok"
                self._count = 0
            return action

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "mode": self._mode,
                "remaining": self._count,
                "status": self._status,
                "seconds": self._seconds,
            }


def build_handler(store: ReceiverStore, secret: str, faults: FaultController):
    class GatewayHandler(BaseHTTPRequestHandler):
        server_version = "EmergencyGateway/1.0"

        def log_message(self, *_args: Any) -> None:
            pass  # 由上层统一日志，避免冒烟输出噪音

        # ---- 工具 ----
        def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _drop(self) -> None:
            try:
                self.wfile.flush()
            except OSError:
                pass
            try:
                self.connection.shutdown(2)  # SHUT_RDWR
            except OSError:
                pass
            self.connection.close()

        # ---- GET ----
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._send_json(
                    200, {"status": "ok", "accepted": store.count()}
                )
                return
            if self.path == "/admin/faults":
                self._send_json(200, faults.snapshot())
                return
            if self.path == "/admin/deliveries":
                self._send_json(200, {
                    "deliveries": [
                        {
                            "deliveryId": d["delivery_id"],
                            "alertId": d["alert_id"],
                            "state": d["state"],
                            "sealed": d["state"] == STATE_SEALED,
                            "acceptedAt": d["accepted_at"],
                        }
                        for d in store.list_deliveries()
                    ]
                })
                return
            self._send_json(404, {"error": "not found"})

        # ---- POST ----
        def do_POST(self) -> None:  # noqa: N802
            if self.path == "/admin/faults":
                self._configure_faults()
                return
            if self.path == "/admin/reset":
                store.reset()
                self._send_json(200, {"message": "已清空接纳记录"})
                return
            # 终态核对不经过故障脚本：它的存在意义就是在投递链路超时/中断时
            # 给出确定结论，故必须始终可判定。
            if self.path == RESOLVE_PATH:
                try:
                    self._handle_resolve()
                except (sqlite3.Error, OSError):
                    # 存储异常/进程关闭竞态：返回 5xx，发送端按“结论未知”继续
                    # 核对，绝不向 stderr 抛栈。
                    try:
                        self._send_json(500, {"error": "终态核对暂不可用，请重试"})
                    except OSError:
                        pass
                return
            if self.path != DELIVERIES_PATH:
                self._send_json(404, {"error": "not found"})
                return
            try:
                self._handle_delivery()
            except (sqlite3.Error, OSError):
                # 进程关闭竞态（存储已关/连接已断）：当作连接中断处理，
                # 客户端会凭 deliveryId 重试，绝不向 stderr 抛栈。
                try:
                    self._drop()
                except OSError:
                    pass

        def _configure_faults(self) -> None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                faults.configure(
                    mode=str(payload.get("mode", "ok")),
                    count=int(payload.get("count", 0)),
                    status=int(payload.get("status", 503)),
                    seconds=float(payload.get("seconds", 10.0)),
                )
                self._send_json(200, {"message": "故障脚本已设置", **faults.snapshot()})
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self._send_json(400, {"error": f"故障配置无效: {exc}"})

        def _handle_delivery(self) -> None:
            delivery_id = self.headers.get("X-Delivery-Id", "")
            alert_id = self.headers.get("X-Alert-Id", "")
            signature = self.headers.get("X-Signature", "")
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json(400, {"error": "Content-Length 无效"})
                return
            body = self.rfile.read(length)

            action = faults.take()

            # 先接纳后断连：提交去重表后立刻掐断，客户端只看到连接错误。
            if action["mode"] == "drop_after_accept":
                self._admit_then_drop(body, alert_id, delivery_id, signature)
                return
            if action["mode"] == "drop_before_accept":
                self._drop()
                return
            if action["mode"] == "stall":
                time.sleep(action["seconds"])
            if action["mode"] == "http":
                self._send_json(
                    action["status"],
                    {"message": f"注入故障 HTTP {action['status']}"},
                )
                return

            # ---- 正常校验与幂等接纳 ----
            if not delivery_id or not alert_id:
                self._send_json(400, {"error": "缺少 X-Delivery-Id/X-Alert-Id 头"})
                return
            if not signature_valid(secret, delivery_id, body, signature):
                self._send_json(401, {"error": "签名校验失败"})
                return
            try:
                payload = json.loads(body.decode("utf-8"))
                assert isinstance(payload, dict)
            except (json.JSONDecodeError, UnicodeDecodeError, AssertionError):
                self._send_json(400, {"error": "请求体不是合法 JSON 对象"})
                return
            if not payload.get("alertKey") or not payload.get("station"):
                self._send_json(400, {"error": "告警字段不完整"})
                return

            fingerprint = hashlib.sha256(body).hexdigest()
            mode = store.admit(delivery_id, alert_id, fingerprint)
            if mode == "new":
                self._send_json(
                    201,
                    {
                        "message": "告警已唯一接纳",
                        "deliveryId": delivery_id,
                        "duplicate": False,
                    },
                )
            elif mode == "duplicate":
                self._send_json(
                    200,
                    {
                        "message": "该 deliveryId 此前已接纳，本次为幂等重放",
                        "deliveryId": delivery_id,
                        "duplicate": True,
                    },
                )
            elif mode == "sealed":
                # 发送端已经终态核对并判失败；此 deliveryId 永不接纳，
                # 迟到的慢处理请求必须被拒绝。
                self._send_json(
                    410,
                    {
                        "error": "该 deliveryId 已终态封存，永不接纳",
                        "deliveryId": delivery_id,
                    },
                )
            else:
                self._send_json(
                    409,
                    {"error": "同 deliveryId 请求体与首次接纳不一致，疑似篡改"},
                )

        def _handle_resolve(self) -> None:
            """投递用尽后的原子终态核对，结论可判定且不被故障脚本影响。"""
            delivery_id = self.headers.get("X-Delivery-Id", "")
            alert_id = self.headers.get("X-Alert-Id", "")
            signature = self.headers.get("X-Signature", "")
            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
            except ValueError:
                self._send_json(400, {"error": "Content-Length 无效"})
                return
            # 与投递路径相同的共享密钥鉴权：未持有密钥者不得封存他人投递。
            if (
                not delivery_id
                or not signature_valid(secret, delivery_id, raw, signature)
            ):
                self._send_json(401, {"error": "终态核对签名校验失败"})
                return
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except (json.JSONDecodeError, UnicodeDecodeError):
                payload = None
            if not isinstance(payload, dict):
                payload = {}
            delivery_id = str(payload.get("deliveryId") or delivery_id)
            alert_id = str(payload.get("alertId") or alert_id)
            if not delivery_id or not alert_id:
                self._send_json(
                    400, {"error": "缺少 deliveryId/alertId"}
                )
                return
            outcome = store.resolve(delivery_id, alert_id)
            if outcome == "accepted":
                self._send_json(
                    200,
                    {
                        "outcome": "accepted",
                        "message": "该 deliveryId 此前已接纳",
                        "deliveryId": delivery_id,
                        "duplicate": True,
                    },
                )
            else:
                self._send_json(
                    200,
                    {
                        "outcome": "sealed",
                        "message": "确认从未接纳并已封存：此后同 deliveryId 永不接纳",
                        "deliveryId": delivery_id,
                        "duplicate": False,
                    },
                )

        def _admit_then_drop(
            self, body: bytes, alert_id: str, delivery_id: str, signature: str
        ) -> None:
            """模拟“服务端已写库、响应前网络中断”。"""
            if (
                delivery_id
                and alert_id
                and signature_valid(secret, delivery_id, body, signature)
            ):
                fingerprint = hashlib.sha256(body).hexdigest()
                store.admit(delivery_id, alert_id, fingerprint)
            self._drop()

    return GatewayHandler


def create_server(
    host: str,
    port: int,
    store: ReceiverStore,
    secret: str,
    faults: Optional[FaultController] = None,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(
        (host, port), build_handler(store, secret, faults or FaultController())
    )
    server.daemon_threads = True
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="应急广播网关接收模拟器")
    parser.add_argument("--host", default=os.getenv("RECEIVER_HOST", "0.0.0.0"))
    parser.add_argument(
        "--port", type=int, default=int(os.getenv("RECEIVER_PORT", "8081"))
    )
    parser.add_argument(
        "--db", default=os.getenv("RECEIVER_DB", "/data/receiver.db")
    )
    parser.add_argument(
        "--secret", default=os.getenv("SHARED_SECRET", "earthquake-relay-secret")
    )
    args = parser.parse_args()

    store = ReceiverStore(args.db)
    server = create_server(args.host, args.port, store, args.secret)
    print(f"[receiver] 监听 {args.host}:{args.port}，db={args.db}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
