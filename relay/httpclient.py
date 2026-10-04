"""投递 HTTP 客户端与重试策略（仅标准库）。

结果分类：
* 2xx          —— 已被接收端业务接纳（含幂等重复接纳），delivered；
* 4xx          —— 不可重试响应，立即 failed；
* 5xx          —— 可重试，最多再试 3 次；
* 超时/断连    —— 可重试（对方可能已接纳，下一次靠 deliveryId 幂等收敛）。

每次重试用的 body 字节、deliveryId、签名都来自首次受理时入库的同一份数据。

重试耗尽后调用 :func:`finalize_once` 与接收端核对 deliveryId 最终去向
（accepted/closed），核对不到明确结论时保持非终态，避免“发送端判失败、
接收端稍后接纳”的两端不一致。
"""

from __future__ import annotations

import http.client
import json
import random
import socket
import time
from dataclasses import dataclass
from typing import Optional, Tuple

# 首次尝试 + 最多 3 次重试
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_TIMEOUT = 3.0
DEFAULT_BASE_DELAY = 0.5


@dataclass
class DeliveryResult:
    outcome: str  # accepted | unretryable | retryable
    http_status: Optional[int]
    detail: str
    duplicate: bool = False


@dataclass
class FinalizeResult:
    """终态核对结果。

    * ``accepted`` —— 接收端确认已接纳该 deliveryId，发送端应置 delivered；
    * ``closed``   —— 接收端确认未接纳且已关闭该 deliveryId（迟到投递将被拒），
      发送端应置 failed；
    * ``unknown``  —— 核对本身未获响应（超时/断连/非预期响应），发送端必须
      保持非终态并稍后重新核对，绝不能据此判定终态。
    """

    outcome: str  # accepted | closed | unknown
    http_status: Optional[int]
    detail: str


def deliver_once(
    host: str,
    port: int,
    path: str,
    body: bytes,
    alert_id: str,
    delivery_id: str,
    signature: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> DeliveryResult:
    """执行一次投递。注意不做重试，重试编排见 :class:`RetryPolicy`。"""
    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "X-Alert-Id": alert_id,
        "X-Delivery-Id": delivery_id,
        "X-Signature": signature,
    }
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
        except (socket.timeout, TimeoutError):
            return DeliveryResult("retryable", None, "等待响应超时")
        except (ConnectionError, socket.gaierror, OSError) as exc:
            # 对端可能已在写库后断连——结果未知，按可重试处理。
            return DeliveryResult(
                "retryable", None, f"连接中断: {type(exc).__name__}: {exc}"
            )

        status = resp.status
        raw = resp.read(4096)
        detail, duplicate = _parse_response(raw)
        if 200 <= status < 300:
            return DeliveryResult(
                "accepted",
                status,
                detail or "接收端已接纳",
                duplicate=duplicate,
            )
        if 400 <= status < 500:
            return DeliveryResult(
                "unretryable", status, f"不可重试响应 {status}: {detail}"
            )
        if 500 <= status < 600:
            return DeliveryResult(
                "retryable", status, f"接收端 {status}: {detail or '服务内部错误'}"
            )
        return DeliveryResult(
            "retryable", status, f"非预期状态码 {status}: {detail}"
        )
    finally:
        conn.close()


def _parse_response(raw: bytes) -> Tuple[str, bool]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        return raw[:200].decode("utf-8", "replace"), False
    if isinstance(payload, dict):
        return str(payload.get("message", payload)), bool(payload.get("duplicate"))
    return str(payload), False


def finalize_once(
    host: str,
    port: int,
    path: str,
    body: bytes,
    alert_id: str,
    delivery_id: str,
    signature: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> FinalizeResult:
    """向接收端核对某 deliveryId 的最终去向（单次请求，不做重试）。

    只有拿到接收端明确的 accepted/closed 应答才可判定终态；其余一切情况
    （超时、断连、4xx/5xx、响应无法解析）都返回 ``unknown``，由调用方保持
    非终态并稍后重试。
    """
    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "X-Alert-Id": alert_id,
        "X-Delivery-Id": delivery_id,
        "X-Signature": signature,
    }
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
        except (socket.timeout, TimeoutError):
            return FinalizeResult("unknown", None, "终态核对等待响应超时")
        except (ConnectionError, socket.gaierror, OSError) as exc:
            return FinalizeResult(
                "unknown", None, f"终态核对连接中断: {type(exc).__name__}: {exc}"
            )

        status = resp.status
        raw = resp.read(4096)
        if 200 <= status < 300:
            try:
                payload = json.loads(raw.decode("utf-8"))
                outcome = payload.get("outcome") if isinstance(payload, dict) else None
            except (json.JSONDecodeError, UnicodeDecodeError):
                outcome = None
            if outcome in ("accepted", "closed"):
                message = ""
                if isinstance(payload, dict):
                    message = str(payload.get("message", ""))
                return FinalizeResult(outcome, status, message or f"接收端确认 {outcome}")
            return FinalizeResult(
                "unknown", status, f"终态核对响应无法识别: {raw[:200]!r}"
            )
        return FinalizeResult(
            "unknown", status, f"终态核对收到非预期状态码 {status}"
        )
    finally:
        conn.close()


class RetryPolicy:
    """指数退避；attempts 上限默认 4（首次 + 3 次重试）。"""

    def __init__(
        self,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_delay: float = DEFAULT_BASE_DELAY,
        sleep=time.sleep,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self._sleep = sleep
        self._rng = rng or random.Random()

    def backoff(self, attempt_no: int) -> float:
        # attempt_no 为刚失败的尝试序号：1->base, 2->2base, 3->4base
        delay = self.base_delay * (2 ** (attempt_no - 1))
        return delay * (0.8 + 0.4 * self._rng.random())
