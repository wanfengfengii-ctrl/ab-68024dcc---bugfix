"""投递 HTTP 客户端、终态核对与重试策略（仅标准库）。

结果分类：
* 2xx          —— 已被接收端业务接纳（含幂等重复接纳），delivered；
* 4xx          —— 不可重试响应，立即 failed；
* 5xx          —— 可重试，最多再试 3 次；
* 超时/断连    —— 可重试（对方可能已接纳，下一次靠 deliveryId 幂等收敛）。

每次重试用的 body 字节、deliveryId、签名都来自首次受理时入库的同一份数据。

四次投递用尽后若结果仍全部未知（超时/断连/5xx），不能直接判失败：对端可能
正在处理并稍后接纳。:func:`resolve_once` 与接收端做一次原子终态核对：
* ``accepted`` —— 接收端确已接纳该 deliveryId，收敛 delivered；
* ``sealed``   —— 接收端确认从未接纳并已封存该 deliveryId
                  （此后迟到的同 deliveryId 投递一律 410，永不接纳），
                  发送端这才可以安全地判 failed；
* 核对本身超时/断连/5xx —— 结论仍未知，保持非终态稍后再核，绝不提前判失败。
"""

from __future__ import annotations

import http.client
import json
import random
import socket
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from .signing import sign

# 首次尝试 + 最多 3 次重试
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_TIMEOUT = 3.0
DEFAULT_BASE_DELAY = 0.5
# 终态核对端点（投递用尽、结果未知时使用，不计入投递次数）
DEFAULT_RESOLVE_PATH = "/gateway/deliveries/resolve"
# 终态核对重试间隔上限：拿不到确定结论就持续核对，但避免空转打满
RESOLVE_MAX_DELAY = 8.0


@dataclass
class DeliveryResult:
    outcome: str  # accepted | unretryable | retryable
    http_status: Optional[int]
    detail: str
    duplicate: bool = False


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


@dataclass
class ResolveResult:
    """终态核对结果。"""
    outcome: str  # accepted | sealed | unknown
    http_status: Optional[int]
    detail: str
    duplicate: bool = False


def resolve_once(
    host: str,
    port: int,
    path: str,
    alert_id: str,
    delivery_id: str,
    secret: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> ResolveResult:
    """投递用尽后与接收端做一次原子终态核对（不携带告警体，不构成投递）。

    接收端对该 deliveryId 的处置是原子的：
    * 已接纳   -> 200 ``{"outcome":"accepted"}``（可能附 ``duplicate:true``）；
    * 从未接纳 -> 当场封存并返回 200 ``{"outcome":"sealed"}``，此后任何迟到的
      同 deliveryId 投递都得到 410，绝不会再被接纳；
    * 核对请求本身超时/断连/5xx -> outcome=unknown，结论仍未定，稍后再核。
    """
    payload = json.dumps(
        {"alertId": alert_id, "deliveryId": delivery_id},
        separators=(",", ":"),
    ).encode("utf-8")
    # 与投递路径一致的 HMAC：封存能力只授予持有共享密钥的发送端。
    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(payload)),
        "X-Alert-Id": alert_id,
        "X-Delivery-Id": delivery_id,
        "X-Signature": sign(secret, delivery_id, payload),
    }
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        try:
            conn.request("POST", path, body=payload, headers=headers)
            resp = conn.getresponse()
        except (socket.timeout, TimeoutError):
            return ResolveResult("unknown", None, "终态核对等待响应超时")
        except (ConnectionError, socket.gaierror, OSError) as exc:
            # 接收端此刻不可达：不能假设未接纳，结论保持未知。
            return ResolveResult(
                "unknown", None, f"终态核对连接中断: {type(exc).__name__}: {exc}"
            )

        status = resp.status
        raw = resp.read(4096)
        detail = ""
        try:
            body = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = None
        if isinstance(body, dict):
            detail = str(body.get("message", body))
            outcome = body.get("outcome")
            duplicate = bool(body.get("duplicate"))
        else:
            outcome = None
            duplicate = False
            detail = raw[:200].decode("utf-8", "replace")

        if status == 200 and outcome in ("accepted", "sealed"):
            return ResolveResult(str(outcome), status, detail or outcome,
                                 duplicate=duplicate)
        if 500 <= status < 600:
            return ResolveResult("unknown", status,
                                 f"终态核对收到 {status}: {detail or '服务内部错误'}")
        # 核对端点不可用（404/405 等，如对端为旧版本）：无法形成确定结论，
        # 保持未知并继续核对，绝不退化为“先判失败”。
        return ResolveResult(
            "unknown", status, f"终态核对得到非预期响应 {status}: {detail}"
        )
    finally:
        conn.close()


def resolve_backoff_delay(round_no: int, base_delay: float = DEFAULT_BASE_DELAY) -> float:
    """终态核对轮次间退避（封顶，避免长时空转）。"""
    return min(RESOLVE_MAX_DELAY, base_delay * (2 ** min(round_no - 1, 6)))


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
