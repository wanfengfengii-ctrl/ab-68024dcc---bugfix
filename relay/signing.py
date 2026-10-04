"""请求体规范化、内容指纹与 HMAC-SHA256 签名。

签名规则（API 与接收模拟器必须一致）：

    base = delivery_id.encode() + b"." + raw_body
    sig  = "sha256=" + hmac_sha256(shared_secret, base).hexdigest()

raw_body 为 :func:`canonical_body` 产出的字节序列，因此同一业务内容无论
JSON 字段先后顺序如何，字节、指纹、签名都完全一致；每次重试直接复用入库
的原始字节与签名字符串，保证“完全相同的请求体、deliveryId 与签名”。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from typing import Any, Dict, List, Tuple

ALERT_FIELDS: Tuple[str, ...] = (
    "alertKey",
    "station",
    "sequence",
    "level",
    "observedAt",
    "reading",
)

SIGNATURE_PREFIX = "sha256="


def canonical_body(payload: Dict[str, Any]) -> bytes:
    """把告警业务字段规范化为确定性 JSON 字节。"""
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def body_fingerprint(payload: Dict[str, Any]) -> str:
    """同键同内容判定所用的 SHA-256 指纹。"""
    return hashlib.sha256(canonical_body(payload)).hexdigest()


def validate_alert_payload(payload: Any) -> List[str]:
    """返回校验错误列表；为空表示通过。"""
    errors: List[str] = []
    if not isinstance(payload, dict):
        return ["请求体必须是 JSON 对象"]

    for field in ("alertKey", "station", "level", "observedAt"):
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"字段 {field} 必须是非空字符串")

    sequence = payload.get("sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        errors.append("字段 sequence 必须是非负整数")

    reading = payload.get("reading")
    if isinstance(reading, bool) or not isinstance(reading, (int, float)):
        errors.append("字段 reading 必须是数值")
    elif not math.isfinite(reading):
        errors.append("字段 reading 必须是有限数值")

    if set(payload.keys()) - set(ALERT_FIELDS):
        errors.append("包含未声明的额外字段")
    return errors


def sign(secret: str, delivery_id: str, body: bytes) -> str:
    """为一次投递生成（或重放）签名。"""
    mac = hmac.new(
        secret.encode("utf-8"),
        delivery_id.encode("utf-8") + b"." + body,
        hashlib.sha256,
    )
    return SIGNATURE_PREFIX + mac.hexdigest()


def signature_valid(secret: str, delivery_id: str, body: bytes, signature: str) -> bool:
    """常量时间比较，校验接收端收到的签名。"""
    if not signature:
        return False
    expected = sign(secret, delivery_id, body)
    return hmac.compare_digest(expected, signature)
