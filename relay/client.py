"""测试与冒烟脚本共用的极简 HTTP 客户端（标准库 urllib）。"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple


def _request(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 5.0,
) -> Tuple[int, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        status = exc.code
    except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
        raise ConnectionError(f"{method} {url} 失败: {exc}") from exc
    try:
        return status, json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return status, raw.decode("utf-8", "replace")


def get_json(url: str, timeout: float = 5.0) -> Tuple[int, Any]:
    return _request("GET", url, timeout=timeout)


def post_json(
    url: str, payload: Dict[str, Any], timeout: float = 5.0
) -> Tuple[int, Any]:
    return _request("POST", url, payload=payload, timeout=timeout)


def wait_healthy(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last: Optional[Exception] = None
    while time.time() < deadline:
        try:
            status, body = get_json(url, timeout=2)
            if status == 200:
                return
            last = RuntimeError(f"状态码 {status}: {body}")
        except ConnectionError as exc:
            last = exc
        time.sleep(0.3)
    raise TimeoutError(f"健康检查未通过：{url}（{last}）")


def post_alert(api_base: str, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    return post_json(f"{api_base}/api/alerts", payload)


def get_alert(api_base: str, alert_id: str) -> Tuple[int, Dict[str, Any]]:
    return get_json(f"{api_base}/api/alerts/{alert_id}")


def set_fault(
    receiver_admin: str,
    mode: str,
    count: int = 0,
    status: int = 503,
    seconds: float = 10.0,
) -> Tuple[int, Any]:
    return post_json(
        f"{receiver_admin}/admin/faults",
        {"mode": mode, "count": count, "status": status, "seconds": seconds},
    )


def reset_receiver(receiver_admin: str) -> None:
    try:
        post_json(f"{receiver_admin}/admin/reset", {})
    except ConnectionError:
        pass


def poll_alert(
    api_base: str,
    alert_id: str,
    predicate,
    timeout: float = 30.0,
    interval: float = 0.3,
) -> Dict[str, Any]:
    """轮询 GET 直到 predicate(view) 为真；超时抛出最后视图。"""
    deadline = time.time() + timeout
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        status, last = get_alert(api_base, alert_id)
        if status == 200 and predicate(last):
            return last
        time.sleep(interval)
    raise AssertionError(f"轮询超时，最后状态：{json.dumps(last, ensure_ascii=False)}")


def delivery_count(receiver_admin: str) -> int:
    status, body = get_json(f"{receiver_admin}/admin/deliveries")
    if status != 200:
        raise RuntimeError(f"读取接纳记录失败: {body}")
    return len(body["deliveries"])
