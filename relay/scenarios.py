"""端到端投递冒烟场景，verify 服务与手工复跑共用。

每个场景返回 SmokeResult；任何断言失败都会记录明确原因。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from . import client

BASE_ALERT: Dict[str, Any] = {
    "alertKey": "EQ-20261004-0001",
    "station": "台网-首都圈-BJ01",
    "sequence": 1024,
    "level": "red",
    "observedAt": "2026-10-04T13:30:00+08:00",
    "reading": 6.8,
}


@dataclass
class SmokeResult:
    name: str
    ok: bool
    detail: str
    view: Dict[str, Any] = field(default_factory=dict)


def _unique_alert(**overrides: Any) -> Dict[str, Any]:
    alert = dict(BASE_ALERT)
    alert["alertKey"] = f"SMOKE-{int(time.time()*1000)}-{_unique_alert.counter:03d}"
    _unique_alert.counter += 1
    alert.update(overrides)
    return alert


_unique_alert.counter = 0


def _terminal(view: Dict[str, Any]) -> bool:
    return bool(view.get("terminal"))


def _delivered(view: Dict[str, Any]) -> bool:
    return view.get("status") == "delivered"


def _failed(view: Dict[str, Any]) -> bool:
    return view.get("status") == "failed"


def _receiver_delivery_ids(admin: str) -> List[str]:
    status, body = client.get_json(f"{admin}/admin/deliveries")
    assert status == 200, f"读取接收端接纳记录失败: {body}"
    return [d["deliveryId"] for d in body["deliveries"]]


def _post_and_poll(
    api: str, alert: Dict[str, Any], predicate: Callable[[Dict[str, Any]], bool],
    timeout: float = 40.0,
) -> Dict[str, Any]:
    status, resp = client.post_alert(api, alert)
    assert status in (200, 201), f"受理失败 HTTP {status}: {resp}"
    assert resp.get("alertId") and resp.get("deliveryId"), "受理结果缺少 id"
    return client.poll_alert(api, resp["alertId"], predicate, timeout=timeout)


def scenario_happy_path(api: str, admin: str) -> SmokeResult:
    client.set_fault(admin, "ok")
    before = set(_receiver_delivery_ids(admin))
    alert = _unique_alert()
    view = _post_and_poll(api, alert, _delivered)
    ids = _receiver_delivery_ids(admin)
    assert view["attempts"] == 1, f"正常链路应一次成功，实际尝试 {view['attempts']}"
    assert "唯一接纳" in view["conclusion"], view["conclusion"]
    assert view["deliveryId"] in ids and view["deliveryId"] not in before
    assert ids.count(view["deliveryId"]) == 1, "接收端出现重复接纳"
    return SmokeResult("正常投递一次成功并唯一接纳", True,
                       f"attempts=1, deliveryId={view['deliveryId']}", view)


def scenario_replay(api: str, admin: str) -> SmokeResult:
    client.set_fault(admin, "ok")
    alert = _unique_alert()
    status1, r1 = client.post_alert(api, alert)
    assert status1 == 201, r1
    client.poll_alert(api, r1["alertId"], _delivered)
    n = client.delivery_count(admin)

    # 同键同内容原样回放（且故意打乱 JSON 字段顺序）
    reordered = {
        "reading": alert["reading"],
        "observedAt": alert["observedAt"],
        "level": alert["level"],
        "sequence": alert["sequence"],
        "station": alert["station"],
        "alertKey": alert["alertKey"],
    }
    status2, r2 = client.post_alert(api, reordered)
    assert status2 == 200 and r2.get("replayed") is True, r2
    assert (r2["alertId"], r2["deliveryId"]) == (r1["alertId"], r1["deliveryId"])
    assert client.delivery_count(admin) == n, "回放导致重复接纳"
    status3, view = client.get_alert(api, r1["alertId"])
    assert status3 == 200 and view["status"] == "delivered"
    return SmokeResult("同键同内容回放原结果且不重复接纳", True,
                       f"alertId/deliveryId 一致，接收端计数={n}", view)


def scenario_conflict(api: str, admin: str) -> SmokeResult:
    alert = _unique_alert()
    status1, r1 = client.post_alert(api, alert)
    assert status1 == 201, r1
    tampered = dict(alert)
    tampered["reading"] = 9.9
    status2, r2 = client.post_alert(api, tampered)
    assert status2 == 409, f"同键异内容应 409，实际 {status2}: {r2}"
    assert r2.get("existingAlertId") == r1["alertId"]
    return SmokeResult("同键异内容返回冲突 409", True,
                       f"existingAlertId={r1['alertId']}")


def scenario_retry_5xx(api: str, admin: str) -> SmokeResult:
    client.set_fault(admin, "http_error", count=2, status=503)
    try:
        alert = _unique_alert()
        view = _post_and_poll(api, alert, _delivered)
    finally:
        client.set_fault(admin, "ok")
    assert view["attempts"] == 3, f"前两次 503 第三次成功，应 attempts=3，实际 {view['attempts']}"
    ids = _receiver_delivery_ids(admin)
    assert ids.count(view["deliveryId"]) == 1, "重试导致重复接纳"
    return SmokeResult("接收端连续 5xx 后重试成功", True,
                       f"attempts=3, deliveryId={view['deliveryId']}", view)


def scenario_drop_after_accept(api: str, admin: str) -> SmokeResult:
    # 对端已落库接纳后连接中断：客户端只看到断连，重试须幂等收敛。
    client.set_fault(admin, "drop_after_accept", count=1)
    try:
        alert = _unique_alert()
        view = _post_and_poll(api, alert, _delivered)
    finally:
        client.set_fault(admin, "ok")
    assert view["attempts"] == 2, f"应首次断连+重试确认，实际 attempts={view['attempts']}"
    ids = _receiver_delivery_ids(admin)
    assert ids.count(view["deliveryId"]) == 1, "断连重试造成重复接纳"
    return SmokeResult("接纳后断连：凭 deliveryId 幂等收敛且仅接纳一次", True,
                       f"attempts=2, 接收端计数=1, deliveryId={view['deliveryId']}", view)


def scenario_4xx_fatal(api: str, admin: str) -> SmokeResult:
    client.set_fault(admin, "always", status=404)
    try:
        alert = _unique_alert()
        view = _post_and_poll(api, alert, _failed, timeout=20.0)
    finally:
        client.set_fault(admin, "ok")
    assert view["attempts"] == 1, f"4xx 必须立即失败，实际 attempts={view['attempts']}"
    assert view["lastHttpStatus"] == 404
    assert "不可重试" in (view.get("failureReason") or ""), view.get("failureReason")
    assert "最终失败" in view["conclusion"]
    ids = _receiver_delivery_ids(admin)
    assert view["deliveryId"] not in ids, "4xx 失败不应被接收端接纳"
    return SmokeResult("4xx 不可重试：立即最终失败并告知值班员", True,
                       f"attempts=1, reason={view['failureReason']}", view)


def scenario_retries_exhausted(api: str, admin: str) -> SmokeResult:
    client.set_fault(admin, "always", status=503)
    try:
        alert = _unique_alert()
        view = _post_and_poll(api, alert, _failed, timeout=40.0)
    finally:
        client.set_fault(admin, "ok")
    assert view["attempts"] == 4, f"首次+3 次重试应=4，实际 {view['attempts']}"
    assert "重试耗尽" in (view.get("failureReason") or ""), view.get("failureReason")
    assert "最终失败" in view["conclusion"]
    return SmokeResult("持续 5xx：重试三次后耗尽并明确最终失败", True,
                       f"attempts=4, reason={view['failureReason']}", view)


def scenario_timeout_then_ok(api: str, admin: str) -> SmokeResult:
    # stall 30 秒必然超过客户端 3 秒超时；只发生一次随后恢复。
    client.set_fault(admin, "stall", count=1, seconds=30)
    try:
        alert = _unique_alert()
        view = _post_and_poll(api, alert, _delivered)
    finally:
        client.set_fault(admin, "ok")
    assert view["attempts"] == 2, f"超时一次后成功，应 attempts=2，实际 {view['attempts']}"
    return SmokeResult("接收端超时无响应：重试后成功", True,
                       f"attempts=2, deliveryId={view['deliveryId']}", view)


def scenario_late_accept_after_all_timeouts(api: str, admin: str) -> SmokeResult:
    # 连续 4 次请求各延迟 12s（均超过客户端 3s 超时）：4 次尝试全部超时后，
    # 接收端挂起的处理线程仍会醒来并接纳该 deliveryId。发送端重试耗尽后
    # 必须先与接收端核对终态，最终收敛为 delivered，且接收端只接纳一次。
    client.set_fault(admin, "stall", count=4, seconds=12)
    try:
        alert = _unique_alert()
        view = _post_and_poll(api, alert, _delivered, timeout=90.0)
    finally:
        client.set_fault(admin, "ok")
    assert view["attempts"] == 4, (
        f"4 次超时后迟到接纳，应 attempts=4，实际 {view['attempts']}"
    )
    ids = _receiver_delivery_ids(admin)
    assert ids.count(view["deliveryId"]) == 1, "接收端应恰好接纳一次"
    assert "唯一接纳" in view["conclusion"], view["conclusion"]
    return SmokeResult(
        "连续四次超时后接收端迟到接纳：两端收敛一致为 delivered", True,
        f"attempts=4, deliveryId={view['deliveryId']}", view,
    )


SCENARIOS = [
    scenario_happy_path,
    scenario_replay,
    scenario_conflict,
    scenario_retry_5xx,
    scenario_drop_after_accept,
    scenario_4xx_fatal,
    scenario_retries_exhausted,
    scenario_timeout_then_ok,
    scenario_late_accept_after_all_timeouts,
]


def run_all(api: str, admin: str) -> List[SmokeResult]:
    results: List[SmokeResult] = []
    for fn in SCENARIOS:
        try:
            results.append(fn(api, admin))
            print(f"  [PASS] {results[-1].name} —— {results[-1].detail}")
        except Exception as exc:  # 场景失败不中断后续场景
            results.append(SmokeResult(fn.__name__, False, f"{type(exc).__name__}: {exc}"))
            print(f"  [FAIL] {fn.__name__} —— {type(exc).__name__}: {exc}")
    return results
