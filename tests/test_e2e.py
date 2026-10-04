import time
import unittest

from relay import client
from tests.testutil import LocalHarness


def make_alert(key_suffix, **overrides):
    alert = {
        "alertKey": f"IT-{key_suffix}",
        "station": "台网-BJ01",
        "sequence": 1,
        "level": "red",
        "observedAt": "2026-10-04T13:30:00+08:00",
        "reading": 6.8,
    }
    alert.update(overrides)
    return alert


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.h = LocalHarness()

    def tearDown(self):
        self.h.stop()

    def test_post_returns_ids_and_delivers(self):
        status, resp = client.post_alert(self.h.api_base, make_alert("happy"))
        self.assertEqual(status, 201)
        self.assertTrue(resp["alertId"].startswith("alt-"))
        self.assertTrue(resp["deliveryId"].startswith("dlv-"))
        view = client.poll_alert(self.h.api_base, resp["alertId"],
                                 lambda v: v["status"] == "delivered")
        self.assertEqual(view["attempts"], 1)
        self.assertIn("唯一接纳", view["conclusion"])
        self.assertEqual(client.delivery_count(self.h.admin_base), 1)

    def test_replay_same_content(self):
        alert = make_alert("replay")
        s1, r1 = client.post_alert(self.h.api_base, alert)
        self.assertEqual(s1, 201)
        client.poll_alert(self.h.api_base, r1["alertId"],
                          lambda v: v["status"] == "delivered")
        s2, r2 = client.post_alert(self.h.api_base, make_alert("replay"))
        self.assertEqual(s2, 200)
        self.assertTrue(r2["replayed"])
        self.assertEqual(r2["alertId"], r1["alertId"])
        self.assertEqual(r2["deliveryId"], r1["deliveryId"])
        self.assertEqual(client.delivery_count(self.h.admin_base), 1)

    def test_conflict_different_content(self):
        s1, r1 = client.post_alert(self.h.api_base, make_alert("conf"))
        self.assertEqual(s1, 201)
        s2, r2 = client.post_alert(
            self.h.api_base, make_alert("conf", reading=7.2)
        )
        self.assertEqual(s2, 409)
        self.assertEqual(r2["existingAlertId"], r1["alertId"])

    def test_bad_payload_400(self):
        status, resp = client.post_alert(self.h.api_base, {"alertKey": "x"})
        self.assertEqual(status, 400)
        self.assertIn("details", resp)

    def test_unknown_alert_404(self):
        status, _ = client.get_alert(self.h.api_base, "alt-does-not-exist")
        self.assertEqual(status, 404)

    def test_5xx_then_success(self):
        client.set_fault(self.h.admin_base, "http_error", count=2, status=503)
        s, r = client.post_alert(self.h.api_base, make_alert("5xx"))
        view = client.poll_alert(self.h.api_base, r["alertId"],
                                 lambda v: v["status"] == "delivered",
                                 timeout=30)
        self.assertEqual(view["attempts"], 3)
        self.assertEqual(client.delivery_count(self.h.admin_base), 1)

    def test_drop_after_accept_converges_once(self):
        client.set_fault(self.h.admin_base, "drop_after_accept", count=1)
        s, r = client.post_alert(self.h.api_base, make_alert("dropa"))
        view = client.poll_alert(self.h.api_base, r["alertId"],
                                 lambda v: v["status"] == "delivered",
                                 timeout=30)
        self.assertEqual(view["attempts"], 2)
        ids = [d["deliveryId"]
               for d in client.get_json(f"{self.h.admin_base}/admin/deliveries")[1]["deliveries"]]
        self.assertEqual(ids.count(r["deliveryId"]), 1)

    def test_drop_before_accept_retries(self):
        client.set_fault(self.h.admin_base, "drop_before_accept", count=3)
        s, r = client.post_alert(self.h.api_base, make_alert("dropb"))
        view = client.poll_alert(self.h.api_base, r["alertId"],
                                 lambda v: v["status"] == "delivered",
                                 timeout=30)
        self.assertEqual(view["attempts"], 4)
        self.assertEqual(client.delivery_count(self.h.admin_base), 1)

    def test_4xx_fails_immediately(self):
        client.set_fault(self.h.admin_base, "always", status=403)
        s, r = client.post_alert(self.h.api_base, make_alert("4xx"))
        view = client.poll_alert(self.h.api_base, r["alertId"],
                                 lambda v: v["status"] == "failed",
                                 timeout=20)
        self.assertEqual(view["attempts"], 1)
        self.assertEqual(view["lastHttpStatus"], 403)
        self.assertIn("不可重试", view["failureReason"])
        self.assertEqual(client.delivery_count(self.h.admin_base), 0)

    def test_retries_exhausted(self):
        client.set_fault(self.h.admin_base, "always", status=500)
        s, r = client.post_alert(self.h.api_base, make_alert("exhaust"))
        view = client.poll_alert(self.h.api_base, r["alertId"],
                                 lambda v: v["status"] == "failed",
                                 timeout=40)
        self.assertEqual(view["attempts"], 4)
        self.assertIn("重试耗尽", view["failureReason"])
        self.assertIn("最终失败", view["conclusion"])

    def test_restart_during_delivery_converges(self):
        # 用 stall 制造一次长时间在途投递（客户端 1s 超时、对端处理 10s）。
        # 在首次尝试进行中重启 API：新进程从库中恢复 delivering 记录，
        # 凭同一 deliveryId 重投，接收端只接纳一次。
        client.set_fault(self.h.admin_base, "stall", count=1, seconds=10)
        s, r = client.post_alert(self.h.api_base, make_alert("restart"))
        # 等待首次尝试开始（状态变 delivering，尝试数 +1）
        client.poll_alert(
            self.h.api_base, r["alertId"],
            lambda v: v["status"] == "delivering" and v["attempts"] == 1,
        )
        time.sleep(0.3)  # 确保请求仍在对端挂起
        self.h.restart_api()
        client.set_fault(self.h.admin_base, "ok")
        view = client.poll_alert(self.h.api_base, r["alertId"],
                                 lambda v: v["status"] == "delivered",
                                 timeout=30)
        self.assertEqual(view["status"], "delivered")
        ids = [d["deliveryId"]
               for d in client.get_json(f"{self.h.admin_base}/admin/deliveries")[1]["deliveries"]]
        self.assertEqual(ids.count(r["deliveryId"]), 1)
        # 重启不改变既有标识；首次尝试已计数，恢复后第二次成功
        self.assertEqual(view["deliveryId"], r["deliveryId"])
        self.assertEqual(view["attempts"], 2)

    def test_restart_recovers_pending(self):
        # 投递池停止时受理：记录停留在 pending 且从未尝试；
        # 重启 API 后新进程必须自动恢复并投递成功。
        self.h.pool.stop()
        s, r = client.post_alert(self.h.api_base, make_alert("pend-restart"))
        self.assertEqual(s, 201)
        time.sleep(0.5)
        _, still_pending = client.get_alert(self.h.api_base, r["alertId"])
        self.assertEqual(still_pending["status"], "pending")
        self.assertEqual(still_pending["attempts"], 0)
        self.h.restart_api()
        view = client.poll_alert(self.h.api_base, r["alertId"],
                                 lambda v: v["status"] == "delivered",
                                 timeout=30)
        self.assertEqual(view["status"], "delivered")
        self.assertEqual(view["attempts"], 1)

    def test_four_timeouts_late_accept_converges_delivered(self):
        # 题述场景：四次投递都在超时后才被接收端慢处理接纳。
        # 每次 stall 2s（>1s 客户端超时），首个慢线程在第 2 秒已接纳，
        # 四次投递约 4s 用尽后终态核对必须确认“已接纳”并收敛 delivered。
        client.set_fault(self.h.admin_base, "stall", count=4, seconds=2)
        s, r = client.post_alert(self.h.api_base, make_alert("late-accept"))
        self.assertEqual(s, 201)
        view = client.poll_alert(
            self.h.api_base, r["alertId"],
            lambda v: v["terminal"] is True, timeout=20,
        )
        self.assertEqual(view["status"], "delivered")
        self.assertEqual(view["attempts"], 4)
        # 等待所有慢处理线程醒来（均为幂等重放，不重复接纳）
        time.sleep(7)
        ids = [d["deliveryId"]
               for d in client.get_json(f"{self.h.admin_base}/admin/deliveries")[1]["deliveries"]]
        self.assertEqual(ids.count(r["deliveryId"]), 1)
        _, still = client.get_alert(self.h.api_base, r["alertId"])
        self.assertEqual(still["status"], "delivered")

    def test_four_unknown_sealed_then_late_post_rejected(self):
        # 反向竞态：四次超时仅约 4s，慢线程 8s 后才醒来。终态核对先封存，
        # API 判 failed；此后醒来的同 deliveryId 投递必须被 410 拒绝。
        client.set_fault(self.h.admin_base, "stall", count=4, seconds=8)
        s, r = client.post_alert(self.h.api_base, make_alert("seal-race"))
        self.assertEqual(s, 201)
        view = client.poll_alert(
            self.h.api_base, r["alertId"],
            lambda v: v["terminal"] is True, timeout=15,
        )
        self.assertEqual(view["status"], "failed")
        self.assertEqual(view["attempts"], 4)
        self.assertIn("终态核对", view["failureReason"])
        # 等全部慢线程醒来尝试接纳
        time.sleep(9)
        deliveries = client.get_json(
            f"{self.h.admin_base}/admin/deliveries")[1]["deliveries"]
        rec = {d["deliveryId"]: d for d in deliveries}[r["deliveryId"]]
        self.assertTrue(rec["sealed"])
        _, still = client.get_alert(self.h.api_base, r["alertId"])
        self.assertEqual(still["status"], "failed")

    def test_resolve_unreachable_keeps_nonterminal_then_settles(self):
        # 核对期间接收端宕机：四次投递用尽也不得判终态；
        # API 重启后继续核对；接收端带着持久化库恢复后收敛封存判失败，
        # 接收端再重启封存仍在。
        self.h.stop_receiver()
        s, r = client.post_alert(self.h.api_base, make_alert("recon-down"))
        self.assertEqual(s, 201)
        client.poll_alert(
            self.h.api_base, r["alertId"],
            lambda v: v["status"] == "delivering" and v["attempts"] == 4,
            timeout=10,
        )
        for _ in range(10):
            _, v = client.get_alert(self.h.api_base, r["alertId"])
            self.assertEqual(v["status"], "delivering")
            time.sleep(0.1)
        self.h.restart_api()
        time.sleep(0.5)
        _, v = client.get_alert(self.h.api_base, r["alertId"])
        self.assertEqual(v["status"], "delivering")
        self.h.restart_receiver()
        view = client.poll_alert(
            self.h.api_base, r["alertId"],
            lambda x: x["terminal"] is True, timeout=15,
        )
        self.assertEqual(view["status"], "failed")
        self.h.restart_receiver()
        deliveries = client.get_json(
            f"{self.h.admin_base}/admin/deliveries")[1]["deliveries"]
        rec = {d["deliveryId"]: d for d in deliveries}[r["deliveryId"]]
        self.assertTrue(rec["sealed"])


if __name__ == "__main__":
    unittest.main()
