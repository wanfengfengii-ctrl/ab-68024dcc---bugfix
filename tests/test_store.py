import tempfile
import threading
import unittest
from pathlib import Path

from relay.store import (
    STATUS_CONFIRMING,
    STATUS_DELIVERED,
    STATUS_DELIVERING,
    STATUS_FAILED,
    STATUS_PENDING,
    AlertStore,
    ReceiverStore,
)


class AlertStoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.store = AlertStore(str(Path(self.dir) / "api.db"))

    def tearDown(self):
        self.store.close()

    def test_admit_replay_conflict_lifecycle(self):
        mode, row = self.store.admit("k", b'{"a":1}', "fp1", "dlv-1", "sig-1")
        self.assertEqual(mode, "created")
        self.assertEqual(row["status"], STATUS_PENDING)

        mode2, row2 = self.store.admit("k", b'{"a":1}', "fp1", "dlv-x", "sig-x")
        self.assertEqual(mode2, "replay")
        self.assertEqual(row2["alert_id"], row["alert_id"])
        self.assertEqual(row2["delivery_id"], "dlv-1")

        mode3, row3 = self.store.admit("k", b'{"a":2}', "fp2", "dlv-y", "sig-y")
        self.assertEqual(mode3, "conflict")
        self.assertEqual(row3["alert_id"], row["alert_id"])

    def test_attempts_and_terminal_transitions(self):
        _, row = self.store.admit("k", b"{}", "fp", "dlv-1", "sig")
        aid = row["alert_id"]
        self.assertEqual(self.store.begin_attempt(aid), 1)
        self.assertEqual(self.store.get(aid)["status"], STATUS_DELIVERING)
        self.store.record_attempt(aid, 1, 503, "retryable", "503")
        self.assertEqual(self.store.begin_attempt(aid), 2)
        self.store.record_attempt(aid, 2, None, "retryable", "断连")
        self.store.finish(aid, STATUS_DELIVERED)
        # 终态不可再次置活，也不可改写
        self.assertIsNone(self.store.begin_attempt(aid))
        self.assertFalse(self.store.finish(aid, STATUS_FAILED))
        self.assertEqual(self.store.get(aid)["status"], STATUS_DELIVERED)

    def test_failure_records_reason(self):
        _, row = self.store.admit("k", b"{}", "fp", "dlv-1", "sig")
        aid = row["alert_id"]
        self.store.begin_attempt(aid)
        self.store.record_attempt(aid, 1, 404, "unretryable", "404")
        self.store.finish(aid, STATUS_FAILED, "不可重试响应")
        got = self.store.get(aid)
        self.assertEqual(got["status"], STATUS_FAILED)
        self.assertEqual(got["failure_reason"], "不可重试响应")

    def test_confirming_lifecycle(self):
        _, row = self.store.admit("k", b"{}", "fp", "dlv-1", "sig")
        aid = row["alert_id"]
        self.store.begin_attempt(aid)
        self.assertTrue(self.store.mark_confirming(aid))
        self.assertEqual(self.store.get(aid)["status"], STATUS_CONFIRMING)
        # confirming 不再发起新的投递尝试
        self.assertIsNone(self.store.begin_attempt(aid))
        # confirming 属于非终态：重启恢复与兜底扫描都会捞出
        self.assertIn(
            aid,
            [r["alert_id"] for r in self.store.due_for_attempt(include_active=True)],
        )
        # confirming 可一次性跃迁到终态，之后不可再改写
        self.assertTrue(self.store.finish(aid, STATUS_FAILED, "核对后关闭"))
        self.assertFalse(self.store.mark_confirming(aid))
        self.assertEqual(self.store.get(aid)["status"], STATUS_FAILED)

    def test_concurrent_admit_single_insert(self):
        results = []

        def admit():
            # 每线程独立 deliveryId 也只能有一个 created
            import uuid
            results.append(
                self.store.admit(
                    "same-key", b"{}", "fp",
                    "dlv-" + uuid.uuid4().hex, "sig",
                )
            )

        threads = [threading.Thread(target=admit) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        created = [m for m, _ in results if m == "created"]
        replays = [m for m, _ in results if m == "replay"]
        self.assertEqual(len(created), 1)
        self.assertEqual(len(replays), 7)


class ReceiverStoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.store = ReceiverStore(str(Path(self.dir) / "recv.db"))

    def tearDown(self):
        self.store.close()

    def test_idempotent_dedup_and_tamper(self):
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp"), "new")
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp"), "duplicate")
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp-other"), "tampered")
        self.assertEqual(self.store.count(), 1)

    def test_finalize_unknown_closes_and_rejects_late_admit(self):
        # 未接纳时核对 → closed，此后迟到的接纳请求被拒绝
        self.assertEqual(self.store.finalize("dlv-1"), "closed")
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp"), "closed")
        self.assertEqual(self.store.count(), 0)
        # 核对幂等：重复核对仍是 closed
        self.assertEqual(self.store.finalize("dlv-1"), "closed")
        self.assertEqual(self.store.count(), 0)

    def test_finalize_after_accept_reports_accepted(self):
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp"), "new")
        self.assertEqual(self.store.finalize("dlv-1"), "accepted")
        # 已接纳的 deliveryId 不会被关闭，重放仍是 duplicate
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp"), "duplicate")
        self.assertEqual(self.store.count(), 1)

    def test_accept_and_close_are_linearizable_per_delivery(self):
        # 同一 deliveryId 只能有一个最终去向：先接纳则核对必答 accepted
        self.assertEqual(self.store.admit("dlv-2", "alt-2", "fp"), "new")
        self.assertEqual(self.store.finalize("dlv-2"), "accepted")
        # 先关闭则接纳必答 closed
        self.assertEqual(self.store.finalize("dlv-3"), "closed")
        self.assertEqual(self.store.admit("dlv-3", "alt-3", "fp"), "closed")
        self.assertEqual(self.store.count(), 1)

    def test_reset_clears_closed_tombstones(self):
        self.assertEqual(self.store.finalize("dlv-1"), "closed")
        self.store.reset()
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp"), "new")


if __name__ == "__main__":
    unittest.main()
