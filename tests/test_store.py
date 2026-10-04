import tempfile
import threading
import unittest
from pathlib import Path

from relay.store import (
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

    def test_resolve_seals_unknown_and_blocks_late_admit(self):
        # 终态核对：从未接纳 -> 当场封存
        self.assertEqual(self.store.resolve("dlv-1", "alt-1"), "sealed")
        # 封存幂等：再次核对仍是 sealed
        self.assertEqual(self.store.resolve("dlv-1", "alt-1"), "sealed")
        # 迟到投递（慢处理线程醒来）必须被拒绝，且不能改写封存
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp"), "sealed")
        self.assertEqual(self.store.admit("dlv-1", "alt-1", "fp-other"), "sealed")
        [row] = self.store.list_deliveries()
        self.assertEqual(row["state"], "sealed")
        self.assertEqual(self.store.count(), 1)

    def test_resolve_after_accept_reports_accepted(self):
        self.assertEqual(self.store.admit("dlv-2", "alt-2", "fp"), "new")
        self.assertEqual(self.store.resolve("dlv-2", "alt-2"), "accepted")
        # 已接纳记录不被封存影响，仍可幂等重放
        self.assertEqual(self.store.admit("dlv-2", "alt-2", "fp"), "duplicate")
        [row] = self.store.list_deliveries()
        self.assertEqual(row["state"], "accepted")


if __name__ == "__main__":
    unittest.main()
