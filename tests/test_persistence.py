"""持久状态：重启后回放、幂等重传、两个进程竞争同一水位。"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

from support import REPO_ROOT, SolutionTestCase, auth, conn, watermark


class RestartReplayTests(SolutionTestCase):
    def test_state_survives_process_restart(self):
        self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=500),
            conn("c2", media="M-TOK", t=50, seq=2),
            auth("a2", media="M-TOK", t=50, valid_until=200, seq=2),
            watermark("w1", 100),
        ])
        first = self.report()
        # 每个 CLI 调用都是独立进程；这里再起一个新的进程读同一个库。
        second = self.report()
        self.assertEqual(first, second)
        self.assertEqual(first["segment_count"], 1)
        self.assertEqual((first["earliest"]["from"], first["earliest"]["to"]), (50, 101))

    def test_multiple_batches_accumulate_and_replay(self):
        self.ingest([conn("c1", media="M-SEC", t=10), auth("a1", media="M-SEC", t=10,
                                                          valid_until=500),
                     watermark("w1", 40)])
        self.ingest([conn("c2", media="M-TOK", t=60, seq=2),
                     auth("a2", media="M-TOK", t=60, valid_until=200, seq=2),
                     watermark("w2", 300, seq=2)])
        report = self.report()
        self.assertEqual(report["segment_count"], 1)
        self.assertEqual((report["earliest"]["from"], report["earliest"]["to"]), (60, 200))
        self.assertEqual(report["sealed_to"], 300)

    def test_retransmitting_the_same_batch_is_idempotent(self):
        events = [conn("c1", t=10), auth("a1", t=10, valid_until=1000), watermark("w1", 100)]
        first = self.ingest(events)
        second = self.ingest(events)
        self.assertEqual(first["sealed_to"], second["sealed_to"])
        self.assertEqual(sorted(second["retransmitted"]), ["a1", "c1", "w1"])
        self.assertEqual(second["applied"], {"connections": 0, "authorizations": 0, "watermarks": 0})
        # 幂等重传不改变 revision。
        self.assertEqual(first["revision"], second["revision"])

    def test_reusing_an_event_id_with_different_payload_is_rejected(self):
        self.ingest([conn("c1", t=10), watermark("w1", 100)])
        payload = self.ingest_rejected(
            [conn("c1", media="M-TOK", t=10, seq=2)],
            expect_code="event_id_reuse",
        )
        self.assertEqual(payload["errors"][0]["field"], "events[0].id")

    def test_partial_retransmission_only_applies_the_new_events(self):
        self.ingest([conn("c1", t=10), watermark("w1", 100)])
        out = self.ingest([
            conn("c1", t=10),                                    # 重传：幂等
            conn("c2", media="M-TOK", t=150, seq=2),             # 新事件
            watermark("w2", 200, seq=2),                         # 新水位
        ])
        self.assertEqual(out["retransmitted"], ["c1"])
        self.assertEqual(out["applied"], {"connections": 1, "authorizations": 0, "watermarks": 1})
        self.assertEqual(out["sealed_to"], 200)

    def test_retransmitted_watermark_is_reported_separately_from_a_new_one(self):
        self.ingest([conn("c1", t=10), watermark("w1", 100)])
        out = self.ingest([watermark("w1", 100), conn("c2", media="M-TOK", t=150, seq=2),
                           watermark("w2", 200, seq=2)])
        self.assertEqual(sorted(out["retransmitted"]), ["w1"])
        self.assertEqual(out["applied"]["watermarks"], 1)


class ConcurrentWatermarkTests(SolutionTestCase):
    """两个进程竞争同一水位：恰好一个成功，另一个被拒，库保持一致。"""

    def _spawn(self, case_path: str) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-m", "solution", "ingest", case_path, "--db", self.db],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def test_racing_ingests_seal_exactly_once(self):
        path_a = self.case_path([
            conn("c1", media="M-SEC", t=10, seq=1, channel=1),
            watermark("w1", 100, seq=1, channel=9),
        ])
        path_b = self.case_path([
            conn("c2", media="M-TOK", t=20, seq=1, channel=2),
            watermark("w2", 100, seq=2, channel=8),
        ])
        procs = [self._spawn(path_a), self._spawn(path_b)]
        outputs = []
        for proc in procs:
            out, err = proc.communicate()
            self.assertFalse(err, err.decode("utf-8", "replace"))
            outputs.append((proc.returncode, json.loads(out.decode("utf-8"))))

        codes = sorted(code for code, _ in outputs)
        self.assertEqual(codes, [0, 1], outputs)  # 一个成功，一个被明确拒绝
        rejected = [payload for code, payload in outputs if code == 1][0]
        self.assertIn(rejected["errors"][0]["code"],
                      ("late_event", "watermark_already_sealed"))

        report = self.report()
        self.assertEqual(report["sealed_to"], 100)
        self.assertEqual(report["revision"], 1)

    def test_many_racing_processes_all_converge(self):
        paths = [
            self.case_path([watermark(f"w{i}", 100 + i, seq=1, channel=20 + i)])
            for i in range(4)
        ]
        procs = [self._spawn(p) for p in paths]
        results = []
        for proc in procs:
            out, err = proc.communicate()
            self.assertFalse(err, err.decode("utf-8", "replace"))
            results.append(proc.returncode)
        # 第一个推进到 100，之后 101/102/103 各自可推进一次；要么按序成功，要么被拒。
        self.assertIn(0, results)
        report = self.report()
        self.assertGreaterEqual(report["sealed_to"], 100)
        self.assertLessEqual(report["sealed_to"], 103)


class DeterminismTests(SolutionTestCase):
    def test_output_is_byte_identical_across_runs(self):
        events = [
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=300),
            conn("c2", media="M-TOK", t=50, seq=2),
            auth("a2", media="M-TOK", t=50, valid_until=200, seq=2),
            watermark("w1", 400),
        ]
        path = self.case_path(events)
        first = subprocess.run([sys.executable, "-m", "solution", "ingest", path,
                                "--db", os.path.join(self.tmp, "a.sqlite")],
                               cwd=REPO_ROOT, capture_output=True)
        second = subprocess.run([sys.executable, "-m", "solution", "ingest", path,
                                 "--db", os.path.join(self.tmp, "b.sqlite")],
                                cwd=REPO_ROOT, capture_output=True)
        self.assertEqual(first.returncode, 0, first.stdout)
        self.assertEqual(second.returncode, 0, second.stdout)
        self.assertEqual(first.stdout, second.stdout)

        rep_a = subprocess.run([sys.executable, "-m", "solution", "report", path,
                                "--db", os.path.join(self.tmp, "a.sqlite")],
                               cwd=REPO_ROOT, capture_output=True)
        rep_b = subprocess.run([sys.executable, "-m", "solution", "report", path,
                                "--db", os.path.join(self.tmp, "b.sqlite")],
                               cwd=REPO_ROOT, capture_output=True)
        self.assertEqual(rep_a.stdout, rep_b.stdout)

    def test_event_order_in_the_file_does_not_change_the_result(self):
        forward = [
            conn("c1", media="M-SEC", t=10, seq=1, channel=1),
            auth("a1", media="M-SEC", t=10, seq=1, channel=2),
            conn("c2", media="M-TOK", t=50, seq=2, channel=1),
            auth("a2", media="M-TOK", t=50, seq=2, channel=2),
            watermark("w1", 400, seq=1, channel=9),
        ]
        self.ingest(forward)
        self.tearDown_db()
        self.ingest(list(reversed(forward)))  # 显式乱序：通道序号仍决定结果
        report = self.report()
        self.assertEqual(report["segment_count"], 1)
        self.assertEqual((report["earliest"]["from"], report["earliest"]["to"]), (50, 401))

    def tearDown_db(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            path = self.db + suffix
            if os.path.exists(path):
                os.remove(path)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()


class PendingReplayTests(SolutionTestCase):
    """未封存事件在新批次中被重放时，不重复输出已发布过的时刻与提示。"""

    def test_replayed_pending_events_do_not_reemit_moments(self):
        self.ingest([conn("c1", media="M-SEC", t=10),
                     auth("a1", media="M-SEC", t=10, valid_until=1000)])
        # 尚未封存：c1/a1 仍在 pending 中。
        self.assertEqual(self.state()["pending_ids"], ["a1", "c1"])
        out = self.ingest([watermark("w1", 100)])
        # 本批只有水位，没有新事件时刻 → moments 为空。
        self.assertEqual(out["moments"], [])
        self.assertEqual(out["sealed_to"], 100)
        self.assertEqual(out["readable"], [{"station": "WS-A", "media": ["M-SEC"]}])

    def test_replayed_pending_events_do_not_repeat_rule_violations(self):
        self.ingest([conn("c1", media="M-SEC", t=10),
                     conn("c1x", media="M-SEC", t=20, seq=2, op="disconnect")])
        out = self.ingest([watermark("w1", 100, seq=1, channel=9)])
        self.assertEqual(out["rule_violations"], [])
        self.assertEqual(out["moments"], [])

    def test_split_authorization_and_connection_across_batches(self):
        self.ingest([auth("a1", media="M-SEC", t=10, valid_until=500)])
        out = self.ingest([conn("c1", media="M-SEC", t=30, seq=2),
                           watermark("w1", 100, seq=2)])
        self.assertEqual(out["readable"], [{"station": "WS-A", "media": ["M-SEC"]}])
        self.assertEqual([m["t"] for m in out["moments"]], [30])


class GenerationHistoryPersistenceTests(SolutionTestCase):
    def test_consumed_station_generation_pair_survives_a_batch_boundary(self):
        self.ingest([conn("c1", gen=1, t=10),
                     conn("c1x", gen=1, t=20, seq=2, op="disconnect"),
                     watermark("w1", 30, seq=2)])
        self.ingest_rejected([
            conn("c2", gen=1, t=35, seq=1, channel=1),
            watermark("w2", 50, seq=2, channel=9),
        ], expect_code="connection_generation_regression")

    def test_same_generation_migration_to_another_station_is_allowed(self):
        out = self.ingest([
            conn("c1", station="WS-A", gen=1, t=10),
            conn("c1x", station="WS-A", gen=1, t=20, seq=2, op="disconnect"),
            conn("c2", station="WS-B", gen=1, t=30, seq=3),
            watermark("w1", 100, seq=2),
        ])
        self.assertEqual(out["rule_violations"], [])
