"""原子性、水位与迟到事件：失败批次绝不留下半个状态。"""

from __future__ import annotations

import os
import unittest

from support import SolutionTestCase, auth, conn, watermark


class WatermarkTests(SolutionTestCase):
    def test_watermark_seals_and_report_reflects_it(self):
        out = self.ingest([
            conn("c1", t=10), auth("a1", t=10, valid_until=1000),
            watermark("w1", 100),
        ])
        self.assertEqual(out["sealed_to"], 100)
        self.assertEqual(out["sealed_now"], 100)
        self.assertEqual(self.report()["sealed_to"], 100)

    def test_watermark_is_monotonic(self):
        self.ingest([conn("c1", t=10), auth("a1", t=10, valid_until=1000), watermark("w1", 100)])
        # 倒退的水位低于已封存水位：不得重复推进已发布的区间。
        self.ingest_rejected([watermark("w2", 90, seq=2)],
                             expect_code="watermark_already_sealed")
        # 相等也不行：区间已经封存过。
        payload = self.ingest_rejected([watermark("w3", 100, seq=3)],
                                       expect_code="watermark_already_sealed")
        self.assertEqual(payload["errors"][0]["field"], "events[0].t")
        # 前进到更高的水位是允许的。
        out = self.ingest([watermark("w4", 200, seq=4)])
        self.assertEqual(out["sealed_to"], 200)

    def test_events_beyond_watermark_stay_pending(self):
        self.ingest([conn("c1", t=10), auth("a1", t=10, valid_until=1000), watermark("w1", 50)])
        state = self.state()
        self.assertEqual(state["sealed_to"], 50)
        self.assertEqual(state["pending_events"], 0)
        self.ingest([conn("c2", media="M-TOK", t=80, seq=2)])
        self.assertEqual(self.state()["pending_ids"], ["c2"])
        self.assertEqual(self.report()["sealed_to"], 50)


class LateEventTests(SolutionTestCase):
    def test_late_event_is_rejected_not_silently_merged(self):
        self.ingest([conn("c1", t=10), auth("a1", t=10, valid_until=1000), watermark("w1", 100)])
        before = self.report()
        payload = self.ingest_rejected(
            [conn("c2", media="M-TOK", t=50, seq=2), watermark("w2", 200, seq=2)],
            expect_code="late_event",
        )
        self.assertEqual(payload["errors"][0]["field"], "events[0].t")
        after = self.report()
        self.assertEqual(before["sealed_to"], after["sealed_to"])
        self.assertEqual(before["segments"], after["segments"])
        self.assertEqual(before["revision"], after["revision"])

    def test_event_exactly_at_watermark_is_late(self):
        self.ingest([watermark("w1", 100)])
        self.ingest_rejected([conn("c1", t=100)], expect_code="late_event")

    def test_event_one_tick_after_watermark_is_accepted(self):
        self.ingest([watermark("w1", 100)])
        out = self.ingest([conn("c1", t=101, seq=2)])
        self.assertEqual(out["sealed_to"], 100)


class AtomicRollbackTests(SolutionTestCase):
    def test_late_event_in_a_mixed_batch_rolls_back_everything(self):
        self.ingest([conn("c1", t=10), auth("a1", t=10, valid_until=1000), watermark("w1", 100)])
        before = self.state()
        self.ingest_rejected([
            conn("c-fresh", media="M-TOK", t=150, seq=5, channel=1),  # 本来合法
            conn("c-late", media="M-SEC", t=90, seq=1, channel=2),    # 迟到
            watermark("w2", 300, seq=6, channel=9),
        ], expect_code="late_event")
        after = self.state()
        self.assertEqual(before["sealed_to"], after["sealed_to"])
        self.assertEqual(before["revision"], after["revision"])
        self.assertEqual(before["pending_ids"], after["pending_ids"])
        self.assertNotIn("c-fresh", self._seen_ids())

    def test_fatal_rule_in_a_mixed_batch_rolls_back_everything(self):
        before = self.state()
        self.ingest_rejected([
            conn("c-ok", media="M-TOK", t=10),
            conn("c1", media="M-SEC", t=10, seq=2),
            conn("c-dup", media="M-SEC", t=10, seq=3),   # 同刻重复连接
            watermark("w1", 100, seq=3),
        ], expect_code="duplicate_connection")
        after = self.state()
        self.assertEqual(before["revision"], after["revision"])
        self.assertEqual(after["sealed_to"], -1)
        self.assertEqual(after["pending_ids"], [])

    def test_rejected_batch_can_be_retried_cleanly(self):
        self.ingest_rejected([
            conn("c1", media="M-SEC", t=10),
            conn("c2", media="M-SEC", t=10, seq=2),
            watermark("w1", 100, seq=2),
        ], expect_code="duplicate_connection")
        out = self.ingest([conn("c1", media="M-SEC", t=10), watermark("w1", 100, seq=2)])
        self.assertEqual(out["sealed_to"], 100)

    def _seen_ids(self) -> list[str]:
        db = self.db
        import sqlite3
        with sqlite3.connect(db) as conn_:
            return [row[0] for row in conn_.execute("SELECT id FROM seen")]


class RuleViolationPersistenceTests(SolutionTestCase):
    def test_non_fatal_rule_violations_are_reported_but_do_not_block_sealing(self):
        out = self.ingest([
            conn("c1x", media="M-SEC", t=10, op="disconnect"),
            watermark("w1", 100),
        ])
        self.assertEqual(out["sealed_to"], 100)
        codes = [v["code"] for v in out["rule_violations"]]
        self.assertEqual(codes, ["disconnect_without_connection"])
        self.assertEqual(out["rule_violations"][0]["field"], "events[0].media")

    def test_rule_violations_only_reported_for_the_current_batch(self):
        self.ingest([conn("c1x", media="M-SEC", t=10, op="disconnect"), watermark("w1", 50)])
        out = self.ingest([watermark("w2", 100, seq=2)])
        self.assertEqual(out["rule_violations"], [])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
