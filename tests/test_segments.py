"""违规段、最早违规、以及"同一时刻多种中间状态但最终无违规"。"""

from __future__ import annotations

import os
import sys
import unittest

from support import REPO_ROOT, SolutionTestCase, auth, conn, watermark

sys.path.insert(0, REPO_ROOT)
from solution.engine import Claim, Ctx, Segment, merge_segments  # noqa: E402
from solution.model import Pair  # noqa: E402


class SegmentReportTests(SolutionTestCase):
    def test_single_violation_segment_has_both_evidence_ids(self):
        out = self.ingest([
            conn("c-sec", media="M-SEC", t=10),
            auth("a-sec", media="M-SEC", t=10, valid_until=300),
            conn("c-tok", media="M-TOK", t=150, seq=2),
            auth("a-tok", media="M-TOK", t=150, valid_until=400, seq=2),
            watermark("w1", 500),
        ])
        self.assertEqual(len(out["violations"]), 1)
        segment = out["violations"][0]
        self.assertEqual(segment["code"], "mutual_exclusion")
        self.assertEqual(segment["station"], "WS-A")
        self.assertEqual(segment["media"], ["M-SEC", "M-TOK"])
        self.assertEqual((segment["from"], segment["to"]), (150, 300))
        for eid in ("c-sec", "a-sec", "c-tok", "a-tok"):
            self.assertIn(eid, segment["evidence_ids"])

    def test_report_returns_earliest_and_all_segments(self):
        self.ingest([
            conn("s1", media="M-SEC", station="WS-A", t=10, seq=1, channel=1),
            auth("sa1", media="M-SEC", station="WS-A", t=10, seq=1, channel=2),
            conn("t1", media="M-TOK", station="WS-A", t=50, seq=2, channel=1),
            auth("ta1", media="M-TOK", station="WS-A", t=50, seq=2, channel=2),
            conn("s1x", media="M-SEC", station="WS-A", t=60, seq=3, channel=1, op="disconnect"),
            conn("t1x", media="M-TOK", station="WS-A", t=70, seq=4, channel=1, op="disconnect"),
            conn("st1", media="M-SEC", station="WS-B", t=80, seq=5, channel=1),
            auth("sa2", media="M-SEC", station="WS-B", t=80, seq=3, channel=2),
            conn("tt1", media="M-TOK", station="WS-B", t=200, seq=6, channel=1),
            auth("ta2", media="M-TOK", station="WS-B", t=200, seq=4, channel=2),
            watermark("w1", 1000),
        ])
        report = self.report()
        self.assertEqual(report["segment_count"], 2, report)
        self.assertEqual(report["earliest"]["station"], "WS-A")
        self.assertEqual((report["earliest"]["from"], report["earliest"]["to"]), (50, 60))
        self.assertEqual(report["earliest"], report["segments"][0])
        self.assertEqual([s["station"] for s in report["segments"]], ["WS-A", "WS-B"])
        # report 返回"已封存区间的最早违规"，而不是每个时刻的刻度。
        self.assertEqual((report["segments"][1]["from"], report["segments"][1]["to"]), (200, 1000))

    def test_segments_on_different_stations_are_both_reported_in_time_order(self):
        self.ingest([
            conn("a1c", media="M-SEC", station="WS-A", t=10),
            auth("a1a", media="M-SEC", station="WS-A", t=10, valid_until=1000),
            conn("a2c", media="M-TOK", station="WS-A", t=50, seq=2),
            auth("a2a", media="M-TOK", station="WS-A", t=50, valid_until=1000, seq=2),
            conn("a1x", media="M-SEC", station="WS-A", t=60, seq=5, op="disconnect"),
            conn("a2x", media="M-TOK", station="WS-A", t=70, seq=6, op="disconnect"),
            conn("b1c", media="M-SEC", station="WS-B", t=80, seq=7),
            auth("b1a", media="M-SEC", station="WS-B", t=80, valid_until=1000, seq=7),
            conn("b2c", media="M-TOK", station="WS-B", t=200, seq=8),
            auth("b2a", media="M-TOK", station="WS-B", t=200, valid_until=1000, seq=8),
            watermark("w1", 1000),
        ])
        report = self.report()
        self.assertEqual(report["segment_count"], 2)
        self.assertEqual([s["station"] for s in report["segments"]], ["WS-A", "WS-B"])
        self.assertEqual(report["earliest"], report["segments"][0])
        self.assertEqual(report["segment_count"], len(report["segments"]))
        # 不枚举时间刻度：两个 950 长的时间跨度只产生 2 个段。
        self.assertLess(report["segment_count"], 10)

    def test_different_evidence_produces_separate_segments(self):
        self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=1000),
            conn("c2", media="M-TOK", t=100, seq=2),
            auth("a2", media="M-TOK", t=100, valid_until=300, seq=2),
            conn("c2x", media="M-TOK", t=350, seq=3, op="disconnect"),
            conn("c3", media="M-TOK", t=400, seq=4, gen=1),
            auth("a3", media="M-TOK", gen=1, t=400, valid_until=600, seq=4),
            watermark("w1", 1000),
        ])
        report = self.report()
        self.assertEqual(report["segment_count"], 2)
        first, second = report["segments"]
        self.assertEqual((first["from"], first["to"]), (100, 300))
        self.assertEqual((second["from"], second["to"]), (400, 600))
        self.assertNotEqual(first["evidence_ids"], second["evidence_ids"])

    def test_no_violation_across_stations(self):
        out = self.ingest([
            conn("c1", media="M-SEC", station="WS-A", t=10),
            auth("a1", media="M-SEC", station="WS-A", t=10, valid_until=1000),
            conn("c2", media="M-TOK", station="WS-B", t=100, seq=2),
            auth("a2", media="M-TOK", station="WS-B", t=100, valid_until=1000, seq=2),
            watermark("w1", 1000),
        ])
        self.assertEqual(out["violations"], [])
        report = self.report()
        self.assertEqual(report["segment_count"], 0)
        self.assertIsNone(report["earliest"])

    def test_non_conflicting_classes_can_share_a_station(self):
        out = self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=1000),
            conn("c2", media="M-AUD", t=10, seq=2),
            auth("a2", media="M-AUD", t=10, valid_until=1000, seq=2),
            watermark("w1", 100),
        ])
        self.assertEqual(out["violations"], [])


class SegmentMergeUnitTests(unittest.TestCase):
    """``merge_segments`` 的同证据合并是纯函数，直接测。"""

    def test_adjacent_segments_with_same_evidence_merge(self):
        merged = merge_segments([
            Segment("WS-A", ("M-SEC", "M-TOK"), 10, 50, ["c1", "a1", "c2", "a2"]),
            Segment("WS-A", ("M-SEC", "M-TOK"), 50, 90, ["c1", "a1", "c2", "a2"]),
        ])
        self.assertEqual(len(merged), 1)
        self.assertEqual((merged[0].start, merged[0].end), (10, 90))

    def test_gap_keeps_segments_separate(self):
        merged = merge_segments([
            Segment("WS-A", ("M-SEC", "M-TOK"), 10, 50, ["c1", "a1", "c2", "a2"]),
            Segment("WS-A", ("M-SEC", "M-TOK"), 60, 90, ["c1", "a1", "c2", "a2"]),
        ])
        self.assertEqual(len(merged), 2)

    def test_different_evidence_is_not_merged(self):
        merged = merge_segments([
            Segment("WS-A", ("M-SEC", "M-TOK"), 10, 50, ["c1", "a1"]),
            Segment("WS-A", ("M-SEC", "M-TOK"), 50, 90, ["c1", "a1", "c2", "a2"]),
        ])
        self.assertEqual(len(merged), 2)

    def test_intersection_is_half_open(self):
        ctx = Ctx({"M-SEC": "SECRET", "M-TOK": "TOOL"},
                  {"WS-A": ["SECRET", "TOOL"]}, frozenset({Pair.of("SECRET", "TOOL")}))
        from solution.engine import compute_segments
        segments = compute_segments([
            Claim("WS-A", "M-SEC", 10, 100, "c1", "a1"),
            Claim("WS-A", "M-TOK", 100, 200, "c2", "a2"),
        ], ctx)
        self.assertEqual(segments, [])  # 首尾相接，不重叠


class IntermediateStateTests(SolutionTestCase):
    def test_many_intermediate_states_but_no_final_violation(self):
        # 同一时刻 t=50：SECRET 授权到期、SECRET 断开（另有两条冗余断开）、
        # TOOL 连接并授权。任何正确的实现都必须只检查"批量变化处理完"的状态。
        out = self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=50),
            conn("c1x", media="M-SEC", t=50, seq=2, op="disconnect"),
            conn("c1y", media="M-SEC", t=50, seq=3, op="disconnect"),
            conn("c2", media="M-TOK", t=50, seq=4),
            auth("a2", media="M-TOK", t=50, valid_until=1000, seq=4),
            watermark("w1", 200),
        ])
        self.assertEqual(out["violations"], [])
        self.assertEqual(self.report()["segment_count"], 0)
        moments = {m["t"]: m["readable"] for m in out["moments"]}
        self.assertEqual(moments[50], [{"station": "WS-A", "media": ["M-TOK"]}])
        codes = [v["code"] for v in out["rule_violations"]]
        self.assertEqual(codes.count("disconnect_without_connection"), 1)
        self.assertEqual(codes.count("disconnect_binding_mismatch"), 0)

    def test_disconnect_and_connect_at_same_tick_is_not_a_violation(self):
        out = self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=1000),
            conn("c1x", media="M-SEC", t=50, seq=2, op="disconnect"),
            conn("c2", media="M-TOK", t=50, seq=3),
            auth("a2", media="M-TOK", t=50, valid_until=1000, seq=3),
            watermark("w1", 200),
        ])
        self.assertEqual(out["violations"], [])

    def test_violation_starts_only_when_the_authorization_completes_the_pair(self):
        out = self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=1000),
            conn("c2", media="M-TOK", t=20, seq=2),
            watermark("w1", 20, seq=3),
        ])
        self.assertEqual(out["violations"], [])
        out2 = self.ingest([
            auth("a2", media="M-TOK", t=30, valid_until=100, seq=3),
            watermark("w2", 200, seq=4),
        ])
        self.assertEqual(len(out2["violations"]), 1)
        self.assertEqual((out2["violations"][0]["from"], out2["violations"][0]["to"]), (30, 100))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
