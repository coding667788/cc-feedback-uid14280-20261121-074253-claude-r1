"""可读性语义：连接与授权必须都到齐；授权先到不算可读。"""

from __future__ import annotations

import unittest

from support import SolutionTestCase, auth, conn, watermark


class ReadabilityTests(SolutionTestCase):
    def test_connection_alone_is_not_readable(self):
        out = self.ingest([conn("c1", t=10), watermark("w1", 100)])
        self.assertEqual(out["moments"][0]["readable"], [])
        self.assertEqual(out["readable"], [])

    def test_authorization_alone_is_not_readable(self):
        out = self.ingest([auth("a1", t=10), watermark("w1", 100)])
        self.assertEqual(out["moments"][0]["readable"], [])

    def test_authorization_arriving_before_connection_becomes_readable_when_both_present(self):
        # 授权先到（t=10），连接后到（t=20）：只有 20 之后才可读。
        out = self.ingest([
            auth("a1", t=10, valid_until=1000),
            conn("c1", t=20),
            watermark("w1", 200),
        ])
        moments = {m["t"]: m["readable"] for m in out["moments"]}
        self.assertEqual(moments[10], [])
        self.assertEqual(moments[20], [{"station": "WS-A", "media": ["M-SEC"]}])

    def test_authorization_arriving_after_connection_becomes_readable_at_authorization_time(self):
        out = self.ingest([
            conn("c1", t=10),
            auth("a1", t=30, valid_until=1000),
            watermark("w1", 200),
        ])
        moments = {m["t"]: m["readable"] for m in out["moments"]}
        self.assertEqual(moments[10], [])
        self.assertEqual(moments[30], [{"station": "WS-A", "media": ["M-SEC"]}])

    def test_authorization_with_other_generation_is_not_readable(self):
        out = self.ingest([
            conn("c1", gen=0, t=10),
            auth("a1", gen=1, t=10, valid_until=1000),
            watermark("w1", 100),
        ])
        self.assertEqual(out["readable"], [])

    def test_authorization_with_other_device_is_not_readable(self):
        out = self.ingest([
            conn("c1", device="DEV-1", t=10),
            auth("a1", device="DEV-2", t=10, valid_until=1000),
            watermark("w1", 100),
        ])
        self.assertEqual(out["readable"], [])

    def test_authorization_with_other_station_is_not_readable(self):
        out = self.ingest([
            conn("c1", station="WS-A", t=10),
            auth("a1", station="WS-B", t=10, valid_until=1000),
            watermark("w1", 100),
        ])
        self.assertEqual(out["readable"], [])

    def test_multiple_media_on_one_station_are_all_readable(self):
        # 工位可同时容纳多件介质；SECRET 与 AUDIT 不互斥。
        out = self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=1000),
            conn("c2", media="M-AUD", t=10, seq=2),
            auth("a2", media="M-AUD", t=10, valid_until=1000, seq=2),
            watermark("w1", 100),
        ])
        self.assertEqual(out["readable"], [{"station": "WS-A", "media": ["M-AUD", "M-SEC"]}])
        self.assertEqual(out["violations"], [])

    def test_authorization_window_is_half_open(self):
        # [10, 100)：100 已过期，90 仍可读。
        body = [conn("c1", t=10), auth("a1", t=10, valid_until=100)]
        first = self.ingest(body + [watermark("w1", 90)])
        self.assertEqual(first["readable"], [{"station": "WS-A", "media": ["M-SEC"]}])
        self.tearDown_db()
        second = self.ingest(body + [watermark("w2", 100)])
        self.assertEqual(second["readable"], [])

    def tearDown_db(self) -> None:
        import os
        for suffix in ("", "-wal", "-shm"):
            path = self.db + suffix
            if os.path.exists(path):
                os.remove(path)


class ExpiryAtDisconnectTests(SolutionTestCase):
    def test_expiry_exactly_at_disconnect_leaves_no_violation(self):
        out = self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=50),
            conn("c1x", media="M-SEC", t=50, seq=2, op="disconnect"),
            conn("c2", media="M-TOK", t=50, seq=3),
            auth("a2", media="M-TOK", t=50, valid_until=1000, seq=3),
            watermark("w1", 100),
        ])
        self.assertEqual(out["violations"], [])
        self.assertEqual(self.report()["segment_count"], 0)

    def test_disconnect_before_expiry_does_leave_a_violation(self):
        out = self.ingest([
            conn("c1", media="M-SEC", t=10),
            auth("a1", media="M-SEC", t=10, valid_until=100),
            conn("c2", media="M-TOK", t=50, seq=2),
            auth("a2", media="M-TOK", t=50, valid_until=1000, seq=2),
            conn("c1x", media="M-SEC", t=80, seq=3, op="disconnect"),
            watermark("w1", 200),
        ])
        self.assertEqual(len(out["violations"]), 1)
        segment = out["violations"][0]
        self.assertEqual((segment["from"], segment["to"]), (50, 80))
        self.assertEqual(segment["station"], "WS-A")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
