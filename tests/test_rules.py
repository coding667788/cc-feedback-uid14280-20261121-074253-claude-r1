"""批内规则：代次、重复连接、跨工位迁移、旧代授权。"""

from __future__ import annotations

import unittest

from support import SolutionTestCase, auth, conn, watermark


class GenerationRuleTests(SolutionTestCase):
    def test_generation_regression_is_rejected_atomically(self):
        payload = self.ingest_rejected(
            [conn("c1", gen=2, t=10), conn("c2", gen=1, t=20, seq=2), watermark("w1", 100)],
            expect_code="connection_generation_regression",
        )
        self.assertEqual(payload["errors"][0]["field"], "events[1].gen")
        self.assertEqual(self.state()["sealed_to"], -1)

    def test_generation_skip_is_allowed_but_stale_authorization_is_ignored(self):
        out = self.ingest([
            conn("c1", gen=3, t=10),
            auth("a0", gen=1, t=10, valid_until=1000),
            auth("a1", gen=3, t=10, valid_until=1000, seq=2),
            watermark("w1", 100),
        ])
        codes = [v["code"] for v in out["rule_violations"]]
        self.assertIn("authorization_stale_generation", codes)
        # 旧代授权不参与可读性，但同代授权正常生效。
        self.assertEqual(out["readable"], [{"station": "WS-A", "media": ["M-SEC"]}])

    def test_disconnect_then_reconnect_with_same_generation_is_rejected(self):
        # 断开后不得复用已用过的代次：必须以更高代次重新连接。
        self.ingest_rejected([
            conn("c1", gen=1, t=10),
            auth("a1", gen=1, t=10, valid_until=1000),
            conn("c1x", gen=1, t=20, seq=2, op="disconnect"),
            conn("c2", gen=1, t=30, seq=3),
            watermark("w1", 100),
        ], expect_code="connection_generation_regression")

    def test_disconnect_then_reconnect_with_lower_generation_is_rejected(self):
        self.ingest([
            conn("c1", gen=3, t=10),
            conn("c1x", gen=3, t=20, seq=2, op="disconnect"),
            watermark("w1", 50, seq=2),
        ])
        self.ingest_rejected([
            conn("c2", gen=2, t=60, seq=1, channel=1),
            watermark("w2", 100, seq=2, channel=9),
        ], expect_code="connection_generation_regression")

    def test_reconnect_after_disconnect_with_higher_generation_is_clean(self):
        out = self.ingest([
            conn("c1", gen=1, t=10),
            auth("a1", gen=1, t=10, valid_until=1000),
            conn("c1x", gen=1, t=20, seq=2, op="disconnect"),
            conn("c2", gen=2, t=30, seq=3),
            auth("a2", gen=2, t=30, valid_until=1000, seq=3),
            watermark("w1", 100),
        ])
        self.assertEqual(out["rule_violations"], [])
        self.assertEqual(out["readable"], [{"station": "WS-A", "media": ["M-SEC"]}])


class DuplicateConnectionTests(SolutionTestCase):
    def test_same_connection_different_id_is_rejected(self):
        payload = self.ingest_rejected(
            [conn("c1", t=10), conn("c2", t=10, seq=2), watermark("w1", 100)],
            expect_code="duplicate_connection",
        )
        self.assertEqual(payload["errors"][0]["field"], "events[1].id")

    def test_duplicate_connection_does_not_advance_watermark(self):
        self.ingest_rejected(
            [conn("c1", t=10), conn("c2", t=10, seq=2), watermark("w1", 100)],
            expect_code="duplicate_connection",
        )
        self.assertEqual(self.state()["sealed_to"], -1)
        # 重试一次干净的批次可以正常推进。
        out = self.ingest([conn("c3", t=10), watermark("w2", 100, seq=2)])
        self.assertEqual(out["sealed_to"], 100)


class MigrationTests(SolutionTestCase):
    def test_migration_without_disconnect_is_rejected(self):
        payload = self.ingest_rejected(
            [conn("c1", station="WS-A", t=10), conn("c2", station="WS-B", t=50, seq=2),
             watermark("w1", 100)],
            expect_code="cross_station_migration_without_disconnect",
        )
        self.assertEqual(payload["errors"][0]["field"], "events[1].station")

    def test_migration_is_legal_when_old_station_disconnects_at_same_moment(self):
        out = self.ingest([
            conn("c1", station="WS-A", t=10),
            auth("a1", station="WS-A", t=10, valid_until=1000),
            conn("c1x", station="WS-A", t=50, seq=2, op="disconnect"),
            conn("c2", station="WS-B", t=50, seq=3),
            auth("a2", station="WS-B", t=50, valid_until=1000, seq=3),
            watermark("w1", 100),
        ])
        self.assertEqual(out["rule_violations"], [])
        self.assertEqual(out["violations"], [])
        moments = {m["t"]: m["readable"] for m in out["moments"]}
        self.assertEqual(moments[50], [{"station": "WS-B", "media": ["M-SEC"]}])

    def test_migration_with_disconnect_in_a_separate_earlier_batch(self):
        self.ingest([conn("c1", station="WS-A", t=10), watermark("w1", 20)])
        out = self.ingest([
            conn("c1x", station="WS-A", t=30, seq=2, op="disconnect"),
            conn("c2", station="WS-B", t=30, seq=3),
            watermark("w2", 100, seq=2),
        ])
        self.assertEqual(out["rule_violations"], [])


class DisconnectBindingTests(SolutionTestCase):
    def test_disconnect_without_connection_is_a_recorded_rule_violation(self):
        out = self.ingest([
            conn("c1x", t=10, op="disconnect"),
            watermark("w1", 100),
        ])
        codes = [v["code"] for v in out["rule_violations"]]
        self.assertIn("disconnect_without_connection", codes)
        self.assertEqual(out["violations"], [])

    def test_disconnect_with_wrong_generation_is_a_recorded_rule_violation(self):
        out = self.ingest([
            conn("c1", gen=0, t=10),
            auth("a1", gen=0, t=10, valid_until=1000),
            conn("c1x", gen=5, t=20, seq=2, op="disconnect"),
            watermark("w1", 100),
        ])
        codes = [v["code"] for v in out["rule_violations"]]
        self.assertIn("disconnect_binding_mismatch", codes)
        self.assertEqual(out["readable"], [{"station": "WS-A", "media": ["M-SEC"]}])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
