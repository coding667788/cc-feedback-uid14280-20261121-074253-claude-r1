"""结构校验：每个错误都带可定位的错误码与字段路径，且绝不产生副作用。"""

from __future__ import annotations

import json
import os
import unittest

from support import BASE_CASE, SolutionTestCase, auth, conn, watermark


class StructureErrorTests(SolutionTestCase):
    def _raw_case(self, mutate) -> str:
        doc = json.loads(json.dumps(BASE_CASE))
        doc["events"] = []
        mutate(doc)
        return self.write_raw(json.dumps(doc, ensure_ascii=False), name="mutated.json")

    def _expect_error(self, path: str, code: str, field: str | None = None) -> dict:
        rc, payload = self.run_cli("ingest", path)
        self.assertEqual(rc, 3, payload)
        self.assertEqual(payload["status"], "error")
        codes = [e["code"] for e in payload["errors"]]
        self.assertIn(code, codes, payload)
        if field is not None:
            matched = [e for e in payload["errors"] if e["code"] == code]
            self.assertEqual(matched[0]["field"], field, payload)
        return payload

    def test_missing_media_file_is_reported(self):
        rc, payload = self.run_cli("ingest", os.path.join(self.tmp, "nope.json"))
        self.assertEqual(rc, 3)
        self.assertEqual(payload["errors"][0]["code"], "case_not_found")

    def test_malformed_json_reports_line_and_column(self):
        path = self.write_raw('{"case_id": "x", "media": [ }')
        rc, payload = self.run_cli("ingest", path)
        self.assertEqual(rc, 3)
        error = payload["errors"][0]
        self.assertEqual(error["code"], "invalid_json")
        self.assertRegex(error["field"], r"^line \d+ col \d+$")

    def test_nan_constant_is_rejected(self):
        path = self.write_raw('{"case_id": "x", "media": NaN, "stations": [], '
                              '"conflicts": [], "events": []}')
        rc, payload = self.run_cli("ingest", path)
        self.assertEqual(rc, 3)
        self.assertEqual(payload["errors"][0]["code"], "invalid_json")

    def test_missing_top_level_field_names_the_field(self):
        def mutate(doc):
            del doc["conflicts"]
        self._expect_error(self._raw_case(mutate), "missing_field", "conflicts")

    def test_unknown_top_level_field_is_reported(self):
        def mutate(doc):
            doc["extra"] = 1
        self._expect_error(self._raw_case(mutate), "unknown_field", "extra")

    def test_media_element_missing_class_points_into_array(self):
        def mutate(doc):
            doc["media"] = [{"id": "M-SEC"}]
        self._expect_error(self._raw_case(mutate), "missing_field", "media[0].class")

    def test_boolean_is_not_an_integer(self):
        def mutate(doc):
            doc["events"] = [{"type": "connection", "id": "c1", "media": "M-SEC",
                              "station": "WS-A", "gen": True, "t": 10, "seq": 1,
                              "channel": 1}]
        self._expect_error(self._raw_case(mutate), "invalid_type", "events[0].gen")

    def test_float_generation_is_rejected(self):
        def mutate(doc):
            doc["events"] = [{"type": "connection", "id": "c1", "media": "M-SEC",
                              "station": "WS-A", "gen": 1.0, "t": 10, "seq": 1,
                              "channel": 1}]
        self._expect_error(self._raw_case(mutate), "invalid_type", "events[0].gen")

    def test_negative_time_is_out_of_range(self):
        def mutate(doc):
            doc["events"] = [{"type": "watermark", "id": "w1", "t": -1, "seq": 1,
                              "channel": 9}]
        self._expect_error(self._raw_case(mutate), "out_of_range", "events[0].t")

    def test_unknown_event_type(self):
        def mutate(doc):
            doc["events"] = [{"type": "teleport", "id": "x"}]
        self._expect_error(self._raw_case(mutate), "unknown_event_type", "events[0].type")

    def test_unknown_media_reference(self):
        def mutate(doc):
            doc["events"] = [conn("c1", media="M-GHOST") | {"media": "M-GHOST"}]
        self._expect_error(self._raw_case(mutate), "unknown_media", "events[0].media")

    def test_unknown_station_reference(self):
        def mutate(doc):
            doc["events"] = [conn("c1", station="WS-GHOST")]
        self._expect_error(self._raw_case(mutate), "unknown_station", "events[0].station")

    def test_incompatible_station_class(self):
        def mutate(doc):
            doc["media"] = [{"id": "M-SEC", "class": "SECRET"}]
            doc["stations"] = [{"id": "WS-A", "classes": ["TOOL"]}]
            doc["events"] = [conn("c1", media="M-SEC", station="WS-A")]
        self._expect_error(self._raw_case(mutate), "incompatible_station", "events[0].station")

    def test_invalid_validity_window(self):
        def mutate(doc):
            doc["events"] = [auth("a1", t=100, valid_until=100)]
        self._expect_error(self._raw_case(mutate), "invalid_validity_window",
                           "events[0].valid_until")

    def test_conflict_pair_must_have_two_entries(self):
        def mutate(doc):
            doc["conflicts"] = [["SECRET"]]
        self._expect_error(self._raw_case(mutate), "invalid_shape", "conflicts[0]")

    def test_conflict_pair_with_unknown_class(self):
        def mutate(doc):
            doc["conflicts"] = [["SECRET", "GHOST"]]
        self._expect_error(self._raw_case(mutate), "unknown_class", "conflicts[0][1]")

    def test_duplicate_seq_within_a_channel(self):
        def mutate(doc):
            doc["events"] = [
                {"type": "watermark", "id": "w1", "t": 10, "seq": 1, "channel": 1},
                {"type": "watermark", "id": "w2", "t": 20, "seq": 1, "channel": 1},
            ]
        self._expect_error(self._raw_case(mutate), "duplicate_seq", "events[1].seq")

    def test_channel_time_regression(self):
        def mutate(doc):
            doc["events"] = [
                {"type": "watermark", "id": "w1", "t": 50, "seq": 1, "channel": 1},
                {"type": "watermark", "id": "w2", "t": 40, "seq": 2, "channel": 1},
            ]
        self._expect_error(self._raw_case(mutate), "channel_time_regression", "events[1].t")

    def test_duplicate_media_id(self):
        def mutate(doc):
            doc["media"] = [{"id": "M-SEC", "class": "SECRET"},
                            {"id": "M-SEC", "class": "TOOL"}]
        self._expect_error(self._raw_case(mutate), "duplicate_media", "media[1].id")

    def test_structure_error_has_no_side_effects(self):
        # 先准备一个已有状态的库，再做一次结构错误的 ingest。
        self.ingest([conn("c1", t=10), watermark("w1", 100)])
        before = self.state()
        path = self.write_raw('{"case_id": "x", "media": []}')
        rc, _ = self.run_cli("ingest", path)
        self.assertEqual(rc, 3)
        self.assertEqual(self.state(), before)

    def test_root_must_be_an_object(self):
        path = self.write_raw('[1, 2, 3]')
        rc, payload = self.run_cli("ingest", path)
        self.assertEqual(rc, 3)
        self.assertEqual(payload["errors"][0]["code"], "invalid_type")
        self.assertEqual(payload["errors"][0]["field"], "$")

    def test_error_output_is_valid_utf8_json(self):
        path = self.write_raw(json.dumps({"case_id": "案例", "media": [], "stations": [],
                                          "conflicts": [], "events": []},
                                         ensure_ascii=False))
        rc, payload = self.run_cli("ingest", path)
        self.assertEqual(rc, 0, payload)


class UsageTests(SolutionTestCase):
    def test_unknown_command_is_usage_error(self):
        code, payload = self.run_cli("explode", self.case_path([]))
        self.assertEqual(code, 2)
        self.assertEqual(payload["errors"][0]["code"], "usage_error")

    def test_missing_arguments_is_usage_error(self):
        import subprocess
        import sys
        from support import REPO_ROOT
        proc = subprocess.run([sys.executable, "-m", "solution"], cwd=REPO_ROOT,
                              capture_output=True)
        self.assertEqual(proc.returncode, 2)
        payload = json.loads(proc.stdout.decode("utf-8"))
        self.assertEqual(payload["errors"][0]["code"], "usage_error")


class CaseIdPassthroughTests(SolutionTestCase):
    def test_case_id_is_echoed_for_every_command(self):
        out = self.ingest([watermark("w1", 100)])
        self.assertEqual(out["case_id"], "test-case")
        self.assertEqual(self.report()["case_id"], "test-case")
        self.assertEqual(self.state()["case_id"], "test-case")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
