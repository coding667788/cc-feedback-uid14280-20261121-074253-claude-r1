"""共享测试工具：在临时目录里跑真实 CLI，全程只用本地虚构夹具。"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BASE_CASE = {
    "case_id": "test-case",
    "media": [
        {"id": "M-SEC", "class": "SECRET"},
        {"id": "M-TOK", "class": "TOOL"},
        {"id": "M-AUD", "class": "AUDIT"},
    ],
    "stations": [
        {"id": "WS-A", "classes": ["SECRET", "TOOL", "AUDIT"]},
        {"id": "WS-B", "classes": ["SECRET", "TOOL"]},
    ],
    "conflicts": [["SECRET", "TOOL"]],
}


def conn(eid, media="M-SEC", station="WS-A", device="DEV-1", gen=0, t=10,
         seq=1, channel=1, op="connect"):
    ev = {"type": "connection", "id": eid, "media": media, "station": station,
          "device": device, "gen": gen, "t": t, "seq": seq, "channel": channel}
    if op == "disconnect":
        ev["op"] = "disconnect"
    return ev


def auth(eid, media="M-SEC", station="WS-A", device="DEV-1", gen=0, t=10,
         valid_until=1000, seq=1, channel=2):
    return {"type": "authorization", "id": eid, "media": media, "station": station,
            "device": device, "gen": gen, "t": t, "valid_until": valid_until,
            "seq": seq, "channel": channel}


def watermark(eid, t, seq=1, channel=9):
    return {"type": "watermark", "id": eid, "t": t, "seq": seq, "channel": channel}


class SolutionTestCase(unittest.TestCase):
    """每个测试一个临时目录 + 一个全新的 SQLite 库。"""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="solution-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.db = os.path.join(self.tmp, "state.sqlite")
        self._counter = 0

    # -- 夹具 -------------------------------------------------------------

    def case_path(self, events, **overrides) -> str:
        self._counter += 1
        doc = dict(BASE_CASE)
        doc.update(overrides)
        doc["events"] = events
        path = os.path.join(self.tmp, f"case-{self._counter}.json")
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(doc, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh_name = path
        return fh_name

    def write_raw(self, text: str, name: str = "raw.json") -> str:
        path = os.path.join(self.tmp, name)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
        return path

    # -- 调用 -------------------------------------------------------------

    def run_cli(self, command: str, case_path: str, db: str | None = None,
                extra: list[str] | None = None) -> tuple[int, dict]:
        argv = [sys.executable, "-m", "solution", command, case_path]
        target = db or self.db
        if command in ("ingest", "report", "state"):
            argv += ["--db", target]
        argv += extra or []
        proc = subprocess.run(argv, cwd=REPO_ROOT, capture_output=True)
        if proc.stderr:
            self.fail(f"CLI 写入了标准错误：{proc.stderr.decode('utf-8', 'replace')}")
        try:
            payload = json.loads(proc.stdout.decode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self.fail(f"标准输出不是合法 UTF-8 JSON：{exc}\n{proc.stdout!r}")
        return proc.returncode, payload

    def ingest(self, events, **overrides) -> dict:
        code, payload = self.run_cli("ingest", self.case_path(events, **overrides))
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["status"], "ok")
        return payload

    def ingest_rejected(self, events, expect_code: str | None = None, **overrides) -> dict:
        code, payload = self.run_cli("ingest", self.case_path(events, **overrides))
        self.assertEqual(code, 1, payload)
        self.assertEqual(payload["status"], "error")
        if expect_code:
            self.assertIn(expect_code, [e["code"] for e in payload["errors"]])
        return payload

    def report(self) -> dict:
        code, payload = self.run_cli("report", self.case_path([]))
        self.assertEqual(code, 0, payload)
        return payload

    def state(self) -> dict:
        code, payload = self.run_cli("state", self.case_path([]))
        self.assertEqual(code, 0, payload)
        return payload
