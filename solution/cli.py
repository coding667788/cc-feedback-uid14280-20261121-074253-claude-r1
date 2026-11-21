"""命令行入口：``python -m solution <命令> CASE.json [--db DB.sqlite] [--out FILE]``。

约定
----
* 标准输出永远是 UTF-8 JSON（``ensure_ascii=False``、键名排序、2 空格缩进），
  同一逻辑输入得到逐字节相同的输出。
* 退出码：``0`` 成功；``1`` 案例被语义拒绝（输出含可定位诊断）；``2`` 用法错误；
  ``3`` 案例结构错误。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import os
import sys
import tempfile
from typing import Any

from . import store
from .engine import Ctx
from .model import CaseError, Diagnostic, load_case, normalize, validate_case

COMMANDS = ("ingest", "report", "state")
PERSISTENT = ("ingest", "report", "state")

EXIT_OK = 0
EXIT_REJECTED = 1
EXIT_USAGE = 2
EXIT_INVALID_CASE = 3


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # 覆盖默认的 stderr + exit(2)
        raise _UsageError(message)


class _UsageError(Exception):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="python -m solution", add_help=True,
                     description="受管工位连接事件模拟器")
    parser.add_argument("command", choices=COMMANDS, help="ingest / report / state")
    parser.add_argument("case", help="案例 JSON 文件路径")
    parser.add_argument("--db", help="持久状态数据库（SQLite）；缺省时按案例文件名派生")
    parser.add_argument("--out", help="同时把 JSON 结果写入该文件（便于跨进程比对）")
    return parser


def emit(payload: dict[str, Any], out_path: str | None) -> None:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    data = text + "\n"
    if out_path:
        with open(out_path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(data)
    sys.stdout.buffer.write(data.encode("utf-8"))
    sys.stdout.buffer.flush()


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = build_parser().parse_args(argv)
    except _UsageError as exc:
        return _fail(_diagnostic("usage_error", str(exc), None), "usage", EXIT_USAGE)

    try:
        doc = load_case(args.case)
        case = normalize(doc)
    except CaseError as exc:
        return _fail(_as_json(exc.diagnostics), args.command, EXIT_INVALID_CASE)

    structural = validate_case(case)
    if structural:
        return _fail(_as_json(structural), args.command, EXIT_INVALID_CASE)

    db_path = args.db or store.default_db_path(args.case)
    ctx = Ctx.from_case(case)
    try:
        with store.open_db(db_path) as conn:
            if args.command == "ingest":
                payload = store.ingest(conn, case, ctx)
            elif args.command == "report":
                payload = store.report(conn, case.case_id)
            else:
                payload = store.state(conn, case.case_id)
    except store.Rejected as exc:
        return _fail_json(exc.diagnostics, args.command, case.case_id, EXIT_REJECTED)
    except store.CaseError as exc:
        return _fail(_as_json(exc.diagnostics), args.command, EXIT_INVALID_CASE)
    except sqlite3.OperationalError as exc:
        return _fail([_diagnostic("db_unwritable", f"数据库不可用或不可写：{exc}", None)],
                     args.command, EXIT_USAGE)
    except Exception as exc:  # pragma: no cover - 兜底：标准输出仍是合法 JSON
        return _fail([_diagnostic("internal_error", f"{type(exc).__name__}: {exc}", None)],
                     args.command, EXIT_USAGE)

    emit(payload, args.out)
    return EXIT_OK


def _diagnostic(code: str, message: str, field: str | None) -> dict[str, Any]:
    return Diagnostic(code, message, field).to_json()


def _as_json(items: list[Any]) -> list[dict[str, Any]]:
    """把 ``Diagnostic`` 或已序列化字典统一成可输出的字典列表。"""
    out: list[dict[str, Any]] = []
    for item in items:
        out.append(item.to_json() if isinstance(item, Diagnostic) else item)
    return out


def _fail(diagnostics: list[dict[str, Any]] | dict[str, Any], command: str, code: int) -> int:
    diags = diagnostics if isinstance(diagnostics, list) else [diagnostics]
    return _fail_json(diags, command, None, code)


def _fail_json(diagnostics: list[dict[str, Any]], command: str, case_id: str | None,
               code: int) -> int:
    payload = {
        "command": command,
        "status": "error",
        "case_id": case_id,
        "errors": diagnostics,
    }
    emit(payload, None)
    return code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
