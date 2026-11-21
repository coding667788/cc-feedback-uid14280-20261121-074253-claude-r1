"""SQLite 持久层：ingest 事务、水位封存、report 查询。

并发与原子性
------------
* 每次 ingest 使用 ``BEGIN IMMEDIATE``，两个进程竞争同一水位时由 SQLite 串行化；
  落败的一方要么看到"已经是重传"（幂等），要么得到 ``watermark_already_sealed``
  冲突，绝不会写出半个批次。
* 任何冲突都在事务内抛出，由调用方 ``ROLLBACK``：水位、可读集合、违规段、
  已见事件表全部保持原样。

状态模型
--------
* ``seen``      —— 已接受事件的键、ID 与确定性摘要，用于重传归并与 ID 复用检测。
* ``pending``   —— 已接受但尚未封存（``t`` 大于当前水位）的事件，按原始载荷保存。
* ``snap_*``    —— 封存水位处的快照：仍打开的连接与全部被接受的授权。
* ``claims``    —— 已封存的可读区间（``[start, end)``，证据 = 连接 ID + 授权 ID）。
* ``segments``  —— 由 claims 归约出的违规段。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Iterator

from .engine import (
    Auth,
    Claim,
    Conn,
    Ctx,
    FatalRule,
    RuleViolation,
    Segment,
    Simulation,
    compute_segments,
    merge_segments,
    summarize_segments,
)
from .model import Case, CaseError, Diagnostic, canonical_text, input_evidence, order_key

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seen (
    k TEXT PRIMARY KEY,          -- = 事件 ID
    id TEXT NOT NULL,            -- 事件 ID（与 k 相同，便于直接查询）
    digest TEXT NOT NULL,        -- 规范化载荷摘要（重传判定）
    kind TEXT NOT NULL,
    t INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pending (
    k TEXT PRIMARY KEY,
    t INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snap_conns (
    media TEXT PRIMARY KEY,
    station TEXT NOT NULL,
    device TEXT,
    gen INTEGER NOT NULL,
    conn_id TEXT NOT NULL,
    start INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snap_auths (
    id TEXT PRIMARY KEY,
    media TEXT NOT NULL,
    station TEXT NOT NULL,
    device TEXT,
    gen INTEGER NOT NULL,
    t INTEGER NOT NULL,
    valid_until INTEGER NOT NULL,
    seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snap_max_gen (
    media TEXT PRIMARY KEY,
    gen INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snap_consumed (
    media TEXT NOT NULL,
    station TEXT NOT NULL,
    gen INTEGER NOT NULL,
    PRIMARY KEY (media, station, gen)
);
CREATE TABLE IF NOT EXISTS claims (
    station TEXT NOT NULL,
    media TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    conn_id TEXT NOT NULL,
    auth_id TEXT NOT NULL,
    PRIMARY KEY (station, media, start, end, conn_id, auth_id)
);
CREATE TABLE IF NOT EXISTS segments (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    station TEXT NOT NULL,
    media_a TEXT NOT NULL,
    media_b TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    evidence TEXT NOT NULL
);
"""

# 会被持久化、并在重启后重放的输入字段（与结构无关，只保留语义字段）。
_PAYLOAD_FIELDS = (
    "index", "kind", "op", "id", "media", "station", "device",
    "gen", "t", "valid_until", "seq", "channel",
)


class Rejected(Exception):
    """语义拒绝：整批不生效，附可定位诊断。"""

    def __init__(self, diagnostics: list[dict[str, Any]]):
        self.diagnostics = diagnostics
        super().__init__(diagnostics[0]["message"] if diagnostics else "rejected")


def event_key(ev: dict[str, Any]) -> str:
    """事件 ID 就是身份：同一 ID 无论类型如何都指向同一个事件槽位。"""
    return ev["id"]


def event_digest(ev: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_text(input_evidence(ev)).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 连接与事务
# ---------------------------------------------------------------------------

@contextmanager
def open_db(path: str) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(path, timeout=15.0, isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout=15000")
        _retry_locked(lambda: conn.execute("PRAGMA journal_mode=WAL"))
        conn.execute("PRAGMA synchronous=FULL")
        _retry_locked(lambda: conn.executescript(SCHEMA))
        if _retry_locked(lambda: _meta(conn, "schema_version")) is None:
            with immediate(conn):
                conn.execute("INSERT OR REPLACE INTO meta(k, v) VALUES ('schema_version', ?)",
                             (str(SCHEMA_VERSION),))
                conn.execute("INSERT OR REPLACE INTO meta(k, v) VALUES ('sealed_to', '-1')")
                conn.execute("INSERT OR REPLACE INTO meta(k, v) VALUES ('revision', '0')")
        yield conn
    finally:
        conn.close()


def _retry_locked(action, retries: int = 60):
    """执行 ``action``，遇到 ``locked``/``busy`` 时退避重试。

    初始化阶段（PRAGMA、建表）也要参与竞争：两个进程同时首次打开新库时，
    建表语句可能短暂拿到排它锁。
    """
    attempt = 0
    while True:
        try:
            return action()
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            attempt += 1
            if ("locked" not in message and "busy" not in message) or attempt >= retries:
                raise
            time.sleep(min(0.02 * attempt, 0.25))


@contextmanager
def immediate(conn: sqlite3.Connection, retries: int = 60) -> Iterator[None]:
    """``BEGIN IMMEDIATE`` + 锁竞争重试；退出时提交，异常时回滚。"""
    _retry_locked(lambda: conn.execute("BEGIN IMMEDIATE"), retries)
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    return None if row is None else row[0]


def sealed_to(conn: sqlite3.Connection) -> int:
    raw = _meta(conn, "sealed_to")
    return -1 if raw is None else int(raw)


def revision(conn: sqlite3.Connection) -> int:
    raw = _meta(conn, "revision")
    return 0 if raw is None else int(raw)


# ---------------------------------------------------------------------------
# 快照
# ---------------------------------------------------------------------------

def load_snapshot(conn: sqlite3.Connection) -> tuple[list[Conn], list[Auth], dict[str, int],
                                                     set[tuple[str, str, int]]]:
    conns = [
        Conn(media, station, device, gen, conn_id, start)
        for media, station, device, gen, conn_id, start in conn.execute(
            "SELECT media, station, device, gen, conn_id, start FROM snap_conns ORDER BY media")
    ]
    auths = [
        Auth(aid, media, station, device, gen, t, until, seq)
        for aid, media, station, device, gen, t, until, seq in conn.execute(
            "SELECT id, media, station, device, gen, t, valid_until, seq FROM snap_auths ORDER BY seq")
    ]
    max_gen = {media: gen for media, gen in conn.execute("SELECT media, gen FROM snap_max_gen")}
    consumed = {(media, station, gen) for media, station, gen in conn.execute(
        "SELECT media, station, gen FROM snap_consumed")}
    return conns, auths, max_gen, consumed


def load_pending(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [json.loads(row[0]) for row in
            conn.execute("SELECT payload FROM pending ORDER BY t, rowid")]


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------

def _diagnostic(code: str, message: str, field: str | None) -> dict[str, Any]:
    return Diagnostic(code, message, field).to_json()


def _violation_diag(v: RuleViolation) -> dict[str, Any]:
    return _diagnostic(v.code, v.message, v.field)


def _dedupe(case: Case) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """批内同 ID 归并：完全一致视为重传，载荷不同则整批拒绝。"""
    unique: dict[str, dict[str, Any]] = {}
    conflicts: list[dict[str, Any]] = []
    for ev in case.events():
        key = event_key(ev)
        if key in unique:
            first = unique[key]
            if event_digest(first) != event_digest(ev):
                conflicts.append(_diagnostic(
                    "duplicate_id_conflict",
                    f"事件 ID {ev['id']} 在批内出现两次且载荷不同"
                    f"（events[{first['index']}] 与 events[{ev['index']}]）；同 ID 只允许完全一致的重传",
                    f"events[{ev['index']}].id",
                ))
            continue
        unique[key] = ev
    return sorted(unique.values(), key=order_key), conflicts


def _check_against_seen(conn: sqlite3.Connection, events: list[dict[str, Any]],
                        base: int) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    seen = {k: digest for k, digest in conn.execute("SELECT k, digest FROM seen")}
    fresh: list[dict[str, Any]] = []
    retransmitted: list[str] = []
    conflicts: list[dict[str, Any]] = []
    for ev in events:
        key = event_key(ev)
        digest = event_digest(ev)
        if key in seen:
            if seen[key] == digest:
                retransmitted.append(ev["id"])
            else:
                conflicts.append(_diagnostic(
                    "event_id_reuse",
                    f"事件 ID {ev['id']} 已以不同载荷入库，禁止复用 ID",
                    f"events[{ev['index']}].id",
                ))
            continue
        if ev["kind"] != "watermark" and ev["t"] <= base:
            conflicts.append(_diagnostic(
                "late_event",
                f"事件 {ev['id']} 的时间 {ev['t']} 不晚于已封存水位 {base}；"
                f"超过水位到达的迟报不会被静默并入已发布历史",
                f"events[{ev['index']}].t",
            ))
            continue
        if ev["kind"] == "watermark" and ev["t"] <= base:
            conflicts.append(_diagnostic(
                "watermark_already_sealed",
                f"水位 {ev['t']} 不高于已封存水位 {base}；该区间已发布，不能重复推进",
                f"events[{ev['index']}].t",
            ))
            continue
        fresh.append(ev)
    return fresh, retransmitted, conflicts


def ingest(conn: sqlite3.Connection, case: Case, ctx: Ctx) -> dict[str, Any]:
    """一个事务内完成：归并 → 校验 → 整段模拟 → 封存。"""
    with immediate(conn):
        return _ingest_locked(conn, case, ctx)


def _ingest_locked(conn: sqlite3.Connection, case: Case, ctx: Ctx) -> dict[str, Any]:
    base = sealed_to(conn)
    rev = revision(conn)

    events, conflicts = _dedupe(case)
    if conflicts:
        raise Rejected(conflicts)
    fresh, retransmitted, conflicts = _check_against_seen(conn, events, base)
    if conflicts:
        raise Rejected(conflicts)

    new_wm = [ev for ev in fresh if ev["kind"] == "watermark"]
    current_ids = {ev["id"] for ev in fresh}
    current_times = {ev["t"] for ev in fresh if ev["kind"] != "watermark"}
    pending = load_pending(conn)
    snap_conns, snap_auths, max_gen, consumed = load_snapshot(conn)

    replay = sorted([ev for ev in pending] + [ev for ev in fresh if ev["kind"] != "watermark"],
                    key=order_key)
    sim = Simulation(ctx, snap_conns, snap_auths, max_gen, consumed, sealed_from=base)
    try:
        sim.run(replay)
    except FatalRule as fatal:
        raise Rejected([_violation_diag(fatal.violation)]) from None

    new_seal = max((ev["t"] for ev in new_wm), default=None)
    if new_seal is not None and new_seal > base:
        clip_from, clip_to = base + 1, new_seal + 1
    else:
        clip_from = clip_to = base + 1  # 不封存：只为空

    sealed_now = base
    new_segments: list[Segment] = []
    if new_seal is not None:
        sealed_now = new_seal

    # --- 落库 ------------------------------------------------------------
    for ev in fresh:
        conn.execute("INSERT INTO seen(k, id, digest, kind, t) VALUES (?,?,?,?,?)",
                     (event_key(ev), ev["id"], event_digest(ev), ev["kind"], ev["t"]))
        if ev["kind"] != "watermark":
            conn.execute("INSERT OR REPLACE INTO pending(k, t, payload) VALUES (?,?,?)",
                         (event_key(ev), ev["t"], canonical_text(_payload(ev))))

    if new_seal is not None:
        claims = sim.claims(clip_from, clip_to)
        conn.executemany(
            "INSERT OR IGNORE INTO claims(station, media, start, end, conn_id, auth_id) "
            "VALUES (?,?,?,?,?,?)",
            [(c.station, c.media, c.start, c.end, c.conn_id, c.auth_id) for c in claims])

        live = [c for c in sim.lifetimes()
                if c.start <= new_seal and (c.end is None or c.end > new_seal)]
        conn.execute("DELETE FROM snap_conns")
        conn.executemany(
            "INSERT INTO snap_conns(media, station, device, gen, conn_id, start) VALUES (?,?,?,?,?,?)",
            [(c.media, c.station, c.device, c.gen, c.conn_id, c.start) for c in live])
        conn.execute("DELETE FROM snap_auths")
        conn.executemany(
            "INSERT INTO snap_auths(id, media, station, device, gen, t, valid_until, seq) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [(a.id, a.media, a.station, a.device, a.gen, a.t, a.valid_until, a.seq)
             for a in sim.auths])
        conn.execute("DELETE FROM snap_max_gen")
        conn.executemany("INSERT INTO snap_max_gen(media, gen) VALUES (?,?)",
                         sorted(sim.max_gen.items()))
        conn.execute("DELETE FROM snap_consumed")
        conn.executemany("INSERT INTO snap_consumed(media, station, gen) VALUES (?,?,?)",
                         sorted(sim.consumed))
        conn.execute("DELETE FROM pending WHERE t <= ?", (new_seal,))
        conn.execute("UPDATE meta SET v=? WHERE k='sealed_to'", (str(new_seal),))

        all_claims = [Claim(*row) for row in conn.execute(
            "SELECT station, media, start, end, conn_id, auth_id FROM claims")]
        new_segments = compute_segments(all_claims, ctx)
        conn.execute("DELETE FROM segments")
        conn.executemany(
            "INSERT INTO segments(station, media_a, media_b, start, end, evidence) VALUES (?,?,?,?,?,?)",
            [(s.station, s.media[0], s.media[1], s.start, s.end, canonical_text(s.evidence_ids))
             for s in new_segments])

    # 纯重传不改变任何状态，revision 保持不变（便于上层判断"有无进展"）。
    if fresh:
        conn.execute("UPDATE meta SET v=? WHERE k='revision'", (str(rev + 1),))
    new_revision = rev + 1 if fresh else rev

    return {
        "command": "ingest",
        "status": "ok",
        "case_id": case.case_id,
        "applied": {
            "connections": sum(1 for ev in fresh if ev["kind"] == "connection"),
            "authorizations": sum(1 for ev in fresh if ev["kind"] == "authorization"),
            "watermarks": len(new_wm),
        },
        "retransmitted": sorted(retransmitted),
        "sealed_to": sealed_now,
        "sealed_now": new_seal,
        "revision": new_revision,
        "pending_ids": sorted(ev["id"] for ev in load_pending(conn)),
        "readable": _readable_view(conn, sealed_now),
        "moments": [
            {"t": t, "readable": readable, "rule_violations": _moment_rules(sim, t, current_ids)}
            # 只回显"本批新事件"所在的时刻：重放未封存事件不重复输出已发布过的时刻。
            for t, readable in sim.moment_readable if t in current_times
        ],
        "rule_violations": [v.to_json() for v in sim.rule_violations
                            if v.evidence_id in current_ids],
        "violations": [s.to_json() for s in new_segments],
    }


def _moment_rules(sim: Simulation, t: int, current_ids: set[str]) -> list[dict[str, Any]]:
    """只回显由 *本批新事件* 引起的非致命规则提示（重放旧事件不重复报）。"""
    return [v.to_json() for v in sim.rule_violations
            if v.t == t and v.evidence_id in current_ids]


def _payload(ev: dict[str, Any]) -> dict[str, Any]:
    out = {k: ev[k] for k in _PAYLOAD_FIELDS if k in ev}
    out["type"] = ev["kind"]
    return out


def _readable_view(conn: sqlite3.Connection, at: int) -> list[dict[str, Any]]:
    if at < 0:
        return []
    rows = conn.execute(
        "SELECT station, media FROM claims WHERE start <= ? AND ? < end GROUP BY station, media",
        (at, at))
    by_station: dict[str, list[str]] = {}
    for station, media in rows:
        by_station.setdefault(station, []).append(media)
    return [{"station": station, "media": sorted(media)}
            for station, media in sorted(by_station.items())]


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

def ensure_writable(conn: sqlite3.Connection) -> None:
    """确认库文件可写；只读介质上给出可定位的 ``db_unwritable``。"""
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError as exc:
        raise CaseError([Diagnostic("db_unwritable", f"数据库不可写：{exc}", None)]) from exc


def load_segments(conn: sqlite3.Connection) -> list[Segment]:
    return [
        Segment(station, (a, b), start, end, json.loads(evidence))
        for station, a, b, start, end, evidence in conn.execute(
            "SELECT station, media_a, media_b, start, end, evidence FROM segments")
    ]


def load_claims(conn: sqlite3.Connection) -> list[Claim]:
    return [Claim(*row) for row in conn.execute(
        "SELECT station, media, start, end, conn_id, auth_id FROM claims "
        "ORDER BY start, end, station, media")]


def report(conn: sqlite3.Connection, case_id: str | None) -> dict[str, Any]:
    base = sealed_to(conn)
    segments = merge_segments(load_segments(conn))
    view = summarize_segments(segments)
    return {
        "command": "report",
        "status": "ok",
        "case_id": case_id,
        "sealed_to": base,
        "revision": revision(conn),
        "readable": _readable_view(conn, base),
        "earliest": view["earliest"],
        "segments": view["segments"],
        "segment_count": view["segment_count"],
        "stations": view["stations"],
        "evidence_ids": view["evidence_ids"],
    }


def state(conn: sqlite3.Connection, case_id: str | None) -> dict[str, Any]:
    base = sealed_to(conn)
    pending = load_pending(conn)
    return {
        "command": "state",
        "status": "ok",
        "case_id": case_id,
        "sealed_to": base,
        "revision": revision(conn),
        "readable": _readable_view(conn, base),
        "claims": [
            {"station": c.station, "media": c.media, "from": c.start, "to": c.end,
             "evidence_ids": sorted({c.conn_id, c.auth_id})}
            for c in load_claims(conn)
        ],
        "pending_events": len(pending),
        "pending_ids": sorted(ev["id"] for ev in pending),
    }


def default_db_path(case_path: str) -> str:
    directory = os.path.dirname(os.path.abspath(case_path))
    stem = os.path.splitext(os.path.basename(case_path))[0] or "case"
    return os.path.join(directory, f"{stem}.state.sqlite")
