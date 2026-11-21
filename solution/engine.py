"""规则引擎：同一逻辑时刻的批量归约，以及可读区间（claim）推导。

两层语义刻意分开：

**批内规则（legality）** —— 在逻辑时刻 ``t`` 上，该时刻的全部事件合并成一次变化，
按"先断开 → 再连接 → 再应用授权"求值，检查连接代次、重复连接、跨工位迁移等
状态规则。违反 *致命* 规则会使整批 ingest 原子回滚；违反 *非致命* 规则只记为
``rule_violations``（例如再插入旧代授权是"无效"，不是"报错"）。

**可读区间（claims）** —— 可读性的唯一判据是"连接存活区间 ∩ 授权窗口"：

    claim = 工位 × 介质 × [start, end) × 证据(连接 ID, 授权 ID)
    可读 ⟺ 存在匹配的连接（工位/设备/介质/代次一致）且 t ∈ [授权生效, 授权截止)

于是：

* "未到齐期间不标为可读"由定义直接给出：连接或授权任一缺席就没有区间；
* "同一时刻批量变化处理完再输出"是表示法自带的性质：同一时刻的断开让旧区间在
  ``t`` 结束、连接让新区间在 ``t`` 开始，半开区间互不重叠，不会产生瞬时假违规，
  也不需要逐刻度枚举时间；
* 授权截止与解绑落在同一时刻时区间长度为零，自然消失（左闭右开）。

同一工位上两个互斥类别的 claim 在时间上重叠即构成违规段。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .model import Case, Pair, order_key

CODE_MUTEX = "mutual_exclusion"

FATAL_CODES = frozenset({
    "connection_generation_regression",
    "duplicate_connection",
    "cross_station_migration_without_disconnect",
})


class FatalRule(Exception):
    """致命规则冲突：整批 ingest 必须原子回滚，水位不推进。"""

    def __init__(self, violation: "RuleViolation"):
        super().__init__(violation.message)
        self.violation = violation


# ---------------------------------------------------------------------------
# 实体
# ---------------------------------------------------------------------------

@dataclass
class Conn:
    media: str
    station: str
    device: str | None
    gen: int
    conn_id: str
    start: int
    end: int | None = None


@dataclass
class Auth:
    id: str
    media: str
    station: str
    device: str | None
    gen: int
    t: int
    valid_until: int
    seq: int = 0

    def applies_to(self, conn: Conn) -> bool:
        return (
            self.media == conn.media
            and self.station == conn.station
            and self.gen == conn.gen
            and (self.device is None or conn.device is None or self.device == conn.device)
        )


@dataclass
class Claim:
    """一段可读区间 ``[start, end)``，证据为连接 ID 与授权 ID。"""

    station: str
    media: str
    start: int
    end: int
    conn_id: str
    auth_id: str

    def evidence_ids(self) -> tuple[str, ...]:
        return tuple(sorted({self.conn_id, self.auth_id}))


@dataclass
class RuleViolation:
    t: int
    code: str
    evidence_id: str
    field: str | None
    message: str

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"t": self.t, "code": self.code, "evidence_id": self.evidence_id}
        if self.field is not None:
            out["field"] = self.field
        out["message"] = self.message
        return out


@dataclass(frozen=True)
class Ctx:
    media: dict[str, str]
    station_classes: dict[str, list[str]]
    conflicts: frozenset[Pair]

    @classmethod
    def from_case(cls, case: Case) -> "Ctx":
        return cls(case.media, case.stations, case.conflicts)

    def class_of(self, media_id: str) -> str | None:
        return self.media.get(media_id)

    def conflict(self, a: str, b: str) -> bool:
        ca, cb = self.media.get(a), self.media.get(b)
        if ca is None or cb is None or ca == cb:
            return False
        return Pair.of(ca, cb) in self.conflicts

    def station_accepts(self, station: str, media_id: str) -> bool:
        return self.media.get(media_id) in self.station_classes.get(station, ())


# ---------------------------------------------------------------------------
# 单批模拟
# ---------------------------------------------------------------------------

class Simulation:
    """一次 ingest 的整段模拟：从封存快照出发，重放全部未封存事件。

    致命冲突立即抛出 :class:`FatalRule`，调用方据此整批回滚。
    """

    def __init__(self, ctx: Ctx, open_conns: Iterable[Conn], auths: Iterable[Auth],
                 max_gen: dict[str, int], consumed: Iterable[tuple[str, str, int]],
                 sealed_from: int):
        self.ctx = ctx
        self.open_conns: dict[str, Conn] = {c.media: c for c in open_conns}
        self.auths: list[Auth] = list(auths)
        self.max_gen: dict[str, int] = dict(max_gen)
        self.sealed_from = sealed_from
        self.rule_violations: list[RuleViolation] = []
        self.moment_readable: list[tuple[int, list[dict[str, Any]]]] = []
        self.closed_conns: list[Conn] = []
        # 每件介质已消耗的 (工位, 代次) 组合：同一组合不得再次连接。
        # 这样"同代次跨工位迁移"合法，而"同工位同代次断开后重连"非法。
        self.consumed: set[tuple[str, str, int]] = set(consumed) | {
            (c.media, c.station, c.gen) for c in self.open_conns.values()}

    # -- 规则记录 ---------------------------------------------------------

    def _rule(self, t: int, code: str, evidence: str, field: str | None, message: str) -> None:
        violation = RuleViolation(t, code, evidence, field, message)
        self.rule_violations.append(violation)
        if code in FATAL_CODES:
            raise FatalRule(violation)

    # -- 批次 -------------------------------------------------------------

    def run(self, events: list[dict[str, Any]]) -> None:
        """按 ``(t, channel, seq)`` 处理；同一时刻先断开、再连接、再应用授权。"""
        grouped: dict[int, list[dict[str, Any]]] = {}
        for ev in events:
            grouped.setdefault(ev["t"], []).append(ev)
        for at in sorted(grouped):
            moment = sorted(grouped[at], key=order_key)
            self._apply_moment(at, moment)
            self.moment_readable.append((at, self.readable_at(at)))

    def _apply_moment(self, at: int, moment: list[dict[str, Any]]) -> None:
        disconnects = [e for e in moment if e["op"] == "disconnect"]
        connects = [e for e in moment if e["op"] == "connect"]
        auths = sorted((e for e in moment if e["op"] == "authorize"),
                       key=lambda e: (e["valid_until"], order_key(e)))
        # 1) 先断开   2) 再连接   3) 再应用授权
        for ev in disconnects:
            self._disconnect(at, ev)
        for ev in connects:
            self._connect(at, ev)
        for ev in auths:
            self._authorize(at, ev)

    # -- 先断开 -----------------------------------------------------------

    def _disconnect(self, at: int, ev: dict[str, Any]) -> None:
        cur = self.open_conns.get(ev["media"])
        field = f"events[{ev['index']}]"
        if cur is None:
            self._rule(at, "disconnect_without_connection", ev["id"], f"{field}.media",
                       "要求断开，但该介质此刻没有活动连接；按幂等空操作处理")
            return
        if cur.gen != ev["gen"] or cur.station != ev["station"]:
            self._rule(at, "disconnect_binding_mismatch", ev["id"], f"{field}.gen",
                       f"断开声明 (工位={ev['station']}, 代次={ev['gen']}) 与活动连接 "
                       f"{cur.conn_id} (工位={cur.station}, 代次={cur.gen}) 不一致；按幂等空操作处理")
            return
        cur.end = at
        self.closed_conns.append(cur)
        del self.open_conns[cur.media]

    # -- 再连接 -----------------------------------------------------------

    def _connect(self, at: int, ev: dict[str, Any]) -> None:
        media_id, gen = ev["media"], ev["gen"]
        field = f"events[{ev['index']}]"
        cur = self.open_conns.get(media_id)
        seen = self.max_gen.get(media_id, -1)
        consumed_key = (media_id, ev["station"], gen)
        if cur is not None:
            if gen < cur.gen:
                self._rule(at, "connection_generation_regression", ev["id"], f"{field}.gen",
                           f"介质 {media_id} 的活动连接已是代次 {cur.gen}，不能回退到 {gen}")
            if gen == cur.gen:
                if cur.station == ev["station"] and cur.device == ev["device"]:
                    self._rule(at, "duplicate_connection", ev["id"], f"{field}.id",
                               f"介质 {media_id} 在工位 {cur.station} 上已有代次 {gen} 的活动连接 "
                               f"{cur.conn_id}；不同 ID 的同一连接按冲突拒绝")
                self._rule(at, "cross_station_migration_without_disconnect", ev["id"],
                           f"{field}.station",
                           f"介质 {media_id} 从工位 {cur.station} 迁移到 {ev['station']}，"
                           f"但同一逻辑时刻没有先断开旧工位的连接（代次仍为 {gen}）")
            cur.end = at
            self.closed_conns.append(cur)
            del self.open_conns[media_id]
        elif gen < seen:
            self._rule(at, "connection_generation_regression", ev["id"], f"{field}.gen",
                       f"介质 {media_id} 已使用过代次 {seen}，断开后不能回退到 {gen}")
        elif consumed_key in self.consumed:
            self._rule(at, "connection_generation_regression", ev["id"], f"{field}.gen",
                       f"介质 {media_id} 已在工位 {ev['station']} 上使用过代次 {gen}；"
                       f"断开后必须以更高代次重连，或以同代次迁移到 *另一个* 工位")
        self.consumed.add(consumed_key)
        conn = Conn(media_id, ev["station"], ev["device"], gen, ev["id"], at)
        self.open_conns[media_id] = conn
        self.max_gen[media_id] = max(seen, gen)

    # -- 再应用授权 -------------------------------------------------------

    def _authorize(self, at: int, ev: dict[str, Any]) -> None:
        media_id = ev["media"]
        field = f"events[{ev['index']}]"
        auth = Auth(ev["id"], media_id, ev["station"], ev["device"], ev["gen"],
                    ev["t"], ev["valid_until"], seq=ev["seq"])
        seen = self.max_gen.get(media_id, -1)
        # 规则：再插入旧代授权无效 —— 无效则不参与可读性计算。
        if seen > auth.gen:
            self._rule(at, "authorization_stale_generation", ev["id"], f"{field}.gen",
                       f"介质 {media_id} 的连接已推进到代次 {seen}，代次 {auth.gen} 的授权无效，"
                       f"不参与可读性")
            return
        self.auths.append(auth)

    # -- 可读集合 ---------------------------------------------------------

    def readable_at(self, at: int) -> list[dict[str, Any]]:
        """逻辑时刻 ``at`` 处理完全部批量变化后的可读集合（需要连接 ∧ 授权）。"""
        by_station: dict[str, set[str]] = {}
        for media_id, conn in self.open_conns.items():
            if conn.start > at:
                continue
            if not self._is_readable(conn, at):
                continue
            by_station.setdefault(conn.station, set()).add(media_id)
        return [
            {"station": station, "media": sorted(media)}
            for station, media in sorted(by_station.items())
            if media
        ]

    def _is_readable(self, conn: Conn, at: int) -> bool:
        return any(a.applies_to(conn) and a.t <= at < a.valid_until for a in self.auths)

    # -- 结束后：可读区间 ------------------------------------------------

    def lifetimes(self) -> list[Conn]:
        return sorted(self.closed_conns + list(self.open_conns.values()),
                      key=lambda c: (c.start, c.conn_id))

    def claims(self, clip_from: int, clip_to: int) -> list[Claim]:
        """截断到 ``[clip_from, clip_to)`` 内的全部可读区间。"""
        out: list[Claim] = []
        for conn in self.lifetimes():
            s = max(conn.start, clip_from)
            e = clip_to if conn.end is None else min(conn.end, clip_to)
            if s >= e:
                continue
            out.extend(self._claims_for(conn, s, e))
        return merge_claims(out)

    def _claims_for(self, conn: Conn, s: int, e: int) -> list[Claim]:
        applicable = [a for a in self.auths if a.applies_to(conn)]
        if not applicable:
            return []
        points = {s, e}
        for a in applicable:
            if s < a.t < e:
                points.add(a.t)
            if s < a.valid_until < e:
                points.add(a.valid_until)
        ordered = sorted(points)
        out: list[Claim] = []
        for x, y in zip(ordered, ordered[1:]):
            covering = [a for a in applicable if a.t <= x < a.valid_until]
            if not covering:
                continue
            chosen = min(covering, key=lambda a: (a.t, a.seq, a.id))
            out.append(Claim(conn.station, conn.media, x, y, conn.conn_id, chosen.id))
        return out


def merge_claims(claims: list[Claim]) -> list[Claim]:
    """同 (工位, 介质, 连接, 授权) 且首尾相接的区间合并成最大区间。"""
    out: list[Claim] = []
    for claim in sorted(claims, key=lambda c: (c.station, c.media, c.start, c.end, c.auth_id)):
        if out:
            last = out[-1]
            if (last.station == claim.station and last.media == claim.media
                    and last.conn_id == claim.conn_id and last.auth_id == claim.auth_id
                    and last.end == claim.start):
                out[-1] = Claim(last.station, last.media, last.start, claim.end,
                                last.conn_id, last.auth_id)
                continue
        out.append(Claim(claim.station, claim.media, claim.start, claim.end,
                         claim.conn_id, claim.auth_id))
    return out


# ---------------------------------------------------------------------------
# 违规段
# ---------------------------------------------------------------------------

@dataclass
class Segment:
    station: str
    media: tuple[str, str]
    start: int
    end: int
    evidence_ids: list[str]

    @property
    def code(self) -> str:
        return CODE_MUTEX

    def group_key(self) -> tuple:
        return (self.station, self.media, tuple(self.evidence_ids))

    def to_json(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "station": self.station,
            "media": list(self.media),
            "from": self.start,
            "to": self.end,
            "evidence_ids": list(self.evidence_ids),
        }


def compute_segments(claims: Iterable[Claim], ctx: Ctx) -> list[Segment]:
    """由可读区间求出全部 *最大同证据* 违规段；不枚举时间刻度。

    做法是按工位分组，对每个互斥介质对其可读区间做归并连接（merge-join），
    代价与区间数量及实际重叠数成正比，而非与时间跨度成正比。
    """
    by_station: dict[str, dict[str, list[Claim]]] = {}
    for claim in claims:
        if claim.start < claim.end:
            by_station.setdefault(claim.station, {}).setdefault(claim.media, []).append(claim)
    raw: list[Segment] = []
    for station, per_media in by_station.items():
        media_ids = sorted(per_media)
        for media_list in per_media.values():
            media_list.sort(key=lambda c: (c.start, c.end, c.auth_id))
        for i in range(len(media_ids)):
            for j in range(i + 1, len(media_ids)):
                a, b = media_ids[i], media_ids[j]
                if not ctx.conflict(a, b):
                    continue
                raw.extend(_intersect(station, a, b, per_media[a], per_media[b]))
    return merge_segments(raw)


def _intersect(station: str, a: str, b: str, left: list[Claim], right: list[Claim]) -> list[Segment]:
    """两条已排序区间列表的求交；每段证据取两侧证据之并。"""
    out: list[Segment] = []
    i = k = 0
    while i < len(left) and k < len(right):
        p, q = left[i], right[k]
        start, end = max(p.start, q.start), min(p.end, q.end)
        if start < end:
            evidence = sorted(set(p.evidence_ids()) | set(q.evidence_ids()))
            out.append(Segment(station, (a, b), start, end, evidence))
        if p.end <= q.end:
            i += 1
        else:
            k += 1
    return out


def merge_segments(segments: Iterable[Segment]) -> list[Segment]:
    """同 (工位, 介质对, 证据集) 且相接/重叠的段合并为最大段。"""
    buckets: dict[tuple, list[Segment]] = {}
    for seg in segments:
        buckets.setdefault(seg.group_key(), []).append(seg)
    out: list[Segment] = []
    for items in buckets.values():
        items.sort(key=lambda s: (s.start, s.end))
        cur = items[0]
        for nxt in items[1:]:
            if nxt.start <= cur.end:
                cur = Segment(cur.station, cur.media, cur.start,
                              max(cur.end, nxt.end), cur.evidence_ids)
            else:
                out.append(cur)
                cur = nxt
        out.append(cur)
    out.sort(key=lambda s: (s.start, s.end, s.station, s.media, s.evidence_ids))
    return out


def summarize_segments(segments: list[Segment]) -> dict[str, Any]:
    """report 的公共视图：已封存区间的最早违规 + 全部最大同证据段。"""
    earliest = segments[0] if segments else None
    return {
        "earliest": earliest.to_json() if earliest else None,
        "segments": [s.to_json() for s in segments],
        "segment_count": len(segments),
        "stations": sorted({s.station for s in segments}),
        "evidence_ids": sorted({e for s in segments for e in s.evidence_ids}),
    }
