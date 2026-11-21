"""案例文件的结构定义、JSON 加载、规范化与校验。

所有诊断都带 ``code`` 与 ``field``（形如 ``events[3].gen`` 的 JSON 路径），便于
定位。结构错误绝不产生副作用，也不会推进 watermark。语义阶段（ingest 事务内的
冲突）则用 ``events[id=<事件 ID>].<字段>`` 定位到具体事件。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

# 事件时间上限（微秒，自 Unix 纪元），保证 ``t`` 的算术不溢出。
MAX_TIME = 2**62


@dataclass
class Diagnostic:
    code: str
    message: str
    field: str | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"code": self.code}
        if self.field is not None:
            out["field"] = self.field
        out["message"] = self.message
        return out


class CaseError(Exception):
    """案例错误。``diagnostics`` 中每一条都可定位。"""

    def __init__(self, diagnostics: list[Diagnostic]):
        super().__init__(diagnostics[0].message if diagnostics else "invalid case")
        self.diagnostics = diagnostics


# ---------------------------------------------------------------------------
# JSON 基础设施
# ---------------------------------------------------------------------------

def canonical_text(value: Any) -> str:
    """确定性序列化：排序键、紧凑分隔符、不转义非 ASCII。"""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _types_identical(a: Any, b: Any) -> bool:
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(_types_identical(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(_types_identical(x, y) for x, y in zip(a, b))
    return True


def is_canonical_json(value: Any) -> bool:
    """值能否 *无损* 往返 ``json.dumps`` / ``json.loads``。

    浮点数（含 ``-0.0``）、NaN/Inf 以及 JSON 无法表达的类型都不接受，从根上
    杜绝"同一逻辑输入因序列化细节不同而产生不同结果"。
    """
    try:
        back = json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError):
        return False
    return _types_identical(value, back)


def _reject_constant(name: str) -> Any:
    raise CaseError([Diagnostic("invalid_json", f"不允许的 JSON 常量 {name}；只接受标准 JSON")])


def load_case_text(text: str) -> dict[str, Any]:
    try:
        doc = json.loads(text, parse_constant=_reject_constant)
    except CaseError:
        raise
    except json.JSONDecodeError as exc:
        raise CaseError([
            Diagnostic("invalid_json",
                       f"第 {exc.lineno} 行第 {exc.colno} 列无法解析：{exc.msg}",
                       f"line {exc.lineno} col {exc.colno}")
        ]) from exc
    if not isinstance(doc, dict):
        raise CaseError([Diagnostic("invalid_type", "案例根节点必须是 JSON 对象", "$")])
    return doc


def load_case(path: str) -> dict[str, Any]:
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError as exc:
        raise CaseError([Diagnostic("case_not_found", f"案例文件不存在：{path}")]) from exc
    except OSError as exc:
        raise CaseError([Diagnostic("case_unreadable", f"无法读取案例文件：{exc}")]) from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CaseError([Diagnostic("invalid_utf8", f"案例文件不是合法 UTF-8：{exc}")]) from exc
    return load_case_text(text)


# ---------------------------------------------------------------------------
# 规范化实体
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Pair:
    """无序的保管类别对（互斥类别集合的元素）。"""

    a: str
    b: str

    @classmethod
    def of(cls, x: str, y: str) -> "Pair":
        return cls(x, y) if x <= y else cls(y, x)


@dataclass(frozen=True)
class Case:
    case_id: str
    media: dict[str, str]              # 介质 ID -> 保管类别（保序）
    stations: dict[str, list[str]]     # 工位 ID -> 允许的保管类别（去重保序）
    conflicts: frozenset[Pair]
    connections: list[dict[str, Any]]
    authorizations: list[dict[str, Any]]
    watermarks: list[dict[str, Any]]
    raw: dict[str, Any]                # 规范化后的完整文档（用于元信息）

    def events(self) -> list[dict[str, Any]]:
        """全部事件，按 ``(t, channel, seq)`` 排序。

        通道内序号与事件时间方向一致（见 :func:`validate_case`），所以这个键是
        全局确定的全序：同一逻辑输入无论事件在文件里怎么写，结果都一样；跨批次
        也许可比较（不依赖批次内的文档位置）。
        """
        merged = self.connections + self.authorizations + self.watermarks
        return sorted(merged, key=order_key)


# ---------------------------------------------------------------------------
# 基本字段读取
# ---------------------------------------------------------------------------

def _path(base: str, key: str | int) -> str:
    if isinstance(key, int):
        return f"{base}[{key}]" if base else f"[{key}]"
    return f"{base}.{key}" if base else key


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def order_key(ev: dict[str, Any]) -> tuple[int, int, int, int]:
    """事件的确定性全序：``(t, channel, seq, 类型序)``。

    同一 ``(channel, seq)`` 只可能属于一个事件（见 :func:`validate_case`），
    所以这个键在全案例内唯一，与文件里的书写顺序无关；跨批次也可直接比较，
    重放结果因此与"哪批先到"无关。
    """
    return (ev["t"], ev["channel"], ev["seq"], _KIND_ORDER.get(ev["kind"], 9))


_KIND_ORDER = {"connection": 0, "authorization": 1, "watermark": 2}



def _check_keys(obj: dict[str, Any], where: str, required: list[str],
                optional: list[str] | None = None) -> list[Diagnostic]:
    out: list[Diagnostic] = []
    optional = optional or []
    for key in required:
        if key not in obj:
            out.append(Diagnostic("missing_field", f"缺少必需字段 {key}", _path(where, key)))
    allowed = set(required) | set(optional)
    for key in obj:
        if key not in allowed:
            out.append(Diagnostic("unknown_field", f"未知字段 {key}", _path(where, key)))
    return out


def _str(obj: dict[str, Any], where: str, key: str, required: bool = True,
         default: str | None = None) -> tuple[str | None, list[Diagnostic]]:
    field = _path(where, key)
    if key not in obj:
        if required:
            return None, [Diagnostic("missing_field", f"缺少必需字段 {key}", field)]
        return default, []
    value = obj[key]
    if not isinstance(value, str):
        return None, [Diagnostic("invalid_type", f"{key} 必须是字符串", field)]
    if not value:
        return None, [Diagnostic("invalid_value", f"{key} 不能为空字符串", field)]
    return value, []


def _int(obj: dict[str, Any], where: str, key: str, required: bool = True,
         default: int | None = None, minimum: int | None = None,
         maximum: int | None = None) -> tuple[int | None, list[Diagnostic]]:
    field = _path(where, key)
    if key not in obj:
        if required:
            return None, [Diagnostic("missing_field", f"缺少必需字段 {key}", field)]
        return default, []
    value = obj[key]
    if not _is_int(value):
        return None, [Diagnostic("invalid_type", f"{key} 必须是整数（JSON 数值且非布尔）", field)]
    if minimum is not None and value < minimum:
        return None, [Diagnostic("out_of_range", f"{key} 必须 >= {minimum}", field)]
    if maximum is not None and value > maximum:
        return None, [Diagnostic("out_of_range", f"{key} 必须 <= {maximum}", field)]
    return value, []


def _list(obj: dict[str, Any], where: str, key: str) -> tuple[list[Any] | None, list[Diagnostic]]:
    field = _path(where, key)
    if key not in obj:
        return None, [Diagnostic("missing_field", f"缺少必需字段 {key}", field)]
    value = obj[key]
    if not isinstance(value, list):
        return None, [Diagnostic("invalid_type", f"{key} 必须是数组", field)]
    return value, []


# ---------------------------------------------------------------------------
# 规范化
# ---------------------------------------------------------------------------

def normalize(doc: dict[str, Any]) -> Case:
    """把已解析的文档规范化为 :class:`Case`；有任何结构错误都整体拒绝。"""
    errors: list[Diagnostic] = []
    errors += _check_keys(doc, "", ["case_id", "media", "stations", "conflicts", "events"],
                          ["watermarks"])

    case_id, e = _str(doc, "", "case_id")
    errors += e

    media = _normalize_media(doc, errors)
    stations = _normalize_stations(doc, errors)
    conflicts = _normalize_conflicts(doc, media, errors)
    connections, authorizations, watermarks = _normalize_events(doc, media, stations, errors)

    if errors:
        raise CaseError(errors)
    return Case(
        case_id=case_id or "",
        media=media,
        stations=stations,
        conflicts=conflicts,
        connections=connections,
        authorizations=authorizations,
        watermarks=watermarks,
        raw=doc,
    )


def _normalize_media(doc: dict[str, Any], errors: list[Diagnostic]) -> dict[str, str]:
    items, e = _list(doc, "", "media")
    errors += e
    if items is None:
        return {}
    out: dict[str, str] = {}
    for i, item in enumerate(items):
        where = f"media[{i}]"
        if not isinstance(item, dict):
            errors.append(Diagnostic("invalid_type", "media 元素必须是对象", where))
            continue
        errors += _check_keys(item, where, ["id", "class"])
        media_id, e1 = _str(item, where, "id")
        media_class, e2 = _str(item, where, "class")
        errors += e1 + e2
        if media_id is None or media_class is None:
            continue
        if media_id in out:
            errors.append(Diagnostic("duplicate_media", f"介质 ID {media_id} 重复定义", _path(where, "id")))
            continue
        out[media_id] = media_class
    return out


def _normalize_stations(doc: dict[str, Any], errors: list[Diagnostic]) -> dict[str, list[str]]:
    items, e = _list(doc, "", "stations")
    errors += e
    if items is None:
        return {}
    out: dict[str, list[str]] = {}
    for i, item in enumerate(items):
        where = f"stations[{i}]"
        if not isinstance(item, dict):
            errors.append(Diagnostic("invalid_type", "stations 元素必须是对象", where))
            continue
        errors += _check_keys(item, where, ["id", "classes"])
        station_id, e1 = _str(item, where, "id")
        classes_raw, e2 = _list(item, where, "classes")
        errors += e1 + e2
        if station_id is None or classes_raw is None:
            continue
        if station_id in out:
            errors.append(Diagnostic("duplicate_station", f"工位 ID {station_id} 重复定义", _path(where, "id")))
            continue
        classes: list[str] = []
        seen: set[str] = set()
        for j, cls in enumerate(classes_raw):
            cwhere = _path(_path(where, "classes"), j)
            if not isinstance(cls, str) or not cls:
                errors.append(Diagnostic("invalid_type", "保管类别必须是非空字符串", cwhere))
                continue
            if cls in seen:
                continue  # 语义重复，无害：去重
            seen.add(cls)
            classes.append(cls)
        out[station_id] = classes
    return out


def _normalize_conflicts(doc: dict[str, Any], media: dict[str, str],
                         errors: list[Diagnostic]) -> frozenset[Pair]:
    items, e = _list(doc, "", "conflicts")
    errors += e
    if items is None:
        return frozenset()
    known = set(media.values())
    pairs: set[Pair] = set()
    for i, item in enumerate(items):
        where = f"conflicts[{i}]"
        if not isinstance(item, list) or len(item) != 2:
            errors.append(Diagnostic("invalid_shape", "conflicts 元素必须是恰好两个保管类别的数组", where))
            continue
        bad = False
        for j, val in enumerate(item):
            if not isinstance(val, str) or not val:
                errors.append(Diagnostic("invalid_type", "互斥类别必须是非空字符串", _path(where, j)))
                bad = True
        if bad:
            continue
        x, y = item
        if x == y:
            errors.append(Diagnostic("invalid_value", "互斥类别不能是同一个类别", where))
            continue
        for j, val in enumerate(item):
            if val not in known:
                errors.append(Diagnostic("unknown_class", f"互斥类别 {val} 未在 media 中出现", _path(where, j)))
        pairs.add(Pair.of(x, y))
    return frozenset(pairs)


def _normalize_events(doc: dict[str, Any], media: dict[str, str], stations: dict[str, list[str]],
                      errors: list[Diagnostic]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    items, e = _list(doc, "", "events")
    errors += e
    connections: list[dict[str, Any]] = []
    authorizations: list[dict[str, Any]] = []
    watermarks: list[dict[str, Any]] = []
    if items is None:
        items = []
    for i, item in enumerate(items):
        where = f"events[{i}]"
        if not isinstance(item, dict):
            errors.append(Diagnostic("invalid_type", "events 元素必须是对象", where))
            continue
        kind, e0 = _str(item, where, "type")
        errors += e0
        if kind is None:
            continue
        handler = {
            "connection": _normalize_connection,
            "authorization": _normalize_authorization,
            "watermark": _normalize_watermark,
        }.get(kind)
        if handler is None:
            errors.append(Diagnostic("unknown_event_type", f"未知事件类型 {kind}", _path(where, "type")))
            continue
        ev = handler(item, where, i, media, stations, errors)
        if kind == "connection":
            connections.append(ev)
        elif kind == "authorization":
            authorizations.append(ev)
        else:
            watermarks.append(ev)
    return connections, authorizations, watermarks


def _normalize_connection(obj: dict[str, Any], where: str, index: int, media: dict[str, str],
                          stations: dict[str, list[str]], errors: list[Diagnostic]) -> dict[str, Any]:
    errors += _check_keys(obj, where, ["type", "id", "media", "station", "gen", "t", "seq", "channel"],
                          ["device", "op"])
    out: dict[str, Any] = {"index": index, "kind": "connection"}
    out["id"], e = _str(obj, where, "id")
    errors += e
    out["device"], e = _str(obj, where, "device", required=False)
    errors += e
    out["media"], e = _str(obj, where, "media")
    errors += e
    out["station"], e = _str(obj, where, "station")
    errors += e
    out["gen"], e = _int(obj, where, "gen", minimum=0)
    errors += e
    out["t"], e = _int(obj, where, "t", minimum=0, maximum=MAX_TIME)
    errors += e
    out["seq"], e = _int(obj, where, "seq", minimum=0)
    errors += e
    out["channel"], e = _int(obj, where, "channel", minimum=0)
    errors += e
    op, e = _str(obj, where, "op", required=False)
    errors += e
    if op is not None and op not in ("connect", "disconnect"):
        errors.append(Diagnostic("invalid_value", "op 只能是 connect 或 disconnect", _path(where, "op")))
        op = None
    out["op"] = op or "connect"
    out["valid_until"] = None

    media_id, station_id = out["media"], out["station"]
    if media_id is not None and media_id not in media:
        errors.append(Diagnostic("unknown_media", f"介质 {media_id} 未在 media 中登记", _path(where, "media")))
        media_id = None
    if station_id is not None and station_id not in stations:
        errors.append(Diagnostic("unknown_station", f"工位 {station_id} 未在 stations 中登记", _path(where, "station")))
        station_id = None
    if media_id is not None and station_id is not None and out["op"] == "connect":
        media_class = media[media_id]
        if media_class not in stations[station_id]:
            errors.append(Diagnostic(
                "incompatible_station",
                f"保管类别 {media_class} 不能出现在工位 {station_id}",
                _path(where, "station"),
            ))
    return out


def _normalize_authorization(obj: dict[str, Any], where: str, index: int, media: dict[str, str],
                             stations: dict[str, list[str]], errors: list[Diagnostic]) -> dict[str, Any]:
    errors += _check_keys(obj, where,
                          ["type", "id", "media", "station", "gen", "t", "seq", "channel"],
                          ["device", "valid_until"])
    out: dict[str, Any] = {"index": index, "kind": "authorization", "op": "authorize"}
    out["id"], e = _str(obj, where, "id")
    errors += e
    out["device"], e = _str(obj, where, "device", required=False)
    errors += e
    out["media"], e = _str(obj, where, "media")
    errors += e
    out["station"], e = _str(obj, where, "station")
    errors += e
    out["gen"], e = _int(obj, where, "gen", minimum=0)
    errors += e
    out["t"], e = _int(obj, where, "t", minimum=0, maximum=MAX_TIME)
    errors += e
    until, e = _int(obj, where, "valid_until", required=False, minimum=0, maximum=MAX_TIME)
    errors += e
    out["valid_until"] = MAX_TIME if until is None and "valid_until" not in obj else until
    out["seq"], e = _int(obj, where, "seq", minimum=0)
    errors += e
    out["channel"], e = _int(obj, where, "channel", minimum=0)
    errors += e

    media_id, station_id = out["media"], out["station"]
    if media_id is not None and media_id not in media:
        errors.append(Diagnostic("unknown_media", f"介质 {media_id} 未在 media 中登记", _path(where, "media")))
        media_id = None
    if station_id is not None and station_id not in stations:
        errors.append(Diagnostic("unknown_station", f"工位 {station_id} 未在 stations 中登记", _path(where, "station")))
        station_id = None
    if media_id is not None and station_id is not None:
        media_class = media[media_id]
        if media_class not in stations[station_id]:
            errors.append(Diagnostic(
                "incompatible_station",
                f"保管类别 {media_class} 不能出现在工位 {station_id}",
                _path(where, "station"),
            ))
    if out["valid_until"] is not None and out["t"] is not None and out["valid_until"] <= out["t"]:
        errors.append(Diagnostic("invalid_validity_window",
                                 f"截止时间 {out['valid_until']} 必须晚于事件时间 {out['t']}（左闭右开）",
                                 _path(where, "valid_until")))
    return out


def _normalize_watermark(obj: dict[str, Any], where: str, index: int, media: dict[str, str],
                         stations: dict[str, list[str]], errors: list[Diagnostic]) -> dict[str, Any]:
    errors += _check_keys(obj, where, ["type", "id", "t", "seq", "channel"])
    out: dict[str, Any] = {"index": index, "kind": "watermark", "op": "watermark"}
    out["id"], e = _str(obj, where, "id")
    errors += e
    out["t"], e = _int(obj, where, "t", minimum=0, maximum=MAX_TIME)
    errors += e
    out["seq"], e = _int(obj, where, "seq", minimum=0)
    errors += e
    out["channel"], e = _int(obj, where, "channel", minimum=0)
    errors += e
    out["media"] = None
    out["station"] = None
    out["gen"] = None
    out["device"] = None
    out["valid_until"] = None
    return out


# ---------------------------------------------------------------------------
# 证据摘要与案例自检
# ---------------------------------------------------------------------------

def input_evidence(event: dict[str, Any]) -> dict[str, Any]:
    """输入事件的确定性摘要；同 ID 重试必须与之完全一致。"""
    keep = ["type", "id", "op", "media", "station", "device", "gen", "t", "valid_until", "seq", "channel"]
    out = {k: event.get(k) for k in keep if event.get(k) is not None}
    out["type"] = event["kind"]
    out["media"] = event.get("media")
    return out


def validate_case(case: Case) -> list[Diagnostic]:
    """案例自检：通道重传规则、通道内时间有序、水位唯一。

    同一事件 ID 的重复交给 ingest 事务归并（完全一致视为重传，载荷不同则整批
    拒绝），所以这里不做 ID 唯一性检查，以免把合法的重传误判为结构错误。
    """
    errors: list[Diagnostic] = []
    deduped: dict[str, dict[str, Any]] = {}
    for ev in sorted(case.connections + case.authorizations + case.watermarks,
                     key=lambda e: e["index"]):
        deduped.setdefault(ev["id"], ev)

    by_channel: dict[int, list[dict[str, Any]]] = {}
    for ev in deduped.values():
        if ev.get("seq") is not None and ev.get("channel") is not None:
            by_channel.setdefault(ev["channel"], []).append(ev)
    for channel, evs in sorted(by_channel.items()):
        ordered = sorted(evs, key=lambda e: (e["seq"], e["index"]))
        prev: dict[str, Any] | None = None
        for ev in ordered:
            if prev is not None and ev["seq"] == prev["seq"]:
                errors.append(Diagnostic(
                    "duplicate_seq",
                    f"通道 {channel} 的序号 {ev['seq']} 已被 events[{prev['index']}] 占用；"
                    f"不同 ID 不得占用同一通道序号",
                    f"events[{ev['index']}].seq",
                ))
                continue
            if prev is not None and ev["t"] < prev["t"]:
                errors.append(Diagnostic(
                    "channel_time_regression",
                    f"通道 {channel} 内事件时间倒退：{ev['t']} < {prev['t']}（seq {ev['seq']}）",
                    f"events[{ev['index']}].t",
                ))
            prev = ev

    wm_times = [wm["t"] for wm in case.watermarks]
    if len(set(wm_times)) != len(wm_times):
        errors.append(Diagnostic("duplicate_watermark", "水位时刻重复声明", "events"))
    return errors
