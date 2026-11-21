# 受管工位连接事件模拟器

离线工位的设备占用与敏感介质互斥：把"实际连接"和"可信授权"两条通道上乱序到达的
事件，归约成**已封存区间**内的可读集合与互斥违规段。

* 只依赖 **Windows 原生 Python 3.12 标准库**（`sqlite3` 随附），单核即可，不需要网络。
* 所有夹具都是**本地虚构数据**：设备、工位、介质、人员均为编造标识。
* 同一逻辑输入产生**逐字节相同**的标准输出。

---

## 1. 快速开始

```bash
# 干净案例：无违规
python -m solution ingest examples/01_clean.json --db /tmp/a.sqlite
python -m solution report examples/01_clean.json --db /tmp/a.sqlite

# 互斥违规：SECRET 与 TOOL 在 WS-NORTH-1 上同时可读 [1200, 1500)
python -m solution ingest examples/02_mutex.json --db /tmp/b.sqlite
python -m solution report examples/02_mutex.json --db /tmp/b.sqlite

# 同刻迁移：旧工位先断开，再在新工位连接 → 合法
python -m solution ingest examples/03_migration.json --db /tmp/c.sqlite

# 迟到整批回滚：先封存到 1200，再提交含 t=1100 的批次 → 整批拒绝
python -m solution ingest examples/04_seal_part1.json --db /tmp/d.sqlite
python -m solution ingest examples/04_late_part2.json --db /tmp/d.sqlite   # exit=1, late_event
python -m solution report examples/04_seal_part1.json --db /tmp/d.sqlite  # sealed_to 仍为 1200

# 重启后回放：两个进程顺序处理两批，第二个进程从 SQLite 恢复状态
python -m solution ingest examples/05_replay_after_restart.json --db /tmp/e.sqlite
python -m solution ingest examples/06_replay_part2.json     --db /tmp/e.sqlite
python -m solution report examples/05_replay_after_restart.json --db /tmp/e.sqlite
```

`--db` 省略时会按案例文件名派生成 `<案例名>.state.sqlite`，与案例文件同目录。
`state` 命令是 `report` 的调试视图（额外列出逐条可读区间与未封存事件）。

运行测试：

```bash
python -m unittest discover -s tests -v
```

---

## 2. 命令

```
python -m solution <命令> CASE.json [--db DB.sqlite] [--out FILE.json]
```

| 命令 | 需要 `--db` | 含义 |
| --- | --- | --- |
| `ingest` | 是 | 原子地提交案例中的全部事件；成功则推进水位 |
| `report` | 是 | 返回已封存区间的最早违规与全部最大同证据段 |
| `state` | 是 | 调试视图：封存点、可读区间、未封存事件 |

标准输出始终是 UTF-8 JSON（`ensure_ascii=False`、键名排序、2 空格缩进），末尾一个
换行。`--out` 会把同样的字节另存一份，便于跨进程比对。

### 退出码

| 码 | 含义 | 输出 |
| --- | --- | --- |
| `0` | 成功 | 对应命令的 JSON |
| `1` | 语义拒绝（批内冲突、迟到、水位重复） | `status=error` + 可定位诊断 |
| `2` | 用法 / 环境错误（参数、数据库不可写） | `status=error` |
| `3` | 案例结构错误 | `status=error` + 可定位诊断 |

所有拒绝都是**原子的**：失败批次不写入任何事件、不推进水位、不改动可读集合与
违规段。

---

## 3. 输入格式（CASE.json）

```jsonc
{
  "case_id": "任意字符串，用于回显",
  "media": [
    {"id": "MED-SECRET-071", "class": "SECRET"}
  ],
  "stations": [
    {"id": "WS-NORTH-1", "classes": ["SECRET", "TOOL"]}
  ],
  "conflicts": [["SECRET", "TOOL"]],
  "events": [ /* 见下 */ ]
}
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `case_id` | string，必需 | 非空，只用于回显 |
| `media` | array，必需 | 虚拟介质：固定 ID + 保管类别（`class`），ID 全局唯一 |
| `stations` | array，必需 | 工位：ID + 允许出现的保管类别 `classes`（可含多件介质） |
| `conflicts` | array，必需 | 互斥类别对，每项恰好两个保管类别（必须都在 `media` 出现过） |
| `events` | array，必需 | 事件序列；为空合法 |

**互斥语义**：`conflicts` 中的类别对**在同一工位内**不得同时可读；跨工位不受限。
"工位组合"即 `stations[].classes` —— 一个工位只接受它声明的类别。

### 事件

三种 `type`，全部带 `id`、`t`、`seq`、`channel`。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | string，必需 | 事件 ID，全局唯一；**同 ID 重传必须载荷完全一致** |
| `t` | 整数，必需 | 设备事件时间，微秒，≥ 0。这是**逻辑时刻** |
| `seq` | 整数，必需 | 通道序号，≥ 0；同一 `channel` 内严格递增（不允许跳号缺省） |
| `channel` | 整数，必需 | 通道编号，≥ 0 |

#### `connection` — 实际连接

```jsonc
{"type": "connection", "id": "conn-c1", "media": "MED-SECRET-071",
 "station": "WS-NORTH-1", "device": "DEVC-77", "gen": 0,
 "t": 1000, "seq": 1, "channel": 1}
```

| 字段 | 必需 | 说明 |
| --- | --- | --- |
| `media` | 是 | 必须已在 `media` 登记 |
| `station` | 是 | 必须已在 `stations` 登记；连接时该工位必须接受介质的类别 |
| `device` | 否 | 设备标识；参与授权绑定校验（两侧都有值时才比较） |
| `gen` | 是 | 连接代次，≥ 0 |
| `op` | 否 | 缺省 `"connect"`；`"disconnect"` 表示显式断开 |

#### `authorization` — 可信授权

```jsonc
{"type": "authorization", "id": "auth-a1", "media": "MED-SECRET-071",
 "station": "WS-NORTH-1", "device": "DEVC-77", "gen": 0,
 "t": 1000, "valid_until": 1400, "seq": 1, "channel": 2}
```

| 字段 | 必需 | 说明 |
| --- | --- | --- |
| `media` / `station` / `gen` | 是 | 必须与目标连接**全部一致**才算绑定成功 |
| `device` | 否 | 与连接一致时才允许；任一侧缺省则不比 |
| `t` | 是 | 生效时刻（闭端） |
| `valid_until` | 否 | 截止时刻（**开端**，左闭右开）；缺省视为永久有效。若给出则必须 `> t` |

授权窗口是 **`[t, valid_until)`**：`t` 当刻可读，`valid_until` 当刻已过期。

#### `watermark` — 水位

```jsonc
{"type": "watermark", "id": "wm-1", "t": 1700, "seq": 1, "channel": 9}
```

`t = T` 表示**该时刻及之前不会再产生合法新事件**。ingest 收到水位后，把
`(上一水位, T]` 整个区间封存：可读区间、违规段与证据 ID 一并落库。

---

## 4. 语义

### 4.1 可读的充要条件

一件介质在工位 `S` 于时刻 `t` **可读**，当且仅当：

1. 存在 `media` 的**活动连接**，且该连接的工位 = `S`、代次 = `g`；
2. 存在**已应用授权**，满足工位 = `S`、介质一致、代次 = `g`（`device` 双方都有值时须一致）；
3. `auth.t <= t < auth.valid_until`。

**连接或授权任一缺席就不是可读**——这就是"连接与授权跨通道，未到齐期间不标为可读"。
授权先到（`t_auth < t_conn`）时，可读从连接到达的那一时刻才开始。

### 4.2 同一时刻的批量归约

同一逻辑时刻 `t` 上的全部事件**合并成一次变化**再输出，求值顺序固定：

1. **先断开** —— `op="disconnect"` 的连接事件；
2. **再连接** —— 新连接（含代次推进）；
3. **再应用授权** —— 新到达的授权（按 `valid_until` 升序，先断旧再插新）。

每一步之后都检查状态，因此半开区间不会产生"瞬时假违规"：
`t` 断开 A 并连接 B 时，A 的区间在 `t` 结束、B 的区间在 `t` 开始，互不重叠。

### 4.3 违规段与证据

同一工位上两个**互斥类别**的可读区间若在时间上重叠，重叠部分即为违规段：

* 证据 ID = 参与重叠的两条可读区间各自的**连接 ID + 授权 ID**（通常是 4 个）；
* 同一 `(工位, 介质对, 证据集)` 且**相接或重叠**的段合并为**最大段**；
* 输出按 `(from, to, station, media)` 排序，`earliest` 即第一条。

不枚举时间刻度：代价与区间数量成正比，而非与时间跨度成正比。

### 4.4 连接代次规则

| 情形 | 处理 |
| --- | --- |
| 活动连接存在，新 `gen` 更小 | 拒绝整批：`connection_generation_regression` |
| 活动连接存在，同 `gen`、同工位、同设备、不同 ID | 拒绝整批：`duplicate_connection` |
| 活动连接存在，同 `gen`、不同工位 | 拒绝整批：`cross_station_migration_without_disconnect` |
| 活动连接存在，`gen` 更大 | 合法：旧的隐式断开（记 `implicit_disconnect_attempt` 提示） |
| 无活动连接，`gen` 更小 | 拒绝整批：`connection_generation_regression` |
| 无活动连接，`(工位, gen)` 组合已用过 | 拒绝整批：`connection_generation_regression` |
| 同代次迁移到**另一个**工位（已先断开） | 合法 |

同一个 `(介质, 工位, 代次)` 组合最多出现一次，因此"断开后同工位同代次重连"会被拒，
而同代次迁移到新工位是允许的。此历史跨批次持久化。

### 4.5 非致命规则（记录但不断批）

这些情况会写进输出的 `rule_violations`，但不阻断封存：

| 错误码 | 含义 |
| --- | --- |
| `authorization_stale_generation` | 再插入旧代授权 → **无效**，不参与可读性 |
| `disconnect_without_connection` | 断言断开但无活动连接 → 幂等空操作 |
| `disconnect_binding_mismatch` | 断开声明的工位/代次与活动连接不一致 → 幂等空操作 |
| `implicit_disconnect_attempt` | 更高代次连接顶替了旧连接，旧连接被隐式断开 |

### 4.6 水位与迟到

* 水位 `T` 封存 `(-1, T]`；`t <= 已封存水位` 的**非水位事件**一律以
  `late_event` **整批拒绝**——绝不静默并入已发布历史。
* `t <= 已封存水位` 的**水位**事件以 `watermark_already_sealed` 拒绝（不能重复推进）。
* 一个批次里只要有一条违规事件，**整批回滚**：没有半个水位、没有半个可读集合。

### 4.7 结构校验

案例先做纯结构校验（不改动任何状态），错误带 `code` 与 `field`：

| 错误码 | `field` 示例 |
| --- | --- |
| `invalid_json` | `line 3 col 14` |
| `missing_field` | `conflicts`、`events[2].gen` |
| `unknown_field` | `events[0].extra` |
| `invalid_type` | `events[0].gen`（布尔或浮点数） |
| `out_of_range` | `events[0].t`（负数或超大） |
| `unknown_media` / `unknown_station` | `events[1].media` |
| `incompatible_station` | `events[0].station` |
| `invalid_validity_window` | `events[0].valid_until` |
| `unknown_class` / `invalid_shape` | `conflicts[0][1]` |
| `duplicate_media` / `duplicate_station` | `media[1].id` |
| `duplicate_seq` | `events[1].seq` |
| `channel_time_regression` | `events[1].t` |
| `duplicate_watermark` | `events` |
| `unknown_event_type` | `events[0].type` |

**确定性**：浮点数、`NaN`/`Infinity`、布尔冒充整数一律拒绝；事件序由
`(t, channel, seq)` 决定，与事件在文件里的书写顺序无关。

---

## 5. 输出格式

### 5.1 `ingest`（成功）

```jsonc
{
  "command": "ingest",
  "status": "ok",
  "case_id": "demo-mutex",
  "applied": {"connections": 2, "authorizations": 2, "watermarks": 1},
  "retransmitted": [],                 // 本批中"已入库且载荷一致"被跳过的 ID
  "sealed_to": 2000,                   // 处理完之后的封存水位
  "sealed_now": 2000,                  // 本批推进到的水位；未推进则为 null
  "revision": 1,                       // 每次有实际变更 +1；纯重传不变
  "pending_ids": [],                   // 尚未封存的已接受事件
  "readable": [],                      // sealed_to 当刻的可读集合（两条授权都在 1800 前过期）
  "moments": [                         // 本批新事件所在的逻辑时刻（批量处理完之后）
    {"t": 1000, "readable": [{"station": "WS-NORTH-1", "media": ["MED-SECRET-071"]}],
     "rule_violations": []},
    {"t": 1200, "readable": [{"station": "WS-NORTH-1",
                              "media": ["MED-SECRET-071", "MED-TOOL-114"]}],
     "rule_violations": []}
  ],
  "rule_violations": [{"t": 1200, "code": "...", "evidence_id": "...",
                       "field": "events[3].gen", "message": "..."}],
  "violations": [ /* 本次封存后的全部违规段，格式同 report.segments */ ]
}
```

`readable` 是 **`sealed_to` 当刻**的可读集合（没有封存时为 `[]`）。
`moments[].readable` 是该时刻**处理完全部批量变化后**的可读集合。

### 5.2 `report`

```jsonc
{
  "command": "report",
  "status": "ok",
  "case_id": "demo-mutex",
  "sealed_to": 2000,
  "revision": 1,
  "readable": [...],
  "earliest": { /* 最早一条违规段，或 null */ },
  "segments": [ /* 全部最大同证据段 */ ],
  "segment_count": 1,
  "stations": ["WS-NORTH-1"],
  "evidence_ids": ["auth-a1", "auth-a2", "conn-c1", "conn-c2"]
}
```

违规段：

```jsonc
{
  "code": "mutual_exclusion",
  "station": "WS-NORTH-1",
  "media": ["MED-SECRET-071", "MED-TOOL-114"],
  "from": 1200,
  "to": 1500,
  "evidence_ids": ["auth-a1", "auth-a2", "conn-c1", "conn-c2"]
}
```

`from`/`to` 是**半开区间** `[from, to)`，单位与输入 `t` 相同。区间可以越过
`sealed_to`（延伸的部分由后续批次截断或延伸）。

### 5.3 `state`

```jsonc
{
  "command": "state", "status": "ok", "case_id": "...",
  "sealed_to": 1200, "revision": 1,
  "readable": [...],
  "claims": [{"station": "WS-NORTH-1", "media": "MED-SECRET-071",
              "from": 1000, "to": 1201, "evidence_ids": ["l-a1", "l-c1"]}],
  "pending_events": 0,           // 已接受但尚未封存的事件数
  "pending_ids": []
}
```

### 5.4 错误

```jsonc
{
  "command": "ingest",
  "status": "error",
  "case_id": "demo-late-rollback",
  "errors": [
    {"code": "late_event", "field": "events[2].t",
     "message": "事件 l-late 的时间 1100 不晚于已封存水位 1200；..."}
  ]
}
```

`field` 用 JSON 路径：`media[1].class`、`events[3].gen`、`conflicts[0][1]`、
畸形 JSON 用 `line L col C`。

---

## 6. 持久状态与并发

* 单个 SQLite 文件，WAL 模式 + `synchronous=FULL`。
* 每次 `ingest` 用 `BEGIN IMMEDIATE`，因此**两个进程竞争同一水位**时由 SQLite 串行化：
  恰好一个成功封存，另一个得到 `late_event` 或 `watermark_already_sealed` 并原子回滚。
* 已写内容：`seen`（ID + 载荷摘要，用于重传归并与 ID 复用检测）、`pending`（未封存事件）、
  `snap_*`（封存点快照：活动连接、已接受授权、最大代次、已消耗的 `(工位,代次)` 组合）、
  `claims`（可读区间）、`segments`（违规段）。
* **重启无需特殊处理**：状态全在库里，下一个进程直接接着跑。

---

## 7. 目录

```
solution/
  __main__.py   入口
  cli.py        参数、退出码、UTF-8 JSON 输出
  model.py      JSON 加载、规范化、结构校验、确定性序
  engine.py     时刻归约、可读区间、违规段
  store.py      SQLite 事务、封存、report 查询
examples/       可直接运行的虚构案例
tests/          python -m unittest discover -s tests -v
```
