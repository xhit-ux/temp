# OPC UA 数据实时接入 YMatrix PoC 实施文档

## 1. 项目背景

工业设备数据通常通过 OPC UA 协议采集。本项目实现一套 OPC UA 数据实时接入 YMatrix 的 PoC，验证从模拟设备、实时订阅、数据写入到查询展示的完整链路。

题目要求：

1. 模拟 3–5 台 OPC UA 设备，每台设备包含若干点位。
2. 实时采集点位数据。
3. 将数据写入 YMatrix，支持批量写入、失败重试、写入速率记录和错误日志。
4. 提供最近 5 分钟趋势、异常点位、设备聚合和简单告警 SQL。
5. 输出接入架构、数据模型、写入性能、断线重连策略和生产化风险报告。

本项目是数据库方向 FDE PoC。OPC UA 模拟服务是测试数据源，重点是证明 YMatrix 能够稳定接入、可靠写入、正确查询并从故障中恢复。

## 2. 实施原则

- OPC UA 模拟服务保持够用，不继续扩展复杂协议功能。
- 采集必须使用 Subscription / DataChangeNotification，不使用轮询。
- Python 负责 OPC UA 协议适配，Go 负责 YMatrix 写入和查询服务。
- 正常数据以吞吐为目标，使用攒批和 `pgx.CopyFrom`。
- 异常数据以低延迟为目标，使用独立通道立即写入。
- 正常数据和异常数据都必须支持重试、死信、告警和补偿。
- PoC 按题目要求使用 3–5 台设备，不额外构造大规模设备压测。
- “写入性能”记录实际 PoC 运行速率和写入耗时，不做数据库极限压力测试。

## 3. 系统架构

```text
┌──────────────────────────────────────────────┐
│ Python OPC UA Server :4840                  │
│ 4 台模拟设备，每台 6 个点位                  │
└──────────────────────┬───────────────────────┘
                       │ Subscription / DataChange
                       ▼
┌──────────────────────────────────────────────┐
│ Python OPC UA Collector                     │
│ 数据提取、字段映射、断线重连、发送缓冲        │
└──────────────────────┬───────────────────────┘
                       │ HTTP JSON
                       ▼
┌──────────────────────────────────────────────┐
│ Go Backend :4048                            │
│ 校验、分流、内存 channel、写库、查询、推送    │
├───────────────────┬──────────────────────────┤
│ 正常数据           │ 异常/告警数据            │
│ 数量/时间触发攒批  │ 独立优先通道              │
│ CopyFrom 批量写入  │ 单条事务立即写入          │
└─────────┬─────────┴─────────────┬────────────┘
          │                       │
          └───────────┬───────────┘
                      ▼
┌──────────────────────────────────────────────┐
│ YMatrix :5432                               │
│ 测点数据、告警数据、查询分析                 │
└──────────────────────┬───────────────────────┘
                       │ REST + WebSocket/SSE
                       ▼
┌──────────────────────────────────────────────┐
│ React 实时看板                              │
└──────────────────────────────────────────────┘
```

## 4. 组件与端口

| 端口 | 组件 | 用途 |
|---|---|---|
| `4840` | Python OPC UA Server | 提供标准 OPC UA 服务和订阅通知 |
| 无监听端口 | Python Collector | 主动连接 OPC UA Server 和 Go 后端 |
| `4048` | Go Backend | 数据接入、查询 API、实时推送和健康检查 |
| `5432` | YMatrix | PostgreSQL 协议数据库连接 |

模拟设备是 OPC UA 地址空间中的 Object，不为每台设备分配独立端口。

Python 与 Go 是独立进程，不能直接共享 Python Queue 或 Go channel。PoC 使用 HTTP 作为进程间传输，内存队列分别位于两个进程内部。

## 5. OPC UA 模拟服务

现有 `opcua/server/` 实现继续使用，不再扩大开发范围。

### 5.1 模拟设备

使用 4 台设备，符合题目要求的 3–5 台范围：

- `Device_01`
- `Device_02`
- `Device_03`
- `Device_04`

每台设备包含：

- `temperature`
- `pressure`
- `speed`
- `current`
- `voltage`
- `device_clock`

其中 `device_clock` 是数据质量演示点位，不是题目强制点位，不影响核心写入链路。

### 5.2 技术约束

- `asyncua` 统一使用 `1.1.x`。
- 使用 `await server.register_namespace(...)`。
- 不使用已被移除的 `write_display_name()`。
- Server 周期更新点位，客户端通过 Subscription 接收通知。
- 保留现有 Quality 异常、数值突变和短时离线模拟能力。
- 不实现 OPC UA Historizing。

## 6. Python 采集器

现有 `subscription_probe.py` 只用于验证订阅是否有效。正式链路新增 Collector，但不在 Python 中实现数据库写入。

Collector 职责：

1. 连接 OPC UA Server。
2. 获取 Namespace Index。
3. 订阅全部设备点位。
4. 从 DataChangeNotification 提取值、Quality 和时间戳。
5. 生成稳定的 `event_id`。
6. 将事件写入有界 `asyncio.Queue`。
7. 后台发送任务将事件提交给 Go。
8. 连接断开后指数退避并重建 Session 和 Subscription。

DataChange 回调只进行轻量数据提取和 Queue 入队，不直接执行 HTTP、数据库或阻塞式文件操作。

当 Go 暂时不可用时，Python 发送任务进行退避重试；长时间无法发送的事件写入采集端本地暂存文件，防止仅依赖进程内存。

## 7. 标准事件模型

题目建议字段为：

```text
device_id, point_name, point_value, quality, event_time, ingest_time
```

PoC 在此基础上保留数据类型和原始时间语义：

```json
{
  "event_id": "01900000-0000-7000-8000-000000000001",
  "device_id": "Device_01",
  "point_name": "temperature",
  "data_type": "Double",
  "unit": "C",
  "value_number": 46.37,
  "value_text": null,
  "value_time": null,
  "quality_code": 0,
  "quality_name": "Good",
  "source_timestamp": "2026-07-16T10:00:00.000Z",
  "server_timestamp": "2026-07-16T10:00:00.001Z",
  "collector_timestamp": "2026-07-16T10:00:00.008Z",
  "event_type": "data_change"
}
```

字段约束：

- `event_id` 首次采集时生成，重试和补偿沿用原值。**重试提交的批次必须复用原始批次完整、未增删的 `event_id` 集合**，这是第 13 节幂等推理成立的前提，不能因为坏数据隔离等逻辑在重试路径上改变批次内容。
- `value_number`、`value_text`、`value_time` 三选一。
- `quality_code` 保存 OPC UA 原始状态码。
- `quality_name` 保存便于查询和展示的质量名称。
- `source_timestamp` 对应题目中的 `event_time`。
- `collector_timestamp` 和 Go 生成的 `received_at` 对应接入过程时间。
- 所有时间使用 UTC。

`device_clock` 作为普通 DateTime 点位传输，其值放入 `value_time`。核心异常规则不依赖该点位，时钟偏差只作为报告中的数据质量风险说明。

## 8. Go 后端职责

Go 后端是本项目的主要实施部分，负责：

- 接收 Python 提交的事件。
- 校验必填字段和类型。
- 判断正常、异常和告警路径。
- 使用有界 channel 实现进程内缓冲。
- 正常数据攒批并通过 `pgx.CopyFrom` 写入。
- 异常数据立即使用单条事务写入。
- 对两类数据执行重试、死信和补偿。
- 记录写入速率、写入耗时和错误日志。
- 提供查询 API 和 WebSocket/SSE 实时推送。
- 处理 SIGTERM/SIGINT 并执行最后一次 flush。

## 9. 正常数据链路

```text
HTTP 接收
  -> 数据校验
  -> normal channel
  -> 批次聚合器
  -> writer worker
  -> pgx.CopyFrom
  -> YMatrix
```

正常数据使用多触发器：

- 数量触发：默认达到 `500` 条时 flush。
- 时间触发：默认最老事件等待 `5s` 时 flush。
- 关闭触发：收到 SIGTERM/SIGINT 时强制 flush。
- 手动触发：管理接口执行 flush。

当前 4 台设备、每台 6 个点位、每秒更新一次时，理论输入速率约为：

```text
4 × 6 × 1 = 24 events/s
```

在该速率下，500 条批次约需 20.8 秒才能攒满，因此日常运行主要由 5 秒时间触发器 flush，预期批次约 120 条。数量阈值仍保留，用于应对数据突发。

正常数据必须具备完整可靠性：

- `CopyFrom` 失败后保持原 `batch_id` 整批重试，且重试批次的 `event_id` 集合必须与原始批次完全一致。
- 重试成功前不得释放或确认批次。
- 重试耗尽后将批次拆成逐事件死信记录。
- 字段或类型错误导致批次失败时，隔离坏事件，让合法事件继续写入；坏数据隔离只针对“数据本身不合法”这类不可恢复错误，不应用于连接失败触发的重试路径，避免破坏重试批次与原始批次的一致性。
- normal channel 满时返回 `429` 或 `503`，不能静默丢弃。

## 10. 异常与告警链路

异常事件进入独立 priority channel，不等待普通数据批次。

异常来源：

- Quality 为 Bad 或 Uncertain。
- 数值超过点位配置的上下限。
- OPC UA 会话断开。
- 设备在指定时间窗口内没有新数据。

处理方式：

- 立即单条写入测点表。
- 同一事务内写入告警表。
- 使用独立 worker 或预留数据库连接，避免被普通批次阻塞。
- 立即向前端推送。
- 写入失败时同样执行重试，耗尽后进入高优先级死信。

异常数据“不攒批”只代表降低延迟，不代表可以绕过可靠性机制。

## 11. 重试、死信与补偿

### 11.1 错误分类与重试

写入错误分为三级，`copyFrom()` 需要能够区分并向上层返回可识别的结果，而不是笼统的 `error`：

| 级别 | 判定条件 | 处理方式 |
|---|---|---|
| **确认成功（ambiguous commit resolved）** | `*pgconn.PgError` 且 `Code == "23505"`（unique_violation） | 不计入失败计数，记 INFO 级日志“批次已存在，判定为重试确认成功”，`metrics` 按 `totalAmbiguousResolved` 单独累计，不进入死信 |
| **可重试** | 网络连接中断、数据库连接失败、YMatrix 暂时不可用、数据库超时、连接池暂时耗尽等临时错误 | 指数退避重试，默认最多 `5` 次：`1s -> 2s -> 4s -> 8s -> 16s`，每次等待增加少量随机抖动，避免多个 writer 同时重试 |
| **不可恢复** | 字段缺失、数据类型不匹配、`NOT NULL` 违反、SQL 结构错误等 | 不进行无意义重复请求，直接隔离并进入死信 |

**为什么命中一次 `23505` 就可以把整个批次判定为“已成功”，而不是仅判定这一行成功：**

原始 COPY 提交是一次单一事务，要么整批全部提交、要么整批全部回滚，不存在“上一批 500 条里只有部分行落库”的中间状态。因此重试批次只要有任意一行命中唯一约束冲突，就能反推出原始那次 COPY 必然是完整提交成功的，否则这一行不会存在于表中。既然原始提交是“全有或全无”，此时这一整批数据必然已经全部在库中，可以直接判定为成功，不需要逐行核对其余数据是否也已存在。

这个推理成立的前提是重试批次与原始批次的 `event_id` 集合完全一致（见第 7 节字段约束），实现时需要作为不变量保护，不能在重试路径上做坏数据增删。

### 11.2 死信

目录：

```text
opc2ymatrix/data/dead-letter/
```

每条 NDJSON 至少包含：

```json
{
  "path": "normal",
  "batch_id": "...",
  "event": {},
  "error_class": "transient",
  "error_message": "...",
  "retry_count": 5,
  "first_failed_at": "...",
  "last_failed_at": "..."
}
```

要求：

- 正常数据和异常数据都能进入死信。
- `23505`（unique_violation）不进入死信——数据已经落库，没有补偿必要，关键在于 writer 源头就将其拦截并按成功处理，避免污染死信和监控数字。
- 文件按日期或大小滚动。
- 采用追加写入，进程重启后仍存在。
- 保留原始 `event_id` 和 `batch_id`。
- 补偿成功前不删除原始记录。
- 定时任务按小批次补偿写入。
- 提供管理接口手动触发补偿。
- 记录死信数量、文件大小和最老事件时间。

### 11.3 补偿写入的幂等性

死信补偿统一使用 `InsertOnConflict()`（`INSERT ... ON CONFLICT (event_id) DO NOTHING`）而非 `CopyFrom`，天然幂等，即使同一条死信被补偿多次也不会产生重复数据。当前先预留方法签名，按小批次或逐条处理；补偿数据量增大后，改为使用 `unnest` 数组做批量 `INSERT ... ON CONFLICT DO NOTHING`，避免逐条循环的开销，这一优化作为 TODO 注释保留，不在当前阶段实现。

## 12. YMatrix 数据模型

### 12.1 环境预检

建表前记录数据库版本和 Segment 数量：

```sql
SELECT version();
SELECT current_database(), current_user;

SELECT count(*) AS primary_segment_count
FROM gp_segment_configuration
WHERE role = 'p' AND content >= 0;
```

确认当前用户具备建表、建索引和写入权限，并验证 `uuid`、临时表、`COPY` 和唯一索引能力。

### 12.2 分布键选择

PoC 只有 3–5 个设备键。直接 `DISTRIBUTED BY (device_id)` 会让数据集中到少量 Segment，产生低基数分布倾斜。

本次主表选择：

```sql
DISTRIBUTED BY (event_id)
```

原因：

- `event_id` 高基数，能够让少量设备产生的数据均匀分布。
- 重试使用同一个 `event_id`，相同事件始终路由到同一 Segment。
- 分布键与唯一索引选在同一列上，天然满足“唯一约束必须包含分布键”的限制，使 `event_id` 唯一索引可以直接建立，不需要额外设计复合约束。
- 当前数据规模很小，按设备查询产生的跨 Segment 扫描可以接受。

生产环境不能直接沿用该结论。真实设备数量较多且主要按设备查询时，应重新评估 `device_id` 或复合分布策略，并根据真实数据倾斜决定。

### 12.3 测点表

```sql
CREATE TABLE IF NOT EXISTS opc_point_data (
    event_id             uuid        NOT NULL,
    device_id            text        NOT NULL,
    point_name           text        NOT NULL,
    data_type            text        NOT NULL,
    unit                 text,
    value_number         double precision,
    value_text           text,
    value_time           timestamptz,
    quality_code         bigint      NOT NULL,
    quality_name         text        NOT NULL,
    event_type           text        NOT NULL,
    is_abnormal          boolean     NOT NULL DEFAULT false,
    abnormal_reason      text,
    source_timestamp     timestamptz,
    server_timestamp     timestamptz,
    collector_timestamp  timestamptz NOT NULL,
    received_at          timestamptz NOT NULL DEFAULT now(),
    batch_id             uuid
)
DISTRIBUTED BY (event_id);
```

```sql
CREATE UNIQUE INDEX IF NOT EXISTS ux_opc_point_event
ON opc_point_data (event_id);

CREATE INDEX IF NOT EXISTS idx_opc_point_device_time
ON opc_point_data (device_id, point_name, source_timestamp);

CREATE INDEX IF NOT EXISTS idx_opc_point_abnormal_time
ON opc_point_data (is_abnormal, source_timestamp);
```

唯一索引需要在当前 YMatrix 环境中实际验证。如果目标表类型不支持唯一索引，则保留稳定 `event_id`，并使用去重视图兜底。

唯一索引的存在会让每一行 `COPY` 都执行一次 B-tree 查找（无论是否冲突），开销是微秒级的索引查找，不会对 `CopyFrom` 吞吐构成可观测影响；这与“无冲突时不做检查”的说法不同，检查本身总会发生，只是代价极小。

### 12.4 告警表

```sql
CREATE TABLE IF NOT EXISTS opc_alarm_event (
    alarm_id          uuid        NOT NULL,
    event_id          uuid        NOT NULL,
    device_id         text        NOT NULL,
    point_name        text,
    severity          text        NOT NULL,
    alarm_type        text        NOT NULL,
    message           text        NOT NULL,
    status            text        NOT NULL DEFAULT 'active',
    occurred_at       timestamptz NOT NULL,
    recovered_at      timestamptz,
    acknowledged_at   timestamptz,
    acknowledged_by   text,
    created_at        timestamptz NOT NULL DEFAULT now()
)
DISTRIBUTED BY (event_id);
```

## 13. 幂等与查询去重

传输语义采用 at-least-once。网络在数据库提交阶段断开时，客户端可能无法确认事务是否成功，重试可能产生重复。

处理策略：

1. 所有重试沿用原 `event_id`，且重试批次不改变原始批次的事件集合。
2. YMatrix 支持时建立 `event_id` 唯一索引（见 12.3），分布键与唯一索引同列，二者互相支撑。
3. 正常路径的 `CopyFrom` 遇到 `23505` 时，按 11.1 节的三级分类直接判定为“确认成功”，不重试、不进入死信。
4. 补偿路径统一使用 `InsertOnConflict()`，天然幂等，不依赖上层额外判断。
5. 查询层在数据量增长、且实测证明有必要时再引入去重视图或改为增量去重表；PoC 当前阶段先直接查询主表加 `is_abnormal`/时间范围等条件，去重视图作为阶段四按实测结果决定是否启用（见 14 节说明）。

去重视图定义（预留，是否默认启用见 14 节）：

```sql
CREATE OR REPLACE VIEW opc_point_data_dedup AS
SELECT
    event_id,
    device_id,
    point_name,
    data_type,
    unit,
    value_number,
    value_text,
    value_time,
    quality_code,
    quality_name,
    event_type,
    is_abnormal,
    abnormal_reason,
    source_timestamp,
    server_timestamp,
    collector_timestamp,
    received_at,
    batch_id
FROM (
    SELECT
        p.*,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY received_at DESC
        ) AS rn
    FROM opc_point_data p
) d
WHERE rn = 1;
```

由于窗口函数的分区键是 `event_id`，而查询过滤条件（`device_id`、`source_timestamp`、`is_abnormal`）与分区键不同，数据库通常无法把外层 WHERE 条件下推到窗口函数计算之前，可能导致每次查询先对全表计算 `row_number()` 再过滤。数据量小时不明显，但语义上与“最近 5 分钟趋势”这类应该轻量的查询有一定矛盾，因此该视图是否作为默认查询入口，需要在阶段四用真实数据量跑 `EXPLAIN ANALYZE` 后再决定（见 14 节）。

由于源头写入路径已经通过 `23505` 分类和 `InsertOnConflict()` 幂等两道机制保证重复概率极低，去重视图更多是兜底而非常规依赖，不应默认成为四类查询的唯一实现路径。

## 14. 查询分析 SQL

阶段一先使用主表直接查询验证四类 SQL 语义正确性。阶段四实现查询 API 时，在 YMatrix 中插入规模化测试数据并执行 `EXPLAIN ANALYZE`：

- 若主表直接查询已能满足性能要求，四类查询直接读取 `opc_point_data`，不引入去重视图。
- 若发现明显重复数据影响聚合结果，且视图导致全表扫描，则改为在查询 SQL 中使用 `DISTINCT ON (event_id)` 或按查询自身的过滤条件重写子查询去重，使 WHERE 条件能在去重之前先缩小数据范围，而不是依赖 13 节定义的通用视图。

以下 SQL 以主表为例，实际使用去重视图或 `DISTINCT ON` 改写时保持查询语义不变。

### 14.1 最近 5 分钟趋势

```sql
SELECT
    source_timestamp,
    device_id,
    point_name,
    value_number,
    quality_name
FROM opc_point_data
WHERE device_id = $1
  AND point_name = $2
  AND source_timestamp >= now() - interval '5 minutes'
ORDER BY source_timestamp;
```

### 14.2 异常点位

```sql
SELECT
    event_id,
    device_id,
    point_name,
    value_number,
    value_text,
    value_time,
    quality_name,
    abnormal_reason,
    source_timestamp
FROM opc_point_data
WHERE is_abnormal = true
  AND source_timestamp >= now() - interval '24 hours'
ORDER BY source_timestamp DESC
LIMIT $1;
```

### 14.3 按设备聚合统计

```sql
SELECT
    device_id,
    count(*) AS sample_count,
    count(*) FILTER (WHERE is_abnormal) AS abnormal_count,
    min(source_timestamp) AS first_sample_time,
    max(source_timestamp) AS last_sample_time
FROM opc_point_data
WHERE source_timestamp >= $1
  AND source_timestamp < $2
GROUP BY device_id
ORDER BY device_id;
```

### 14.4 简单告警查询

```sql
SELECT
    alarm_id,
    device_id,
    point_name,
    severity,
    alarm_type,
    message,
    status,
    occurred_at,
    recovered_at
FROM opc_alarm_event
WHERE status = 'active'
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        ELSE 3
    END,
    occurred_at DESC;
```

接入报告中说明每条 SQL 的业务用途，并保存实际执行结果，包括是否使用了去重视图/`DISTINCT ON`改写及其原因。PoC 不要求系统性查询压测。

## 15. 写入速率与错误日志

题目要求记录写入速率，不要求数据库极限压测。

Go 每 10 秒输出一次统计：

- 接收事件数。
- 成功提交行数（`totalWritten`，本窗口内真实新提交的行数）。
- 确认成功但判定为重复提交的行数（`totalAmbiguousResolved`，本窗口内命中 `23505` 并被判定为“之前已提交成功”的行数，单独统计，不与 `totalWritten` 合并）。
- 正常批次数。
- 平均批次大小。
- 当前写入速率 `rows/s`。
- 平均和最大批次写入耗时。
- 正常 channel 和 priority channel 深度。
- 重试次数。
- 错误次数（不包含 `23505`）。
- 死信数量。

推荐计算方式：

```text
write_rate = 当前统计窗口 totalWritten / 窗口秒数
```

`totalAmbiguousResolved` 不计入 `write_rate` 分子：这批数据的真实写入吞吐贡献发生在上一次（原始）尝试所在的时间窗口，而不是当前这次重试确认的窗口，直接合并会让当前窗口的写入速率显得高于实际瞬时吞吐。报告中两个数字分别列出，不合并为一个吞吐率。

结构化错误日志至少包含：

- `event_id` 或 `batch_id`
- 正常或异常路径
- 批次行数
- 写入耗时
- 重试次数
- PostgreSQL SQLSTATE
- 错误分类（确认成功 / 可重试 / 不可恢复）
- 是否进入死信

报告中的“写入性能”使用真实 PoC 运行结果：

- 连续运行建议不少于 10 分钟。
- 记录理论输入速率和实际写入速率。
- 记录平均批次大小和批次写入耗时。
- 记录是否出现重试、错误和死信。
- 分别列出 `totalWritten` 和 `totalAmbiguousResolved`，不合并为单一吞吐数字。
- 不把该结果描述为 YMatrix 的最大吞吐能力。

## 16. 断线重连与优雅关闭

### 16.1 OPC UA 断线

1. Python Collector 检测 Session 或 Subscription 断开。
2. 使用指数退避重新连接。
3. 重新获取 Namespace Index。
4. 重建 Subscription 和 MonitoredItem。
5. 恢复后继续发送新 DataChange 事件。
6. 断线期间无 OPC UA 历史缓存的数据可能无法补采，写入生产化风险。

### 16.2 YMatrix 断线

1. Go writer 保留当前正常批次或异常事件。
2. 指数退避重试。
3. 若重试过程中命中 `23505`，判定为该批次此前已提交成功，按 11.1 节直接确认，不进入死信。
4. 其余情形重试耗尽后写入死信。
5. 立即记录错误并产生运行告警。
6. YMatrix 恢复后由补偿任务通过 `InsertOnConflict()` 重新写入。

### 16.3 优雅关闭

Go 收到 SIGTERM/SIGINT 后：

1. readiness 切换为不可接收新流量。
2. 停止接收新的 ingest 请求。
3. 等待正在处理的 HTTP 请求结束。
4. 排空 normal 和 priority channel。
5. 强制 flush 未达到 500 条或 5 秒的正常批次。
6. 等待 writer 完成。
7. 超时未完成的数据写入死信。
8. 关闭数据库连接池和实时连接。

Python Collector 关闭时停止新订阅，排空发送 Queue；超时未发送的数据写入本地暂存文件。

## 17. Go API 与 React 展示

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/v1/ingest/events` | Python 提交事件 |
| `GET` | `/api/v1/points/latest` | 最新点位 |
| `GET` | `/api/v1/trends` | 最近 5 分钟趋势 |
| `GET` | `/api/v1/abnormal-points` | 异常点位 |
| `GET` | `/api/v1/device-statistics` | 设备聚合 |
| `GET` | `/api/v1/alarms` | 告警列表 |
| `GET` | `/api/v1/stream` | WebSocket/SSE 实时推送 |
| `POST` | `/api/v1/admin/flush` | 手动 flush |
| `POST` | `/api/v1/admin/dead-letter/replay` | 手动补偿 |
| `GET` | `/api/v1/health/live` | 存活检查 |
| `GET` | `/api/v1/health/ready` | 数据库和队列就绪检查 |

React 只实现验收需要的页面：

- 4 台设备最新状态。
- 最近 5 分钟趋势图。
- 异常点位列表。
- 设备聚合统计。
- 当前告警列表。

实时更新使用 WebSocket 或 SSE，不通过前端轮询模拟实时效果。

## 18. 实施目录

```text
temp/
├── opcua/
│   ├── server/                    # 已完成的模拟服务
│   ├── client/
│   │   ├── subscription_probe.py # 订阅验证工具
│   │   ├── collector.py          # 正式采集器
│   │   ├── event_mapper.py
│   │   └── sender.py
│   ├── config/
│   └── requirements.txt
├── opc2ymatrix/
│   ├── cmd/server/main.go
│   ├── internal/
│   │   ├── ingest/
│   │   ├── batcher/
│   │   ├── writer/
│   │   ├── retry/
│   │   ├── deadletter/
│   │   ├── query/
│   │   └── stream/
│   ├── migrations/
│   ├── data/dead-letter/
│   ├── reports/
│   ├── web/
│   ├── go.mod
│   └── .env.example
└── OPC_UA实时接入YMatrix_PoC实施文档.md
```

## 19. 实施阶段

### 阶段一：YMatrix 预检与建模

- 确认版本、Segment、权限和数据库连接。
- 创建测点表、告警表、索引和去重视图（视图先创建但不默认启用）。
- 验证 `DISTRIBUTED BY (event_id)` 和唯一索引。
- 手工插入样例数据并验证四类 SQL。

### 阶段二：Go 写入服务

- 实现事件接入、校验和分类。
- 实现 normal channel 和 priority channel。
- 实现多触发器攒批、CopyFrom 和异常单条写入。
- 实现写入速率、耗时和错误日志，区分 `totalWritten` 与 `totalAmbiguousResolved`。

### 阶段三：可靠性

- 实现正常和异常路径的指数退避。
- 实现 `copyFrom()` 对 `23505` 的识别与 `ErrAmbiguousCommit` 处理。
- 实现坏数据隔离、死信和补偿（补偿使用 `InsertOnConflict()`）。
- 实现队列背压和优雅关闭 flush。

### 阶段四：Python Collector 联调

- 将现有 Subscription 接入标准事件模型。
- 实现 HTTP 发送、重试和本地暂存。
- 验证 OPC UA 断线重连和订阅重建。

### 阶段五：查询、前端和报告

- 实现四类查询 API，先用主表验证，按 14 节流程决定是否启用去重视图。
- 完成 React 实时展示。
- 连续运行不少于 10 分钟并记录写入速率。
- 完成数据库断线、恢复、死信补偿和优雅关闭演示。
- 输出最终接入报告。

## 20. 接入报告结构

### 20.1 接入架构

- 各组件职责。
- 端口和数据流。
- Python 与 Go 的边界。
- 正常和异常双路径。

### 20.2 数据模型

- OPC UA 到数据库字段映射。
- 测点表和告警表。
- `event_id` 幂等设计，及“重试批次事件集合不变”这一前提。
- `DISTRIBUTED BY (event_id)` 的选择原因，及其与唯一索引同列的设计考虑。
- 3–5 台设备导致 `device_id` 低基数的取舍。

### 20.3 写入性能

- 理论输入速率。
- 实际平均写入速率（`totalWritten`）。
- 确认成功的重复提交行数（`totalAmbiguousResolved`），与写入速率分开列出。
- 平均批次大小。
- 批次写入耗时。
- 重试、错误和死信数量。
- 明确说明结果是当前 PoC 负载表现，不是数据库极限吞吐。

### 20.4 断线重连策略

- OPC UA Session 和 Subscription 重建。
- Python 到 Go 发送重试。
- YMatrix 写入重试、`23505` 确认成功判定和死信。
- 数据库恢复后的补偿。
- SIGTERM flush。

### 20.5 生产化风险

- HTTP 加内存队列不是持久化消息系统，强制退出仍可能丢失内存窗口数据。
- 生产环境可使用 Kafka 等可靠消息队列。
- OPC UA 断线期间如果源端不支持历史补采，数据无法恢复。
- Quality 不能忽略，否则 Bad 数据可能被当成正常值。
- SourceTimestamp、采集时间和设备业务时钟可能不同步。
- 网络提交结果不确定可能产生重复，依赖稳定 `event_id`、唯一索引和 `23505` 判定机制共同兜底；若坏数据隔离逻辑在重试路径上误改了事件集合，该幂等推理将失效，需要作为不变量在代码层面保护。
- 去重视图在数据量增长后可能出现全表窗口计算的性能问题，需要评估物化视图或增量去重表方案。
- 生产环境分布键需要根据真实设备基数、热点和查询模式重新评估。
- 数据保留周期增长后需要评估时间分区、时序表、压缩和索引。
- 死信目录需要容量限制、滚动、磁盘告警和清理策略。
- 告警需要防抖、合并、恢复和确认机制。
- OPC UA 生产接入需要证书、安全策略、用户认证和信任列表。
- 优雅关闭不能覆盖断电、崩溃和强制杀进程。

## 21. 验收清单

- [x] 模拟设备数量为 3–5 台。（已有 Device_01–04，`opcua/config/config.yaml`）
- [x] 每台设备包含 temperature、pressure、speed、current、voltage 等点位。（配备 device_clock 共 6 个）
- [x] 数据采集使用 Subscription，不存在轮询读取。（`opcua/client/collector.py` → `SubscriptionHandler`）
- [x] Python Collector 能正确传输值、Quality 和时间戳。（`opcua/client/event_mapper.py` 标准事件映射）
- [x] Go 能接收并区分正常与异常数据。（`handler/ingest.go` 双通道路由，`model/IngestEvent.IsAbnormal()`）
- [x] 正常数据支持数量、时间、关闭和手动 flush。（`writer/batcher.go`：数量 500 条 / 时间 5s / 停机 flush）
- [ ] 正常批次支持失败重试、坏数据隔离、死信和补偿。（重试已实现；坏数据隔离、死信文件、补偿写入待阶段三）
- [ ] 异常数据支持低延迟写入、重试、死信和补偿。（低延迟单条写入已实现；死信、补偿待阶段三）
- [x] `copyFrom()` 能正确识别 `23505` 并判定为确认成功，不计入失败和死信。（`writer/batcher.go` → `ErrAmbiguousCommit`）
- [x] `totalWritten` 与 `totalAmbiguousResolved` 在日志和报告中分开统计。（`metrics/metrics.go` 独立计数器）
- [x] Go 能记录实际写入速率和错误日志。（`metrics.Tracker` 每 10s 输出 + 结构化错误日志）
- [ ] YMatrix 主表分布策略已在当前环境验证。（`DISTRIBUTED BY (event_id)` 已写入建表 DDL，待连接真实 YMatrix 环境验证）
- [ ] 稳定 `event_id` 和查询去重策略已验证，且已用 `EXPLAIN ANALYZE` 评估去重视图的执行计划。（待阶段一连接 YMatrix 后执行）
- [ ] 最近 5 分钟趋势 SQL 可执行。（SQL 已定义，待 Go 查询 API 实现和数据库验证）
- [ ] 异常点位 SQL 可执行。（同上）
- [ ] 按设备聚合统计 SQL 可执行。（同上）
- [ ] 简单告警 SQL 可执行。（同上）
- [x] OPC UA 断线后能够自动重连并重建订阅。（`opcua/client/collector.py` → `OPCUACollector.connect_with_retry()` 指数退避）
- [ ] YMatrix 中断后正常和异常数据都不会静默丢失。（重试逻辑已实现，死信落盘待阶段三）
- [ ] YMatrix 恢复后死信能够补偿写入。（`InsertOnConflict()` 方法已预留，补偿任务待阶段三）
- [x] SIGTERM 能 flush 最后一个未满批次。（`main.go` 优雅关闭：cancel → close channel → writer drain → flush）
- [ ] React 能实时展示点位、趋势、异常、聚合和告警。（待阶段五实现）
- [ ] 接入报告包含题目要求的五个部分。（待全部阶段完成后输出）
