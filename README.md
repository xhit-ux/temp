# OPC UA 数据实时接入 YMatrix PoC

## 1. 项目目标

实现一套从 OPC UA 模拟设备到 YMatrix 的实时数据接入 PoC，验证以下核心链路：

```
OPC UA 设备 → Subscription 实时采集 → 事件映射 → HTTP 传输
→ Go 后端校验分流 → 批量/立即写入 YMatrix → 查询分析 → 实时看板
```

**覆盖能力**：批量写入、失败重试、死信补偿、断线重连、写入速率监控、异常/告警实时推送。

**暂未覆盖**：数据库极限压测、生产级安全、大规模设备仿真。

OPC UA相关组件文档：[README.md](opcua\README.md)

---

## 2. 环境依赖

> 本文档默认您已经安装好相关基础环境

### 2.1 系统环境（需提前手动安装）

| 组件 | 版本要求 | 用途 | 安装方式 |
|---|---|---|---|
| Python | 3.10+ | OPC UA Server 和 Collector | 系统包管理器或 [python.org](https://python.org) |
| Go | 1.21+ | 后端写入/查询/SSE 服务 | 系统包管理器或 [go.dev](https://go.dev/dl) |
| YMatrix | 最新版即可 | 时序数据存储 | [YMatrix 官方文档](https://ymatrix.cn/zh/doc/)  |

### 2.2 Python 依赖（pip install 自动安装）

| 组件 | 版本要求 | 用途 |
|---|---|---|
| `asyncua` | >=1.1.5, <2.0.0 | OPC UA 协议库（Server + Client） |
| `aiohttp` | >=3.9.0, <4.0.0 | Python HTTP 客户端（Collector → Go Backend） |
| `PyYAML` | >=6.0.1, <7.0.0 | YAML 配置文件解析 |

### 2.3 Go 依赖（go mod download 自动安装）

| 组件 | 版本要求 | 用途 |
|---|---|---|
| `pgx/v5` | v5.5.5 | Go PostgreSQL 驱动 · CopyFrom 批量写入 |
| `godotenv` | v1.5.1 | .env 环境变量加载 |
| `google/uuid` | v1.6.0 | UUID 生成（event_id / batch_id） |

**端口规划（默认端口）**：

| 端口 | 组件 | 说明 |
|---|---|---|
| `4840` | Python OPC UA Server | `opc.tcp://0.0.0.0:4840/freeopcua/server/` |
| `4048` | Go Backend | HTTP API + SSE 推送 |
| `5432` | YMatrix | PostgreSQL 协议 |

---

## 3. 运行步骤

### 3.1 克隆项目

```bash
git clone https://github.com/xhit-ux/temp.git
cd temp
```

### 3.2 安装 Python 依赖

```bash
cd opcua
pip install -r requirements.txt
```

### 3.3 安装 Go 依赖

```bash
cd src/backend
go mod download
```

### 3.4 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填写数据库连接信息
```

### 3.5 初始化数据库（可选/可跳过）

Go 后端首次启动时会自动建库、建表、建索引。如需手动初始化：

```bash
psql -h <host> -U <user> -d <database> -f src/backend/migrations/001_init.sql
```

---

## 4. 配置说明

### 4.1 OPC UA 模拟配置

文件：`opcua/config/config.yaml`

```yaml
server:
  endpoint: "opc.tcp://0.0.0.0:4840/freeopcua/server/"
  update_interval_seconds: 1.0     # 点位更新周期（秒）

simulation:
  quality:
    bad_probability: 0.01          # Bad quality 注入概率
    uncertain_probability: 0.02    # Uncertain quality 注入概率
  spike:
    enabled: true
    probability: 0.01              # 数值突变概率
    multiplier_min: 1.15
    multiplier_max: 1.45
  offline:
    enabled: true
    probability_per_cycle: 0.002   # 设备离线概率（每周期）
    duration_seconds: 8            # 离线持续时间

devices:                           # 3-5 台设备
  - id: "Device_01"
    display_name: "Mixing Tank 01"
    clock_offset_seconds: 0        # 设备时钟偏移
    drift_seconds_per_minute: 0.0  # 时钟漂移速率
  - id: "Device_02"
    display_name: "Pump Station 02"
    clock_offset_seconds: 15
    drift_seconds_per_minute: 0.1
  # ... 更多设备

points:                            # 每台设备的点位定义
  temperature:
    data_type: "Double"
    unit: "C"
    mode: "random_walk"            # 随机游走 + 趋势
    initial: 45.0
    min: 20.0
    max: 90.0
  pressure:
    mode: "sine"                   # 正弦波 + 噪声
  speed:
    mode: "step"                   # 阶梯变化（模拟启停）
  current:
    mode: "correlated_current"     # 与 speed 关联
  voltage:
    mode: "noise"                  # 小幅噪声
  device_clock:
    data_type: "DateTime"
    mode: "device_clock"           # 设备业务时钟
```

### 4.2 项目环境参数配置

文件：`.env`（从 `.env.example` 复制）

```bash
# YMatrix / PostgreSQL 连接
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=mxadmin
DB_PASSWORD=your_password_here
DB_DATABASE=opc

# HTTP 服务
SERVER_HOST=localhost
SERVER_PORT=4048

# 批量写入
BATCH_SIZE=500          # 触发 flush 的事件数阈值
FLUSH_SECONDS=5         # 触发 flush 的最大等待秒数

# 内存队列
QUEUE_CAPACITY=10000                # normal channel 容量
PRIORITY_QUEUE_CAPACITY=2000        # priority channel 容量

# 重试
MAX_RETRIES=5                       # 最大重试次数
RETRY_BASE_DELAY_SECONDS=1          # 重试基础延迟（指数退避：1s→2s→4s→8s→16s）

# 死信
DEAD_LETTER_DIR=data/dead-letter
DEAD_LETTER_REPLAY_INTERVAL_SECONDS=30   # 补偿扫描间隔
DEAD_LETTER_REPLAY_BATCH_SIZE=100        # 每批补偿事件数

# 写入连接池
WRITER_POOL_SIZE=2                  # normal writer 并发数
```

---

## 5. 运行方式

### 5.1 启动 OPC UA 模拟 Server

```bash
cd opcua
python -m server.main --config config/config.yaml
```

可选参数：

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--config` | 配置文件路径 | `config/config.yaml` |
| `--validate-only` | 仅校验配置，不启动 | — |
| `--duration-seconds` | 运行 N 秒后自动退出（0=持续） | `0` |
| `--log-level` | DEBUG/INFO/WARNING/ERROR | `INFO` |

### 5.2 启动 Go 后端

```bash
cd src/backend
go run main.go --env ../../.env
```

首次启动自动执行：
- 检测目标数据库是否存在，不存在则自动创建
- 创建 `opc_point_data`、`opc_alarm_event` 和 `opc_operation_log` 表
- 创建唯一索引和复合索引
- 检测 Greenplum/YMatrix 并自动添加 `DISTRIBUTED BY`

### 5.3 启动 OPC UA Collector

```bash
cd opcua
python -m client.collector --config config/config.yaml --backend-url http://localhost:4048
```

可选参数：

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--backend-url` | Go 后端地址 | `http://localhost:4048` |
| `--queue-size` | 内部事件队列长度 | `10000` |
| `--batch-size` | 每次 HTTP 批量发送数 | `50` |
| `--send-interval-seconds` | 发送最大等待间隔 | `0.5` |
| `--publishing-interval-ms` | OPC UA 订阅发布间隔 | `500` |
| `--device-stale-timeout` | 设备心跳超时阈值（秒） | `30` |
| `--dead-letter-dir` | 死信存储目录 | `data/dead-letter` |

### 5.4 打开实时看板

浏览器打开 `src/frontend/index.html`，自动连接 Go 后端 SSE 推送。

### 5.5 订阅验证探针（可选）

轻量工具，仅打印 DataChange 通知，不写库不发送，用于验证 OPC UA 订阅链路：

```bash
cd opcua
python -m client.subscription_probe --config config/config.yaml --duration-seconds 15
```

---

## 6. 示例输出

### 6.1 OPC UA Server 启动日志

```
2026-07-16 10:00:00 INFO [server.main] Loaded config: endpoint=opc.tcp://0.0.0.0:4840/... devices=4 points=6
2026-07-16 10:00:00 INFO [server.main] Starting OPC UA server at opc.tcp://0.0.0.0:4840/freeopcua/server/
2026-07-16 10:00:01 INFO [server.data_generator] Started 4 device simulation tasks.
```

### 6.2 Go 后端启动日志

```
[Main] Starting OPC2YMatrix Go Backend on port 4048
[Main] YMatrix target: localhost:5432/opc
[DB] Connected to YMatrix/PostgreSQL database "opc"
[DB] Table opc_point_data ready
[DB] Table opc_alarm_event ready
[DB] Table opc_operation_log ready
[DB] Bootstrap complete (tables + indexes)
[Main] Channels: normal=10000 priority=2000
[Main] Writer pool: 2 normal + 1 priority
[Main] Dead-letter replayer started (interval=30s, batch_size=100)
[Logger] Started (min_level=INFO, buffer=2000, retention=24h0m0s)
[Logger] OPC2YMatrix Go Backend started
[Main] HTTP server listening on :4048
```

### 6.3 Collector 运行日志

```
2026-07-16 10:00:05 INFO [client.collector] Connected to OPC UA server at opc.tcp://127.0.0.1:4840/...
2026-07-16 10:00:05 INFO [client.collector] Built node map with 24 entries
2026-07-16 10:00:05 INFO [client.collector] Created subscription: 24 nodes, publishing_interval=500ms
2026-07-16 10:00:05 INFO [client.collector] Collector running: endpoint=... devices=4 points=6 queue_size=10000
2026-07-16 10:00:06 INFO [client.sender] Sent 24 events successfully
```

### 6.4 Go 后端 Metrics（每 10 秒输出）

```
[Metrics] received=240 written=238 failed=0 dropped=0 ambig=0 dlq=0 dlq_rpl=0 queue=2 rate=23.8/s batches(ok=2 fail=0 active=0) avgLat=12ms maxLat=45ms
```

### 6.5 写入性能 API 响应

```json
{
  "total_received": 14400,
  "total_written": 14380,
  "total_failed": 0,
  "total_dropped": 0,
  "total_ambiguous_resolved": 0,
  "total_dead_letter": 0,
  "total_dead_replayed": 0,
  "current_in_queue": 2,
  "batches_written": 120,
  "batches_failed": 0,
  "batches_active": 0,
  "rate_per_second": 23.8,
  "avg_write_latency": "12ms",
  "max_write_latency": "45ms"
}
```

### 6.6 趋势查询 API 响应

```json
{
  "points": [
    {
      "source_timestamp": "2026-07-16T10:05:00Z",
      "device_id": "Device_01",
      "point_name": "temperature",
      "value_number": 46.37,
      "quality_name": "Good"
    }
  ],
  "latency_ms": 3
}
```

### 6.7 标准事件模型

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440001",
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

---

## 7. 已知限制

### 7.1 PoC 环境约束

- **单节点部署**：当前 PoC 环境为 YMatrix 单节点部署，所有数据落在同一个 Segment 上。`DISTRIBUTED BY (event_id)` 的设计是面向未来多 Segment 扩容时的兼容性选择——`event_id` 高基数能保证数据均匀打散，且与唯一索引同列可满足 Greenplum "唯一约束必须包含分布键"的限制。但该设计**尚未在多 Segment 环境下验证过实际分布效果**，生产扩容时需根据真实设备基数和查询模式重新评估分布策略。

- **4 台设备 × 6 点位 × 1s 更新 = 24 events/s**：远低于 YMatrix 写入极限，写入性能数据仅代表当前 PoC 负载表现，不代表数据库最大吞吐能力。

- **无历史补采**：OPC UA Server 不启用 Historizing，Collector 断线期间的数据无法补采。

### 7.2 可靠性边界

- **内存队列非持久化**：Python `asyncio.Queue` 和 Go `channel` 均为进程内存，强制 kill 或断电会丢失队列中未写入的数据。生产环境建议引入 Kafka 等持久化消息队列。

- **优雅关闭不覆盖崩溃**：SIGTERM/SIGINT 可触发排空和 flush，但 SIGKILL、断电、OOM Kill 等场景无法保证。

- **HTTP 传输无背压传播**：Python Collector 发送 HTTP 后不感知 Go 后端的 channel 深度，Go 返回 429 时 Python 重试但不主动降速。

### 7.3 告警通知

- **告警生命周期管理已实现**：PriorityWriter 在写入异常事件时通过事务联动同步写入 `opc_alarm_event` 表，SSE Broker 实时推送 `alarm` 事件，前端弹窗 + "🔔 告警" 标签页展示完整告警列表。

- **外部通知渠道待接入**：当前告警通知仅限前端看板，生产环境应接入飞书/企业微信 Webhook 机器人实现即时通讯触达。

- **去重视图未启用**：`opc_point_data_dedup` 视图已预留定义，当前直接查询主表。数据量增长后需 `EXPLAIN ANALYZE` 评估是否启用。

- **SQL 控制台无安全保护**：`/api/v1/admin/sql` 可执行任意 SQL，仅限 PoC 调试使用，生产环境必须移除或加认证。

### 7.4 时序精度

- 同一设备的 6 个点位各自独立生成 `SourceTimestamp`，存在微秒级差异，不适合精确时间对齐分析。

- `device_clock` 模拟的时钟偏移和漂移仅用于演示数据质量风险，不参与核心写入逻辑。

### 7.5 死信与补偿

- 死信文件无容量上限和自动清理，长时间运行需人工监控磁盘。

- 补偿重放使用逐行 `INSERT ON CONFLICT DO NOTHING`，高死信量下性能不佳（已预留 `unnest` 批量优化 TODO）。

---

## 8. 项目结构

```
temp/
├── .env.example                    # 环境变量模板
├── .gitignore
├── README.md
│
├── opcua/                          # Python OPC UA 模块
│   ├── config/
│   │   └── config.yaml             # 模拟配置（设备/点位/异常注入）
│   ├── server/
│   │   ├── __init__.py
│   │   ├── address_space.py        # OPC UA 地址空间构建
│   │   ├── config_loader.py        # YAML 配置加载与校验
│   │   ├── data_generator.py       # 仿真数据生成器
│   │   └── main.py                 # Server 入口
│   ├── client/
│   │   ├── __init__.py
│   │   ├── collector.py            # 正式采集器（订阅→队列→发送→重连）
│   │   ├── event_mapper.py         # DataChange → 标准 JSON 事件
│   │   ├── sender.py               # HTTP 发送器（批量/重试/死信）
│   │   └── subscription_probe.py   # 订阅验证探针
│   ├── requirements.txt
│   └── README.md
│
├── src/
│   ├── backend/                    # Go 后端
│   │   ├── main.go                 # 入口（启动/路由/优雅关闭）
│   │   ├── go.mod / go.sum
│   │   ├── config/config.go        # .env 配置加载
│   │   ├── model/event.go          # IngestEvent 模型与校验
│   │   ├── handler/ingest.go       # HTTP 接收器（双通道路由）
│   │   ├── writer/batcher.go       # 批量写入器（CopyFrom + 重试 + 死信）
│   │   ├── logger/logger.go         # 运维日志模块（DB优先/文件降级）
│   │   ├── store/db.go             # 连接池/自动建库建表
│   │   ├── deadletter/writer.go    # 死信 NDJSON 写入
│   │   ├── deadletter/replayer.go  # 死信补偿重放
│   │   ├── stream/broker.go        # SSE 实时推送
│   │   ├── metrics/metrics.go      # 写入速率/延迟指标
│   │   ├── query/handler.go        # 查询 API + SQL 控制台
│   │   └── migrations/001_init.sql # 手动建表脚本
│   └── frontend/
│       └── index.html              # React 实时看板（单文件 SPA）
│
└── docs_dev/                       # 开发文档
```

---

## 9. 查询分析 SQL

### 9.1 最近 5 分钟趋势

```sql
SELECT source_timestamp, device_id, point_name, value_number, quality_name
FROM opc_point_data
WHERE device_id = 'Device_01'
  AND point_name = 'temperature'
  AND source_timestamp >= now() - interval '5 minutes'
ORDER BY source_timestamp;
```

### 9.2 异常点位查询

```sql
SELECT event_id, device_id, point_name, value_number, quality_name,
       abnormal_reason, source_timestamp
FROM opc_point_data
WHERE is_abnormal = true
  AND source_timestamp >= now() - interval '24 hours'
ORDER BY source_timestamp DESC
LIMIT 100;
```

### 9.3 按设备聚合统计

```sql
SELECT device_id,
       count(*) AS sample_count,
       count(*) FILTER (WHERE is_abnormal) AS abnormal_count,
       min(source_timestamp) AS first_sample_time,
       max(source_timestamp) AS last_sample_time
FROM opc_point_data
WHERE source_timestamp >= now() - interval '1 hour'
GROUP BY device_id
ORDER BY device_id;
```

### 9.4 简单告警查询

```sql
SELECT alarm_id, device_id, point_name, severity, alarm_type,
       message, status, occurred_at, recovered_at
FROM opc_alarm_event
WHERE status = 'active'
ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
         occurred_at DESC;
```

---

## 10. API 参考

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/v1/ingest/events` | Python 提交事件（支持批量） |
| `GET` | `/api/v1/trends?device_id=&point_name=` | 最近 5 分钟趋势 |
| `GET` | `/api/v1/abnormal-points?limit=` | 异常点位列表 |
| `GET` | `/api/v1/device-statistics?from=&to=` | 设备聚合统计 |
| `GET` | `/api/v1/alarms` | 活跃告警列表 |
| `GET` | `/api/v1/stream` | SSE 实时事件推送 |
| `GET` | `/api/v1/metrics` | 写入速率与指标快照 |
| `POST` | `/api/v1/admin/flush` | 手动触发 flush |
| `POST` | `/api/v1/admin/dead-letter/replay` | 手动触发死信补偿 |
| `GET` | `/api/v1/admin/dead-letter/stats` | 死信文件统计 |
| `GET` | `/api/v1/logs/query?level=&keyword=&module=&from=&to=&limit=&offset=` | 运维日志查询 |
| `GET` | `/api/v1/logs/stats` | 运维日志统计（按级别计数） |
| `POST` | `/api/v1/admin/sql` | SQL 控制台（仅调试） |
| `GET` | `/api/v1/health/live` | 存活检查 |
| `GET` | `/api/v1/health/ready` | 就绪检查（含数据库 ping） |

---

## 11. 数据模型

### 11.1 测点表 `opc_point_data`

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | uuid (PK) | 事件唯一标识，重试沿用原值 |
| `device_id` | text | 设备标识 |
| `point_name` | text | 点位名称 |
| `data_type` | text | 数据类型（Double/DateTime） |
| `unit` | text | 工程单位 |
| `value_number` | double precision | 数值型值 |
| `value_text` | text | 文本型值 |
| `value_time` | timestamptz | 时间型值 |
| `quality_code` | bigint | OPC UA StatusCode |
| `quality_name` | text | 质量名称（Good/Bad/Uncertain） |
| `event_type` | text | 事件类型（data_change/quality_alarm/device_offline/device_online） |
| `is_abnormal` | boolean | 是否异常 |
| `abnormal_reason` | text | 异常原因 |
| `source_timestamp` | timestamptz | OPC UA SourceTimestamp（≈event_time） |
| `server_timestamp` | timestamptz | OPC UA ServerTimestamp |
| `collector_timestamp` | timestamptz | Collector 采集时间（≈ingest_time） |
| `received_at` | timestamptz | Go 后端接收时间（DEFAULT now()） |
| `batch_id` | uuid | 写入批次标识 |

**索引**：
- `ux_opc_point_event`：`event_id` 唯一索引
- `idx_opc_point_device_time`：`(device_id, point_name, source_timestamp)` 复合索引
- `idx_opc_point_abnormal_time`：`(is_abnormal, source_timestamp)` 复合索引

### 11.2 告警表 `opc_alarm_event`

| 字段 | 类型 | 说明 |
|---|---|---|
| `alarm_id` | uuid | 告警唯一标识 |
| `event_id` | uuid | 关联事件 |
| `device_id` | text | 设备标识 |
| `point_name` | text | 点位名称 |
| `severity` | text | 严重级别（critical/warning/info） |
| `alarm_type` | text | 告警类型 |
| `message` | text | 告警描述 |
| `status` | text | 状态（active/recovered/acknowledged） |
| `occurred_at` | timestamptz | 发生时间 |
| `recovered_at` | timestamptz | 恢复时间 |
| `acknowledged_at` | timestamptz | 确认时间 |
| `acknowledged_by` | text | 确认人 |
| `created_at` | timestamptz | 记录创建时间 |

### 11.3 运维日志表 `opc_operation_log`

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | varchar(36) | 日志记录唯一标识 (UUID) |
| `timestamp` | timestamptz | 日志产生时间 (UTC) |
| `level` | text | 日志级别 (DEBUG/INFO/WARN/ERROR) |
| `module` | text | 产生日志的模块名称 |
| `message` | text | 日志消息正文 |
| `extra` | text | 附加上下文信息 (可选) |

**索引**：
- `idx_ops_log_ts`：`timestamp DESC` 时间排序索引
- `idx_ops_log_level`：`(level, timestamp DESC)` 按级别+时间的复合索引

日志系统采用 **DB 优先落库 + 文件降级** 策略。DB 正常时写入 `opc_operation_log` 表，不可用时自动降级到本地 NDJSON 文件 (`data/logs/`)，24 小时自动清理。前端看板提供 "📋 运维日志" 标签页，支持按级别筛选、关键词搜索、分页。

### 11.4 分布键说明

```sql
DISTRIBUTED BY (event_id)  -- Greenplum/YMatrix 专用
```

选择 `event_id` 作为分布键的原因：

1. **高基数**：UUID 保证数据均匀打散到各 Segment
2. **与唯一索引同列**：满足 Greenplum "唯一约束必须包含分布键"的限制
3. **重试幂等**：同一 `event_id` 始终路由到同一 Segment

> ⚠️ **当前 PoC 为单节点部署**，所有数据落在同一个 Segment 上，分布键选择不影响实际性能。该设计是面向未来多 Segment 扩容时的兼容性选择，尚未在多 Segment 环境下验证过分布效果。生产扩容时需根据真实设备基数和查询模式重新评估。
