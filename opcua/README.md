# OPC UA 模拟服务与采集器

本目录包含一个完整的 OPC UA 模拟 Server 和正式数据采集器（Collector）。

- **Server**：在 `4840` 端口提供标准 OPC UA Server，模拟 4 台工业设备的 24 个点位数据。
- **Collector**：通过 Subscription 订阅 Server，将 DataChange 映射为标准 JSON 事件，并通过 HTTP 批量发送到 Go 后端。

```text
opc.tcp://0.0.0.0:4840/freeopcua/server/
```

所有采集均通过 Subscription / MonitoredItem 接收 DataChangeNotification，**不使用轮询读取**。

---

## 目录结构

```text
opcua/
├── config/
│   └── config.yaml              # Server 与采集器共享的仿真配置
├── server/
│   ├── __init__.py
│   ├── address_space.py         # 构建 OPC UA 地址空间（设备/点位节点）
│   ├── config_loader.py         # YAML 配置加载与校验（6 个必需点位）
│   ├── data_generator.py        # 仿真数据生成器（random_walk/sine/step/correlated/noise）
│   └── main.py                  # OPC UA Server 入口
├── client/
│   ├── __init__.py
│   ├── subscription_probe.py    # 订阅验证探针（仅打印 DataChange，不做持久化）
│   ├── event_mapper.py          # DataChange → 标准 JSON 事件映射
│   ├── sender.py                # HTTP 发送器（批量发送、指数退避重试、死信落盘）
│   └── collector.py             # 正式采集器主程序（订阅 → 队列 → 发送 → 断线重连）
├── requirements.txt
└── README.md
```

---

## 地址空间

```text
Objects
└── Devices (Folder)
    ├── Device_01 (Object)
    │   ├── temperature   (Variable, Double)
    │   ├── pressure      (Variable, Double)
    │   ├── speed         (Variable, Double)
    │   ├── current       (Variable, Double)
    │   ├── voltage       (Variable, Double)
    │   └── device_clock  (Variable, DateTime)
    ├── Device_02
    │   └── ...（同结构）
    ├── Device_03
    └── Device_04
```

NodeId 格式：`ns=<namespace_index>;s=<device_id>.<point_name>`，例如：

```text
ns=2;s=Device_01.temperature
```

---

## 模拟能力

### 设备与点位

- 4 台设备（Device_01 ~ Device_04），可在 `config/config.yaml` 中增减。
- 每台设备 6 个点位，数据类型和变化模式如下：

| 点位 | 类型 | 单位 | 变化模式 | 说明 |
|---|---|---|---|---|
| `temperature` | Double | C | `random_walk` | 随机游走 + 缓慢趋势，20~90 ℃ |
| `pressure` | Double | MPa | `sine` | 正弦波 + 噪声，0.1~2.0 MPa，周期 60s |
| `speed` | Double | rpm | `step` | 阶梯变化（模拟启停），0/800/1500/2200/2800 rpm |
| `current` | Double | A | `correlated_current` | 与 speed 线性相关：`3 + speed × 0.014 + noise` |
| `voltage` | Double | V | `noise` | 小幅噪声波动，400 ± 2.5 V |
| `device_clock` | DateTime | — | `device_clock` | 设备自身业务时钟，支持固定偏移和渐进漂移 |

### 设备时钟配置

每台设备可独立配置时钟偏移和漂移速率，用于模拟工业现场的时钟不同步问题：

| 设备 | 时钟偏移 | 漂移速率 |
|---|---|---|
| Device_01 | 0s | 0.0 s/min |
| Device_02 | +15s | +0.1 s/min |
| Device_03 | -20s | -0.05 s/min |
| Device_04 | +30s | 0.0 s/min |

### 异常注入

所有异常注入均可通过 `config/config.yaml` 的 `simulation` 段配置开关和概率：

| 异常类型 | 配置项 | 默认概率 | 效果 |
|---|---|---|---|
| Quality Bad | `simulation.quality.bad_probability` | 1% | `quality_code` 设为 Bad |
| Quality Uncertain | `simulation.quality.uncertain_probability` | 2% | `quality_code` 设为 Uncertain |
| 数值突变 | `simulation.spike.probability` | 1% | 值乘以 1.15~1.45 的随机系数 |
| 设备离线 | `simulation.offline.probability_per_cycle` | 0.2% | 暂停数据更新 8 秒 |

---

## 快速开始

### 安装依赖

```bash
cd opcua
pip install -r requirements.txt
```

依赖清单：
- `asyncua >=1.1.5, <2.0.0` — OPC UA 协议库
- `PyYAML >=6.0.1, <7.0.0` — 配置加载
- `aiohttp >=3.9.0, <4.0.0` — HTTP 客户端

### 1. 启动 OPC UA 模拟 Server

```bash
python -m server.main --config config/config.yaml
```

可选参数：

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--config` | 仿真配置文件路径 | `config/config.yaml` |
| `--validate-only` | 仅校验配置，不启动 Server | — |
| `--duration-seconds` | 运行 N 秒后自动退出（0 = 持续运行） | `0` |
| `--log-level` | 日志级别：DEBUG / INFO / WARNING / ERROR | `INFO` |

```bash
# 仅校验配置
python -m server.main --config config/config.yaml --validate-only

# 运行 30 秒后自动退出
python -m server.main --config config/config.yaml --duration-seconds 30
```

### 2. 启动正式采集器（Collector）

```bash
python -m client.collector --config config/config.yaml --backend-url http://localhost:4048
```

可选参数：

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--config` | 仿真配置文件路径 | `config/config.yaml` |
| `--backend-url` | Go 后端地址 | `http://127.0.0.1:4048` |
| `--queue-size` | 内部事件队列最大长度 | `10000` |
| `--batch-size` | 每次 HTTP 批量发送的最大事件数 | `50` |
| `--send-interval-seconds` | 发送批次的最大等待间隔（秒） | `0.5` |
| `--publishing-interval-ms` | OPC UA 订阅发布间隔（毫秒） | `500` |
| `--device-stale-timeout` | 设备心跳超时阈值（秒），超时后生成离线事件 | `30` |
| `--dead-letter-dir` | 死信 NDJSON 文件存储目录 | `data/dead-letter` |
| `--log-level` | 日志级别 | `INFO` |

### 3. 订阅验证探针（可选）

轻量验证工具，仅打印 DataChange 通知，不落库、不重试、不发送到后端。用于快速验证 OPC UA 订阅链路是否通畅。

```bash
python -m client.subscription_probe --config config/config.yaml --duration-seconds 15
```

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--config` | 配置文件路径 | `config/config.yaml` |
| `--endpoint` | 覆盖 OPC UA 端点地址 | 从配置读取 |
| `--duration-seconds` | 探针运行时长（秒） | `10` |
| `--publishing-interval-ms` | 订阅发布间隔（毫秒） | `500` |
| `--max-nodes` | 最大订阅节点数（0=全部） | `6` |

---

## Collector 架构

```
OPC UA Server (4840)
    │ Subscription / DataChangeNotification
    ▼
SubscriptionHandler.datachange_notification()
    │ call_soon_threadsafe → queue.put_nowait (非阻塞)
    ▼
asyncio.Queue (bounded, 默认 10,000)
    │
    ├──▶ background_sender (批量消费)
    │       │ 攒批：50 条 或 0.5s 超时
    │       │ POST /api/v1/ingest/events
    │       ▼
    │    Go Backend (4048)
    │       │ 失败 → 指数退避重试（1s→2s→4s→8s→16s，±25% 抖动）
    │       │ 重试耗尽 → 死信落盘 (data/dead-letter/dlq-YYYY-MM-DD.ndjson)
    │       ▼
    │    优雅关闭：排空队列 → 剩余事件尝试发送（超时则写死信）
    │
    └──▶ StaleDeviceMonitor (心跳检测，独立 asyncio.Task)
            │ 每 10s 检查一次
            │ 超时 30s → device_offline 事件入队
            │ 恢复 → device_online 事件入队
```

### 核心组件

| 组件 | 职责 |
|---|---|
| `OPCUACollector` | 管理 OPC UA 连接生命周期：连接、订阅、断线重连、优雅断开 |
| `SubscriptionHandler` | 接收 DataChange 通知，通过 `call_soon_threadsafe` 安全入队，预建 NodeId→设备/点位映射避免热路径查找 |
| `StaleDeviceMonitor` | 后台 Task，周期检测设备心跳，超时生成 `device_offline`，恢复生成 `device_online` |
| `background_sender` | 批量消费队列，攒批（50 条/0.5s）POST 到 Go 后端，支持优雅关闭排空 |
| `EventSender` | HTTP 发送器：批量 POST、指数退避重试、429/503 背压感知、死信落盘 |
| `BackoffStrategy` | 指数退避策略：`base_delay × 2^(attempt-1)`，上限 30s，±25% 随机抖动 |
| `DeadLetterWriter` | 死信 NDJSON 写入，按日滚动文件，asyncio.Lock 保护并发写入 |
| `EventMapper` | DataChange → 标准 JSON 事件映射，UUID1 生成 event_id |

### 事件模型

每个 DataChange 映射为以下 JSON 结构：

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

字段约束：
- `event_id`：UUID1 格式，首次采集时生成，**重试沿用原值**（幂等前提）
- `value_number` / `value_text` / `value_time`：三选一，由 `data_type` 决定
- `quality_code`：OPC UA 原始 StatusCode（0=Good，非零=异常）
- `event_type`：`data_change`（正常）/ `quality_alarm`（Quality 异常）/ `device_offline` / `device_online`

### 断线重连

| 场景 | 策策 |
|---|---|
| 启动时 Server 不可用 | 指数退避重试（2s → 4s → 8s → ... → 60s），直到连接成功或收到停止信号 |
| 运行中订阅中断 | `OPCUACollector` 检测断开，重新建立 Session → 获取 Namespace Index → 重建 Subscription + MonitoredItem |
| 设备心跳超时 | `StaleDeviceMonitor` 检测 30s 无数据 → 生成 `device_offline` 事件入队；恢复后生成 `device_online` |
| Go 后端不可用 | `EventSender` 指数退避重试（1s→2s→4s→8s→16s），重试耗尽写死信 |

### 可靠性与死信

**正常数据路径**：

```
HTTP 发送失败
  → 指数退避重试（5 次，1s→2s→4s→8s→16s，±25% 抖动）
  → 重试耗尽 → 追加写入 data/dead-letter/dlq-YYYY-MM-DD.ndjson
```

**设备事件**（offline/online）：同正常数据路径，复用相同的重试和死信机制。

**队列满载**：`asyncio.Queue` 满时 `put_nowait` 抛 `QueueFull`，事件被丢弃并记录 WARNING 日志。这是有界队列的背压行为——Go 后端长时间不可用时，事件经过重试和死信后不会在内存中无限积压。

**优雅关闭**（SIGINT / Ctrl+C）：

```
1. 设置 stop_event
2. StaleDeviceMonitor 停止
3. OPCUAExecutor 断开 OPC UA 连接
4. background_sender 排空队列
   → 剩余事件尝试发送（超时 10s 则写入死信）
5. 输出统计：总通知数
```

### 死信文件格式

按日滚动的 NDJSON 文件，每行一条 JSON：

```json
{
  "event_id": "550e8400-...",
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
  "event_type": "data_change",
  "dlq_reason": "connection error: ...",
  "dlq_retry_count": 5,
  "dlq_first_failure_at": "2026-07-16T10:00:05.000Z",
  "dlq_written_at": "2026-07-16T10:00:21.000Z"
}
```

字段说明：
- `dlq_reason`：最后一次重试的错误信息
- `dlq_retry_count`：已重试次数
- `dlq_first_failure_at`：首次失败时间
- `dlq_written_at`：写入死信时间

---

## 配置说明

文件：`config/config.yaml`

```yaml
server:
  endpoint: "opc.tcp://0.0.0.0:4840/freeopcua/server/"
  server_name: "OPC2YMatrix Simulator"
  namespace_uri: "urn:opc2ymatrix:simulator"
  update_interval_seconds: 1.0       # 点位更新周期（秒）

simulation:
  random_seed: 20260715              # 可复现的随机种子
  quality:
    bad_probability: 0.01            # Bad quality 概率
    uncertain_probability: 0.02      # Uncertain quality 概率
  spike:
    enabled: true
    probability: 0.01                # 数值突变概率
    multiplier_min: 1.15             # 突变最小倍数
    multiplier_max: 1.45             # 突变最大倍数
  offline:
    enabled: true
    probability_per_cycle: 0.002     # 每周期离线概率
    duration_seconds: 8              # 离线持续时间

devices:                             # 设备列表（3~5 台）
  - id: "Device_01"
    display_name: "Mixing Tank 01"
    clock_offset_seconds: 0          # 时钟固定偏移（秒）
    drift_seconds_per_minute: 0.0    # 时钟漂移速率（秒/分钟）
  # ... 更多设备

points:                              # 点位定义（6 个必需）
  temperature:
    data_type: "Double"
    unit: "C"
    mode: "random_walk"
    initial: 45.0
    min: 20.0
    max: 90.0
    step: 1.5                        # 随机游走步长
    trend: 0.03                      # 每周期趋势增量
  pressure:
    mode: "sine"
    amplitude: 0.35
    period_seconds: 60
    noise: 0.03
  speed:
    mode: "step"
    step_values: [0.0, 800.0, 1500.0, 2200.0, 2800.0]
    min_hold_seconds: 8
    max_hold_seconds: 25
  current:
    mode: "correlated_current"
    base: 3.0
    speed_factor: 0.014
    noise: 1.2
  voltage:
    mode: "noise"
    initial: 400.0
    noise: 2.5
  device_clock:
    data_type: "DateTime"
    mode: "device_clock"
```

配置校验规则（`config_loader.py`）：
- 必须包含 6 个点位：`temperature`、`pressure`、`speed`、`current`、`voltage`、`device_clock`
- `data_type` 仅支持 `Double` 和 `DateTime`
- `devices` 必须是非空列表

---

## 注意事项

- `subscription_probe.py` 仅用于验证 DataChange 链路，不作为正式数据采集进程。正式采集必须使用 `collector.py`。
- DataChange 回调（`SubscriptionHandler.datachange_notification`）中**禁止执行 HTTP 请求、数据库操作或阻塞式日志写入**，仅做轻量数据提取和队列入队。所有 IO 由 `background_sender` 在独立协程中完成。
- Collector 不监听业务端口，主动连接 OPC UA Server（4840）和 Go 后端（4048）。
- 所有时间统一使用带时区的 UTC 时间。
- 采集端死信目录（`data/dead-letter/`）建议定期监控，防止磁盘写满。
- `sender.py` 中 `EventSender` 每次发送会创建新的 `aiohttp.ClientSession`，高吞吐场景下建议改为长生命周期 session 以减少 TCP 连接开销。
