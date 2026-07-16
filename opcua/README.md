# OPC UA 模拟服务

本目录包含一个完整的 OPC UA 模拟 Server 和正式数据采集器（Collector）。

- **Server**：在 `4840` 端口提供标准 OPC UA Server，模拟多台工业设备的多点位数据。
- **Collector**：通过 Subscription 订阅 Server，将 DataChange 映射为标准 JSON 事件，并通过 HTTP 发送到 Go 后端。

```text
opc.tcp://0.0.0.0:4840/freeopcua/server/
```

模拟服务使用 `asyncua` 创建地址空间，并周期性更新变量值。所有采集均通过 Subscription / MonitoredItem 接收 DataChangeNotification，**不使用轮询读取**。

## 目录结构

```text
opcua/
  config/
    config.yaml                  # Server 与采集器共享的仿真配置
  server/
    __init__.py
    address_space.py             # 构建 OPC UA Address Space
    config_loader.py             # YAML 配置加载与校验
    data_generator.py            # 仿真数据生成器（random_walk / sine / step 等）
    main.py                      # OPC UA Server 入口
  client/
    __init__.py
    subscription_probe.py        # 订阅验证探针（仅用于证明 DataChange 链路可用）
    event_mapper.py              # DataChange → 标准 JSON 事件映射
    sender.py                    # HTTP 发送器（批量、指数退避重试、死信落盘）
    collector.py                 # 正式采集器主程序（订阅 → 队列 → 发送 → 断线重连）
  requirements.txt
  README.md
```

## 地址空间

```text
Objects
  Devices
    Device_01
      temperature
      pressure
      speed
      current
      voltage
      device_clock
    Device_02
      ...
```

每个变量 NodeId 使用稳定字符串，例如：

```text
ns=<namespace_index>;s=Device_01.temperature
```

## 模拟能力

- 4 台设备，可在 `config/config.yaml` 中调整。
- 每台设备 6 个点位：`temperature`、`pressure`、`speed`、`current`、`voltage`、`device_clock`。
- 点位值不是纯随机噪声：
  - `temperature`：随机游走和缓慢趋势
  - `pressure`：正弦波动和噪声
  - `speed`：阶梯变化
  - `current`：与 speed 相关
  - `voltage`：小幅噪声
  - `device_clock`：设备侧业务时钟，支持偏移和漂移
- 支持 Quality 异常（Bad/Uncertain）、数值突变（spike）、设备短暂离线模拟。

---

## 快速开始

### 安装依赖

```powershell
cd .\opcua
pip install -r requirements.txt
```

### 1. 启动 OPC UA 模拟 Server

```powershell
python -m server.main --config config\config.yaml
```

可选参数：

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--config` | 仿真配置文件路径 | `config/config.yaml` |
| `--validate-only` | 仅校验配置，不启动 Server | — |
| `--duration-seconds` | 运行 N 秒后自动退出（0 = 持续运行） | `0` |
| `--log-level` | 日志级别：DEBUG / INFO / WARNING / ERROR | `INFO` |

示例：

```powershell
# 仅校验配置
python -m server.main --config config\config.yaml --validate-only

# 运行 30 秒后自动退出
python -m server.main --config config\config.yaml --duration-seconds 30
```

### 2. 启动正式采集器（Collector）

采集器连接 OPC UA Server → 创建 Subscription → 接收 DataChange → 映射为标准事件 → HTTP 发送到 Go 后端。

```powershell
python -m client.collector --config config\config.yaml --backend-url http://localhost:4048
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

### 3. 订阅验证探针（仅用于链路验证）

探针仅打印 DataChange 通知，不落库、不重试、不发送到后端。用于快速验证 OPC UA 订阅链路是否通畅。

```powershell
# 先启动 Server，再开另一个终端：
python -m client.subscription_probe --config config\config.yaml --duration-seconds 15
```

---

## Collector 架构

```
OPC UA Server (4840)
    │ Subscription / DataChangeNotification
    ▼
SubscriptionHandler
    │ put_nowait (非阻塞，禁止 HTTP/IO)
    ▼
asyncio.Queue (bounded, 默认 10,000)
    │
    ├──▶ backgroud_sender (批量消费)
    │       │ POST /api/v1/ingest/events
    │       ▼
    │    Go Backend (4048)
    │       │ 失败 → 指数退避重试（1s→2s→4s→8s→16s）
    │       │ 重试耗尽 → 死信落盘 (data/dead-letter/dlq-YYYY-MM-DD.ndjson)
    │
    └──▶ StaleDeviceMonitor (心跳检测)
            │ 超时 30s → device_offline 事件
            │ 恢复 → device_online 事件
```

### 事件模型

Collector 将每个 DataChange 映射为以下 JSON 结构（参见 PoC 实施文档 Section 5）：

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

### 断线重连

- Collector 启动时若无法连接 Server，以指数退避自动重试（2s → 4s → 8s → ... → 60s）。
- OPC UA 订阅中断（如 Server 重启）时，Collector 在后台重新建立 Session、Subscription 和 MonitoredItem。
- 设备长时间无心跳（默认 30s），自动生成 `device_offline` 事件；恢复后生成 `device_online`。

### 可靠性与死信

- **正常数据**：HTTP 发送失败 → 指数退避重试 → 重试耗尽 → 追加写入 `data/dead-letter/dlq-YYYY-MM-DD.ndjson`
- **设备事件**（offline/online）：同正常数据路径
- **队列满载**：队列满时丢弃新事件并记录 WARNING 日志（Go 后端不可用时会经过重试和死信，不会持续积压）
- **优雅关闭**：SIGINT/Ctrl+C 触发排空队列 → 剩余事件尝试发送（超时则写入死信） → 断开 OPC UA 连接

### 死信文件格式（NDJSON）

每行一条 JSON，保留原始 `event_id` 并附加失败原因：

```json
{"event_id":"...","device_id":"Device_01",...,"dlq_reason":"connection error: ...","dlq_written_at":"..."}
```

---

## 注意事项

- `subscription_probe.py` 仅用于验证 DataChange 链路，不作为正式数据采集进程。
- 正式采集必须使用 `collector.py`，它包含完整的队列管理、重试、死信和优雅关闭机制。
- DataChange 回调中**禁止执行 HTTP 请求、数据库操作或阻塞式日志写入**，避免阻塞 OPC UA 通知线程。
- Collector 不监听业务端口，主动连接 OPC UA Server 和 Go 后端。
- 所有时间统一使用带时区的 UTC 时间。
- 采集端死信目录建议定期监控，防止磁盘写满。
