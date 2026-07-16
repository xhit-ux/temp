# OPC UA 模拟系统搭建规划（Python 实现）

## 1. 目标

搭建一个**协议层真实、行为可控**的 OPC UA 模拟服务，用于验证"OPC UA 实时接入 YMatrix"PoC 的完整链路：模拟设备产生数据 → 订阅推送 → 采集写入 → 查询分析。

核心原则：模拟服务必须是一个**标准 OPC UA Server**（走 `opc.tcp://` 协议，支持 Subscription/MonitoredItem），而不是自造的伪协议，否则后续断线重连、死区过滤等验证都失去意义。

---

## 2. 技术选型

| 组件 | 选型 | 说明 |
|---|---|---|
| OPC UA 协议库 | `asyncua`（原 `opcua-asyncio`） | Python 生态最成熟的异步 OPC UA 实现，Server/Client 都支持，原生支持 Subscription、DataChangeFilter、安全策略 |
| 运行时 | Python 3.10+ / asyncio | 模拟多设备并发生成数据需要异步调度 |
| 进程管理 | 单进程内多设备 + 定时任务（`asyncio.create_task`） | PoC阶段无需拆分多进程，便于统一控制模拟节奏 |
| 配置管理 | YAML（设备/点位/异常规则配置化） | 避免硬编码，方便调整设备数量、点位类型、异常注入策略 |

---

## 3. 总体架构

```
┌─────────────────────────────────────────────┐
│           OPC UA Server (asyncua)            │
│  opc.tcp://0.0.0.0:4840/freeopcua/server/     │
│                                               │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐   │
│  │ Device_01 │ │ Device_02 │ │ Device_0N │   │
│  │ (Object)  │ │ (Object)  │ │ (Object)  │   │
│  │  ├temperature│  ├temperature│ ...      │   │
│  │  ├pressure   │  ├pressure   │          │   │
│  │  ├speed      │  ├speed      │          │   │
│  │  ├current    │  ├current    │          │   │
│  │  ├voltage    │  ├voltage    │          │   │
│  │  └device_clock│ └device_clock│         │   │
│  └───────────┘ └───────────┘ └───────────┘   │
│                                               │
│  数据生成器（异步任务，周期性更新变量值）        │
└───────────────────┬───────────────────────────┘
                     │ Subscription / DataChangeNotification
                     ▼
        ┌─────────────────────────┐
        │   OPC UA Client 采集端    │  （另一独立进程，本次规划暂不展开）
        └─────────────────────────┘
```

---

## 4. 地址空间设计（Address Space）

采用 **一个自定义 Namespace + 每台设备一个 Object 节点** 的结构：

```
Objects
 └─ Devices (Folder)
     ├─ Device_01 (Object)
     │    ├─ temperature (Variable, Double)
     │    ├─ pressure    (Variable, Double)
     │    ├─ speed       (Variable, Double)
     │    ├─ current     (Variable, Double)
     │    ├─ voltage     (Variable, Double)
     │    └─ device_clock(Variable, DateTime)   ← 设备自身时钟点位
     ├─ Device_02 (Object)
     │    └─ ...（同结构）
     └─ Device_0N ...
```

- 每个点位变量节点均设置 `NodeId`（如 `ns=2;s=Device_01.temperature`），便于采集端按规则批量枚举订阅，不需要一个个硬编码。
- 每个变量节点开启 **Historizing** 属性为可选项（PoC阶段可先关闭，避免Server侧额外存储压力，历史数据由YMatrix承担）。

---

## 5. 点位参数设计（含时间戳讨论）

### 5.1 基础业务点位（每台设备 5 个）

| 点位名 | 数据类型 | 取值范围（模拟） | 变化模式 |
|---|---|---|---|
| temperature | Double | 20 ~ 90 ℃ | 随机游走 + 缓慢趋势 |
| pressure | Double | 0.1 ~ 2.0 MPa | 正弦波动 + 噪声 |
| speed | Double | 0 ~ 3000 rpm | 阶梯变化（模拟启停） |
| current | Double | 0 ~ 50 A | 与 speed 相关联（非独立随机） |
| voltage | Double | 380 ~ 420 V | 小幅噪声波动 |

### 5.2 关于"时间戳点位"的设计说明

OPC UA 协议在 `DataValue` 层已经自带三个时间戳，**这一层不需要额外建点位**：

- `SourceTimestamp`：数据源（模拟Server）产生该值的时间
- `ServerTimestamp`：Server 处理/发布该通知的时间
- 客户端采集时自行记录的 `ingest_time`：即数据到达采集端/写入数据库的时间

这三者已经能满足之前设计的 `event_time` / `ingest_time` 字段需求。

**额外新增一个业务点位：`device_clock`（DateTime 类型）**，代表"设备自身上报的本地时钟"，与协议时间戳**并列存在、互不替代**。加入它的原因：

1. **模拟时钟漂移场景**：真实工业现场中，PLC/网关本地时钟可能与采集服务器不同步（无 NTP 或 NTP 故障），`device_clock` 可以人为注入固定偏移（如 +30s）或渐进漂移，用于验证下游"时钟不同步导致 event_time 不可信"这一生产化风险点。
2. **模拟延迟/断线补采场景**：断线重连后，可以对比 `device_clock` 与 `SourceTimestamp`/`ingest_time` 的差值，验证补采数据的时间一致性处理逻辑。
3. **与协议时间戳解耦**：`device_clock` 是业务数据（存入 `point_value` 对应字段），协议时间戳是元数据（存入 `event_time`），两者语义不同，不冲突。

因此最终每台设备的点位集合为：

```
temperature, pressure, speed, current, voltage, device_clock
```

采集端写入数据库时，字段设计相应调整为：

```
device_id, point_name, point_value, quality,
source_timestamp,   -- 来自OPC UA SourceTimestamp
server_timestamp,   -- 来自OPC UA ServerTimestamp（可选记录）
ingest_time          -- 采集端写入时间
```

其中 `device_clock` 作为普通点位随其他点位一起采集，其值本身就是一个时间类型的业务数值，用于后续做"时钟偏移分析"的专项查询，不影响其余点位的处理逻辑。

---

## 6. 模拟数据生成与异常注入策略

| 策略 | 说明 |
|---|---|
| 正常波动 | 每个点位按各自的变化模式（随机游走/正弦/阶梯）周期性更新，周期建议 1~2 秒 |
| Quality 异常注入 | 按可配置概率（如 1%）将某次采样标记为 `Bad`/`Uncertain`，用于验证异常点位查询 |
| 数值突变注入 | 按可配置概率产生超出正常范围的跳变值，用于验证告警查询 |
| 设备离线模拟 | 可手动/定时暂停某设备的数据更新任务，模拟设备离线，用于验证离线检测查询 |
| 时钟漂移注入 | 对 `device_clock` 按设备维度设置固定偏移或渐进漂移速率，用于验证时钟不同步场景 |

以上策略建议全部放入 YAML 配置文件（如 `config.yaml`），运行时加载，不写死在代码中，便于调整测试用例而不改代码。

---

## 7. 项目目录结构（规划）

```
opcua-sim/
├─ config/
│   └─ config.yaml     # 设备数量、点位范围、异常注入规则
├─ server/
│   ├─ address_space.py           # 地址空间构建（设备/点位节点定义）
│   ├─ data_generator.py          # 各点位数值生成逻辑
│   └─ main.py                    # Server 启动入口
├─ requirements.txt               # asyncua 等依赖
└─ README.md
```

---

## 8. 后续待展开事项（本文档暂不涉及）

- Python 客户端订阅代码骨架（Subscription / MonitoredItem / DataChangeFilter）
- 批量写入 YMatrix 的缓冲与重试逻辑
- WS 实时展示层（可选，用于前端看板）

---

## 9. 验收自查清单

- [ ] Server 可通过标准 OPC UA 客户端工具（如 UaExpert）连接并浏览地址空间
- [ ] 每台设备的 6 个点位（含 device_clock）均可正常读取与订阅
- [ ] 异常注入（quality/数值突变/离线/时钟漂移）均可通过配置开关独立控制
- [ ] 数据变化频率、幅度符合工业设备的典型特征（非纯随机噪声）