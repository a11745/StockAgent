# Stock Agent - 分布式多模态智能体系统

AI 驱动的股票分析智能体，采用分布式架构设计，支持动态扩容。

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Stock Agent Cluster                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐     ┌─────────────────────────────────────────────────┐   │
│  │  Frontend   │────▶│              Web Gateway Node                   │   │
│  │  (Vue 3)    │◀────│  - REST API / WebSocket                         │   │
│  └─────────────┘     │  - JWT Auth / Task Dispatcher (负载均衡)         │   │
│                      └──────────────────┬──────────────────────────────┘   │
│                                         │                                   │
│                                         ▼                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                        Message Bus (Redis)                            │  │
│  │  - Task Queue (agent:tasks)                                           │  │
│  │  - Result Pub/Sub (agent:results:*)                                   │  │
│  │  - Node Registry (agent:nodes:*) - TTL 15s                            │  │
│  └─────────────────────────────┬────────────────────────────────────────┘  │
│                                │                                            │
│        ┌───────────────────────┼───────────────────────┐                   │
│        ▼                       ▼                       ▼                   │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────────────┐    │
│  │ Data Sync     │     │ MCP Service   │     │ Inference Agents      │    │
│  │ Node          │     │ Node          │     │ (可动态扩容)            │    │
│  │               │────▶│               │◀────│                       │    │
│  │ - Scheduler   │     │ - Tool Server │     │ Agent-1 │ Agent-2     │    │
│  │ - Collectors  │     │ - 令牌桶限流   │     │ Agent-3 │ Agent-N ... │    │
│  └───────────────┘     └───────────────┘     └───────────────────────┘    │
│                                                                             │
│                      ┌──────────────────────────────────────────────────┐  │
│                      │                Storage Layer                      │  │
│                      │  MongoDB │ Milvus (向量) │ Redis (缓存)           │  │
│                      └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📁 项目结构

```
AgentServer/
├── core/                       # 核心基础设施
│   ├── settings.py             # 全局配置 (Pydantic Settings)
│   ├── base/                   # 基类定义
│   │   ├── tool.py             # BaseTool
│   │   ├── collector.py        # BaseCollector
│   │   └── node.py             # BaseNode
│   ├── protocols.py            # 消息协议 (AgentTask, AgentResponse + TraceID)
│   ├── rpc/                    # gRPC 节点间通信
│   └── managers/               # 资源管理器 (全局单例模式)
│       ├── redis_manager.py    # Redis 单例
│       ├── mongo_manager.py    # MongoDB 单例
│       ├── data_source_manager.py  # 统一数据源管理
│       ├── llm_manager.py      # LLM 单例
│       ├── milvus_manager.py   # Milvus 向量存储
│       └── notification_manager.py  # 通知推送
│
├── src/                        # 业务逻辑
│   ├── data_sources/           # 数据源适配器
│   │   ├── base.py             # 数据源基类
│   │   └── tushare_adapter.py  # Tushare 适配器
│   └── collector/              # 新闻采集框架
│       ├── collector.py        # 采集器基类
│       ├── types.py            # 类型定义
│       └── sources/            # 数据源基类
│
├── nodes/                      # 分布式节点
│   ├── web/                    # Web 网关节点
│   │   ├── app.py              # FastAPI 应用
│   │   ├── api/                # REST API
│   │   │   ├── auth.py         # 认证
│   │   │   ├── market.py       # 市场数据
│   │   │   ├── stock.py        # 股票数据
│   │   │   ├── news.py         # 新闻数据
│   │   │   ├── report.py       # 报告回顾
│   │   │   └── task.py         # 任务派发
│   │   └── websocket.py        # WebSocket 推送
│   │
│   ├── data_sync/              # 数据同步节点
│   │   ├── node.py             # 节点实现
│   │   ├── collectors/         # 数据采集器
│   │   │   ├── stock/          # 股票数据采集
│   │   │   └── news/           # 新闻采集
│   │   │       ├── multi_source.py  # 多源聚合
│   │   │       └── sources/    # 新闻源
│   │   │           ├── cls.py       # 财联社
│   │   │           ├── thepaper.py  # 澎湃新闻
│   │   │           ├── gov.py       # 国务院
│   │   │           ├── miit.py      # 工信部
│   │   │           └── ...
│   │   ├── tasks/              # 定时任务
│   │   └── generators/         # 报告生成
│   │
│   ├── mcp/                    # MCP 服务节点
│   │   ├── node.py             # 节点实现
│   │   └── tools/              # MCP 工具集
│   │
│   ├── inference/              # 推理智能体节点
│   │   ├── node.py             # 节点实现
│   │   └── graph/              # LangGraph 工作流
│   │
│   └── listener/               # 行情监听节点
│       └── node.py             # 实时行情推送
│
├── frontend/                   # Vue 3 前端
│   ├── src/
│   │   ├── views/              # 页面组件
│   │   ├── api/                # API 封装
│   │   └── stores/             # Pinia 状态管理
│   └── ...
│
├── scripts/                    # 工具脚本
├── main.py                     # 统一入口
└── requirements.txt            # 依赖
```

## 🔧 核心设计

### 1. 统一数据源管理

所有数据访问通过 `data_source_manager` 统一管理：

```python
from core.managers import data_source_manager

# 初始化 (在节点启动时)
await data_source_manager.initialize()

# 使用
data = await data_source_manager.get_daily("000001.SZ")
calendar = await data_source_manager.get_trade_calendar()
```

### 2. 多源新闻采集

支持多个新闻源，按分组差异化调度：

| 分组 | 采集源 | 采集间隔 |
|------|--------|----------|
| 财经快讯 | 财联社、华尔街见闻、金十数据 | 5 分钟 |
| 财经讨论 | 雪球、东方财富 | 10 分钟 |
| 政策文件 | 工信部、国务院 | 每天 |
| 科技综合 | 澎湃新闻、稀土掘金 | 60 分钟 |

采集时间戳持久化到 MongoDB，服务重启后不会重复采集。

### 3. 心跳机制

节点每 5 秒向 Redis 写入心跳，TTL 15 秒：

```python
# Key: agent:nodes:{node_id}
await redis_manager.register_node(node_id, node_info, ttl=15)
```

### 4. 任务派发 (负载均衡)

派发任务前查询活跃节点，选择负载最低的：

```python
nodes = await redis_manager.get_all_nodes(node_type="inference")
available = [n for n in nodes if n["status"] != "busy"]
best_node = min(available, key=lambda n: n["current_tasks"] / n["max_tasks"])
```

### 5. TraceID 透传

所有任务和日志都包含 trace_id：

```python
task = AgentTask(
    trace_id=request.state.trace_id,
    task_type=TaskType.STOCK_ANALYSIS,
    ...
)

# 日志输出
# 2026-03-11 10:00:00 | INFO | node.web-abc123 | trace_id=xxxx | Processing task...
```

## 🚀 快速开始

### 1. 环境准备

```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 安装依赖
pip install -r requirements.txt

# 前端依赖
cd frontend && npm install
```

### 2. 配置环境变量

```bash
cp env.example .env
```

必要配置：

```env
# Tushare (必须)
TUSHARE_TOKEN=your_tushare_token

# LLM (必须)
LLM_PROVIDER=dashscope
LLM_API_KEY=your_api_key

# 数据库 (可使用默认值)
REDIS_HOST=localhost
MONGO_HOST=localhost
MILVUS_HOST=localhost
```

### 3. 启动服务

#### 使用 Docker Compose (推荐)

```bash
cd deploy
docker-compose up -d
```

#### 手动启动

```bash
# 启动基础设施
docker run -d -p 27017:27017 mongo
docker run -d -p 6379:6379 redis
docker run -d -p 19530:19530 milvusdb/milvus:latest

# 启动后端节点
NODE_TYPE=web python main.py          # Web 网关
NODE_TYPE=data_sync python main.py    # 数据同步
NODE_TYPE=mcp python main.py          # MCP 服务
NODE_TYPE=inference python main.py    # 推理节点

# 启动前端
cd frontend && npm run dev
```

### 4. 访问服务

- 前端: http://localhost:5173
- API 文档: http://localhost:8000/docs
- 健康检查: http://localhost:8000/health

## 📊 功能特性

- **市场总览**: 实时大盘指数、涨跌统计、板块资金流向
- **个股分析**: K线图、技术指标、AI 分析
- **新闻聚合**: 多源新闻采集、事件聚类、热点追踪
- **报告生成**: 每日早报/午报自动生成
- **智能问答**: 基于 LLM 的股票分析问答

## 🔍 可观测性

### 日志格式

```
2026-03-11 10:00:00 | INFO | node.web-abc123 | trace_id=xxxx | Processing task...
```

### Loki 集成

```env
OBS_LOKI_ENABLED=true
OBS_LOKI_URL=http://loki:3100/loki/api/v1/push
```

## 📝 License

MIT
