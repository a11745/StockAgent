<div align="center">
  <h1>🚀 StockAgent</h1>
  <p><strong>AI 驱动的智能量化分析平台</strong></p>
  <p>
    <a href="#功能特性">功能特性</a> •
    <a href="#系统架构">系统架构</a> •
    <a href="#快速开始">快速开始</a> •
    <a href="#部署指南">部署指南</a> •
    <a href="#技术栈">技术栈</a>
  </p>
  <p>
    <img src="https://img.shields.io/badge/Python-3.11+-blue?logo=python" alt="Python">
    <img src="https://img.shields.io/badge/Vue-3.4+-green?logo=vue.js" alt="Vue">
    <img src="https://img.shields.io/badge/FastAPI-0.100+-teal?logo=fastapi" alt="FastAPI">
    <img src="https://img.shields.io/badge/MongoDB-7.0-green?logo=mongodb" alt="MongoDB">
    <img src="https://img.shields.io/badge/License-MIT-yellow" alt="License">
  </p>
</div>

---

## 📖 项目简介

**StockAgent** 是一个面向 A 股市场的智能量化分析平台，融合了 **大语言模型 (LLM)**、**多因子选股**、**量化回测**、**多源新闻聚合** 等技术，帮助投资者进行智能化的市场分析和策略验证。

### ✨ 核心亮点

- 🤖 **AI 智能分析** - 集成 GPT-4、DeepSeek、通义千问等多种 LLM，提供智能股票分析报告
- 📊 **多因子选股** - 内置 17+ 选股因子，支持动量、价值、质量、成长等多维度策略
- 📈 **向量化回测** - 高性能回测引擎，支持 A 股 T+1 规则、佣金印花税、涨跌停限制
- 📰 **多源新闻聚合** - 自动采集财联社、澎湃、国务院、工信部等 10+ 新闻源
- 📋 **智能报告生成** - 每日早报/午报自动生成，热点事件聚类分析
- 🔄 **实时数据同步** - 自动同步 Tushare 行情数据，支持定时调度
- 🌐 **分布式架构** - 微服务设计，各节点可独立扩展
- 🎨 **现代化 UI** - Vue3 + Element Plus 构建的专业级交易界面

---

## 🖼️ 界面预览

<details>
<summary>点击展开截图</summary>

### 仪表盘
![Dashboard](docs/images/dashboard.png)

### 量化回测
![Backtest](docs/images/backtest.png)

### 行情分析
![Factor Selection](docs/images/analyzer.png)

### 热点追踪
![Hot News](docs/images/hot-news.png)

</details>

---

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      Frontend (Vue3 + Vite)                      │
│                   Element Plus + ECharts + SCSS                  │
└─────────────────────────────┬───────────────────────────────────┘
                              │ HTTP / WebSocket
┌─────────────────────────────▼───────────────────────────────────┐
│                     Web Node (FastAPI)                           │
│              REST API • JWT Auth • WebSocket                     │
└─────────────────────────────┬───────────────────────────────────┘
                              │ gRPC (内部通信)
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│  Data Sync    │     │   Inference   │     │   Backtest    │
│    Node       │     │     Node      │     │     Node      │
│ ─────────────│     │ ─────────────│       │ ───────────── │
│ • 行情同步    │     │ • LLM 推理    │      │ • 单股回测     │
│ • 新闻采集    │     │ • 智能分析    │      │ • 因子选股     │
│ • 报告生成    │     │ • 报告生成    │      │ • 组合回测     │
└───────┬───────┘     └───────┬───────┘     └───────┬───────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MongoDB • Redis • Milvus                     │
│               数据存储 • 缓存队列 • 向量检索                      │
└─────────────────────────────────────────────────────────────────┘
```

### 节点说明

| 节点 | 职责 | 可扩展 |
|------|------|--------|
| **Web Node** | HTTP API 网关、用户认证、请求路由 | ✅ |
| **Data Sync Node** | 行情同步、新闻采集、报告生成 | ❌ (单实例) |
| **Inference Node** | LLM 推理、智能分析 | ✅ |
| **Backtest Node** | 量化回测、因子计算、绩效分析 | ✅ |
| **MCP Node** | Model Context Protocol 服务 | ❌ |
| **Listener Node** | 实时行情监听、异动提醒 | ❌ |

---

## 🎯 功能特性

### 📊 市场分析

- **大盘概览** - 主要指数行情、涨跌分布、成交热力图
- **板块分析** - 行业/概念板块资金流向、强弱对比
- **涨跌停分析** - 涨停板复盘、连板统计、封板强度

### 📰 新闻聚合

多源新闻自动采集，按分组差异化调度：

| 分组 | 采集源 | 采集间隔 |
|------|--------|----------|
| 财经快讯 | 财联社、华尔街见闻、金十数据 | 5 分钟 |
| 财经讨论 | 雪球、东方财富 | 10 分钟 |
| 政策文件 | 国务院、工信部 | 每天 |
| 科技综合 | 澎湃新闻、稀土掘金 | 60 分钟 |

- **热点追踪** - 实时热榜、多源聚合
- **事件聚类** - LLM 智能分析，相似新闻自动归类
- **报告回顾** - 历史报告查询，按日期筛选

### 🔬 智能分析

- **AI 股票分析** - 基于 LLM 的多维度分析报告
- **技术面诊断** - 自动识别 K 线形态、支撑压力位
- **基本面评估** - 财务指标评分、估值对比
- **资金面解读** - 主力资金流向、龙虎榜解析
- **每日报告** - 早报/午报自动生成推送

### 📈 量化回测

#### 单股回测
- 支持自定义因子权重
- A 股交易规则（T+1、涨跌停、佣金印花税）
- 收益曲线、回撤分析、交易明细

#### 因子选股回测
- **17+ 内置因子**：

| 分类 | 因子 |
|------|------|
| 动量 | 5日/20日/60日动量 |
| 价值 | PE_TTM、PB、PS_TTM、股息率 |
| 质量 | ROE、ROA、毛利率 |
| 成长 | 营收增长率、利润增长率 |
| 波动 | 20日/60日波动率 |
| 流动性 | 换手率、成交额、总市值 |
| 技术 | 均线偏离、RSI、价格位置 |

- **灵活配置**：调仓频率、选股数量、权重方法
- **基准对比**：策略收益 vs 沪深300
- **详细报告**：夏普比率、最大回撤、超额收益

### 🗂️ 策略管理

- **自选股** - 分组管理、实时行情
- **市场监听** - 条件触发、实时提醒
- **历史记录** - 回测结果存档、策略对比

---

## 🛠️ 技术栈

### 后端

| 技术 | 用途 |
|------|------|
| **Python 3.11+** | 主开发语言 |
| **FastAPI** | Web 框架、REST API |
| **gRPC** | 节点间通信 |
| **Pydantic** | 数据验证、配置管理 |
| **APScheduler** | 定时任务调度 |
| **Pandas/NumPy** | 数据处理、向量化计算 |
| **LangChain** | LLM 应用框架 |
| **httpx** | 异步 HTTP 客户端 |

### 前端

| 技术 | 用途 |
|------|------|
| **Vue 3.4** | 前端框架 |
| **TypeScript** | 类型安全 |
| **Vite 5** | 构建工具 |
| **Element Plus** | UI 组件库 |
| **ECharts** | 图表可视化 |
| **Pinia** | 状态管理 |
| **Vue Router** | 路由管理 |

### 存储

| 技术 | 用途 |
|------|------|
| **MongoDB 7** | 主数据库 |
| **Redis 7** | 缓存、消息队列、分布式锁 |
| **Milvus** | 向量数据库 (语义搜索) |

### 数据源

| 数据源 | 数据类型 |
|--------|----------|
| **Tushare Pro** | A股行情、财务、资金流向等 |
| **财联社** | 财经快讯、电报 |
| **澎湃新闻** | 综合热榜、财经资讯 |
| **国务院/工信部** | 政策文件、公告 |

### LLM 支持

- OpenAI (GPT-4o, GPT-4o-mini)
- DeepSeek (deepseek-chat)
- 阿里云 DashScope (qwen-plus)
- 智谱 AI (GLM-4)
- Ollama (本地部署)

---

## 🚀 快速开始

### 环境要求

- Python 3.11+
- Node.js 18+
- MongoDB 7.0
- Redis 7.0
- Milvus 2.4+ (可选，用于语义搜索)
- Docker & Docker Compose (推荐)

### 1. 克隆项目

```bash
git clone https://github.com/your-username/StockAgent.git
cd StockAgent
```

### 2. 启动基础设施

```bash
cd AgentServer/deploy

# 启动全部服务 (推荐)
docker compose up -d mongodb redis milvus

# 或仅启动核心服务 (不含向量搜索)
docker compose up -d mongodb redis
```

> 💡 **Milvus** 用于事件聚类、语义搜索等 AI 功能。如不需要这些功能可跳过。

### 3. 配置环境变量

```bash
cd AgentServer
cp .env.example .env
# 编辑 .env，填入必要配置
```

**必须配置**：
- `TUSHARE_TOKEN` - [Tushare Pro](https://tushare.pro) 账号 Token
- `LLM_API_KEY` - LLM 服务的 API Key

### 4. 安装依赖

```bash
cd AgentServer

# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
.\venv\Scripts\activate    # Windows
# source venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt
```

前端依赖：

```bash
cd frontend
npm install
```

### 5. 启动服务

#### 方式 A：使用管理脚本 (推荐)

```powershell
# 进入项目根目录
cd stockAgent
.\manager.ps1
```

```
[1] Start Web
[2] Start Inference
[3] Start DataSync
[4] Start Listener
[5] Start Backtest
[6] Start Frontend

[A] Start all backend nodes
[F] Start full stack (backend + frontend)
```

按需选择启动，或选择 `[F]` 启动全部。

#### 方式 B：手动启动

```bash
cd AgentServer

# 启动 Web 节点
NODE_TYPE=web python main.py

# 新终端：启动数据同步节点
NODE_TYPE=data_sync python main.py

# 新终端：启动前端
cd frontend && npm run dev
```

### 6. 访问服务

- 前端界面: http://localhost:5173
- API 文档: http://localhost:8000/docs

---

## 📦 项目结构

```
StockAgent/
├── AgentServer/                 # 后端服务
│   ├── main.py                  # 统一入口
│   ├── requirements.txt         # Python 依赖
│   ├── .env.example             # 环境变量模板
│   │
│   ├── core/                    # 核心模块
│   │   ├── settings.py          # 配置管理
│   │   ├── protocols.py         # 协议定义
│   │   ├── rpc/                 # gRPC 通信
│   │   └── managers/            # 管理器 (MongoDB, Redis, DataSource)
│   │
│   ├── src/                     # 业务逻辑
│   │   ├── data_sources/        # 数据源适配器
│   │   └── collector/           # 新闻采集框架
│   │
│   ├── nodes/                   # 节点实现
│   │   ├── web/                 # Web 节点 (FastAPI)
│   │   │   └── api/             # API 路由
│   │   ├── data_sync/           # 数据同步节点
│   │   │   ├── collectors/      # 数据采集器
│   │   │   │   ├── stock/       # 股票数据
│   │   │   │   └── news/        # 新闻采集
│   │   │   ├── tasks/           # 定时任务
│   │   │   └── generators/      # 报告生成
│   │   ├── inference/           # 推理节点
│   │   ├── backtest_engine/     # 回测引擎
│   │   ├── mcp/                 # MCP 节点
│   │   └── listener/            # 监听节点
│   │
│   ├── scripts/                 # 工具脚本
│   └── deploy/                  # 部署配置
│
├── frontend/                    # 前端应用
│   ├── src/
│   │   ├── api/                 # API 封装
│   │   ├── views/               # 页面视图
│   │   ├── stores/              # 状态管理
│   │   └── router/              # 路由配置
│   └── ...
│
├── manager.ps1                  # Windows 管理脚本
├── backup.ps1                   # 备份脚本
└── README.md                    # 本文件
```

---

## 📖 部署指南

详见 [AgentServer/deploy/README.md](AgentServer/deploy/README.md)

### Docker Compose 部署 (推荐)

```bash
cd AgentServer/deploy

# 启动所有服务
docker compose up -d

# 查看状态
docker compose ps
```

### 按需启动

```bash
# 只启动核心服务
docker compose up -d mongodb redis web data-sync backtest
```

---

## 🔧 配置说明

完整配置项见 `AgentServer/.env.example`，主要配置：

| 配置项 | 说明 | 必须 |
|--------|------|------|
| `TUSHARE_TOKEN` | Tushare Pro Token | ✅ |
| `LLM_PROVIDER` | LLM 提供商 | ✅ |
| `LLM_API_KEY` | LLM API Key | ✅ |
| `MONGO_*` | MongoDB 连接配置 | ✅ |
| `REDIS_*` | Redis 连接配置 | ✅ |
| `JWT_SECRET` | JWT 签名密钥 | ✅ |

---

## 🗺️ 路线图

- [x] 基础架构搭建
- [x] 数据同步模块
- [x] 单股量化回测
- [x] 因子选股回测
- [x] 多源新闻采集
- [x] 热点新闻聚合
- [x] 事件聚类分析
- [x] 每日报告生成
- [x] 报告回顾功能
- [ ] 实时行情 WebSocket
- [ ] 策略可视化编排
- [ ] 自定义因子编写
- [ ] 移动端适配
- [ ] 多账户支持
- [ ] 实盘对接

---

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

---

## 📄 许可证

本项目采用 [MIT License](LICENSE) 开源许可证。

---

## ⚠️ 免责声明

本项目仅供学习和研究使用，不构成任何投资建议。股市有风险，投资需谨慎。使用本软件进行的任何投资决策，用户需自行承担风险。

---

## 🙏 致谢

- [Tushare](https://tushare.pro) - 金融数据接口
- [LangChain](https://langchain.com) - LLM 应用框架
- [FastAPI](https://fastapi.tiangolo.com) - Web 框架
- [Vue.js](https://vuejs.org) - 前端框架
- [Element Plus](https://element-plus.org) - UI 组件库

---

<div align="center">
  <p>如果这个项目对你有帮助，请给一个 ⭐ Star 支持一下！</p>
</div>
