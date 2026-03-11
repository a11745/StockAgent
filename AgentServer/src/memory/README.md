# 记忆系统 (Memory System)

基于认知心理学的三层记忆架构，支持完整的遗忘机制和交易体系分析。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                        用户输入/系统事件                          │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    感觉记忆 (Sensory Memory)                      │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │
│  │  行情流     │   │  新闻流     │   │  用户输入   │            │
│  │ (Redis)     │   │ (Redis)     │   │ (Redis)     │            │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘            │
│         │  TTL: 30s       │  TTL: 30s       │  TTL: 30s         │
└─────────┼─────────────────┼─────────────────┼───────────────────┘
          │                 │                 │
          └────────────────┬┴─────────────────┘
                           │ 注意力门控 (Attention Gate)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    工作记忆 (Working Memory)                      │
│  ┌─────────────────────────────────────────────────────┐        │
│  │  容量: 7±2 项   TTL: 30 分钟                        │        │
│  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐          │        │
│  │  │ 项1 │ │ 项2 │ │ 项3 │ │ ... │ │ 项N │          │        │
│  │  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘          │        │
│  │  (Redis Hash + Sorted Set)                         │        │
│  └────────────────────────────┬────────────────────────┘        │
│                               │ 重要性 > 0.6                    │
└───────────────────────────────┼─────────────────────────────────┘
                                │ 巩固 (Consolidation)
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    长期记忆 (Long-term Memory)                    │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐          │
│  │  语义记忆     │ │  情景记忆     │ │  程序性记忆   │          │
│  │  (Semantic)   │ │  (Episodic)   │ │  (Procedural) │          │
│  │               │ │               │ │               │          │
│  │  • 新闻资讯   │ │  • 分析记录   │ │  • 入场策略   │          │
│  │  • 研报知识   │ │  • 对话历史   │ │  • 出场策略   │          │
│  │  • 公司信息   │ │  • 交易决策   │ │  • 风险控制   │          │
│  │               │ │               │ │  • 仓位管理   │          │
│  │  Milvus      │ │  Milvus+Mongo │ │  MongoDB      │          │
│  │  (公共)       │ │  (按用户隔离) │ │  (按用户隔离) │          │
│  └───────────────┘ └───────────────┘ └───────────────┘          │
│                                                                  │
│  遗忘机制: TTL 过期 | 访问衰减 | 重要性衰减 | 容量淘汰           │
└─────────────────────────────────────────────────────────────────┘
```

## 目录结构

```
memory/
├── __init__.py           # 模块入口
├── types.py              # 类型定义
├── longterm/             # 长期记忆
│   ├── __init__.py
│   ├── semantic.py       # 语义记忆 (Milvus)
│   ├── episodic.py       # 情景记忆 (Milvus + MongoDB)
│   └── procedural.py     # 程序性记忆 (交易体系)
├── sensory/              # 感觉记忆
│   ├── __init__.py
│   ├── stream.py         # Redis Stream 实现
│   └── attention.py      # 注意力门控
├── working/              # 工作记忆
│   ├── __init__.py
│   ├── buffer.py         # 工作缓冲区
│   └── context.py        # 上下文窗口
├── consolidation.py      # 记忆巩固
├── decay.py              # 遗忘机制
├── retrieval.py          # 统一检索
├── manager.py            # 记忆管理器
└── README.md             # 本文档
```

## 核心组件

### 1. 感觉记忆 (Sensory Memory)

处理实时数据流，秒级 TTL 自动过期。

```python
from src.memory import SensoryStream, AttentionGate

# 初始化
stream = SensoryStream(default_ttl_seconds=30)
gate = AttentionGate()

# 推送实时数据
await stream.push(
    user_id="user1",
    stream_type="quote",
    data={"ts_code": "000001.SZ", "price": 10.5, "pct_chg": 2.5},
)

# 设置关注列表
gate.set_watchlist("user1", ["000001.SZ", "600519.SH"])

# 读取并过滤
items = await stream.read_latest("user1", "quote", count=10)
working_items = await gate.filter(items, "user1")
```

### 2. 工作记忆 (Working Memory)

当前任务的信息处理，容量有限 (7±2 项)。

```python
from src.memory import WorkingBuffer, ContextWindow

# 工作缓冲区
buffer = WorkingBuffer(capacity=9)

# 添加工作记忆
await buffer.add(user_id, working_item)

# 获取所有工作记忆 (按重要性排序)
items = await buffer.get_all(user_id)

# 获取任务相关记忆
task_items = await buffer.get_by_task(user_id, task_id)

# 上下文窗口 (用于 LLM 调用)
context = ContextWindow(max_tokens=4000)

# 添加对话消息
await context.add_message(user_id, "user", "分析一下茅台")
await context.add_message(user_id, "assistant", "好的，我来分析...")

# 构建 LLM 消息
messages = await context.build_messages(
    user_id, working_items, system_prompt
)
```

### 3. 长期记忆 (Long-term Memory)

#### 3.1 语义记忆 (公共知识)

```python
from src.memory import SemanticStore

store = SemanticStore()

# 存储新闻
await store.insert([news_item])

# 语义检索
result = await store.search(
    query="茅台业绩",
    top_k=10,
    ts_code="600519.SH",
    date_from="20240101",
)
```

#### 3.2 情景记忆 (个人经历)

```python
from src.memory import EpisodicStore

store = EpisodicStore()

# 存储分析记录 (按用户隔离)
await store.insert(user_id, [analysis_item])

# 检索个人经历
result = await store.search(
    user_id=user_id,
    query="上次分析茅台的结论",
)

# 获取时间线
timeline = await store.get_timeline(user_id, limit=20)
```

#### 3.3 程序性记忆 (交易体系)

这是帮助用户完善交易体系的核心模块。

```python
from src.memory import ProceduralStore, TradingPattern
from src.memory.longterm.procedural import TradeRecord, PatternType

store = ProceduralStore()

# 1. 记录交易
trade = TradeRecord(
    user_id="user1",
    ts_code="600519.SH",
    ts_name="贵州茅台",
    direction="buy",
    entry_price=1800.0,
    entry_date="20240101",
    entry_reason="突破年线，放量上涨，白酒板块走强",
    quantity=100,
)
await store.record_trade(trade)

# 2. 关闭交易
closed = await store.close_trade(
    trade_id=trade.id,
    user_id="user1",
    exit_price=2000.0,
    exit_date="20240315",
    exit_reason="达到目标价，获利了结",
    lessons_learned="板块轮动行情中顺势操作效果好",
)

# 3. 分析持仓
holdings = [
    {
        "ts_code": "600519.SH",
        "ts_name": "贵州茅台",
        "quantity": 100,
        "cost_price": 1800,
        "current_price": 1900,
        "entry_date": "20240101",
        "entry_reason": "突破年线",
    },
    # ... 更多持仓
]

analysis = await store.analyze_holdings("user1", holdings)
# 返回:
# {
#     "holdings_analysis": [...],      # 每只股票的分析
#     "patterns_identified": [...],    # 识别出的交易模式
#     "trading_system_review": {...},  # 交易体系评估
#     "risk_warnings": [...]           # 风险提示
# }

# 4. 从历史交易学习模式
new_patterns = await store.learn_patterns_from_trades(
    user_id="user1",
    min_samples=5,
)

# 5. 获取入场建议
suggestions = await store.get_entry_suggestions(
    user_id="user1",
    ts_code="000001.SZ",
    current_price=10.5,
    market_data={"vol_ratio": 2.5, "ma5": 10.2},
)
```

### 4. 记忆巩固 (Consolidation)

将工作记忆转化为长期记忆。

```python
from src.memory import ConsolidationEngine

engine = ConsolidationEngine(
    min_importance=0.6,        # 最小重要性阈值
    similarity_threshold=0.85, # 去重相似度阈值
)

# 执行巩固
result = await engine.consolidate(user_id)
print(f"巩固: {result.consolidated_count} 条")
print(f"  语义: {result.to_semantic}")
print(f"  情景: {result.to_episodic}")
print(f"  程序性: {result.to_procedural}")
print(f"  识别模式: {result.patterns_detected}")

# 巩固对话
result = await engine.consolidate_conversation(
    user_id=user_id,
    session_id=session_id,
    messages=conversation_history,
)
```

### 5. 遗忘机制 (Decay)

完整的遗忘系统，包含多种衰减策略。

```python
from src.memory import DecayEngine

engine = DecayEngine(
    access_decay_rate=0.05,      # 每天衰减 5%
    importance_decay_rate=0.02,   # 每天衰减 2%
    max_long_term_per_user=10000, # 每用户最多 1 万条
    min_importance_threshold=0.1, # 低于此阈值删除
)

# 执行一次衰减
result = await engine.run_decay(user_id)
print(f"过期: {result.expired_count}")
print(f"衰减: {result.decayed_count}")

# 提升记忆重要性 (被访问时)
await engine.boost_importance(user_id, item_id, boost_amount=0.1)

# 标记为永久保留
await engine.mark_as_permanent(user_id, item_id)

# 启动定时衰减 (每 24 小时)
await engine.start_scheduled_decay(interval_hours=24)
```

### 6. 统一检索 (Unified Retrieval)

跨层级的记忆检索。

```python
from src.memory import UnifiedRetriever, RetrievalQuery

retriever = UnifiedRetriever()

# 构建查询
query = RetrievalQuery(
    text="茅台的最新分析和交易建议",
    user_id="user1",
    ts_code="600519.SH",
    top_k=10,
    include_working=True,
    include_semantic=True,
    include_episodic=True,
    include_procedural=True,
    recency_weight=0.3,
    relevance_weight=0.5,
    importance_weight=0.2,
)

# 执行检索
result = await retriever.search(query)
print(f"找到 {result.total_count} 条相关记忆")
print(f"  工作记忆: {len(result.from_working)}")
print(f"  语义记忆: {len(result.from_semantic)}")
print(f"  情景记忆: {len(result.from_episodic)}")
print(f"  交易模式: {len(result.from_procedural)}")

# 获取任务上下文
context = await retriever.get_context_for_task(
    user_id="user1",
    task_description="分析茅台近期走势",
    ts_codes=["600519.SH"],
    max_items=10,
)
```

### 7. 统一管理器 (Memory Manager)

记忆系统的统一入口。

```python
from src.memory import memory_manager

# 便捷方法
await memory_manager.add_to_working(
    user_id="user1",
    content="茅台突破年线",
    importance=0.8,
    ts_code="600519.SH",
)

# 存储新闻
await memory_manager.store_news(
    content="茅台发布年报...",
    ts_code="600519.SH",
    publish_date="20240320",
    source="财联社",
)

# 存储分析
await memory_manager.store_analysis(
    user_id="user1",
    content="茅台分析结论: ...",
    ts_code="600519.SH",
    importance=0.8,
)

# 统一检索
result = await memory_manager.search(
    user_id="user1",
    query="茅台分析",
    ts_code="600519.SH",
)

# 分析持仓
analysis = await memory_manager.analyze_holdings(user_id, holdings)

# 获取交易模式
patterns = await memory_manager.get_trading_patterns(user_id)

# 学习新模式
new_patterns = await memory_manager.learn_patterns(user_id)

# 记忆生命周期
await memory_manager.consolidate(user_id)
await memory_manager.run_decay(user_id)

# 上下文管理
await memory_manager.add_message(user_id, "user", "分析茅台")
messages = await memory_manager.build_llm_messages(
    user_id, system_prompt
)
```

## 遗忘机制详解

### 衰减策略

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| TTL | 时间过期 | 感觉记忆、临时数据 |
| ACCESS_DECAY | 访问频率衰减 | 默认策略，常用记忆保留 |
| IMPORTANCE_DECAY | 重要性衰减 | 低重要性内容逐渐遗忘 |
| NEVER | 永不遗忘 | 重要交易记录、核心知识 |

### 衰减公式

```
衰减后重要性 = 当前重要性 × (1 - 衰减率) ^ 天数
```

- `access_decay_rate = 0.05`: 未访问时每天衰减 5%
- 访问会提升重要性 (boost)
- 低于 `min_importance_threshold` (0.1) 的记忆会被删除

### 容量控制

每用户最多保留 `max_long_term_per_user` (10000) 条长期记忆。
超出时按重要性排序，淘汰最不重要的记忆。

## 多用户隔离

| 记忆类型 | 隔离方式 | 说明 |
|----------|----------|------|
| 感觉记忆 | 完全隔离 | Redis key 包含 user_id |
| 工作记忆 | 完全隔离 | Redis key 包含 user_id |
| 语义记忆 | 共享 (PUBLIC) | 新闻、公告等公共知识 |
| 情景记忆 | 完全隔离 | 个人分析记录 |
| 程序性记忆 | 完全隔离 | 个人交易体系 |

## 设计原则

1. **异步优先**: 所有 I/O 操作使用 `async/await`
2. **延迟加载**: Manager 在首次使用时才导入
3. **单例复用**: 使用 `core.managers` 中的单例
4. **日志追踪**: 所有操作支持 `trace_id`
5. **类型安全**: 使用 Pydantic 模型验证数据

## 依赖服务

- **Redis**: 感觉记忆、工作记忆
- **Milvus**: 长期记忆向量检索
- **MongoDB**: 长期记忆元数据
- **LLM**: 向量化、模式识别、分析

## 测试

```bash
# 运行测试
cd AgentServer
python -m pytest src/memory/tests/ -v
```
