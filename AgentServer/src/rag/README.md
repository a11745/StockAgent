# RAG 检索增强生成系统

基于知识库和记忆系统的统一 RAG 架构。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                          用户查询                                │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      UnifiedRAGPipeline                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                      多路检索                             │   │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐           │   │
│  │  │固定知识库   │ │用户知识库   │ │记忆系统     │           │   │
│  │  │            │ │            │ │            │           │   │
│  │  │• 复盘规则  │ │• 交易规则  │ │• 语义记忆  │           │   │
│  │  │• 技术分析  │ │• 个人策略  │ │• 情景记忆  │           │   │
│  │  │• 因子解读  │ │• 复盘模板  │ │• 程序性记忆│           │   │
│  │  └────────────┘ └────────────┘ └────────────┘           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                │                                 │
│                                ▼                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                      重排序 & 融合                        │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                │                                 │
│                                ▼                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                      上下文组装                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                │                                 │
│                                ▼                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                      LLM 生成                             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 目录结构

```
src/rag/
├── __init__.py           # 模块入口
├── README.md             # 本文档
├── retriever.py          # 基础向量检索器
├── pipeline.py           # 原始 RAG Pipeline
├── unified.py            # 统一 RAG Pipeline (核心)
└── knowledge/            # 知识库
    ├── __init__.py
    ├── types.py          # 类型定义
    ├── fixed/            # 固定知识库
    │   ├── __init__.py
    │   ├── store.py      # FixedKnowledgeStore
    │   └── loader.py     # 知识加载器
    └── user/             # 用户知识库
        ├── __init__.py
        ├── store.py      # UserKnowledgeStore
        └── assistant.py  # AI 辅助知识整理
```

## 快速开始

### 1. 统一 RAG 查询

```python
from src.rag import rag_pipeline, RetrievalConfig

# 简单查询
result = await rag_pipeline.query(
    user_id="user1",
    query="如何分析筹码峰",
)

print(result.response)  # LLM 生成的回答
print(result.items)     # 检索到的内容
print(result.citations) # 引用信息
```

### 2. 技术分析查询

```python
from src.rag import rag_pipeline, FixedKnowledgeCategory

# 查询特定技术分析知识
result = await rag_pipeline.query_technical(
    user_id="user1",
    query="这根K线是什么形态",
    category=FixedKnowledgeCategory.TECH_CANDLESTICK_SINGLE,
)
```

### 3. 复盘辅助

```python
# 结合复盘规则、用户规则、历史分析进行复盘
result = await rag_pipeline.query_review(
    user_id="user1",
    query="帮我复盘这笔茅台的交易",
    ts_code="600519.SH",
)
```

### 4. 个股分析

```python
# 查询个股相关信息
result = await rag_pipeline.query_stock(
    user_id="user1",
    query="茅台最近有什么利好",
    ts_code="600519.SH",
)
```

## 固定知识库

### 知识分类

```python
from src.rag import FixedKnowledgeCategory

# 大盘复盘
FixedKnowledgeCategory.MARKET_REVIEW_OPEN      # 开盘分析
FixedKnowledgeCategory.MARKET_REVIEW_INTRADAY  # 盘中观察
FixedKnowledgeCategory.MARKET_REVIEW_CLOSE     # 收盘复盘

# 策略因子
FixedKnowledgeCategory.FACTOR_VOLUME_PRICE     # 量价因子
FixedKnowledgeCategory.FACTOR_MOMENTUM         # 动量因子
FixedKnowledgeCategory.FACTOR_SENTIMENT        # 情绪因子

# 技术分析 - 筹码
FixedKnowledgeCategory.TECH_CHIP_PEAK          # 筹码峰
FixedKnowledgeCategory.TECH_CHIP_DISTRIBUTION  # 筹码分布

# 技术分析 - K线
FixedKnowledgeCategory.TECH_CANDLESTICK_SINGLE # 单根K线
FixedKnowledgeCategory.TECH_CANDLESTICK_DOUBLE # 双K组合
FixedKnowledgeCategory.TECH_CANDLESTICK_PATTERN # K线形态
```

### 加载知识

```python
from src.rag import FixedKnowledgeStore, KnowledgeLoader

# 加载 Markdown 文件
loader = KnowledgeLoader("data/knowledge")
items, result = await loader.load_all(generate_vectors=True)

# 存储到知识库
store = FixedKnowledgeStore()
await store.insert_batch(items)

# 搜索知识
result = await store.search(
    query="锤子线",
    category=FixedKnowledgeCategory.TECH_CANDLESTICK_SINGLE,
    top_k=5,
)
```

### 知识文件格式

使用 Markdown + Front Matter:

```markdown
---
id: candlestick_hammer
category: tech_candlestick_single
title: 锤子线
tags: [K线, 反转, 底部]
importance: high
summary: 锤子线是底部反转信号...
---

# 锤子线

## 形态特征
...
```

## 用户知识库

### 知识类型

```python
from src.rag import UserKnowledgeType

UserKnowledgeType.TRADING_RULE     # 交易规则 (入场/出场/仓位/风控)
UserKnowledgeType.STRATEGY         # 个人策略
UserKnowledgeType.REVIEW_TEMPLATE  # 复盘模板
UserKnowledgeType.CHECKLIST        # 检查清单
UserKnowledgeType.LESSON           # 心得教训
UserKnowledgeType.NOTE             # 学习笔记
```

### 手动创建知识

```python
from src.rag import UserKnowledgeStore, UserKnowledgeItem, UserKnowledgeType

store = UserKnowledgeStore()

# 创建交易规则
rule = UserKnowledgeItem(
    user_id="user1",
    knowledge_type=UserKnowledgeType.TRADING_RULE,
    title="突破年线入场规则",
    content="当股价突破年线且成交量放大时考虑入场",
    conditions=[
        "股价突破年线",
        "成交量 > 5日均量 * 1.5",
        "大盘环境良好",
    ],
    actions=[
        "买入 1/3 仓位",
        "设置止损位: 年线下方 3%",
    ],
    tags=["入场", "技术突破"],
)

await store.create(rule)
```

### AI 辅助整理

```python
from src.rag import KnowledgeAssistant

assistant = KnowledgeAssistant()

# 1. 从对话中提取规则
result = await assistant.extract_rules_from_conversation(
    user_id="user1",
    messages=[
        {"role": "user", "content": "我一般会在股价突破年线时入场"},
        {"role": "assistant", "content": "明白，这是技术突破策略..."},
    ],
    auto_save=True,
)

print(result["rules"])       # 提取的规则
print(result["insights"])    # 发现的交易习惯
print(result["suggestions"]) # 建议补充的规则

# 2. 帮助整理单条规则
result = await assistant.organize_trading_rule(
    user_id="user1",
    raw_input="我喜欢在早盘低开高走的时候买入，仓位一般三分之一",
    auto_save=True,
)

# 3. 生成复盘模板
result = await assistant.generate_review_template(
    user_id="user1",
    style="detailed",  # brief/standard/detailed
    focus_areas=["入场分析", "仓位管理"],
    auto_save=True,
)

# 4. 总结交易心得
result = await assistant.summarize_lesson(
    user_id="user1",
    trade_description="茅台, 1800买入, 1570止损",
    outcome="亏损15%",
    reflection="没有严格执行止损纪律",
    auto_save=True,
)

# 5. 评估交易体系完整性
evaluation = await assistant.evaluate_trading_system(user_id="user1")

print(evaluation["completeness_score"])  # 完整性分数
print(evaluation["analysis"])            # 优劣势分析
print(evaluation["recommendations"])     # 改进建议
```

## 检索配置

```python
from src.rag import RetrievalConfig, FixedKnowledgeCategory

config = RetrievalConfig(
    # 各来源开关
    use_fixed_knowledge=True,    # 固定知识库
    use_user_knowledge=True,     # 用户知识库
    use_semantic_memory=True,    # 语义记忆 (新闻)
    use_episodic_memory=True,    # 情景记忆 (历史分析)
    use_procedural_memory=False, # 程序性记忆 (交易模式)
    
    # 各来源 top_k
    fixed_top_k=5,
    user_top_k=5,
    semantic_top_k=5,
    episodic_top_k=5,
    
    # 过滤条件
    fixed_categories=[FixedKnowledgeCategory.TECH_CHIP_PEAK],
    ts_code="600519.SH",
    date_from="20240101",
    date_to="20240320",
    
    # 重排序
    use_reranker=False,
    rerank_top_k=10,
)

result = await rag_pipeline.query(
    user_id="user1",
    query="筹码峰分析",
    config=config,
)
```

## 检索结果

```python
result = await rag_pipeline.query(user_id, query)

# 检索到的内容
for item in result.items:
    print(f"来源: {item.source}")
    print(f"标题: {item.title}")
    print(f"内容: {item.content}")
    print(f"分数: {item.score}")
    print("---")

# 按来源统计
print(result.by_source)
# {'fixed_knowledge': 3, 'user_knowledge': 2, 'semantic_memory': 2}

# 组装好的上下文
print(result.context)

# LLM 生成的回答
print(result.response)

# 引用信息
print(result.citations)

# 性能统计
print(f"耗时: {result.query_time_ms}ms")
```

## 知识文件目录

```
data/knowledge/
├── market_review/           # 大盘复盘
│   ├── daily_close.md       # 每日收盘复盘
│   ├── daily_open.md        # 每日开盘分析
│   └── weekly.md            # 周复盘
├── technical/               # 技术分析
│   ├── candlestick/         # K线
│   │   ├── hammer.md        # 锤子线
│   │   ├── engulfing.md     # 吞没形态
│   │   └── ...
│   ├── chip/                # 筹码
│   │   ├── chip_peak_basics.md
│   │   └── ...
│   └── moving_average/      # 均线
│       └── ...
└── factors/                 # 策略因子
    ├── volume_price.md      # 量价因子
    ├── momentum.md          # 动量因子
    └── ...
```

## 与记忆系统集成

RAG 系统与记忆系统深度集成:

| 记忆类型 | RAG 检索源 | 用途 |
|---------|-----------|------|
| SemanticStore | semantic_memory | 新闻、研报、公告 |
| EpisodicStore | episodic_memory | 历史分析记录 |
| ProceduralStore | procedural_memory | 交易模式 |

```python
# RAG 可以同时检索知识库和记忆系统
result = await rag_pipeline.query(
    user_id="user1",
    query="茅台分析",
    config=RetrievalConfig(
        use_fixed_knowledge=True,    # 技术分析知识
        use_user_knowledge=True,     # 我的交易规则
        use_semantic_memory=True,    # 茅台相关新闻
        use_episodic_memory=True,    # 我之前的分析
        use_procedural_memory=True,  # 我的交易模式
        ts_code="600519.SH",
    ),
)
```

## 设计原则

1. **异步优先**: 所有操作使用 `async/await`
2. **多路并行**: 检索任务并行执行
3. **按需加载**: 延迟导入 managers
4. **日志追踪**: 支持 `trace_id`
5. **用户隔离**: 用户知识按 user_id 隔离

## 依赖服务

- **Milvus**: 向量检索
- **MongoDB**: 元数据存储
- **LLM**: 向量化 + 生成
