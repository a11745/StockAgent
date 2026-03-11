# 新闻采集框架

提供新闻采集、两阶段去重、事件聚类、分层存储的基础框架。

## 架构

```
src/collector/                     # 框架层 (可迁移)
├── types.py                       # 类型定义 (NewsItem, NewsCategory, CollectResult)
├── dedup.py                       # DeduplicationEngine 快速去重引擎
├── event_cluster.py               # EventClusterEngine 事件聚类引擎 (LLM)
├── lifecycle.py                   # NewsLifecycleManager 数据生命周期管理
├── storage.py                     # NewsStorage 存储接口
├── collector.py                   # BaseNewsCollector 基类
└── sources/
    └── base.py                    # BaseSource 采集源基类

nodes/data_sync/collectors/news/   # 实现层 (业务相关)
├── multi_source.py                # MultiSourceCollector 多源聚合采集
├── event_clustering.py            # EventClusteringCollector 事件聚类任务
├── lifecycle.py                   # NewsLifecycleCollector 生命周期管理任务
├── stock_news.py                  # StockNewsCollector 涨跌停新闻
├── hot_news.py                    # HotNewsCollector 热点新闻
└── sources/                       # 各采集源实现
    ├── cls.py                     # 财联社
    ├── wallstreetcn.py            # 华尔街见闻
    ├── jin10.py                   # 金十数据
    ├── xueqiu.py                  # 雪球
    ├── eastmoney.py               # 东方财富
    ├── miit.py                    # 工信部
    ├── gov.py                     # 国务院
    ├── juejin.py                  # 稀土掘金
    └── thepaper.py                # 澎湃新闻
```

## 两阶段去重

```
┌─────────────────────────────────────────────────────────────┐
│                阶段一：快速去重 (采集时，实时)                 │
├─────────────────────────────────────────────────────────────┤
│  1. 内存去重 - 同批次 content_hash                          │
│  2. 哈希去重 - MongoDB content_hash 精确匹配                │
│  3. 标题相似度 - SequenceMatcher 85% 阈值                   │
│  → 入库 MongoDB (不生成向量)                                 │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              阶段二：深度去重 (异步，每30分钟)                 │
├─────────────────────────────────────────────────────────────┤
│  1. LLM 提取事件指纹 (主体 + 动作 + 关键词)                  │
│  2. 基于指纹相似度聚类 (70% 阈值)                            │
│  3. 只为主新闻生成向量并入库 Milvus                          │
│  4. 相似新闻标记为非主新闻 (不入向量库)                       │
└─────────────────────────────────────────────────────────────┘
```

## 采集源概览

| 分类 | 来源 | 内容类型 | 采集频率 |
|------|------|----------|----------|
| 财经快讯 | 财联社 | 电报 + 头条 | 3分钟 |
| 财经快讯 | 华尔街见闻 | 快讯 + 文章 | 3分钟 |
| 财经快讯 | 金十数据 | 快讯 | 3分钟 |
| 财经讨论 | 雪球 | 热股 + 帖子 | 5分钟 |
| 财经讨论 | 东方财富 | 财富号 + 快讯 | 5分钟 |
| 政策文件 | 工信部 | 政策/公告/标准 | 30分钟 |
| 政策文件 | 国务院 | 政策文库 | 30分钟 |
| 科技资讯 | 稀土掘金 | 热榜 | 10分钟 |
| 综合新闻 | 澎湃新闻 | 热榜 + 财经 | 10分钟 |

## 快速开始

```python
from nodes.data_sync.collectors.news import multi_source_collector

# 1. 采集所有源 (手动)
result = await multi_source_collector.collect_all()
print(f"新增: {result.new_count}, 重复: {result.duplicate_count}")

# 2. 采集指定分组
result = await multi_source_collector.collect_group("finance_flash")

# 3. 采集指定源
result = await multi_source_collector.collect_source("cls")

# 4. 查看采集统计
stats = await multi_source_collector.get_stats()
```

## 采集分组

```python
# 分组定义及采集频率
GROUP_INTERVALS = {
    "finance_flash": 3,    # 财经快讯: 3分钟
    "finance_discuss": 5,  # 财经讨论: 5分钟
    "policy": 30,          # 政策文件: 30分钟
    "tech_general": 10,    # 科技/综合: 10分钟
}

SOURCE_GROUPS = {
    "finance_flash": ["cls", "wallstreetcn", "jin10"],
    "finance_discuss": ["xueqiu", "eastmoney"],
    "policy": ["miit", "gov"],
    "tech_general": ["juejin", "thepaper"],
}
```

## 框架类

### BaseSource

所有采集源的基类：

```python
from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory

class MySource(BaseSource):
    name = NewsSource.OTHER
    display_name = "我的来源"
    default_category = NewsCategory.GENERAL
    
    async def fetch(self, since=None, limit=100, trace_id=None):
        items = []
        
        # 使用内置的 HTTP 客户端
        data = await self.fetch_json("https://api.example.com/news")
        
        for item in data["items"]:
            news_item = self.create_news_item(
                title=item["title"],
                content=item["content"],
                url=item["url"],
                publish_time=datetime.fromisoformat(item["time"]),
            )
            items.append(news_item)
        
        return items
```

### BaseNewsCollector

新闻采集器基类：

```python
from src.collector.collector import BaseNewsCollector

class MyCollector(BaseNewsCollector):
    SOURCE_CLASSES = {"mysource": MySource}
    SOURCE_GROUPS = {"default": ["mysource"]}

collector = MyCollector()
result = await collector.collect_all()
```

### EventClusterEngine

事件聚类引擎：

```python
from src.collector.event_cluster import EventClusterEngine

engine = EventClusterEngine()

# 处理待聚类的新闻
result = await engine.process_pending_news(batch_size=100)
print(f"新事件: {result.new_events}, 合并: {result.merged_news}")

# 获取最近事件
events = await engine.get_recent_events(hours=24, limit=50)
```

## 存储结构

### MongoDB: news

```python
{
    "_id": "cls_20240320_a1b2c3d4e5f6",
    "title": "新闻标题",
    "content": "新闻正文...",
    "summary": "摘要",
    "url": "原文链接",
    "source": "cls",
    "category": "finance_flash",
    "publish_time": datetime,
    "collect_time": datetime,
    "ts_codes": ["600519.SH"],
    "tags": ["电报"],
    "content_hash": "md5...",
    "title_hash": "md5...",
    # 聚类后填充
    "event_id": "evt_xxx",
    "is_primary": True,
    "clustered_at": datetime,
    "vector": [0.12, ...],
    "vector_generated_at": datetime,
}
```

### MongoDB: news_events

```python
{
    "id": "evt_a1b2c3d4e5f6",
    "title": "事件标题 (LLM生成)",
    "summary": "事件摘要",
    "importance": "high/medium/low",
    "category": "policy/company/industry/market",
    "fingerprint": {
        "subject": "事件主体",
        "action": "核心动作",
        "time_ref": "时间参照",
        "keywords": ["关键词"],
    },
    "fingerprint_hash": "md5...",
    "primary_news_id": "cls_20240320_xxx",
    "related_news_ids": ["jin10_20240320_xxx", ...],
    "news_count": 5,
    "first_report_time": datetime,
    "last_update_time": datetime,
    "sources": ["cls", "jin10", "wallstreetcn"],
}
```

### Milvus: semantic_memory

只存储主新闻的向量：

```python
{
    "id": "cls_20240320_a1b2c3d4e5f6",
    "vector": [0.12, -0.34, ...],  # 1536维
    "content": "事件标题",
    "source": "cls,jin10",
    "category": "finance_flash",
    "event_id": "evt_xxx",
    "importance_score": 0.8,
}
```

## 新闻分类

```python
from src.collector import NewsCategory

# 财经
NewsCategory.FINANCE_FLASH     # 财经快讯
NewsCategory.FINANCE_ARTICLE   # 财经文章
NewsCategory.FINANCE_REPORT    # 研究报告

# 政策
NewsCategory.POLICY_RELEASE    # 政策发布
NewsCategory.POLICY_INTERPRET  # 政策解读
NewsCategory.POLICY_NOTICE     # 通知公告
NewsCategory.POLICY_STANDARD   # 行业标准

# 公司
NewsCategory.COMPANY_ANNOUNCE  # 公司公告
NewsCategory.COMPANY_NEWS      # 公司新闻

# 行业
NewsCategory.INDUSTRY_NEWS     # 行业新闻
NewsCategory.INDUSTRY_ANALYSIS # 行业分析
```

## 与 RAG 集成

采集的新闻经过事件聚类后，主新闻存入 `semantic_memory`，可通过 RAG 检索：

```python
from src.rag import rag_pipeline, RetrievalConfig

# 检索新闻
result = await rag_pipeline.query(
    user_id="user1",
    query="茅台最近有什么利好消息",
    config=RetrievalConfig(
        use_semantic_memory=True,
        ts_code="600519.SH",
    ),
)
```

## 分层存储

不同类型新闻有不同的生命周期：

```
┌─────────────────────────────────────────────────────────────┐
│  Hot (完整数据)  →  Warm (压缩数据)  →  Cold (删除)          │
└─────────────────────────────────────────────────────────────┘
```

### 保留策略

| 类型 | Hot | Warm | Cold | 总保留 |
|------|-----|------|------|--------|
| 财经快讯 | 1天 | 6天 | 删除 | 7天 |
| 财经文章 | 7天 | 23天 | 60天 | 90天 |
| 研究报告 | 30天 | 60天 | 275天 | 365天 |
| 政策发布 | 30天 | 150天 | 185天 | 365天 |
| 政策解读 | 14天 | 46天 | 30天 | 90天 |
| 公司公告 | 14天 | 76天 | 90天 | 180天 |
| 行业新闻 | 3天 | 11天 | 16天 | 30天 |
| 一般新闻 | 3天 | 11天 | 16天 | 30天 |

### 压缩策略

Hot → Warm 时删除大字段：
- `content` - 正文内容
- `vector` - 向量数据
- `extra` - 额外信息

保留字段：`title`, `summary`, `url`, `source`, `category`, `ts_codes`, `event_id`

### 使用

```python
from src.collector import NewsLifecycleManager, RETENTION_POLICIES

manager = NewsLifecycleManager()

# 执行生命周期管理
result = await manager.process_lifecycle()
print(f"压缩: {result.compressed}, 删除: {result.deleted}")

# 查看保留策略
policies = NewsLifecycleManager.get_retention_policies()
```

## 调度配置

环境变量：

```bash
# 多源新闻采集 (默认每分钟检查，内部按分组差异化调度)
SYNC_MULTI_SOURCE_NEWS_SCHEDULE="* * * * *"

# 事件聚类 (默认每30分钟)
SYNC_EVENT_CLUSTERING_SCHEDULE="*/30 * * * *"

# 数据生命周期管理 (默认每天凌晨3:00)
SYNC_NEWS_LIFECYCLE_SCHEDULE="0 3 * * *"
```
