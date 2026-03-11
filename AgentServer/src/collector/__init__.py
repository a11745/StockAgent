"""
新闻采集框架

提供新闻采集、去重、入库的基础框架。

特点:
1. 采集框架 - BaseSource / BaseNewsCollector 基类
2. 两阶段去重:
   - 快速去重: 内容哈希 + 标题相似度 (采集时)
   - 深度去重: LLM 事件聚类 (入库后异步)
3. 向量入库 - 存入 Milvus 供 RAG 检索

具体采集源实现在 nodes/data_sync/collectors/ 中。

Example:
    from nodes.data_sync.collectors import multi_source_collector
    
    # 采集所有源
    result = await multi_source_collector.collect()
    
    # 手动聚类
    from src.collector import EventClusterEngine
    engine = EventClusterEngine()
    result = await engine.process_pending_news()
"""

# 类型定义
from .types import (
    NewsItem,
    NewsCategory,
    NewsSource,
    CollectResult,
)
from .dedup import DeduplicationEngine
from .storage import NewsStorage
from .collector import BaseNewsCollector
from .sources import BaseSource
from .event_cluster import (
    EventClusterEngine,
    EventFingerprint,
    NewsEvent,
    EventImportance,
)
from .lifecycle import (
    NewsLifecycleManager,
    RetentionConfig,
    DataTier,
    RETENTION_POLICIES,
)

__all__ = [
    # 类型
    "NewsItem",
    "NewsCategory",
    "NewsSource",
    "CollectResult",
    # 去重
    "DeduplicationEngine",
    # 事件聚类
    "EventClusterEngine",
    "EventFingerprint",
    "NewsEvent",
    "EventImportance",
    # 生命周期管理
    "NewsLifecycleManager",
    "RetentionConfig",
    "DataTier",
    "RETENTION_POLICIES",
    # 存储
    "NewsStorage",
    # 采集器框架
    "BaseNewsCollector",
    # 采集源框架
    "BaseSource",
]
