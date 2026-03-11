"""
记忆系统 (Memory System)

基于认知心理学的三层记忆架构:
1. 感觉记忆 (Sensory): 秒级 TTL，处理实时数据流
2. 工作记忆 (Working): 分钟级 TTL，容量有限，当前任务处理
3. 长期记忆 (Long-term): 持久存储，分为语义、情景、程序性三种

存储技术:
- Redis: 感觉记忆 (Stream)，工作记忆 (Hash + Sorted Set)
- Milvus: 长期记忆向量检索
- MongoDB: 长期记忆元数据
- Neo4j: 知识图谱 (未来扩展)

多用户隔离:
- 感觉/工作记忆: 按 user_id 完全隔离
- 情景记忆: 按 user_id 完全隔离
- 语义记忆: 公共可见 (新闻/公告)
- 程序性记忆: 按 user_id 隔离 (个人交易体系)

核心功能:
- 记忆存储和检索
- 注意力过滤 (感觉 → 工作)
- 记忆巩固 (工作 → 长期)
- 遗忘机制 (TTL/访问衰减/重要性衰减)
- 交易体系分析 (程序性记忆)
"""

# 类型定义
from .types import (
    MemoryType,
    LongTermMemoryType,
    MemoryVisibility,
    DecayStrategy,
    MemoryMetadata,
    StockMetadata,
    HoldingMetadata,
    TradingPatternMetadata,
    BaseMemoryItem,
    SensoryMemoryItem,
    WorkingMemoryItem,
    LongTermMemoryItem,
    InsertResult,
    SearchResult,
    ConsolidationResult,
    DecayResult,
)

# 向后兼容别名
MemoryItem = BaseMemoryItem

# 存储后端 (向后兼容)
from .storage import AbstractStore, MilvusStore

# 感觉记忆
from .sensory import SensoryStream, AttentionGate

# 工作记忆
from .working import WorkingBuffer, ContextWindow

# 长期记忆
from .longterm import (
    SemanticStore,
    EpisodicStore,
    ProceduralStore,
    TradingPattern,
)

# 记忆流程
from .consolidation import ConsolidationEngine
from .decay import DecayEngine
from .retrieval import UnifiedRetriever, RetrievalQuery, RetrievalResult

# 统一管理器
from .manager import MemoryManager, memory_manager

__all__ = [
    # 类型
    "MemoryType",
    "LongTermMemoryType",
    "MemoryVisibility",
    "DecayStrategy",
    "MemoryMetadata",
    "StockMetadata",
    "HoldingMetadata",
    "TradingPatternMetadata",
    "BaseMemoryItem",
    "SensoryMemoryItem",
    "WorkingMemoryItem",
    "LongTermMemoryItem",
    "InsertResult",
    "SearchResult",
    "ConsolidationResult",
    "DecayResult",
    
    # 向后兼容别名
    "MemoryItem",
    "AbstractStore",
    "MilvusStore",
    
    # 感觉记忆
    "SensoryStream",
    "AttentionGate",
    
    # 工作记忆
    "WorkingBuffer",
    "ContextWindow",
    
    # 长期记忆
    "SemanticStore",
    "EpisodicStore",
    "ProceduralStore",
    "TradingPattern",
    
    # 记忆流程
    "ConsolidationEngine",
    "DecayEngine",
    "UnifiedRetriever",
    "RetrievalQuery",
    "RetrievalResult",
    
    # 管理器
    "MemoryManager",
    "memory_manager",
]
