"""
长期记忆 (Long-term Memory)

特点:
- 持续时间长 (可达永久)
- 容量几乎无限
- 分为三种子类型

子类型:
1. 语义记忆 (Semantic): 知识图谱、新闻语料 - Milvus + Neo4j
2. 情景记忆 (Episodic): 用户交互历史、分析记录 - Milvus + MongoDB
3. 程序性记忆 (Procedural): 交易体系、策略模式 - Neo4j + MongoDB
"""

from .semantic import SemanticStore
from .episodic import EpisodicStore
from .procedural import ProceduralStore, TradingPattern

__all__ = [
    "SemanticStore",
    "EpisodicStore",
    "ProceduralStore",
    "TradingPattern",
]
