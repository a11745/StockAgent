"""
固定知识库

存储公共知识:
- 大盘复盘规则
- 策略因子解读
- 技术分析知识 (筹码峰、蜡烛图等)
"""

from .store import FixedKnowledgeStore
from .loader import KnowledgeLoader

__all__ = [
    "FixedKnowledgeStore",
    "KnowledgeLoader",
]
