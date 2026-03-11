"""
RAG 知识库模块

包含:
1. 固定知识库 (Fixed): 公共知识，系统维护
2. 用户知识库 (User): 用户自定义知识，按用户隔离
"""

from .types import (
    KnowledgeCategory,
    FixedKnowledgeCategory,
    UserKnowledgeType,
    KnowledgeItem,
    FixedKnowledgeItem,
    UserKnowledgeItem,
    KnowledgeSearchResult,
    KnowledgeLoadResult,
)
from .fixed.store import FixedKnowledgeStore
from .fixed.loader import KnowledgeLoader
from .user.store import UserKnowledgeStore
from .user.assistant import KnowledgeAssistant

__all__ = [
    # 枚举
    "KnowledgeCategory",
    "FixedKnowledgeCategory",
    "UserKnowledgeType",
    # 类型
    "KnowledgeItem",
    "FixedKnowledgeItem",
    "UserKnowledgeItem",
    "KnowledgeSearchResult",
    "KnowledgeLoadResult",
    # 固定知识
    "FixedKnowledgeStore",
    "KnowledgeLoader",
    # 用户知识
    "UserKnowledgeStore",
    "KnowledgeAssistant",
]
