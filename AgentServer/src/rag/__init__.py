"""
RAG 检索增强生成 (Retrieval-Augmented Generation)

提供基于向量相似度的检索和上下文组装功能。

模块结构:
- retriever: 基础向量检索
- pipeline: 原始 RAG 流程
- knowledge: 知识库 (固定知识 + 用户知识)
- unified: 统一 RAG Pipeline (整合知识库和记忆系统)

Example:
    from src.rag import rag_pipeline, RetrievalConfig
    
    # 统一 RAG 查询
    result = await rag_pipeline.query(
        user_id="user1",
        query="如何分析筹码峰",
    )
    
    # 技术分析查询
    result = await rag_pipeline.query_technical(
        user_id="user1",
        query="这根K线是什么形态",
    )
    
    # 复盘辅助
    result = await rag_pipeline.query_review(
        user_id="user1",
        query="帮我复盘这笔交易",
        ts_code="600519.SH",
    )
"""

# 基础组件
from .retriever import VectorRetriever
from .pipeline import RAGPipeline

# 知识库
from .knowledge import (
    # 类型
    KnowledgeCategory,
    FixedKnowledgeCategory,
    UserKnowledgeType,
    KnowledgeItem,
    FixedKnowledgeItem,
    UserKnowledgeItem,
    # 固定知识
    FixedKnowledgeStore,
    KnowledgeLoader,
    # 用户知识
    UserKnowledgeStore,
    KnowledgeAssistant,
)

# 统一 Pipeline
from .unified import (
    RetrievalSource,
    RetrievalConfig,
    RetrievalItem,
    RAGResult,
    UnifiedRAGPipeline,
    rag_pipeline,
)

__all__ = [
    # 基础
    "VectorRetriever",
    "RAGPipeline",
    # 知识库类型
    "KnowledgeCategory",
    "FixedKnowledgeCategory",
    "UserKnowledgeType",
    "KnowledgeItem",
    "FixedKnowledgeItem",
    "UserKnowledgeItem",
    # 固定知识
    "FixedKnowledgeStore",
    "KnowledgeLoader",
    # 用户知识
    "UserKnowledgeStore",
    "KnowledgeAssistant",
    # 统一 Pipeline
    "RetrievalSource",
    "RetrievalConfig",
    "RetrievalItem",
    "RAGResult",
    "UnifiedRAGPipeline",
    "rag_pipeline",
]
