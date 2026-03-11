"""
统一记忆检索

提供跨层级的记忆检索能力，支持多种检索策略。
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass, field

from .types import (
    BaseMemoryItem,
    SensoryMemoryItem,
    WorkingMemoryItem,
    LongTermMemoryItem,
    LongTermMemoryType,
    MemoryType,
    SearchResult,
)
from .sensory import SensoryStream
from .working import WorkingBuffer
from .longterm import SemanticStore, EpisodicStore, ProceduralStore


@dataclass
class RetrievalQuery:
    """检索查询"""
    text: str                                     # 查询文本
    user_id: str                                  # 用户ID
    
    # 过滤条件
    ts_code: Optional[str] = None                 # 股票代码
    ts_codes: Optional[List[str]] = None          # 多个股票代码
    source: Optional[str] = None                  # 来源
    category: Optional[str] = None                # 分类
    date_from: Optional[str] = None               # 开始日期
    date_to: Optional[str] = None                 # 结束日期
    
    # 检索配置
    top_k: int = 10                               # 返回数量
    include_sensory: bool = False                 # 包含感觉记忆
    include_working: bool = True                  # 包含工作记忆
    include_semantic: bool = True                 # 包含语义记忆
    include_episodic: bool = True                 # 包含情景记忆
    include_procedural: bool = False              # 包含程序性记忆
    
    # 排序权重
    recency_weight: float = 0.3                   # 时间近因权重
    relevance_weight: float = 0.5                 # 相关性权重
    importance_weight: float = 0.2                # 重要性权重


@dataclass
class RetrievalResult:
    """检索结果"""
    items: List[BaseMemoryItem] = field(default_factory=list)
    total_count: int = 0
    query_time_ms: float = 0
    
    # 按来源分类
    from_working: List[WorkingMemoryItem] = field(default_factory=list)
    from_semantic: List[LongTermMemoryItem] = field(default_factory=list)
    from_episodic: List[LongTermMemoryItem] = field(default_factory=list)
    from_procedural: List[Dict] = field(default_factory=list)


class UnifiedRetriever:
    """
    统一记忆检索器
    
    跨越三层记忆进行检索，返回融合后的结果。
    
    检索流程:
    1. 并行检索工作记忆、语义记忆、情景记忆
    2. 融合去重
    3. 重排序 (结合相关性、时间、重要性)
    4. 返回 top_k 结果
    
    Example:
        retriever = UnifiedRetriever()
        
        query = RetrievalQuery(
            text="茅台的最新分析",
            user_id="user1",
            ts_code="600519.SH",
            top_k=10,
        )
        
        result = await retriever.search(query)
    """
    
    def __init__(self):
        self.logger = logging.getLogger("src.memory.UnifiedRetriever")
        
        # 存储组件
        self.sensory_stream = SensoryStream()
        self.working_buffer = WorkingBuffer()
        self.semantic_store = SemanticStore()
        self.episodic_store = EpisodicStore()
        self.procedural_store = ProceduralStore()
        
        self._llm_manager = None
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def search(
        self,
        query: RetrievalQuery,
        trace_id: Optional[str] = None,
    ) -> RetrievalResult:
        """
        执行统一检索
        
        Args:
            query: 检索查询
            trace_id: 追踪ID
            
        Returns:
            检索结果
        """
        start_time = datetime.utcnow()
        result = RetrievalResult()
        
        import asyncio
        
        try:
            # 并行检索
            tasks = []
            
            if query.include_working:
                tasks.append(self._search_working(query, trace_id))
            
            if query.include_semantic:
                tasks.append(self._search_semantic(query, trace_id))
            
            if query.include_episodic:
                tasks.append(self._search_episodic(query, trace_id))
            
            if query.include_procedural:
                tasks.append(self._search_procedural(query, trace_id))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 收集结果
            all_items = []
            
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                    self.logger.warning(f"[{trace_id}] Search task {i} failed: {res}")
                    continue
                
                items, source_type = res
                all_items.extend(items)
                
                # 按来源分类
                if source_type == "working":
                    result.from_working = items
                elif source_type == "semantic":
                    result.from_semantic = items
                elif source_type == "episodic":
                    result.from_episodic = items
                elif source_type == "procedural":
                    result.from_procedural = items
            
            # 去重和重排序
            merged = self._merge_and_rerank(all_items, query)
            
            result.items = merged[:query.top_k]
            result.total_count = len(merged)
            result.query_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            self.logger.debug(
                f"[{trace_id}] Unified search: {result.total_count} items, "
                f"{result.query_time_ms:.1f}ms"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Unified search failed: {e}")
            return result
    
    async def _search_working(
        self,
        query: RetrievalQuery,
        trace_id: Optional[str],
    ) -> tuple:
        """检索工作记忆"""
        items = await self.working_buffer.get_all(query.user_id, trace_id)
        
        # 简单的关键词过滤
        if query.text:
            keywords = query.text.lower().split()
            filtered = []
            for item in items:
                content_lower = item.content.lower()
                if any(kw in content_lower for kw in keywords):
                    filtered.append(item)
            items = filtered
        
        # 股票代码过滤
        if query.ts_code:
            items = [i for i in items if i.metadata.ts_code == query.ts_code]
        
        return items, "working"
    
    async def _search_semantic(
        self,
        query: RetrievalQuery,
        trace_id: Optional[str],
    ) -> tuple:
        """检索语义记忆"""
        result = await self.semantic_store.search(
            query=query.text,
            top_k=query.top_k,
            ts_code=query.ts_code,
            ts_codes=query.ts_codes,
            source=query.source,
            category=query.category,
            date_from=query.date_from,
            date_to=query.date_to,
            trace_id=trace_id,
        )
        
        return result.items, "semantic"
    
    async def _search_episodic(
        self,
        query: RetrievalQuery,
        trace_id: Optional[str],
    ) -> tuple:
        """检索情景记忆"""
        result = await self.episodic_store.search(
            user_id=query.user_id,
            query=query.text,
            top_k=query.top_k,
            ts_code=query.ts_code,
            category=query.category,
            trace_id=trace_id,
        )
        
        return result.items, "episodic"
    
    async def _search_procedural(
        self,
        query: RetrievalQuery,
        trace_id: Optional[str],
    ) -> tuple:
        """检索程序性记忆 (交易模式)"""
        patterns = await self.procedural_store.get_patterns(
            user_id=query.user_id,
            min_confidence=0.3,
            trace_id=trace_id,
        )
        
        # 转换为字典格式
        pattern_dicts = []
        for p in patterns:
            pattern_dicts.append({
                "id": p.id,
                "name": p.name,
                "type": p.pattern_type.value,
                "description": p.description,
                "conditions": p.conditions,
                "success_rate": p.success_rate,
                "avg_return": p.avg_return,
                "confidence": p.confidence,
            })
        
        return pattern_dicts, "procedural"
    
    def _merge_and_rerank(
        self,
        items: List[BaseMemoryItem],
        query: RetrievalQuery,
    ) -> List[BaseMemoryItem]:
        """融合去重并重排序"""
        if not items:
            return []
        
        # 去重 (按内容相似度)
        seen_ids = set()
        unique_items = []
        
        for item in items:
            if isinstance(item, dict):
                # 程序性记忆 (字典格式)
                item_id = item.get("id", "")
            else:
                item_id = item.id
            
            if item_id not in seen_ids:
                seen_ids.add(item_id)
                unique_items.append(item)
        
        # 计算综合分数
        scored_items = []
        now = datetime.utcnow()
        
        for item in unique_items:
            if isinstance(item, dict):
                # 程序性记忆
                score = item.get("confidence", 0.5)
            else:
                # 其他记忆类型
                # 相关性分数 (来自向量检索)
                relevance = item.score if item.score else 0.5
                
                # 时间近因分数
                created_at = item.metadata.created_at
                hours_ago = (now - created_at).total_seconds() / 3600
                recency = max(0, 1 - hours_ago / (24 * 7))  # 一周内衰减到 0
                
                # 重要性分数
                importance = item.metadata.importance_score
                
                # 综合分数
                score = (
                    query.relevance_weight * relevance +
                    query.recency_weight * recency +
                    query.importance_weight * importance
                )
                
                # 更新分数到 item
                item.score = score
            
            scored_items.append((score, item))
        
        # 排序
        scored_items.sort(key=lambda x: x[0], reverse=True)
        
        return [item for _, item in scored_items]
    
    async def search_by_stock(
        self,
        user_id: str,
        ts_code: str,
        top_k: int = 20,
        trace_id: Optional[str] = None,
    ) -> RetrievalResult:
        """按股票代码检索所有相关记忆"""
        query = RetrievalQuery(
            text=ts_code,
            user_id=user_id,
            ts_code=ts_code,
            top_k=top_k,
            include_semantic=True,
            include_episodic=True,
            include_procedural=True,
        )
        
        return await self.search(query, trace_id)
    
    async def search_similar(
        self,
        user_id: str,
        content: str,
        top_k: int = 10,
        trace_id: Optional[str] = None,
    ) -> RetrievalResult:
        """语义相似检索"""
        query = RetrievalQuery(
            text=content,
            user_id=user_id,
            top_k=top_k,
            include_semantic=True,
            include_episodic=True,
            relevance_weight=0.8,
            recency_weight=0.1,
            importance_weight=0.1,
        )
        
        return await self.search(query, trace_id)
    
    async def get_context_for_task(
        self,
        user_id: str,
        task_description: str,
        ts_codes: Optional[List[str]] = None,
        max_items: int = 10,
        trace_id: Optional[str] = None,
    ) -> str:
        """
        为任务获取相关上下文
        
        Args:
            user_id: 用户ID
            task_description: 任务描述
            ts_codes: 相关股票代码
            max_items: 最大返回数量
            trace_id: 追踪ID
            
        Returns:
            格式化的上下文文本
        """
        query = RetrievalQuery(
            text=task_description,
            user_id=user_id,
            ts_codes=ts_codes,
            top_k=max_items,
            include_working=True,
            include_semantic=True,
            include_episodic=True,
            include_procedural=True,
        )
        
        result = await self.search(query, trace_id)
        
        if not result.items:
            return ""
        
        # 格式化上下文
        sections = []
        
        # 工作记忆
        if result.from_working:
            working_text = "\n".join([
                f"- {item.content[:200]}..."
                for item in result.from_working[:3]
            ])
            sections.append(f"## 当前任务相关\n{working_text}")
        
        # 情景记忆 (个人经历)
        if result.from_episodic:
            episodic_text = "\n".join([
                f"- {item.content[:200]}..."
                for item in result.from_episodic[:3]
            ])
            sections.append(f"## 历史分析记录\n{episodic_text}")
        
        # 语义记忆 (公共知识)
        if result.from_semantic:
            semantic_text = "\n".join([
                f"- [{item.metadata.source or '知识'}] {item.content[:200]}..."
                for item in result.from_semantic[:3]
            ])
            sections.append(f"## 相关资讯\n{semantic_text}")
        
        # 程序性记忆 (交易模式)
        if result.from_procedural:
            pattern_text = "\n".join([
                f"- {p['name']}: {p['description'][:100]}... (成功率{p['success_rate']*100:.1f}%)"
                for p in result.from_procedural[:2]
            ])
            sections.append(f"## 交易模式参考\n{pattern_text}")
        
        return "\n\n".join(sections)
