"""
向量检索器

实现基于向量相似度的检索逻辑。
"""

import logging
from typing import Any, Dict, List, Optional

from ..memory import MemoryItem, AbstractStore


class VectorRetriever:
    """
    向量检索器
    
    基于向量相似度从存储中检索相关内容。
    
    Args:
        store: 存储后端实例
        default_top_k: 默认返回数量
        score_threshold: 相似度阈值，低于此值的结果将被过滤
    
    Example:
        retriever = VectorRetriever(store=MilvusStore("news"))
        results = await retriever.retrieve("市场行情分析", top_k=10)
    """
    
    def __init__(
        self,
        store: AbstractStore,
        default_top_k: int = 10,
        score_threshold: float = 0.0,
    ):
        self.store = store
        self.default_top_k = default_top_k
        self.score_threshold = score_threshold
        self.logger = logging.getLogger("src.rag.VectorRetriever")
        self._llm_manager = None
    
    async def _get_llm_manager(self):
        """延迟导入 llm_manager"""
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    def _log(self, level: str, message: str, trace_id: Optional[str] = None) -> None:
        """带 trace_id 的日志记录"""
        prefix = f"[{trace_id}] " if trace_id else ""
        log_method = getattr(self.logger, level, self.logger.info)
        log_method(f"{prefix}{message}")
    
    async def retrieve(
        self,
        query: str,
        top_k: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> List[MemoryItem]:
        """
        根据文本查询检索相关内容
        
        Args:
            query: 查询文本
            top_k: 返回数量
            filters: 过滤条件
            trace_id: 分布式追踪 ID
            
        Returns:
            相关的记忆项列表，按相似度排序
        """
        top_k = top_k or self.default_top_k
        
        self._log("debug", f"Retrieving for query: {query[:50]}...", trace_id)
        
        # 生成查询向量
        llm_manager = await self._get_llm_manager()
        vectors = await llm_manager.embedding([query])
        query_vector = vectors[0]
        
        # 向量检索
        results = await self.retrieve_by_vector(
            query_vector=query_vector,
            top_k=top_k,
            filters=filters,
            trace_id=trace_id,
        )
        
        return results
    
    async def retrieve_by_vector(
        self,
        query_vector: List[float],
        top_k: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> List[MemoryItem]:
        """
        根据向量检索相关内容
        
        Args:
            query_vector: 查询向量
            top_k: 返回数量
            filters: 过滤条件
            trace_id: 分布式追踪 ID
            
        Returns:
            相关的记忆项列表，按相似度排序
        """
        top_k = top_k or self.default_top_k
        
        # 从存储检索
        results = await self.store.search(
            query_vector=query_vector,
            top_k=top_k,
            filters=filters,
            trace_id=trace_id,
        )
        
        # 应用相似度阈值过滤
        if self.score_threshold > 0:
            results = [r for r in results if (r.score or 0) >= self.score_threshold]
        
        self._log("debug", f"Retrieved {len(results)} items", trace_id)
        
        return results
    
    async def retrieve_multi(
        self,
        queries: List[str],
        top_k_per_query: int = 5,
        deduplicate: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> List[MemoryItem]:
        """
        多查询检索并合并结果
        
        Args:
            queries: 查询文本列表
            top_k_per_query: 每个查询返回的数量
            deduplicate: 是否去重
            filters: 过滤条件
            trace_id: 分布式追踪 ID
            
        Returns:
            合并后的记忆项列表
        """
        all_results = []
        seen_ids = set()
        
        for query in queries:
            results = await self.retrieve(
                query=query,
                top_k=top_k_per_query,
                filters=filters,
                trace_id=trace_id,
            )
            
            for item in results:
                if deduplicate:
                    if item.id not in seen_ids:
                        seen_ids.add(item.id)
                        all_results.append(item)
                else:
                    all_results.append(item)
        
        # 按分数排序
        all_results.sort(key=lambda x: x.score or 0, reverse=True)
        
        self._log("debug", f"Multi-query retrieved {len(all_results)} items", trace_id)
        
        return all_results
