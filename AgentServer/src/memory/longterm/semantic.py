"""
语义记忆存储

存储公共知识（新闻、研报、公司信息等）。
使用 Milvus 进行向量检索。
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ..types import (
    LongTermMemoryItem,
    LongTermMemoryType,
    MemoryMetadata,
    MemoryVisibility,
    InsertResult,
    SearchResult,
    DecayStrategy,
)


class SemanticStore:
    """
    语义记忆存储
    
    存储公共知识型内容:
    - 新闻资讯
    - 研究报告
    - 公司基本面
    - 行业知识
    
    特点:
    - 多为公共可见
    - 向量检索为主
    - 不按用户隔离（或弱隔离）
    
    Args:
        collection_name: Milvus 集合名
    
    Example:
        store = SemanticStore()
        
        # 存储新闻
        await store.insert([news_item1, news_item2])
        
        # 向量检索
        results = await store.search(query_vector, top_k=10, ts_code="000001.SZ")
    """
    
    def __init__(
        self,
        collection_name: str = "semantic_memory",
    ):
        self.collection_name = collection_name
        self.logger = logging.getLogger("src.memory.longterm.SemanticStore")
        self._milvus_manager = None
        self._llm_manager = None
    
    async def _get_milvus(self):
        """延迟导入 milvus_manager"""
        if self._milvus_manager is None:
            from core.managers import milvus_manager
            self._milvus_manager = milvus_manager
        return self._milvus_manager
    
    async def _get_llm(self):
        """延迟导入 llm_manager"""
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def insert(
        self,
        items: List[LongTermMemoryItem],
        trace_id: Optional[str] = None,
    ) -> InsertResult:
        """
        插入语义记忆
        
        Args:
            items: 长期记忆项列表
            trace_id: 追踪ID
            
        Returns:
            插入结果
        """
        if not items:
            return InsertResult(success=True, inserted_ids=[], message="No items to insert")
        
        milvus = await self._get_milvus()
        llm = await self._get_llm()
        
        try:
            # 准备数据
            inserted_ids = []
            failed_count = 0
            
            for item in items:
                # 确保有向量
                if not item.vector:
                    vectors = await llm.embedding([item.content])
                    if vectors:
                        item.vector = vectors[0]
                    else:
                        failed_count += 1
                        continue
                
                # 准备元数据
                metadata = {
                    "id": item.id,
                    "content": item.content[:500],  # 截断以避免过长
                    "user_id": item.metadata.user_id,
                    "visibility": item.metadata.visibility.value,
                    "ts_code": item.metadata.ts_code or "",
                    "source": item.metadata.source or "",
                    "category": item.metadata.category or "",
                    "publish_date": getattr(item.metadata, "publish_date", "") or "",
                    "created_at": item.metadata.created_at.isoformat(),
                    "importance_score": item.metadata.importance_score,
                }
                
                # 插入到 Milvus
                await milvus.insert(
                    collection=self.collection_name,
                    vectors=[item.vector],
                    metadata=[metadata],
                )
                inserted_ids.append(item.id)
            
            self.logger.info(
                f"[{trace_id}] Inserted {len(inserted_ids)} semantic memories, "
                f"failed: {failed_count}"
            )
            
            return InsertResult(
                success=True,
                inserted_ids=inserted_ids,
                failed_count=failed_count,
                message=f"Inserted {len(inserted_ids)} items",
            )
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Insert failed: {e}")
            return InsertResult(
                success=False,
                message=str(e),
                failed_count=len(items),
            )
    
    async def search(
        self,
        query: str,
        top_k: int = 10,
        ts_code: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        source: Optional[str] = None,
        category: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        min_importance: float = 0.0,
        trace_id: Optional[str] = None,
    ) -> SearchResult:
        """
        语义检索
        
        Args:
            query: 查询文本
            top_k: 返回数量
            ts_code: 股票代码过滤
            ts_codes: 股票代码列表过滤
            source: 来源过滤
            category: 分类过滤
            date_from: 开始日期 (YYYYMMDD)
            date_to: 结束日期 (YYYYMMDD)
            min_importance: 最小重要性
            trace_id: 追踪ID
            
        Returns:
            检索结果
        """
        milvus = await self._get_milvus()
        llm = await self._get_llm()
        
        try:
            start_time = datetime.utcnow()
            
            # 生成查询向量
            vectors = await llm.embedding([query])
            if not vectors:
                return SearchResult(items=[], total_count=0)
            
            query_vector = vectors[0]
            
            # 构建过滤表达式
            filter_parts = []
            
            if ts_code:
                filter_parts.append(f'ts_code == "{ts_code}"')
            elif ts_codes:
                codes_str = ", ".join([f'"{c}"' for c in ts_codes])
                filter_parts.append(f'ts_code in [{codes_str}]')
            
            if source:
                filter_parts.append(f'source == "{source}"')
            
            if category:
                filter_parts.append(f'category == "{category}"')
            
            if date_from:
                filter_parts.append(f'publish_date >= "{date_from}"')
            
            if date_to:
                filter_parts.append(f'publish_date <= "{date_to}"')
            
            if min_importance > 0:
                filter_parts.append(f'importance_score >= {min_importance}')
            
            filter_expr = " and ".join(filter_parts) if filter_parts else None
            
            # 执行检索
            results = await milvus.search(
                collection=self.collection_name,
                query_vector=query_vector,
                top_k=top_k,
                filter_expr=filter_expr,
                output_fields=["id", "content", "user_id", "ts_code", "source", 
                              "category", "publish_date", "importance_score"],
            )
            
            # 转换为 LongTermMemoryItem
            items = []
            for hit in results:
                entity = hit.get("entity", {})
                item = LongTermMemoryItem(
                    id=entity.get("id", ""),
                    memory_type="long_term",
                    subtype=LongTermMemoryType.SEMANTIC,
                    content=entity.get("content", ""),
                    score=hit.get("distance", 0),
                    metadata=MemoryMetadata(
                        user_id=entity.get("user_id", "system"),
                        visibility=MemoryVisibility.PUBLIC,
                        ts_code=entity.get("ts_code"),
                        source=entity.get("source"),
                        category=entity.get("category"),
                        importance_score=entity.get("importance_score", 0.5),
                    ),
                )
                items.append(item)
            
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return SearchResult(
                items=items,
                total_count=len(items),
                query_time_ms=elapsed_ms,
            )
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Search failed: {e}")
            return SearchResult(items=[], total_count=0)
    
    async def delete_by_ids(
        self,
        ids: List[str],
        trace_id: Optional[str] = None,
    ) -> int:
        """删除指定 ID 的记忆"""
        milvus = await self._get_milvus()
        
        try:
            await milvus.delete(
                collection=self.collection_name,
                ids=ids,
            )
            self.logger.info(f"[{trace_id}] Deleted {len(ids)} semantic memories")
            return len(ids)
        except Exception as e:
            self.logger.error(f"[{trace_id}] Delete failed: {e}")
            return 0
    
    async def get_by_ts_code(
        self,
        ts_code: str,
        limit: int = 20,
        trace_id: Optional[str] = None,
    ) -> List[LongTermMemoryItem]:
        """根据股票代码获取相关记忆"""
        # 使用股票名称作为查询，获取相关内容
        result = await self.search(
            query=ts_code,
            top_k=limit,
            ts_code=ts_code,
            trace_id=trace_id,
        )
        return result.items
