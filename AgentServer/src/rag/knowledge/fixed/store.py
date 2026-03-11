"""
固定知识存储

使用 Milvus 存储公共知识。
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ..types import (
    FixedKnowledgeItem,
    FixedKnowledgeCategory,
    KnowledgeSearchResult,
)
from ....memory.types import InsertResult


class FixedKnowledgeStore:
    """
    固定知识存储
    
    存储公共知识:
    - 大盘复盘规则
    - 策略因子解读
    - 技术分析知识 (筹码峰、蜡烛图等)
    
    Args:
        collection_name: Milvus 集合名
    
    Example:
        store = FixedKnowledgeStore()
        
        # 搜索筹码峰相关知识
        results = await store.search("筹码峰分析方法", category=FixedKnowledgeCategory.TECH_CHIP_PEAK)
        
        # 获取所有蜡烛图知识
        items = await store.get_by_category(FixedKnowledgeCategory.TECH_CANDLESTICK_SINGLE)
    """
    
    def __init__(
        self,
        collection_name: str = "fixed_knowledge",
    ):
        self.collection_name = collection_name
        self.logger = logging.getLogger("src.rag.knowledge.FixedKnowledgeStore")
        self._milvus_manager = None
        self._mongo_manager = None
        self._llm_manager = None
    
    async def _get_milvus(self):
        if self._milvus_manager is None:
            from core.managers import milvus_manager
            self._milvus_manager = milvus_manager
        return self._milvus_manager
    
    async def _get_mongo(self):
        if self._mongo_manager is None:
            from core.managers import mongo_manager
            self._mongo_manager = mongo_manager
        return self._mongo_manager
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def insert(
        self,
        item: FixedKnowledgeItem,
        trace_id: Optional[str] = None,
    ) -> bool:
        """插入单个知识项"""
        result = await self.insert_batch([item], trace_id)
        return result.success and len(result.inserted_ids) > 0
    
    async def insert_batch(
        self,
        items: List[FixedKnowledgeItem],
        trace_id: Optional[str] = None,
    ) -> InsertResult:
        """批量插入知识项"""
        if not items:
            return InsertResult(success=True, inserted_ids=[])
        
        milvus = await self._get_milvus()
        mongo = await self._get_mongo()
        llm = await self._get_llm()
        
        try:
            inserted_ids = []
            
            for item in items:
                # 确保有向量
                if not item.vector:
                    text = f"{item.title}\n{item.summary or ''}\n{item.content[:1000]}"
                    vectors = await llm.embedding([text])
                    if vectors:
                        item.vector = vectors[0]
                
                # 插入到 Milvus
                milvus_data = {
                    "id": item.id,
                    "title": item.title[:500],
                    "content": item.content[:5000],
                    "category": item.category.value,
                    "importance": item.importance,
                    "tags": ",".join(item.tags),
                }
                
                await milvus.insert(
                    collection=self.collection_name,
                    vectors=[item.vector],
                    metadata=[milvus_data],
                )
                
                # 插入完整数据到 MongoDB
                mongo_doc = {
                    "_id": item.id,
                    **item.model_dump(),
                    "vector": None,  # 不在 MongoDB 中存向量
                }
                
                try:
                    await mongo.insert_one(self.collection_name, mongo_doc)
                except Exception:
                    # 可能已存在，尝试更新
                    await mongo.update_one(
                        self.collection_name,
                        {"_id": item.id},
                        {"$set": mongo_doc},
                        upsert=True,
                    )
                
                inserted_ids.append(item.id)
            
            self.logger.info(f"[{trace_id}] Inserted {len(inserted_ids)} fixed knowledge items")
            
            return InsertResult(
                success=True,
                inserted_ids=inserted_ids,
            )
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Insert failed: {e}")
            return InsertResult(success=False, message=str(e))
    
    async def search(
        self,
        query: str,
        top_k: int = 10,
        category: Optional[FixedKnowledgeCategory] = None,
        categories: Optional[List[FixedKnowledgeCategory]] = None,
        tags: Optional[List[str]] = None,
        importance: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> KnowledgeSearchResult:
        """
        语义搜索知识
        
        Args:
            query: 查询文本
            top_k: 返回数量
            category: 分类过滤
            categories: 多分类过滤
            tags: 标签过滤
            importance: 重要性过滤
            trace_id: 追踪ID
            
        Returns:
            搜索结果
        """
        milvus = await self._get_milvus()
        mongo = await self._get_mongo()
        llm = await self._get_llm()
        
        try:
            start_time = datetime.utcnow()
            
            # 生成查询向量
            vectors = await llm.embedding([query])
            if not vectors:
                return KnowledgeSearchResult()
            
            query_vector = vectors[0]
            
            # 构建过滤表达式
            filter_parts = []
            
            if category:
                filter_parts.append(f'category == "{category.value}"')
            elif categories:
                cats = ", ".join([f'"{c.value}"' for c in categories])
                filter_parts.append(f'category in [{cats}]')
            
            if importance:
                filter_parts.append(f'importance == "{importance}"')
            
            filter_expr = " and ".join(filter_parts) if filter_parts else None
            
            # Milvus 搜索
            results = await milvus.search(
                collection=self.collection_name,
                query_vector=query_vector,
                top_k=top_k,
                filter_expr=filter_expr,
                output_fields=["id", "title", "content", "category", "importance", "tags"],
            )
            
            # 从 MongoDB 获取完整数据
            items = []
            for hit in results:
                entity = hit.get("entity", {})
                item_id = entity.get("id", "")
                
                # 从 MongoDB 获取完整内容
                doc = await mongo.find_one(self.collection_name, {"_id": item_id})
                
                if doc:
                    item = FixedKnowledgeItem(
                        id=doc["_id"],
                        title=doc.get("title", ""),
                        content=doc.get("content", ""),
                        category=FixedKnowledgeCategory(doc.get("category", "")),
                        tags=doc.get("tags", []),
                        importance=doc.get("importance", "medium"),
                        summary=doc.get("summary"),
                        key_points=doc.get("key_points", []),
                        examples=doc.get("examples", []),
                        source_file=doc.get("source_file"),
                        score=hit.get("distance", 0),
                    )
                else:
                    # MongoDB 没有，使用 Milvus 返回的数据
                    item = FixedKnowledgeItem(
                        id=item_id,
                        title=entity.get("title", ""),
                        content=entity.get("content", ""),
                        category=FixedKnowledgeCategory(entity.get("category", "")),
                        tags=entity.get("tags", "").split(",") if entity.get("tags") else [],
                        importance=entity.get("importance", "medium"),
                        score=hit.get("distance", 0),
                    )
                
                items.append(item)
            
            # 标签过滤 (后处理)
            if tags:
                items = [
                    item for item in items
                    if any(tag in item.tags for tag in tags)
                ]
            
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return KnowledgeSearchResult(
                items=items,
                fixed_items=items,
                total_count=len(items),
                query_time_ms=elapsed_ms,
            )
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Search failed: {e}")
            return KnowledgeSearchResult()
    
    async def get_by_category(
        self,
        category: FixedKnowledgeCategory,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[FixedKnowledgeItem]:
        """获取指定分类的所有知识"""
        mongo = await self._get_mongo()
        
        try:
            docs = await mongo.find(
                self.collection_name,
                {"category": category.value},
                limit=limit,
            )
            
            items = []
            for doc in docs:
                item = FixedKnowledgeItem(
                    id=doc["_id"],
                    title=doc.get("title", ""),
                    content=doc.get("content", ""),
                    category=FixedKnowledgeCategory(doc.get("category", "")),
                    tags=doc.get("tags", []),
                    importance=doc.get("importance", "medium"),
                    summary=doc.get("summary"),
                    key_points=doc.get("key_points", []),
                    examples=doc.get("examples", []),
                    source_file=doc.get("source_file"),
                )
                items.append(item)
            
            return items
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get by category failed: {e}")
            return []
    
    async def get_by_id(
        self,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[FixedKnowledgeItem]:
        """根据 ID 获取知识项"""
        mongo = await self._get_mongo()
        
        try:
            doc = await mongo.find_one(self.collection_name, {"_id": item_id})
            
            if doc:
                return FixedKnowledgeItem(
                    id=doc["_id"],
                    title=doc.get("title", ""),
                    content=doc.get("content", ""),
                    category=FixedKnowledgeCategory(doc.get("category", "")),
                    tags=doc.get("tags", []),
                    importance=doc.get("importance", "medium"),
                    summary=doc.get("summary"),
                    key_points=doc.get("key_points", []),
                    examples=doc.get("examples", []),
                    source_file=doc.get("source_file"),
                )
            return None
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get by id failed: {e}")
            return None
    
    async def get_related(
        self,
        item_id: str,
        top_k: int = 5,
        trace_id: Optional[str] = None,
    ) -> List[FixedKnowledgeItem]:
        """获取相关知识"""
        item = await self.get_by_id(item_id, trace_id)
        
        if not item:
            return []
        
        # 基于内容搜索相关
        result = await self.search(
            query=f"{item.title} {item.summary or ''}",
            top_k=top_k + 1,
            category=item.category,
            trace_id=trace_id,
        )
        
        # 排除自身
        return [i for i in result.items if i.id != item_id][:top_k]
    
    async def count(self, trace_id: Optional[str] = None) -> int:
        """获取知识总数"""
        mongo = await self._get_mongo()
        
        try:
            return await mongo.count(self.collection_name, {})
        except Exception:
            return 0
    
    async def clear(self, trace_id: Optional[str] = None) -> bool:
        """清空知识库"""
        milvus = await self._get_milvus()
        mongo = await self._get_mongo()
        
        try:
            await mongo.delete_many(self.collection_name, {})
            # Milvus 集合重建在下次插入时自动处理
            self.logger.info(f"[{trace_id}] Cleared fixed knowledge store")
            return True
        except Exception as e:
            self.logger.error(f"[{trace_id}] Clear failed: {e}")
            return False
    
    async def list_categories(
        self,
        trace_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """列出所有分类及其计数"""
        mongo = await self._get_mongo()
        
        try:
            # 聚合统计
            pipeline = [
                {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
            
            results = await mongo.aggregate(self.collection_name, pipeline)
            
            return {doc["_id"]: doc["count"] for doc in results}
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] List categories failed: {e}")
            return {}
