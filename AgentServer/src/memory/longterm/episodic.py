"""
情景记忆存储

存储用户个人经历和交互历史。
使用 Milvus + MongoDB 实现。
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


class EpisodicStore:
    """
    情景记忆存储
    
    存储用户个人经历:
    - 分析记录 (用户问过什么，系统回答了什么)
    - 交易决策 (用户做过的交易决策及其结果)
    - 重要事件 (用户标记的重要信息)
    
    特点:
    - 按用户严格隔离
    - 支持时间线检索
    - 支持向量检索
    
    Args:
        collection_name: Milvus 集合名
        mongo_collection: MongoDB 集合名
    
    Example:
        store = EpisodicStore()
        
        # 存储分析记录
        await store.insert(user_id, [analysis_item])
        
        # 检索相关经历
        results = await store.search(user_id, "上次分析茅台的结论", top_k=5)
    """
    
    def __init__(
        self,
        collection_name: str = "episodic_memory",
        mongo_collection: str = "episodic_memory",
    ):
        self.collection_name = collection_name
        self.mongo_collection = mongo_collection
        self.logger = logging.getLogger("src.memory.longterm.EpisodicStore")
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
        user_id: str,
        items: List[LongTermMemoryItem],
        trace_id: Optional[str] = None,
    ) -> InsertResult:
        """
        插入情景记忆
        
        Args:
            user_id: 用户ID
            items: 记忆项列表
            trace_id: 追踪ID
            
        Returns:
            插入结果
        """
        if not items:
            return InsertResult(success=True, inserted_ids=[])
        
        milvus = await self._get_milvus()
        mongo = await self._get_mongo()
        llm = await self._get_llm()
        
        try:
            inserted_ids = []
            failed_count = 0
            
            for item in items:
                # 确保用户ID正确
                item.metadata.user_id = user_id
                item.metadata.visibility = MemoryVisibility.PRIVATE
                
                # 生成向量
                if not item.vector:
                    vectors = await llm.embedding([item.content])
                    if vectors:
                        item.vector = vectors[0]
                    else:
                        failed_count += 1
                        continue
                
                # 存储到 Milvus (用于向量检索)
                milvus_metadata = {
                    "id": item.id,
                    "user_id": user_id,
                    "content": item.content[:500],
                    "ts_code": item.metadata.ts_code or "",
                    "category": item.metadata.category or "",
                    "created_at": item.metadata.created_at.isoformat(),
                    "importance_score": item.metadata.importance_score,
                }
                
                await milvus.insert(
                    collection=self.collection_name,
                    vectors=[item.vector],
                    metadata=[milvus_metadata],
                )
                
                # 存储完整数据到 MongoDB
                mongo_doc = {
                    "_id": item.id,
                    "user_id": user_id,
                    "content": item.content,
                    "subtype": item.subtype.value,
                    "metadata": item.metadata.model_dump(),
                    "relationships": item.relationships,
                    "created_at": item.metadata.created_at,
                    "updated_at": datetime.utcnow(),
                }
                
                await mongo.insert_one(self.mongo_collection, mongo_doc)
                inserted_ids.append(item.id)
            
            self.logger.info(
                f"[{trace_id}] Inserted {len(inserted_ids)} episodic memories for user {user_id}"
            )
            
            return InsertResult(
                success=True,
                inserted_ids=inserted_ids,
                failed_count=failed_count,
            )
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Insert failed: {e}")
            return InsertResult(success=False, message=str(e))
    
    async def search(
        self,
        user_id: str,
        query: str,
        top_k: int = 10,
        ts_code: Optional[str] = None,
        category: Optional[str] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        trace_id: Optional[str] = None,
    ) -> SearchResult:
        """
        检索情景记忆
        
        Args:
            user_id: 用户ID (严格隔离)
            query: 查询文本
            top_k: 返回数量
            ts_code: 股票代码过滤
            category: 分类过滤
            date_from: 开始日期
            date_to: 结束日期
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
            
            # 构建过滤表达式 (必须包含 user_id)
            filter_parts = [f'user_id == "{user_id}"']
            
            if ts_code:
                filter_parts.append(f'ts_code == "{ts_code}"')
            
            if category:
                filter_parts.append(f'category == "{category}"')
            
            filter_expr = " and ".join(filter_parts)
            
            # 执行向量检索
            results = await milvus.search(
                collection=self.collection_name,
                query_vector=query_vector,
                top_k=top_k,
                filter_expr=filter_expr,
                output_fields=["id", "content", "ts_code", "category", 
                              "created_at", "importance_score"],
            )
            
            # 转换结果
            items = []
            for hit in results:
                entity = hit.get("entity", {})
                item = LongTermMemoryItem(
                    id=entity.get("id", ""),
                    memory_type="long_term",
                    subtype=LongTermMemoryType.EPISODIC,
                    content=entity.get("content", ""),
                    score=hit.get("distance", 0),
                    metadata=MemoryMetadata(
                        user_id=user_id,
                        visibility=MemoryVisibility.PRIVATE,
                        ts_code=entity.get("ts_code"),
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
    
    async def get_timeline(
        self,
        user_id: str,
        limit: int = 20,
        ts_code: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> List[LongTermMemoryItem]:
        """
        获取时间线 (按时间倒序)
        
        Args:
            user_id: 用户ID
            limit: 返回数量
            ts_code: 股票代码过滤
            trace_id: 追踪ID
            
        Returns:
            按时间排序的情景记忆列表
        """
        mongo = await self._get_mongo()
        
        try:
            query = {"user_id": user_id}
            if ts_code:
                query["metadata.ts_code"] = ts_code
            
            docs = await mongo.find(
                self.mongo_collection,
                query,
                sort=[("created_at", -1)],
                limit=limit,
            )
            
            items = []
            for doc in docs:
                item = LongTermMemoryItem(
                    id=doc["_id"],
                    memory_type="long_term",
                    subtype=LongTermMemoryType.EPISODIC,
                    content=doc["content"],
                    relationships=doc.get("relationships", []),
                    metadata=MemoryMetadata(**doc["metadata"]),
                )
                items.append(item)
            
            return items
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get timeline failed: {e}")
            return []
    
    async def get_by_id(
        self,
        user_id: str,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[LongTermMemoryItem]:
        """获取单条情景记忆"""
        mongo = await self._get_mongo()
        
        try:
            doc = await mongo.find_one(
                self.mongo_collection,
                {"_id": item_id, "user_id": user_id},
            )
            
            if doc:
                return LongTermMemoryItem(
                    id=doc["_id"],
                    memory_type="long_term",
                    subtype=LongTermMemoryType.EPISODIC,
                    content=doc["content"],
                    relationships=doc.get("relationships", []),
                    metadata=MemoryMetadata(**doc["metadata"]),
                )
            return None
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get by id failed: {e}")
            return None
    
    async def update_importance(
        self,
        user_id: str,
        item_id: str,
        importance: float,
        trace_id: Optional[str] = None,
    ) -> bool:
        """更新记忆重要性"""
        mongo = await self._get_mongo()
        
        try:
            result = await mongo.update_one(
                self.mongo_collection,
                {"_id": item_id, "user_id": user_id},
                {"$set": {
                    "metadata.importance_score": importance,
                    "updated_at": datetime.utcnow(),
                }},
            )
            return result.modified_count > 0
        except Exception as e:
            self.logger.error(f"[{trace_id}] Update importance failed: {e}")
            return False
    
    async def delete(
        self,
        user_id: str,
        item_ids: List[str],
        trace_id: Optional[str] = None,
    ) -> int:
        """删除情景记忆"""
        milvus = await self._get_milvus()
        mongo = await self._get_mongo()
        
        try:
            # 删除 Milvus 中的向量
            await milvus.delete(collection=self.collection_name, ids=item_ids)
            
            # 删除 MongoDB 中的文档
            result = await mongo.delete_many(
                self.mongo_collection,
                {"_id": {"$in": item_ids}, "user_id": user_id},
            )
            
            self.logger.info(f"[{trace_id}] Deleted {result.deleted_count} episodic memories")
            return result.deleted_count
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Delete failed: {e}")
            return 0
