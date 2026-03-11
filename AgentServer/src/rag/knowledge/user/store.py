"""
用户知识存储

存储用户自定义的交易知识和规则。
按用户隔离。
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ..types import (
    UserKnowledgeItem,
    UserKnowledgeType,
    KnowledgeSearchResult,
)
from ....memory.types import InsertResult


class UserKnowledgeStore:
    """
    用户知识存储
    
    存储用户自定义知识:
    - 交易规则
    - 个人策略
    - 复盘模板
    - 学习笔记
    
    特点:
    - 按用户完全隔离
    - 支持编辑和删除
    - 记录使用统计
    
    Args:
        milvus_collection: Milvus 集合名
        mongo_collection: MongoDB 集合名
    
    Example:
        store = UserKnowledgeStore()
        
        # 添加交易规则
        rule = UserKnowledgeItem(
            user_id="user1",
            knowledge_type=UserKnowledgeType.TRADING_RULE,
            title="我的入场条件",
            content="1. 突破年线...",
            conditions=["股价突破年线", "成交量放大"],
            actions=["买入 1/3 仓位"],
        )
        await store.create(rule)
        
        # 搜索用户知识
        results = await store.search("user1", "入场条件")
    """
    
    def __init__(
        self,
        milvus_collection: str = "user_knowledge",
        mongo_collection: str = "user_knowledge",
    ):
        self.milvus_collection = milvus_collection
        self.mongo_collection = mongo_collection
        self.logger = logging.getLogger("src.rag.knowledge.UserKnowledgeStore")
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
    
    # ==================== CRUD 操作 ====================
    
    async def create(
        self,
        item: UserKnowledgeItem,
        trace_id: Optional[str] = None,
    ) -> bool:
        """创建用户知识项"""
        milvus = await self._get_milvus()
        mongo = await self._get_mongo()
        llm = await self._get_llm()
        
        try:
            # 生成向量
            if not item.vector:
                text = f"{item.title}\n{item.content}"
                if item.conditions:
                    text += "\n条件: " + ", ".join(item.conditions)
                if item.actions:
                    text += "\n动作: " + ", ".join(item.actions)
                
                vectors = await llm.embedding([text])
                if vectors:
                    item.vector = vectors[0]
            
            # 插入 Milvus
            milvus_data = {
                "id": item.id,
                "user_id": item.user_id,
                "title": item.title[:500],
                "content": item.content[:2000],
                "knowledge_type": item.knowledge_type.value,
                "is_active": 1 if item.is_active else 0,
            }
            
            await milvus.insert(
                collection=self.milvus_collection,
                vectors=[item.vector],
                metadata=[milvus_data],
            )
            
            # 插入 MongoDB
            mongo_doc = {
                "_id": item.id,
                **item.model_dump(),
                "vector": None,
            }
            
            await mongo.insert_one(self.mongo_collection, mongo_doc)
            
            self.logger.info(f"[{trace_id}] Created user knowledge: {item.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Create failed: {e}")
            return False
    
    async def update(
        self,
        user_id: str,
        item_id: str,
        updates: Dict[str, Any],
        trace_id: Optional[str] = None,
    ) -> bool:
        """更新用户知识项"""
        mongo = await self._get_mongo()
        milvus = await self._get_milvus()
        llm = await self._get_llm()
        
        try:
            # 获取原项
            doc = await mongo.find_one(
                self.mongo_collection,
                {"_id": item_id, "user_id": user_id},
            )
            
            if not doc:
                return False
            
            # 更新字段
            updates["updated_at"] = datetime.utcnow()
            
            await mongo.update_one(
                self.mongo_collection,
                {"_id": item_id, "user_id": user_id},
                {"$set": updates},
            )
            
            # 如果内容变化，需要更新向量
            if "title" in updates or "content" in updates or "conditions" in updates:
                # 获取更新后的文档
                new_doc = await mongo.find_one(
                    self.mongo_collection,
                    {"_id": item_id},
                )
                
                if new_doc:
                    text = f"{new_doc.get('title', '')}\n{new_doc.get('content', '')}"
                    vectors = await llm.embedding([text])
                    
                    if vectors:
                        # 更新 Milvus 中的向量
                        await milvus.delete(collection=self.milvus_collection, ids=[item_id])
                        
                        milvus_data = {
                            "id": item_id,
                            "user_id": user_id,
                            "title": new_doc.get("title", "")[:500],
                            "content": new_doc.get("content", "")[:2000],
                            "knowledge_type": new_doc.get("knowledge_type", ""),
                            "is_active": 1 if new_doc.get("is_active", True) else 0,
                        }
                        
                        await milvus.insert(
                            collection=self.milvus_collection,
                            vectors=[vectors[0]],
                            metadata=[milvus_data],
                        )
            
            self.logger.info(f"[{trace_id}] Updated user knowledge: {item_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Update failed: {e}")
            return False
    
    async def delete(
        self,
        user_id: str,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> bool:
        """删除用户知识项"""
        mongo = await self._get_mongo()
        milvus = await self._get_milvus()
        
        try:
            # 删除 MongoDB
            result = await mongo.delete_one(
                self.mongo_collection,
                {"_id": item_id, "user_id": user_id},
            )
            
            # 删除 Milvus
            await milvus.delete(collection=self.milvus_collection, ids=[item_id])
            
            self.logger.info(f"[{trace_id}] Deleted user knowledge: {item_id}")
            return result.deleted_count > 0
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Delete failed: {e}")
            return False
    
    async def get(
        self,
        user_id: str,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[UserKnowledgeItem]:
        """获取单个知识项"""
        mongo = await self._get_mongo()
        
        try:
            doc = await mongo.find_one(
                self.mongo_collection,
                {"_id": item_id, "user_id": user_id},
            )
            
            if doc:
                return self._doc_to_item(doc)
            return None
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get failed: {e}")
            return None
    
    # ==================== 检索操作 ====================
    
    async def search(
        self,
        user_id: str,
        query: str,
        top_k: int = 10,
        knowledge_type: Optional[UserKnowledgeType] = None,
        only_active: bool = True,
        trace_id: Optional[str] = None,
    ) -> KnowledgeSearchResult:
        """语义搜索用户知识"""
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
            filter_parts = [f'user_id == "{user_id}"']
            
            if knowledge_type:
                filter_parts.append(f'knowledge_type == "{knowledge_type.value}"')
            
            if only_active:
                filter_parts.append('is_active == 1')
            
            filter_expr = " and ".join(filter_parts)
            
            # Milvus 搜索
            results = await milvus.search(
                collection=self.milvus_collection,
                query_vector=query_vector,
                top_k=top_k,
                filter_expr=filter_expr,
                output_fields=["id", "title", "knowledge_type"],
            )
            
            # 从 MongoDB 获取完整数据
            items = []
            for hit in results:
                entity = hit.get("entity", {})
                item_id = entity.get("id", "")
                
                doc = await mongo.find_one(
                    self.mongo_collection,
                    {"_id": item_id, "user_id": user_id},
                )
                
                if doc:
                    item = self._doc_to_item(doc)
                    item.score = hit.get("distance", 0)
                    items.append(item)
            
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return KnowledgeSearchResult(
                items=items,
                user_items=items,
                total_count=len(items),
                query_time_ms=elapsed_ms,
            )
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Search failed: {e}")
            return KnowledgeSearchResult()
    
    async def get_by_type(
        self,
        user_id: str,
        knowledge_type: UserKnowledgeType,
        only_active: bool = True,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[UserKnowledgeItem]:
        """获取指定类型的知识"""
        mongo = await self._get_mongo()
        
        try:
            query = {
                "user_id": user_id,
                "knowledge_type": knowledge_type.value,
            }
            
            if only_active:
                query["is_active"] = True
            
            docs = await mongo.find(
                self.mongo_collection,
                query,
                sort=[("use_count", -1), ("created_at", -1)],
                limit=limit,
            )
            
            return [self._doc_to_item(doc) for doc in docs]
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get by type failed: {e}")
            return []
    
    async def get_all(
        self,
        user_id: str,
        only_active: bool = False,
        limit: int = 500,
        trace_id: Optional[str] = None,
    ) -> List[UserKnowledgeItem]:
        """获取用户所有知识"""
        mongo = await self._get_mongo()
        
        try:
            query = {"user_id": user_id}
            
            if only_active:
                query["is_active"] = True
            
            docs = await mongo.find(
                self.mongo_collection,
                query,
                sort=[("knowledge_type", 1), ("created_at", -1)],
                limit=limit,
            )
            
            return [self._doc_to_item(doc) for doc in docs]
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get all failed: {e}")
            return []
    
    async def get_trading_rules(
        self,
        user_id: str,
        rule_type: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> List[UserKnowledgeItem]:
        """
        获取用户的交易规则
        
        Args:
            user_id: 用户ID
            rule_type: 规则类型 (entry/exit/position/risk)
            trace_id: 追踪ID
        """
        items = await self.get_by_type(
            user_id,
            UserKnowledgeType.TRADING_RULE,
            trace_id=trace_id,
        )
        
        if rule_type:
            items = [
                item for item in items
                if rule_type in item.tags or rule_type in item.title.lower()
            ]
        
        return items
    
    async def get_review_templates(
        self,
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> List[UserKnowledgeItem]:
        """获取用户的复盘模板"""
        return await self.get_by_type(
            user_id,
            UserKnowledgeType.REVIEW_TEMPLATE,
            trace_id=trace_id,
        )
    
    # ==================== 使用统计 ====================
    
    async def record_use(
        self,
        user_id: str,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> bool:
        """记录知识使用"""
        mongo = await self._get_mongo()
        
        try:
            await mongo.update_one(
                self.mongo_collection,
                {"_id": item_id, "user_id": user_id},
                {
                    "$inc": {"use_count": 1},
                    "$set": {"last_used_at": datetime.utcnow()},
                },
            )
            return True
        except Exception as e:
            self.logger.error(f"[{trace_id}] Record use failed: {e}")
            return False
    
    async def get_most_used(
        self,
        user_id: str,
        limit: int = 10,
        trace_id: Optional[str] = None,
    ) -> List[UserKnowledgeItem]:
        """获取最常用的知识"""
        mongo = await self._get_mongo()
        
        try:
            docs = await mongo.find(
                self.mongo_collection,
                {"user_id": user_id, "is_active": True},
                sort=[("use_count", -1)],
                limit=limit,
            )
            
            return [self._doc_to_item(doc) for doc in docs]
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get most used failed: {e}")
            return []
    
    # ==================== 统计 ====================
    
    async def count(
        self,
        user_id: str,
        knowledge_type: Optional[UserKnowledgeType] = None,
        trace_id: Optional[str] = None,
    ) -> int:
        """统计知识数量"""
        mongo = await self._get_mongo()
        
        try:
            query = {"user_id": user_id}
            if knowledge_type:
                query["knowledge_type"] = knowledge_type.value
            
            return await mongo.count(self.mongo_collection, query)
            
        except Exception:
            return 0
    
    async def get_stats(
        self,
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取用户知识统计"""
        mongo = await self._get_mongo()
        
        try:
            pipeline = [
                {"$match": {"user_id": user_id}},
                {"$group": {
                    "_id": "$knowledge_type",
                    "count": {"$sum": 1},
                    "total_uses": {"$sum": "$use_count"},
                }},
            ]
            
            results = await mongo.aggregate(self.mongo_collection, pipeline)
            
            stats = {
                "by_type": {doc["_id"]: doc["count"] for doc in results},
                "total_count": sum(doc["count"] for doc in results),
                "total_uses": sum(doc["total_uses"] for doc in results),
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get stats failed: {e}")
            return {}
    
    # ==================== 辅助方法 ====================
    
    def _doc_to_item(self, doc: Dict[str, Any]) -> UserKnowledgeItem:
        """将 MongoDB 文档转换为 UserKnowledgeItem"""
        return UserKnowledgeItem(
            id=doc["_id"],
            user_id=doc.get("user_id", ""),
            knowledge_type=UserKnowledgeType(doc.get("knowledge_type", "note")),
            title=doc.get("title", ""),
            content=doc.get("content", ""),
            category=doc.get("category", ""),
            tags=doc.get("tags", []),
            conditions=doc.get("conditions", []),
            actions=doc.get("actions", []),
            related_stocks=doc.get("related_stocks", []),
            related_patterns=doc.get("related_patterns", []),
            is_active=doc.get("is_active", True),
            use_count=doc.get("use_count", 0),
            last_used_at=doc.get("last_used_at"),
            source=doc.get("source", "manual"),
            importance=doc.get("importance", "medium"),
            created_at=doc.get("created_at", datetime.utcnow()),
            updated_at=doc.get("updated_at", datetime.utcnow()),
        )
