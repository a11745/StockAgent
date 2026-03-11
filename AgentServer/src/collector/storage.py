"""
新闻存储

负责将新闻存入 MongoDB (元数据) 和 Milvus (向量)。
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from .types import NewsItem, CollectResult


class NewsStorage:
    """
    新闻存储
    
    存储架构:
    - MongoDB `news`: 新闻元数据 (全量字段)
    - Milvus `semantic_memory`: 新闻向量 (用于 RAG 检索)
    
    Example:
        storage = NewsStorage()
        
        # 存储单条
        success = await storage.save(news_item)
        
        # 批量存储
        result = await storage.save_batch(news_items)
    """
    
    def __init__(
        self,
        mongo_collection: str = "news",
        milvus_collection: str = "semantic_memory",
    ):
        self.mongo_collection = mongo_collection
        self.milvus_collection = milvus_collection
        self.logger = logging.getLogger("src.collector.NewsStorage")
        
        self._mongo_manager = None
        self._milvus_manager = None
        self._llm_manager = None
    
    async def _get_mongo(self):
        if self._mongo_manager is None:
            from core.managers import mongo_manager
            if not mongo_manager.is_initialized:
                await mongo_manager.initialize()
            self._mongo_manager = mongo_manager
        return self._mongo_manager
    
    async def _get_milvus(self):
        if self._milvus_manager is None:
            from core.managers import milvus_manager
            if not milvus_manager.is_initialized:
                await milvus_manager.initialize()
            self._milvus_manager = milvus_manager
        return self._milvus_manager
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            if not llm_manager.is_initialized:
                await llm_manager.initialize()
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def save(
        self,
        item: NewsItem,
        generate_vector: bool = True,
        trace_id: Optional[str] = None,
    ) -> bool:
        """
        存储单条新闻
        
        Args:
            item: 新闻项
            generate_vector: 是否生成向量
            trace_id: 追踪ID
            
        Returns:
            是否成功
        """
        mongo = await self._get_mongo()
        milvus = await self._get_milvus()
        
        try:
            # 生成向量
            if generate_vector and not item.vector:
                llm = await self._get_llm()
                text = item.get_text_for_embedding()
                vectors = await llm.embedding([text])
                if vectors:
                    item.vector = vectors[0]
            
            # 存入 MongoDB
            mongo_doc = {
                "_id": item.id,
                "id": item.id,
                "title": item.title,
                "content": item.content,
                "summary": item.summary,
                "url": item.url,
                "source": item.source.value,
                "category": item.category.value,
                "publish_time": item.publish_time,
                "collect_time": item.collect_time,
                "ts_codes": item.ts_codes,
                "tags": item.tags,
                "keywords": item.keywords,
                "author": item.author,
                "source_id": item.source_id,
                "extra": item.extra,
                "content_hash": item.content_hash,
                "title_hash": item.title_hash,
            }
            
            try:
                await mongo.insert_one(self.mongo_collection, mongo_doc)
            except Exception:
                # 可能已存在，尝试更新
                await mongo.update_one(
                    self.mongo_collection,
                    {"_id": item.id},
                    {"$set": mongo_doc},
                    upsert=True,
                )
            
            # 存入 Milvus
            if item.vector:
                milvus_data = {
                    "id": item.id,
                    "content": item.title[:500],
                    "user_id": "",  # 公共新闻没有用户
                    "visibility": "public",
                    "ts_code": ",".join(item.ts_codes[:3]) if item.ts_codes else "",
                    "source": item.source.value,
                    "category": item.category.value,
                    "publish_date": item.publish_time.strftime("%Y%m%d") if item.publish_time else "",
                    "created_at": item.collect_time.isoformat(),
                    "importance_score": 0.5,
                }
                
                await milvus.insert(
                    collection=self.milvus_collection,
                    vectors=[item.vector],
                    metadata=[milvus_data],
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Save news failed: {e}")
            return False
    
    async def save_batch(
        self,
        items: List[NewsItem],
        generate_vectors: bool = True,
        batch_size: int = 50,
        trace_id: Optional[str] = None,
    ) -> CollectResult:
        """
        批量存储新闻
        
        Args:
            items: 新闻项列表
            generate_vectors: 是否生成向量
            batch_size: 向量化批大小
            trace_id: 追踪ID
            
        Returns:
            存储结果
        """
        result = CollectResult(total_fetched=len(items))
        
        if not items:
            return result
        
        mongo = await self._get_mongo()
        milvus = await self._get_milvus()
        llm = await self._get_llm()
        
        try:
            # 批量生成向量
            if generate_vectors:
                items_need_vector = [item for item in items if not item.vector]
                
                for i in range(0, len(items_need_vector), batch_size):
                    batch = items_need_vector[i:i + batch_size]
                    texts = [item.get_text_for_embedding() for item in batch]
                    
                    vectors = await llm.embedding(texts)
                    
                    if vectors:
                        for item, vector in zip(batch, vectors):
                            item.vector = vector
            
            # 批量存入 MongoDB
            mongo_docs = []
            for item in items:
                mongo_docs.append({
                    "_id": item.id,
                    "id": item.id,
                    "title": item.title,
                    "content": item.content,
                    "summary": item.summary,
                    "url": item.url,
                    "source": item.source.value,
                    "category": item.category.value,
                    "publish_time": item.publish_time,
                    "collect_time": item.collect_time,
                    "ts_codes": item.ts_codes,
                    "tags": item.tags,
                    "keywords": item.keywords,
                    "author": item.author,
                    "source_id": item.source_id,
                    "extra": item.extra,
                    "content_hash": item.content_hash,
                    "title_hash": item.title_hash,
                })
            
            # 使用 upsert 避免重复
            for doc in mongo_docs:
                try:
                    await mongo.update_one(
                        self.mongo_collection,
                        {"_id": doc["_id"]},
                        {"$set": doc},
                        upsert=True,
                    )
                    result.new_count += 1
                    result.new_ids.append(doc["_id"])
                except Exception as e:
                    self.logger.warning(f"[{trace_id}] Insert doc failed: {e}")
                    result.failed_count += 1
            
            # 批量存入 Milvus
            items_with_vector = [item for item in items if item.vector]
            
            for item in items_with_vector:
                try:
                    milvus_data = {
                        "id": item.id,
                        "content": item.title[:500],
                        "user_id": "",
                        "visibility": "public",
                        "ts_code": ",".join(item.ts_codes[:3]) if item.ts_codes else "",
                        "source": item.source.value,
                        "category": item.category.value,
                        "publish_date": item.publish_time.strftime("%Y%m%d") if item.publish_time else "",
                        "created_at": item.collect_time.isoformat(),
                        "importance_score": 0.5,
                    }
                    
                    await milvus.insert(
                        collection=self.milvus_collection,
                        vectors=[item.vector],
                        metadata=[milvus_data],
                    )
                except Exception as e:
                    self.logger.warning(f"[{trace_id}] Insert vector failed: {e}")
            
            self.logger.info(
                f"[{trace_id}] Saved {result.new_count} news items, "
                f"failed: {result.failed_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Save batch failed: {e}")
            result.errors.append(str(e))
            return result
    
    async def exists(
        self,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> bool:
        """检查新闻是否已存在"""
        mongo = await self._get_mongo()
        
        try:
            doc = await mongo.find_one(self.mongo_collection, {"_id": item_id})
            return doc is not None
        except Exception:
            return False
    
    async def get(
        self,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[NewsItem]:
        """获取新闻"""
        mongo = await self._get_mongo()
        
        try:
            doc = await mongo.find_one(self.mongo_collection, {"_id": item_id})
            if doc:
                return self._doc_to_item(doc)
            return None
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get news failed: {e}")
            return None
    
    async def get_recent(
        self,
        limit: int = 100,
        source: Optional[str] = None,
        category: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """获取最近的新闻"""
        mongo = await self._get_mongo()
        
        try:
            query = {}
            if source:
                query["source"] = source
            if category:
                query["category"] = category
            
            docs = await mongo.find_many(
                self.mongo_collection,
                query,
                sort=[("collect_time", -1)],
                limit=limit,
            )
            
            return [self._doc_to_item(doc) for doc in docs]
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get recent failed: {e}")
            return []
    
    async def count(
        self,
        source: Optional[str] = None,
        category: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> int:
        """统计新闻数量"""
        mongo = await self._get_mongo()
        
        try:
            query = {}
            if source:
                query["source"] = source
            if category:
                query["category"] = category
            
            return await mongo.count(self.mongo_collection, query)
        except Exception:
            return 0
    
    def _doc_to_item(self, doc: Dict[str, Any]) -> NewsItem:
        """将 MongoDB 文档转换为 NewsItem"""
        from .types import NewsSource, NewsCategory
        
        return NewsItem(
            id=doc["_id"],
            title=doc.get("title", ""),
            content=doc.get("content", ""),
            summary=doc.get("summary", ""),
            url=doc.get("url", ""),
            source=NewsSource(doc.get("source", "other")),
            category=NewsCategory(doc.get("category", "general")),
            publish_time=doc.get("publish_time"),
            collect_time=doc.get("collect_time", datetime.utcnow()),
            ts_codes=doc.get("ts_codes", []),
            tags=doc.get("tags", []),
            keywords=doc.get("keywords", []),
            author=doc.get("author", ""),
            source_id=doc.get("source_id", ""),
            extra=doc.get("extra", {}),
            content_hash=doc.get("content_hash", ""),
            title_hash=doc.get("title_hash", ""),
        )
