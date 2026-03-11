"""
新闻去重引擎

实现多层去重策略:
1. 精确去重 - 内容哈希匹配 (同源同内容)
2. 跨源去重 - 标题相似度 (不同源同一事件)
3. 向量去重 - 语义相似度 (改写稿件)
"""

import logging
import hashlib
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from difflib import SequenceMatcher


@dataclass
class DeduplicationResult:
    """去重结果"""
    total: int = 0
    new_items: List[Any] = field(default_factory=list)
    duplicate_items: List[Any] = field(default_factory=list)
    similar_items: List[Tuple[Any, Any, float]] = field(default_factory=list)  # (新项, 已存在项, 相似度)
    
    @property
    def new_count(self) -> int:
        return len(self.new_items)
    
    @property
    def duplicate_count(self) -> int:
        return len(self.duplicate_items)
    
    @property
    def similar_count(self) -> int:
        return len(self.similar_items)


class DeduplicationEngine:
    """
    去重引擎
    
    三层去重策略:
    
    1. **精确去重 (Hash)** - O(1)
       - 基于 content_hash 或 title_hash
       - 完全相同的内容
       
    2. **标题相似度去重** - O(n)
       - 基于标题文本相似度
       - 阈值: 0.85 (85%相似视为重复)
       - 用于跨源同一事件检测
       
    3. **向量语义去重** - O(log n)
       - 基于向量余弦相似度
       - 阈值: 0.92 (92%相似视为重复)
       - 用于改写稿件检测
    
    Example:
        engine = DeduplicationEngine()
        
        # 检查单条
        is_dup, reason = await engine.is_duplicate(news_item)
        
        # 批量去重
        result = await engine.deduplicate_batch(news_items)
    """
    
    def __init__(
        self,
        title_similarity_threshold: float = 0.85,
        vector_similarity_threshold: float = 0.92,
        time_window_hours: int = 72,
    ):
        """
        Args:
            title_similarity_threshold: 标题相似度阈值
            vector_similarity_threshold: 向量相似度阈值
            time_window_hours: 去重时间窗口 (小时)
        """
        self.title_threshold = title_similarity_threshold
        self.vector_threshold = vector_similarity_threshold
        self.time_window = timedelta(hours=time_window_hours)
        
        self.logger = logging.getLogger("src.collector.DeduplicationEngine")
        self._mongo_manager = None
        self._milvus_manager = None
    
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
    
    # ==================== 精确去重 ====================
    
    async def check_hash_exists(
        self,
        content_hash: str,
        collection: str = "news",
        trace_id: Optional[str] = None,
    ) -> bool:
        """检查内容哈希是否已存在"""
        mongo = await self._get_mongo()
        
        try:
            doc = await mongo.find_one(collection, {"content_hash": content_hash})
            return doc is not None
        except Exception as e:
            self.logger.error(f"[{trace_id}] Check hash error: {e}")
            return False
    
    async def check_hash_batch(
        self,
        content_hashes: List[str],
        collection: str = "news",
        trace_id: Optional[str] = None,
    ) -> Dict[str, bool]:
        """批量检查内容哈希"""
        mongo = await self._get_mongo()
        
        try:
            docs = await mongo.find_many(
                collection,
                {"content_hash": {"$in": content_hashes}},
                projection={"content_hash": 1},
            )
            existing = {doc["content_hash"] for doc in docs}
            return {h: h in existing for h in content_hashes}
        except Exception as e:
            self.logger.error(f"[{trace_id}] Check hash batch error: {e}")
            return {h: False for h in content_hashes}
    
    # ==================== 标题相似度去重 ====================
    
    def compute_title_similarity(self, title1: str, title2: str) -> float:
        """
        计算标题相似度
        
        使用改进的算法:
        1. 预处理: 去除标点、空格、数字
        2. SequenceMatcher 计算相似度
        """
        # 预处理
        t1 = self._normalize_title(title1)
        t2 = self._normalize_title(title2)
        
        if not t1 or not t2:
            return 0.0
        
        # 计算相似度
        return SequenceMatcher(None, t1, t2).ratio()
    
    def _normalize_title(self, title: str) -> str:
        """标题标准化"""
        # 去除标点符号
        title = re.sub(r'[^\w\s]', '', title)
        # 去除数字
        title = re.sub(r'\d+', '', title)
        # 去除空格
        title = re.sub(r'\s+', '', title)
        return title.lower()
    
    async def find_similar_by_title(
        self,
        title: str,
        collection: str = "news",
        time_window: Optional[timedelta] = None,
        trace_id: Optional[str] = None,
    ) -> List[Tuple[str, str, float]]:
        """
        查找标题相似的新闻
        
        Returns:
            [(id, title, similarity), ...]
        """
        mongo = await self._get_mongo()
        time_window = time_window or self.time_window
        
        try:
            # 查询时间窗口内的新闻
            cutoff = datetime.utcnow() - time_window
            docs = await mongo.find_many(
                collection,
                {"collect_time": {"$gte": cutoff}},
                projection={"_id": 1, "title": 1},
                limit=1000,
            )
            
            similar = []
            for doc in docs:
                sim = self.compute_title_similarity(title, doc.get("title", ""))
                if sim >= self.title_threshold:
                    similar.append((doc["_id"], doc["title"], sim))
            
            # 按相似度排序
            similar.sort(key=lambda x: x[2], reverse=True)
            return similar[:5]  # 返回最相似的5个
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Find similar error: {e}")
            return []
    
    # ==================== 向量语义去重 ====================
    
    async def find_similar_by_vector(
        self,
        vector: List[float],
        collection: str = "semantic_memory",
        threshold: Optional[float] = None,
        trace_id: Optional[str] = None,
    ) -> List[Tuple[str, float]]:
        """
        基于向量查找相似新闻
        
        Returns:
            [(id, similarity), ...]
        """
        milvus = await self._get_milvus()
        threshold = threshold or self.vector_threshold
        
        try:
            results = await milvus.search(
                collection=collection,
                query_vector=vector,
                top_k=5,
                output_fields=["id"],
            )
            
            similar = []
            for hit in results:
                # Milvus 返回的是距离，需要转换为相似度
                distance = hit.get("distance", 0)
                # 假设使用 L2 距离，转换为相似度
                # 或者如果使用 IP (内积)，distance 就是相似度
                similarity = 1 - distance if distance < 1 else 1 / (1 + distance)
                
                if similarity >= threshold:
                    similar.append((hit.get("entity", {}).get("id", ""), similarity))
            
            return similar
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Find similar by vector error: {e}")
            return []
    
    # ==================== 综合去重 ====================
    
    async def is_duplicate(
        self,
        news_item: "NewsItem",
        collection: str = "news",
        trace_id: Optional[str] = None,
    ) -> Tuple[bool, str, Optional[str]]:
        """
        检查新闻是否重复
        
        Args:
            news_item: 新闻项
            collection: MongoDB 集合名
            trace_id: 追踪ID
            
        Returns:
            (是否重复, 原因, 重复项ID)
        """
        # 1. 精确哈希匹配
        if await self.check_hash_exists(news_item.content_hash, collection, trace_id):
            return True, "exact_hash_match", None
        
        # 2. 标题相似度匹配
        similar = await self.find_similar_by_title(
            news_item.title, collection, trace_id=trace_id
        )
        if similar:
            best_match = similar[0]
            return True, f"title_similar({best_match[2]:.2f})", best_match[0]
        
        # 3. 向量语义匹配 (如果有向量)
        if news_item.vector:
            vector_similar = await self.find_similar_by_vector(
                news_item.vector, trace_id=trace_id
            )
            if vector_similar:
                best_match = vector_similar[0]
                return True, f"vector_similar({best_match[1]:.2f})", best_match[0]
        
        return False, "new", None
    
    async def deduplicate_batch(
        self,
        items: List["NewsItem"],
        collection: str = "news",
        check_vector: bool = False,
        trace_id: Optional[str] = None,
    ) -> DeduplicationResult:
        """
        批量去重
        
        Args:
            items: 新闻项列表
            collection: MongoDB 集合名
            check_vector: 是否检查向量相似度
            trace_id: 追踪ID
            
        Returns:
            去重结果
        """
        result = DeduplicationResult(total=len(items))
        
        if not items:
            return result
        
        # 1. 批量哈希检查
        content_hashes = [item.content_hash for item in items]
        hash_exists = await self.check_hash_batch(content_hashes, collection, trace_id)
        
        # 分离精确重复和需要进一步检查的
        to_check = []
        for item in items:
            if hash_exists.get(item.content_hash, False):
                result.duplicate_items.append(item)
            else:
                to_check.append(item)
        
        # 2. 标题相似度检查
        for item in to_check:
            similar = await self.find_similar_by_title(
                item.title, collection, trace_id=trace_id
            )
            if similar:
                best_match = similar[0]
                result.similar_items.append((item, best_match[0], best_match[2]))
            else:
                result.new_items.append(item)
        
        self.logger.info(
            f"[{trace_id}] Deduplication: {result.total} total, "
            f"{result.new_count} new, {result.duplicate_count} dup, "
            f"{result.similar_count} similar"
        )
        
        return result
    
    # ==================== 内存缓存去重 ====================
    
    def deduplicate_in_memory(
        self,
        items: List["NewsItem"],
    ) -> Tuple[List["NewsItem"], List["NewsItem"]]:
        """
        内存中去重 (用于批量采集时的预处理)
        
        Returns:
            (唯一项列表, 重复项列表)
        """
        seen_hashes: Set[str] = set()
        unique = []
        duplicates = []
        
        for item in items:
            if item.content_hash in seen_hashes:
                duplicates.append(item)
            else:
                seen_hashes.add(item.content_hash)
                unique.append(item)
        
        return unique, duplicates
