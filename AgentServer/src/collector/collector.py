"""
新闻采集器框架

提供基础采集器框架，具体实现在 nodes/data_sync/collectors/
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Type
from datetime import datetime, timedelta

from .types import NewsItem, NewsSource, CollectResult
from .dedup import DeduplicationEngine
from .storage import NewsStorage
from .sources.base import BaseSource


class BaseNewsCollector:
    """
    新闻采集器基类
    
    提供统一的采集框架，子类需要注册具体的采集源。
    
    Example:
        class NewsCollector(BaseNewsCollector):
            SOURCE_CLASSES = {
                "cls": CLSSource,
                "miit": MIITSource,
            }
    """
    
    # 子类需要定义
    SOURCE_CLASSES: Dict[str, Type[BaseSource]] = {}
    SOURCE_GROUPS: Dict[str, List[str]] = {}
    
    def __init__(
        self,
        storage: Optional[NewsStorage] = None,
        dedup_engine: Optional[DeduplicationEngine] = None,
    ):
        self.storage = storage or NewsStorage()
        self.dedup = dedup_engine or DeduplicationEngine()
        self.logger = logging.getLogger(f"collector.{self.__class__.__name__}")
        
        # 采集源实例缓存
        self._sources: Dict[str, BaseSource] = {}
    
    def _get_source(self, source_id: str) -> Optional[BaseSource]:
        """获取采集源实例"""
        if source_id not in self._sources:
            source_class = self.SOURCE_CLASSES.get(source_id)
            if source_class:
                self._sources[source_id] = source_class()
        return self._sources.get(source_id)
    
    def _log_fetched_items(self, source: BaseSource, items: List[NewsItem]) -> None:
        """打印抓取的新闻详情"""
        source_name = source.display_name
        self.logger.info(f"[{source_name}] 抓取到 {len(items)} 条新闻:")
        
        for item in items:
            time_str = item.publish_time.strftime("%H:%M:%S") if item.publish_time else "未知时间"
            title = item.title[:60] + "..." if len(item.title) > 60 else item.title
            
            self.logger.info(f"  [{source_name}] {time_str} | {title}")
            
            # 如果有内容摘要，打印前80个字符
            if item.content and item.content != item.title:
                content_preview = item.content[:80] + "..." if len(item.content) > 80 else item.content
                self.logger.debug(f"    └─ {content_preview}")
    
    async def collect_all(
        self,
        since_hours: Optional[int] = None,
        limit_per_source: int = 100,
        save_to_db: bool = True,
        trace_id: Optional[str] = None,
    ) -> CollectResult:
        """采集所有源"""
        self.logger.info(f"[{trace_id}] Starting collect all sources...")
        start_time = datetime.utcnow()
        
        since = None
        if since_hours:
            since = datetime.utcnow() - timedelta(hours=since_hours)
        
        tasks = []
        for source_id in self.SOURCE_CLASSES.keys():
            tasks.append(self.collect_source(
                source_id=source_id,
                since=since,
                limit=limit_per_source,
                save_to_db=save_to_db,
                trace_id=trace_id,
            ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        final_result = CollectResult()
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"[{trace_id}] Source error: {result}")
                final_result.errors.append(str(result))
            elif isinstance(result, CollectResult):
                final_result = final_result.merge(result)
        
        elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        final_result.elapsed_ms = elapsed_ms
        
        self.logger.info(
            f"[{trace_id}] Collect all done: "
            f"fetched={final_result.total_fetched}, "
            f"new={final_result.new_count}, "
            f"dup={final_result.duplicate_count}, "
            f"time={elapsed_ms:.1f}ms"
        )
        
        return final_result
    
    async def collect_source(
        self,
        source_id: str,
        since: Optional[datetime] = None,
        limit: int = 100,
        save_to_db: bool = True,
        trace_id: Optional[str] = None,
    ) -> CollectResult:
        """采集单个源"""
        result = CollectResult(source=source_id)
        start_time = datetime.utcnow()
        
        source = self._get_source(source_id)
        if not source:
            result.success = False
            result.errors.append(f"Unknown source: {source_id}")
            return result
        
        try:
            self.logger.debug(f"[{trace_id}] Fetching from {source_id}...")
            items = await source.fetch(since=since, limit=limit, trace_id=trace_id)
            result.total_fetched = len(items)
            
            # 打印抓取的新闻详情
            if items:
                self._log_fetched_items(source, items)
            
            if not items:
                return result
            
            unique_items, dup_items = self.dedup.deduplicate_in_memory(items)
            
            dedup_result = await self.dedup.deduplicate_batch(
                unique_items,
                collection="news",
                trace_id=trace_id,
            )
            
            result.duplicate_count = len(dup_items) + dedup_result.duplicate_count
            result.similar_count = dedup_result.similar_count
            result.duplicate_ids = [item.id for item in dup_items]
            
            new_items = dedup_result.new_items
            
            if save_to_db and new_items:
                # 采集阶段不生成向量，向量在事件聚类后生成
                save_result = await self.storage.save_batch(
                    new_items,
                    generate_vectors=False,
                    trace_id=trace_id,
                )
                result.new_count = save_result.new_count
                result.new_ids = save_result.new_ids
                result.failed_count = save_result.failed_count
            else:
                result.new_count = len(new_items)
                result.new_ids = [item.id for item in new_items]
            
            result.elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Collect {source_id} error: {e}")
            result.success = False
            result.errors.append(str(e))
        
        return result
    
    async def collect_group(
        self,
        group: str,
        since_hours: Optional[int] = None,
        since: Optional[datetime] = None,
        limit_per_source: int = 100,
        save_to_db: bool = True,
        trace_id: Optional[str] = None,
    ) -> CollectResult:
        """
        按分组采集
        
        Args:
            group: 分组名称
            since_hours: 采集过去N小时的新闻 (与 since 二选一)
            since: 采集该时间之后的新闻 (优先使用)
            limit_per_source: 每个源的采集上限
            save_to_db: 是否保存到数据库
            trace_id: 追踪ID
        """
        source_ids = self.SOURCE_GROUPS.get(group, [])
        
        if not source_ids:
            result = CollectResult(source=group, success=False)
            result.errors.append(f"Unknown group: {group}")
            return result
        
        self.logger.info(f"[{trace_id}] Collecting group '{group}': {source_ids}")
        
        # 优先使用 since 参数，其次使用 since_hours 计算
        if since is None and since_hours:
            since = datetime.utcnow() - timedelta(hours=since_hours)
        
        tasks = []
        for source_id in source_ids:
            tasks.append(self.collect_source(
                source_id=source_id,
                since=since,
                limit=limit_per_source,
                save_to_db=save_to_db,
                trace_id=trace_id,
            ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        final_result = CollectResult(source=group)
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"[{trace_id}] Source error: {result}")
                final_result.errors.append(str(result))
            elif isinstance(result, CollectResult):
                final_result = final_result.merge(result)
        
        return final_result
    
    @classmethod
    def get_groups(cls) -> Dict[str, List[str]]:
        """获取所有分组"""
        return cls.SOURCE_GROUPS.copy()
    
    async def close(self):
        """关闭所有采集源"""
        for source in self._sources.values():
            await source.close()
        self._sources.clear()
    
    async def get_recent_news(
        self,
        source: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 50,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """获取最近的新闻"""
        return await self.storage.get_recent(
            limit=limit,
            source=source,
            category=category,
            trace_id=trace_id,
        )
    
    async def get_stats(
        self,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {"sources": {}, "total": 0}
        
        for source_id in self.SOURCE_CLASSES.keys():
            count = await self.storage.count(source=source_id, trace_id=trace_id)
            stats["sources"][source_id] = count
            stats["total"] += count
        
        return stats
    
    @classmethod
    def get_available_sources(cls) -> List[Dict[str, str]]:
        """获取可用的采集源列表"""
        sources = []
        for source_id, source_class in cls.SOURCE_CLASSES.items():
            sources.append({
                "id": source_id,
                "name": source_class.display_name,
            })
        return sources
    
    @classmethod
    def register_source(cls, source_id: str, source_class: Type[BaseSource]):
        """注册新的采集源"""
        cls.SOURCE_CLASSES[source_id] = source_class
