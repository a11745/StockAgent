"""
事件聚类引擎

深度去重阶段：使用 LLM 识别相同事件，聚类新闻。

工作流程:
1. 定期扫描未聚类的新闻
2. 使用 LLM 提取事件指纹 (主体 + 动作 + 时间)
3. 基于事件指纹进行聚类
4. 标记重复新闻，保留主新闻
"""

import logging
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

from pydantic import BaseModel, Field


class EventImportance(str, Enum):
    """事件重要性"""
    HIGH = "high"        # 重大事件
    MEDIUM = "medium"    # 一般事件
    LOW = "low"          # 次要事件


@dataclass
class EventFingerprint:
    """事件指纹"""
    subject: str          # 主体 (公司/行业/政策)
    action: str           # 动作 (发布/涨跌/收购)
    time_ref: str         # 时间参照 (今日/本周/Q1)
    keywords: List[str]   # 关键词
    
    @property
    def fingerprint_hash(self) -> str:
        """生成指纹哈希"""
        text = f"{self.subject}:{self.action}:{self.time_ref}"
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def similarity(self, other: "EventFingerprint") -> float:
        """计算与另一个指纹的相似度"""
        score = 0.0
        
        # 主体相似度 (权重 0.4)
        if self.subject == other.subject:
            score += 0.4
        elif self._fuzzy_match(self.subject, other.subject):
            score += 0.2
        
        # 动作相似度 (权重 0.3)
        if self.action == other.action:
            score += 0.3
        elif self._fuzzy_match(self.action, other.action):
            score += 0.15
        
        # 关键词重叠 (权重 0.3)
        if self.keywords and other.keywords:
            overlap = len(set(self.keywords) & set(other.keywords))
            total = len(set(self.keywords) | set(other.keywords))
            if total > 0:
                score += 0.3 * (overlap / total)
        
        return score
    
    def _fuzzy_match(self, s1: str, s2: str) -> bool:
        """模糊匹配"""
        return s1 in s2 or s2 in s1


class NewsEvent(BaseModel):
    """新闻事件 (聚类后的事件)"""
    id: str = Field(default="")
    
    # 事件信息
    title: str = Field(description="事件标题")
    summary: str = Field(default="", description="事件摘要")
    importance: EventImportance = Field(default=EventImportance.MEDIUM)
    
    # 关联
    category: str = Field(default="general", description="事件分类")
    ts_codes: List[str] = Field(default_factory=list, description="关联股票")
    tags: List[str] = Field(default_factory=list, description="标签")
    
    # 事件指纹
    fingerprint: Optional[Dict[str, Any]] = Field(default=None)
    fingerprint_hash: str = Field(default="")
    
    # 关联新闻
    primary_news_id: str = Field(default="", description="主新闻ID")
    related_news_ids: List[str] = Field(default_factory=list, description="相关新闻ID")
    news_count: int = Field(default=1, description="新闻数量")
    
    # 时间
    event_time: Optional[datetime] = Field(default=None, description="事件发生时间")
    first_report_time: Optional[datetime] = Field(default=None, description="首次报道时间")
    last_update_time: Optional[datetime] = Field(default=None, description="最后更新时间")
    
    # 元数据
    sources: List[str] = Field(default_factory=list, description="来源列表")
    

@dataclass
class ClusterResult:
    """聚类结果"""
    total_processed: int = 0
    new_events: int = 0
    merged_news: int = 0
    events: List[NewsEvent] = field(default_factory=list)




class EventClusterEngine:
    """
    事件聚类引擎
    
    两阶段工作:
    1. 提取事件指纹 (LLM)
    2. 基于指纹聚类
    
    Example:
        engine = EventClusterEngine()
        
        # 处理未聚类的新闻
        result = await engine.process_pending_news(trace_id="xxx")
        
        # 单条新闻提取指纹
        fingerprint = await engine.extract_fingerprint(news_item)
    """
    
    def __init__(
        self,
        similarity_threshold: float = 0.7,
        time_window_hours: int = 48,
    ):
        self.similarity_threshold = similarity_threshold
        self.time_window = timedelta(hours=time_window_hours)
        self.logger = logging.getLogger("src.collector.EventClusterEngine")
        
        self._llm_service = None
        self._mongo_manager = None
        self._milvus_manager = None
    
    async def _get_llm_service(self):
        """获取 LLM 服务实例"""
        if self._llm_service is None:
            from src.llm import LLMService
            self._llm_service = LLMService()
            await self._llm_service.initialize()
        return self._llm_service
    
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
    
    async def extract_fingerprint(
        self,
        title: str,
        content: str,
        trace_id: Optional[str] = None,
    ) -> Tuple[Optional[EventFingerprint], Optional[Dict[str, Any]]]:
        """
        使用 LLM 提取事件指纹
        
        使用 LLMService 的模板系统和自动解析功能
        """
        llm = await self._get_llm_service()
        
        try:
            # 使用 event_extract 模板，自动解析 JSON
            result = await llm.invoke_and_parse(
                template_name="event_extract",
                title=title,
                content=content[:1000] if content else "",
            )
            
            if not result:
                self.logger.warning(f"[{trace_id}] LLM returned empty or invalid response")
                return None, None
            
            return EventFingerprint(
                subject=result.get("subject", ""),
                action=result.get("action", ""),
                time_ref=result.get("time_ref", ""),
                keywords=result.get("keywords", []),
            ), result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Extract fingerprint error: {e}")
            return None, None
    
    async def find_similar_event(
        self,
        fingerprint: EventFingerprint,
        trace_id: Optional[str] = None,
    ) -> Optional[NewsEvent]:
        """
        查找相似的已有事件
        """
        mongo = await self._get_mongo()
        
        cutoff = datetime.utcnow() - self.time_window
        
        try:
            # 先按指纹哈希精确匹配
            event = await mongo.find_one(
                "news_events",
                {
                    "fingerprint_hash": fingerprint.fingerprint_hash,
                    "last_update_time": {"$gte": cutoff},
                }
            )
            if event:
                return NewsEvent(**event)
            
            # 再按关键词模糊匹配
            events = await mongo.find_many(
                "news_events",
                {
                    "last_update_time": {"$gte": cutoff},
                    "fingerprint.subject": {"$regex": fingerprint.subject, "$options": "i"},
                },
                limit=20,
            )
            
            for event_doc in events:
                existing_fp = EventFingerprint(
                    subject=event_doc.get("fingerprint", {}).get("subject", ""),
                    action=event_doc.get("fingerprint", {}).get("action", ""),
                    time_ref=event_doc.get("fingerprint", {}).get("time_ref", ""),
                    keywords=event_doc.get("fingerprint", {}).get("keywords", []),
                )
                
                if fingerprint.similarity(existing_fp) >= self.similarity_threshold:
                    return NewsEvent(**event_doc)
            
            return None
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Find similar event error: {e}")
            return None
    
    async def create_or_merge_event(
        self,
        news_id: str,
        news_title: str,
        news_source: str,
        fingerprint: EventFingerprint,
        llm_result: Dict[str, Any],
        trace_id: Optional[str] = None,
    ) -> Tuple[NewsEvent, bool]:
        """
        创建新事件或合并到已有事件
        
        Returns:
            (event, is_new)
        """
        mongo = await self._get_mongo()
        
        existing = await self.find_similar_event(fingerprint, trace_id)
        
        if existing:
            # 合并到已有事件
            existing.related_news_ids.append(news_id)
            existing.news_count += 1
            existing.last_update_time = datetime.utcnow()
            if news_source not in existing.sources:
                existing.sources.append(news_source)
            
            await mongo.update_one(
                "news_events",
                {"id": existing.id},
                {
                    "$push": {"related_news_ids": news_id},
                    "$inc": {"news_count": 1},
                    "$set": {"last_update_time": datetime.utcnow()},
                    "$addToSet": {"sources": news_source},
                }
            )
            
            # 标记新闻为已聚类
            await mongo.update_one(
                "news",
                {"id": news_id},
                {
                    "$set": {
                        "event_id": existing.id,
                        "is_primary": False,
                        "clustered_at": datetime.utcnow(),
                    }
                }
            )
            
            return existing, False
        
        # 创建新事件
        event = NewsEvent(
            id=f"evt_{fingerprint.fingerprint_hash[:16]}",
            title=llm_result.get("summary", news_title),
            summary=llm_result.get("summary", ""),
            importance=EventImportance(llm_result.get("importance", "medium")),
            category=llm_result.get("category", "general"),
            fingerprint={
                "subject": fingerprint.subject,
                "action": fingerprint.action,
                "time_ref": fingerprint.time_ref,
                "keywords": fingerprint.keywords,
            },
            fingerprint_hash=fingerprint.fingerprint_hash,
            primary_news_id=news_id,
            related_news_ids=[],
            news_count=1,
            first_report_time=datetime.utcnow(),
            last_update_time=datetime.utcnow(),
            sources=[news_source],
        )
        
        await mongo.insert_one("news_events", event.model_dump())
        
        # 标记新闻为主新闻
        await mongo.update_one(
            "news",
            {"id": news_id},
            {
                "$set": {
                    "event_id": event.id,
                    "is_primary": True,
                    "clustered_at": datetime.utcnow(),
                }
            }
        )
        
        # 为主新闻生成向量并入库 Milvus
        await self._generate_and_store_vector(
            news_id=news_id,
            event=event,
            trace_id=trace_id,
        )
        
        return event, True
    
    async def _generate_and_store_vector(
        self,
        news_id: str,
        event: NewsEvent,
        trace_id: Optional[str] = None,
    ):
        """
        为主新闻生成向量并存入 Milvus
        
        只有主新闻才会入向量库，避免语义重复。
        使用 milvus_manager.add_news_vector() 方法，它会自动处理向量生成和存储。
        """
        mongo = await self._get_mongo()
        milvus = await self._get_milvus()
        
        try:
            # 获取新闻详情
            news_doc = await mongo.find_one("news", {"id": news_id})
            if not news_doc:
                return
            
            # 使用 milvus_manager 的 insert_news 方法
            # 它会自动生成向量并存入正确的 collection
            ts_code = event.ts_codes[0] if event.ts_codes else ""
            trade_date = event.first_report_time.strftime("%Y%m%d") if event.first_report_time else datetime.utcnow().strftime("%Y%m%d")
            news_datetime = event.first_report_time.isoformat() if event.first_report_time else ""
            
            vector_id = await milvus.insert_news(
                ts_code=ts_code,
                title=event.title,
                content=news_doc.get("content", "")[:1000],
                trade_date=trade_date,
                news_datetime=news_datetime,
                source=event.sources[0] if event.sources else "unknown",
            )
            
            if vector_id:
                self.logger.debug(f"[{trace_id}] Vector stored for primary news: {news_id}, vector_id={vector_id}")
            else:
                self.logger.warning(f"[{trace_id}] Failed to store vector for {news_id}")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Generate vector error: {e}")
    
    async def process_pending_news(
        self,
        batch_size: int = 50,
        trace_id: Optional[str] = None,
    ) -> ClusterResult:
        """
        处理待聚类的新闻
        
        扫描未聚类的新闻，提取指纹并聚类。
        """
        mongo = await self._get_mongo()
        result = ClusterResult()
        
        # 查找未聚类的新闻
        pending_news = await mongo.find_many(
            "news",
            {
                "clustered_at": {"$exists": False},
                "collect_time": {"$gte": datetime.utcnow() - self.time_window},
            },
            limit=batch_size,
            sort=[("collect_time", 1)],
        )
        
        result.total_processed = len(pending_news)
        
        for news_doc in pending_news:
            news_id = news_doc.get("id", "")
            title = news_doc.get("title", "")
            content = news_doc.get("content", "")
            source = news_doc.get("source", "")
            
            # 提取指纹
            fingerprint, llm_result = await self.extract_fingerprint(
                title, content, trace_id
            )
            
            if not fingerprint:
                self.logger.warning(f"[{trace_id}] Failed to extract fingerprint: {news_id}")
                continue
            
            # 创建或合并事件
            event, is_new = await self.create_or_merge_event(
                news_id=news_id,
                news_title=title,
                news_source=source,
                fingerprint=fingerprint,
                llm_result=llm_result or {},
                trace_id=trace_id,
            )
            
            if is_new:
                result.new_events += 1
                result.events.append(event)
            else:
                result.merged_news += 1
        
        self.logger.info(
            f"[{trace_id}] Event clustering: processed={result.total_processed}, "
            f"new_events={result.new_events}, merged={result.merged_news}"
        )
        
        return result
    
    async def get_event_by_id(
        self,
        event_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[NewsEvent]:
        """获取事件详情"""
        mongo = await self._get_mongo()
        doc = await mongo.find_one("news_events", {"id": event_id})
        if doc:
            return NewsEvent(**doc)
        return None
    
    async def get_recent_events(
        self,
        hours: int = 24,
        importance: Optional[EventImportance] = None,
        category: Optional[str] = None,
        limit: int = 50,
        trace_id: Optional[str] = None,
    ) -> List[NewsEvent]:
        """获取最近的事件"""
        mongo = await self._get_mongo()
        
        query = {
            "last_update_time": {"$gte": datetime.utcnow() - timedelta(hours=hours)}
        }
        
        if importance:
            query["importance"] = importance.value
        if category:
            query["category"] = category
        
        docs = await mongo.find_many(
            "news_events",
            query,
            limit=limit,
            sort=[("news_count", -1), ("last_update_time", -1)],
        )
        
        return [NewsEvent(**doc) for doc in docs]
